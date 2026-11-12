"""Execution management for ReflowManager."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Any, List, Mapping, Optional, Tuple
from sqlalchemy import and_, exists, or_
from sqlalchemy.orm import Session
from reflowfy.store.models import Execution, Job
from reflowfy.store.states import ExecutionState, JobState, jobs_dispatched

logger = logging.getLogger(__name__)

# Called as fn(bind, execution_id, state) right after an execution was moved to a terminal
# state by update_execution_state — the one place every terminal transition goes through
# (drive loop, /cancel, PATCH state, planning-crash recovery, a run that raised). The
# manager service registers the lifecycle hooks here at startup (lifecycle.install());
# nothing is registered elsewhere (unit tests, the CLI).
TerminalListener = Callable[[Any, str, str], None]
_terminal_listeners: List[TerminalListener] = []


def add_terminal_listener(listener: TerminalListener) -> None:
    """Call ``listener(bind, execution_id, state)`` whenever an execution ends (idempotent)."""
    if listener not in _terminal_listeners:
        _terminal_listeners.append(listener)


def remove_terminal_listener(listener: TerminalListener) -> None:
    """Undo :func:`add_terminal_listener` (no-op if it was not added)."""
    if listener in _terminal_listeners:
        _terminal_listeners.remove(listener)


class ExecutionManager:
    """Manages pipeline execution records."""

    def __init__(self, db_session: Session):
        self.db = db_session

    def create_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        runtime_params: Optional[Dict[str, Any]] = None,
        rate_limit: Optional[float] = None,
        parent_execution_id: Optional[str] = None,
        callback_url: Optional[str] = None,
    ) -> Execution:
        """
        Create new execution record.

        Args:
            execution_id: Unique execution identifier
            pipeline_name: Name of the pipeline
            runtime_params: Runtime parameters for the pipeline
            rate_limit: The request's rate_limit override, if any (jobs/min)
            parent_execution_id: The execution whose hook started this one, if any
            callback_url: URL POSTed the execution summary once it ends, if any

        Returns:
            Created Execution object
        """
        execution = Execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            state=ExecutionState.PENDING,
            runtime_params=runtime_params or {},
            rate_limit=rate_limit,
            plan_complete=False,
            parent_execution_id=parent_execution_id,
            callback_url=callback_url,
        )

        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)

        return execution

    def set_total_jobs(self, execution_id: str, total: int) -> None:
        execution = self.get_execution(execution_id)
        if execution:
            execution.total_jobs = total
            self.db.commit()

    def store_job_counts(
        self, execution_id: str, counts: Mapping[str, int]
    ) -> Tuple[int, int, int]:
        """Write a count per job state onto the execution row; returns (dispatched,
        finished, failed), deduplicated jobs counting as finished."""
        dispatched = jobs_dispatched(counts)
        failed = counts.get(JobState.FAILED, 0)
        deduplicated = counts.get(JobState.DEDUPLICATED, 0)
        finished = counts.get(JobState.COMPLETED, 0) + deduplicated
        execution = self.get_execution(execution_id)
        if execution:
            execution.jobs_dispatched = dispatched
            execution.jobs_completed = finished
            execution.jobs_failed = failed
            execution.deduplicated_jobs = deduplicated
            self.db.commit()
        return dispatched, finished, failed

    def get_execution(self, execution_id: str, for_update: bool = False) -> Optional[Execution]:
        """
        Get execution by ID.

        Args:
            execution_id: Execution identifier
            for_update: If True, lock the row for update (prevents concurrent modifications)

        Returns:
            Execution object or None if not found
        """
        query = self.db.query(Execution).filter(Execution.execution_id == execution_id)
        if for_update:
            query = query.with_for_update()
        return query.first()

    def update_execution_state(
        self,
        execution_id: str,
        state: str,
        error_message: Optional[str] = None,
        notify: bool = True,
        unless_jobs_unfinished: bool = False,
        from_states: Optional[Tuple[str, ...]] = None,
    ) -> Optional[Execution]:
        """Move an execution to ``state`` if it may get there from where it is now.

        One compare-and-set — ``UPDATE … WHERE state IN ExecutionState.FROM[state]`` — so
        of two concurrent transitions only a legal one lands. It used to read, then write
        unconditionally: a ``/cancel`` that landed before the planner's ``running`` was
        overwritten (the cancelled run went ahead in full), a pause racing the end of a run
        turned ``completed`` back into ``paused``, and the drive loop's final state could
        overwrite a cancel.

        Args:
            execution_id: Execution identifier
            state: New state (one of ``ExecutionState.ALL``)
            error_message: Optional error message if failed
            notify: Tell the terminal listeners (lifecycle hooks) when the state is
                terminal. False when the caller finishes the transition first (cancel
                also cancels the jobs) and calls :meth:`notify_terminal` itself.
            unless_jobs_unfinished: Refuse while any job is pending, dispatched or
                running — the drive loop's end, so jobs requeued by ``/retry-failed``
                after it counted them keep the execution open.
            from_states: Narrow the states it may come from (within
                ``ExecutionState.FROM[state]``): the planner moves ``pending -> running``
                only, so a pause that landed first is not taken for a /resume.

        Returns:
            The updated Execution, or None if it does not exist or the transition was
            refused (the execution is in a state ``state`` cannot be entered from).
        """
        allowed = ExecutionState.FROM.get(state)
        if allowed is None:
            raise ValueError(f"Unknown execution state {state!r}")
        if from_states is not None:
            allowed = tuple(s for s in allowed if s in from_states)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        values: Dict[Any, Any] = {"state": state, "updated_at": now}
        if error_message:
            values["error_message"] = error_message
        if state in self.TERMINAL_STATES:
            values["completed_at"] = now

        query = self.db.query(Execution).filter(
            Execution.execution_id == execution_id, Execution.state.in_(allowed)
        )
        if unless_jobs_unfinished:
            query = query.filter(
                ~exists().where(
                    Job.execution_id == execution_id,
                    Job.state.in_(JobState.UNSTARTED + (JobState.RUNNING,)),
                )
            )
        won = query.update(values, synchronize_session=False)
        self.db.commit()

        execution = self.get_execution(execution_id)
        if execution is None:
            return None
        self.db.refresh(execution)
        if not won:
            logger.info(
                "Execution is %s; not moving it to %s",
                execution.state,
                state,
                extra={
                    "event": "execution_transition_refused",
                    "execution_id": execution_id,
                    "state": execution.state,
                    "requested_state": state,
                },
            )
            return None

        if notify and state in self.TERMINAL_STATES:
            self.notify_terminal(execution_id, state)

        return execution

    def notify_terminal(self, execution_id: str, state: str) -> None:
        """Tell the terminal listeners (lifecycle hooks); never raises into the caller."""
        for listener in list(_terminal_listeners):
            try:
                listener(self.db.get_bind(), execution_id, state)
            except Exception:
                logger.warning(
                    "Terminal-state listener failed",
                    exc_info=True,
                    extra={"event": "terminal_listener_error", "execution_id": execution_id},
                )

    def set_rate_limit(self, execution_id: str, rate_limit: Optional[float]) -> None:
        """Persist the rate limit the execution runs at, so a resume reuses it."""
        self.db.query(Execution).filter(Execution.execution_id == execution_id).update(
            {"rate_limit": rate_limit}, synchronize_session=False
        )
        self.db.commit()

    def claim_hooks(self, execution_id: str) -> bool:
        """Claim the right to fire this execution's lifecycle hooks. True exactly once.

        ``UPDATE ... SET hooks_fired_at = now WHERE hooks_fired_at IS NULL``: of any
        number of callers (two manager pods, a cancel racing the drive loop's end) one
        gets the row.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        claimed = (
            self.db.query(Execution)
            .filter(
                Execution.execution_id == execution_id,
                Execution.hooks_fired_at.is_(None),
                # Checked in the same statement: a /retry-failed reopening the execution
                # between the caller's check and this claim must not take the claim the
                # retried run's real end needs.
                Execution.state.in_(ExecutionState.TERMINAL),
            )
            .update({"hooks_fired_at": now}, synchronize_session=False)
        )
        self.db.commit()
        return claimed == 1

    def set_hook_error(self, execution_id: str, error: Optional[str]) -> None:
        """Record what failed in a hook or the callback (never touches ``state``)."""
        self.db.query(Execution).filter(Execution.execution_id == execution_id).update(
            {"hook_error": error}, synchronize_session=False
        )
        self.db.commit()

    def pause_execution(self, execution_id: str) -> Optional[Execution]:
        """Pause a pending or running execution. None if missing or in any other state."""
        return self.update_execution_state(execution_id, ExecutionState.PAUSED)

    def resume_execution(self, execution_id: str) -> Optional[Execution]:
        """Resume a paused execution. None if missing or not paused."""
        if (execution := self.get_execution(execution_id)) is None:
            return None
        if execution.state != ExecutionState.PAUSED:
            return None
        return self.update_execution_state(execution_id, ExecutionState.RUNNING)

    # States an execution can be in, and the ones it never leaves on its own.
    STATES = ExecutionState.ALL
    TERMINAL_STATES = ExecutionState.TERMINAL

    def cancel_execution(self, execution_id: str, notify: bool = True) -> Optional[Execution]:
        """Mark a pending, running or paused execution ``cancelled``.

        Returns None if it does not exist or is already finished. Cancelling its jobs
        is the caller's job (see ``JobManager.cancel_unstarted_jobs``): this only moves the
        execution, which is what stops its dispatch loop and its planner. A caller that
        cancels the jobs passes ``notify=False`` and calls :meth:`notify_terminal` after,
        so the hooks' summary counts them.
        """
        return self.update_execution_state(
            execution_id, ExecutionState.CANCELLED, error_message="Cancelled", notify=notify
        )

    def mark_plan_complete(self, execution_id: str) -> None:
        """Record that every job of the execution is saved (planning finished)."""
        self.db.query(Execution).filter(Execution.execution_id == execution_id).update(
            {"plan_complete": True}, synchronize_session=False
        )
        self.db.commit()

    def reopen_execution(self, execution_id: str) -> Optional[Execution]:
        """Move a settled (terminal) execution back to running so a dispatch loop can
        resume it. None if it does not exist or is not terminal — a pending or running
        execution already has a planner or a loop that will pick its requeued jobs up.

        Compare-and-set, so a ``/retry-failed`` racing the drive loop's end either finds
        the execution still open (its loop sees the requeued jobs) or reopens it after
        the loop settled it (the caller then starts a loop) — never ``running`` with
        no loop.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        reopened = (
            self.db.query(Execution)
            .filter(
                Execution.execution_id == execution_id,
                Execution.state.in_(ExecutionState.TERMINAL),
            )
            .update(
                {
                    "state": ExecutionState.RUNNING,
                    "completed_at": None,
                    "error_message": None,
                    # Its next end fires the hooks again (they reported the run before
                    # the retry).
                    "hooks_fired_at": None,
                    "hook_error": None,
                    "updated_at": now,
                },
                synchronize_session=False,
            )
        )
        self.db.commit()
        if not reopened:
            return None
        execution = self.get_execution(execution_id)
        if execution is not None:
            self.db.refresh(execution)
        return execution

    def claim_driver(self, execution_id: str, driver_id: str, lease_seconds: float) -> bool:
        """Take ownership of dispatching this execution. True if this driver owns it.

        Succeeds when nobody owns it, this driver already does, or the owner's
        heartbeat is older than ``lease_seconds`` (its loop died). A row lease rather
        than a Postgres advisory lock: a session lock held by a force-killed pod stays
        on its zombie connection until TCP keepalive notices (hours), blocking every
        resume in between.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        stale = now - timedelta(seconds=lease_seconds)
        claimed = (
            self.db.query(Execution)
            .filter(
                Execution.execution_id == execution_id,
                or_(
                    Execution.driver_id.is_(None),
                    Execution.driver_id == driver_id,
                    Execution.driver_heartbeat_at < stale,
                ),
            )
            .update(
                {"driver_id": driver_id, "driver_heartbeat_at": now},
                synchronize_session=False,
            )
        )
        self.db.commit()
        return claimed == 1

    def heartbeat_driver(self, execution_id: str, driver_id: str) -> bool:
        """Refresh this driver's lease. False if another loop has taken over."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        alive = (
            self.db.query(Execution)
            .filter(Execution.execution_id == execution_id, Execution.driver_id == driver_id)
            .update({"driver_heartbeat_at": now}, synchronize_session=False)
        )
        self.db.commit()
        return alive == 1

    def release_drivers_on_host(self, hostname: str) -> int:
        """Free every lease whose driver ran on ``hostname`` (``<host>:<pid>:<rand>``).

        Called once at manager startup: such drivers belonged to a previous process
        on this host and are dead. Two live managers sharing a hostname would only
        hand an execution over — the old loop's next heartbeat fails and it stops.
        """
        released = (
            self.db.query(Execution)
            .filter(Execution.driver_id.like(f"{hostname}:%"))
            .update({"driver_id": None, "driver_heartbeat_at": None}, synchronize_session=False)
        )
        self.db.commit()
        return released

    def release_driver(self, execution_id: str, driver_id: str) -> None:
        """Give up ownership (only if still the owner), so a resume needn't wait out the lease."""
        self.db.query(Execution).filter(
            Execution.execution_id == execution_id, Execution.driver_id == driver_id
        ).update({"driver_id": None, "driver_heartbeat_at": None}, synchronize_session=False)
        self.db.commit()

    def get_interrupted_executions(self) -> List[Execution]:
        """
        Find executions that were running when the service crashed.

        These are executions in 'running' state that need to be resumed, plus
        'pending' ones whose planning never finished: the manager can die between
        creating the record and marking it running, which used to leave the execution
        pending forever.

        Returns:
            List of Execution objects to recover
        """
        return (
            self.db.query(Execution)
            .filter(
                or_(
                    Execution.state == ExecutionState.RUNNING,
                    and_(
                        Execution.state == ExecutionState.PENDING,
                        Execution.plan_complete.is_(False),
                    ),
                )
            )
            .all()
        )
