"""Execution management for ReflowManager."""

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from reflowfy.reflow_manager.models import Execution


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
    ) -> Execution:
        """
        Create new execution record.

        Args:
            execution_id: Unique execution identifier
            pipeline_name: Name of the pipeline
            runtime_params: Runtime parameters for the pipeline
            rate_limit: The request's rate_limit override, if any (jobs/min)

        Returns:
            Created Execution object
        """
        execution = Execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            state="pending",
            runtime_params=runtime_params or {},
            rate_limit=rate_limit,
            plan_complete=False,
        )

        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)

        return execution

    def get_execution(self, execution_id: str, for_update: bool = False) -> Optional[Execution]:
        """
        Get execution by ID.

        Args:
            execution_id: Execution identifier
            for_update: If True, lock the row for update (prevents concurrent modifications)

        Returns:
            Execution object or None if not found
        """
        query = self.db.query(Execution).filter(
            Execution.execution_id == execution_id
        )
        if for_update:
            query = query.with_for_update()
        return query.first()

    def update_execution_state(
        self,
        execution_id: str,
        state: str,
        error_message: Optional[str] = None,
    ) -> Optional[Execution]:
        """
        Update execution state.

        Args:
            execution_id: Execution identifier
            state: New state (pending, running, paused, completed, failed)
            error_message: Optional error message if failed

        Returns:
            Updated Execution object or None if not found
        """
        execution = self.get_execution(execution_id)
        if not execution:
            return None

        execution.state = state
        if error_message:
            execution.error_message = error_message

        if state in ["completed", "failed", "cancelled"]:
            execution.completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

        self.db.commit()
        self.db.refresh(execution)

        return execution

    def update_job_counts(
        self,
        execution_id: str,
        jobs_dispatched: Optional[int] = None,
        jobs_completed: Optional[int] = None,
        jobs_failed: Optional[int] = None,
    ) -> Optional[Execution]:
        """
        Update job counts for an execution.

        Note: Does NOT commit - caller should commit after all updates.

        Args:
            execution_id: Execution identifier
            jobs_dispatched: Increment jobs dispatched
            jobs_completed: Increment jobs completed
            jobs_failed: Increment jobs failed

        Returns:
            Updated Execution object or None if not found
        """
        execution = self.get_execution(execution_id)
        if not execution:
            return None

        if jobs_dispatched is not None:
            execution.jobs_dispatched += jobs_dispatched
        if jobs_completed is not None:
            execution.jobs_completed += jobs_completed
        if jobs_failed is not None:
            execution.jobs_failed += jobs_failed

        # Don't commit here - caller will commit
        return execution

    def pause_execution(self, execution_id: str) -> Optional[Execution]:
        """Pause a pending or running execution. None if missing or already settled
        (pausing a completed execution used to flip it back to non-terminal)."""
        execution = self.get_execution(execution_id)
        if not execution or execution.state not in ("pending", "running"):
            return None
        return self.update_execution_state(execution_id, "paused")

    def resume_execution(self, execution_id: str) -> Optional[Execution]:
        """Resume a paused execution."""
        execution = self.get_execution(execution_id)
        if not execution or execution.state != "paused":
            return None

        return self.update_execution_state(execution_id, "running")

    # States an execution can be in, and the ones it never leaves on its own.
    STATES = ("pending", "running", "paused", "completed", "failed", "cancelled")
    TERMINAL_STATES = ("completed", "failed", "cancelled")

    def cancel_execution(self, execution_id: str) -> Optional[Execution]:
        """Mark a pending, running or paused execution ``cancelled``.

        Returns None if it does not exist or is already finished. Cancelling its jobs
        is the caller's job (see ``JobManager.cancel_unstarted_jobs``): this only moves the
        execution, which is what stops its dispatch loop and its planner.
        """
        execution = self.get_execution(execution_id)
        if not execution or execution.state in self.TERMINAL_STATES:
            return None
        return self.update_execution_state(execution_id, "cancelled", error_message="Cancelled")

    def mark_plan_complete(self, execution_id: str) -> None:
        """Record that every job of the execution is saved (planning finished)."""
        self.db.query(Execution).filter(Execution.execution_id == execution_id).update(
            {"plan_complete": True}, synchronize_session=False
        )
        self.db.commit()

    def reopen_execution(self, execution_id: str) -> Optional[Execution]:
        """Move a settled execution back to running so its dispatch loop can resume it."""
        execution = self.get_execution(execution_id)
        if not execution:
            return None
        execution.state = "running"
        execution.completed_at = None
        execution.error_message = None
        self.db.commit()
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

    def update_deduplicated_count(self, execution_id: str, count: int) -> None:
        """Set the deduplicated_jobs count for an execution."""
        execution = self.get_execution(execution_id)
        if execution:
            execution.deduplicated_jobs = count
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
                    Execution.state == "running",
                    and_(Execution.state == "pending", Execution.plan_complete.is_(False)),
                )
            )
            .all()
        )

