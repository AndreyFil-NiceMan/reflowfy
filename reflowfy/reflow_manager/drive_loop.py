"""Phase 2 of an execution: the dispatch loop (``PipelineRunner``'s base class).

Keeps up to ``MAX_IN_FLIGHT_JOBS`` jobs in flight, one loop per execution by a lease on
the execution row (``DRIVER_LEASE_SECONDS``), sweeps job leases, and settles the
execution once no job is left unfinished. See CLAUDE.md "Dispatch loop, leases and
redelivery". Planning is ``pipeline_runner``; resuming after a crash, ``recovery``.
"""

import asyncio
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


from sqlalchemy.exc import DBAPIError

from reflowfy.observability import metrics
from reflowfy.observability.context import bind_log_context
from reflowfy.reflow_manager.dispatcher import BaseDispatcher
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.store.models import Job
from reflowfy.reflow_manager.rate_limiter import execution_bucket_key
from reflowfy.store.leases import JobLeases
from reflowfy.store.states import JobState, ExecutionState

logger = logging.getLogger(__name__)

CHECKPOINT_POLL_INTERVAL = 2.0  # seconds between drive-loop polls when nothing can be sent

# Phase 2 is a continuous loop, not a batch barrier: it keeps up to this many jobs
# in flight (dispatched or running) and tops the window up as jobs finish. This is
# the throughput lever the batch size used to be — enough queued work for KEDA to
# see lag and scale, bounded so a stalled worker pool can't absorb the whole plan.
MAX_IN_FLIGHT_JOBS = int(os.getenv("MAX_IN_FLIGHT_JOBS", "1000"))

# Job leases (store/leases.py). A job a worker claimed (running) with no heartbeat
# for the running lease lost its worker and goes back to pending, or fails after
# max_attempts claims; one dispatched but never claimed is sent again. The
# worker-side claim makes the duplicate message harmless.
JOB_LEASES = JobLeases.from_env()

JOB_RUNNING_LEASE_SECONDS = JOB_LEASES.running_lease_seconds

JOB_DISPATCH_LEASE_SECONDS = JOB_LEASES.dispatch_lease_seconds

JOB_MAX_ATTEMPTS = JOB_LEASES.max_attempts

LEASE_CHECK_INTERVAL = 60.0  # seconds between lease sweeps

# The dispatcher hands control back to the drive loop this often, so lease sweeps
# and the in-flight cap are re-evaluated even at very low rate limits.
DISPATCH_CHUNK_SECONDS = 30.0

DISPATCH_FETCH_LIMIT = 500  # job payloads loaded per drive-loop iteration

STALL_WARN_SECONDS = 600.0  # warn when jobs are in flight but none finished this long

# One dispatch loop per execution, by a lease on the execution row: the owner
# heartbeats about once a second; another loop (a restarted manager, a second pod,
# a resume racing a pausing loop) stands by and takes over once the heartbeat is
# older than this. Must exceed the longest gap between heartbeats — a rate-limit
# token wait (TOKEN_WAIT_SECONDS) plus one chunk, or one in-process job in local mode.
DRIVER_LEASE_SECONDS = float(os.getenv("DRIVER_LEASE_SECONDS", "60"))

DRIVER_RETRY_SECONDS = 15.0


def _finished_count(completed: int, failed: int, deduplicated: int) -> int:
    """Jobs in a terminal state. Deduplicated jobs are a success outcome."""
    return completed + failed + deduplicated


def _run_async(coro: Any) -> Any:
    """Run async code in sync context, handling nested event loops."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop — safe to call asyncio.run directly.
        return asyncio.run(coro)
    else:
        # A loop is already running (e.g. Jupyter, pytest-asyncio).
        # Use nest_asyncio if available for clean re-entry; otherwise
        # fall back to a separate thread to avoid deadlocks.
        try:
            import nest_asyncio

            nest_asyncio.apply()  # pyright: ignore[reportUnknownMemberType]  # no stubs
        except (ImportError, ValueError):
            # ValueError: nest_asyncio can't patch uvloop, which uvicorn runs — the
            # case for the synchronous DLQ "dispatch now" endpoint. Use the thread.
            pass
        else:
            return asyncio.get_event_loop().run_until_complete(coro)
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()


class DriveLoop:
    """The dispatch loop of an execution, and what it reads and settles."""

    def __init__(
        self,
        execution_manager: ExecutionManager,
        job_manager: JobManager,
        dispatcher: BaseDispatcher,
    ):
        """
        Initialize pipeline runner.

        Args:
            execution_manager: ExecutionManager instance
            job_manager: JobManager instance (includes checkpoint functionality)
            dispatcher: BaseDispatcher instance
        """
        self.execution_manager = execution_manager
        self.job_manager = job_manager
        self.dispatcher = dispatcher

    @bind_log_context("execution_id", "pipeline_name")
    def _drive_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        effective_rate_limit: Optional[float],
    ) -> Tuple[int, int, int]:
        """Phase 2: dispatch jobs until every one has finished, then set the final state.

        Only one loop may drive an execution — a second one would double the dispatch
        rate. Ownership is a lease on the execution row (see DRIVER_LEASE_SECONDS).

        Returns:
            Final (dispatched, completed, failed) counts.
        """
        driver_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        try:
            result: Tuple[int, int, int] = _run_async(
                self._drive_async(execution_id, pipeline_name, effective_rate_limit, driver_id)
            )
            return result
        finally:
            try:
                self.execution_manager.release_driver(execution_id, driver_id)
            except Exception:
                logger.warning(
                    "Could not release dispatch lease",
                    exc_info=True,
                    extra={"event": "driver_release_error"},
                )

    @bind_log_context("execution_id")
    async def _wait_for_driver_lease(self, execution_id: str, driver_id: str) -> bool:
        """Claim the dispatch lease, standing by while a live loop holds it.

        Returns False if the execution stops running while waiting.
        """
        announced = False
        while True:
            try:
                if self.execution_manager.claim_driver(
                    execution_id, driver_id, DRIVER_LEASE_SECONDS
                ):
                    return True
                if self._execution_state(execution_id) != ExecutionState.RUNNING:
                    return False
            except DBAPIError:
                logger.warning(
                    "Database error claiming the dispatch lease; retrying",
                    exc_info=True,
                    extra={"event": "driver_claim_db_error"},
                )
                self.job_manager.db.rollback()
            if not announced:
                logger.info(
                    "Execution is being dispatched by another loop; standing by to "
                    "take over if it stops",
                    extra={"event": "driver_standby"},
                )
                announced = True
            await asyncio.sleep(DRIVER_RETRY_SECONDS)

    @bind_log_context("execution_id", "pipeline_name")
    async def _drive_async(
        self,
        execution_id: str,
        pipeline_name: str,
        effective_rate_limit: Optional[float],
        driver_id: str,
    ) -> Tuple[int, int, int]:
        """The drive loop. One event loop for the whole execution, so the Kafka
        producer is created once and closed at the end."""
        if not await self._wait_for_driver_lease(execution_id, driver_id):
            logger.info(
                "Execution is no longer running; not dispatching",
                extra={"event": "dispatch_skipped"},
            )
            return self._sync_counts_from_db(execution_id)
        # Execution-scoped key: concurrent executions of one pipeline don't share a bucket.
        rate_limit_key = execution_bucket_key(pipeline_name, execution_id)
        started = time.time()
        # Loop bookkeeping carried between steps.
        track: Dict[str, float] = {
            "finished": -1.0,
            "progress_at": started,
            "warned_at": started,
            "lease_checked_at": 0.0,
        }
        db_backoff = 0.0
        try:
            while True:
                try:
                    result = await self._drive_step(
                        execution_id,
                        pipeline_name,
                        effective_rate_limit,
                        rate_limit_key,
                        driver_id,
                        track,
                    )
                except DBAPIError:
                    # Postgres went away (restart, failover, idled namespace). The jobs and
                    # their states are safe in the database; keep retrying instead of
                    # ending the loop, which left the execution stuck until a manager restart.
                    db_backoff = min(max(db_backoff * 2, 5.0), 60.0)
                    logger.warning(
                        "Database error in dispatch loop; retrying in %.0fs",
                        db_backoff,
                        exc_info=True,
                        extra={"event": "dispatch_db_error", "retry_in_s": db_backoff},
                    )
                    try:
                        self.job_manager.db.rollback()
                    except Exception:
                        pass  # the connection is gone; the next query reconnects
                    await asyncio.sleep(db_backoff)
                    continue
                db_backoff = 0.0
                if result is not None:
                    return result
        finally:
            try:
                await self.dispatcher.close()
            except Exception:
                logger.warning(
                    "Could not close dispatcher",
                    exc_info=True,
                    extra={"event": "dispatcher_close_error"},
                )
            self._drop_rate_bucket(execution_id, rate_limit_key)

    def _drop_rate_bucket(self, execution_id: str, rate_limit_key: str) -> None:
        """Delete the execution's token bucket once it has ended (best effort).

        A paused execution keeps it: its resumed loop goes on at the same rate.
        """
        limiter = getattr(self.dispatcher, "rate_limiter", None)
        if limiter is None:
            return
        try:
            if self._execution_state(execution_id) in ExecutionManager.TERMINAL_STATES:
                limiter.delete_bucket(rate_limit_key)
        except Exception:
            logger.warning(
                "Could not delete the rate-limit bucket",
                exc_info=True,
                extra={"event": "rate_bucket_cleanup_error"},
            )

    async def _drive_step(
        self,
        execution_id: str,
        pipeline_name: str,
        effective_rate_limit: Optional[float],
        rate_limit_key: str,
        driver_id: str,
        track: Dict[str, float],
    ) -> Optional[Tuple[int, int, int]]:
        """One pass of the drive loop. Returns the final counts when the loop should end."""
        if not self.execution_manager.heartbeat_driver(execution_id, driver_id):
            # Our heartbeat went stale (a long stall) and another loop took over.
            logger.warning(
                "Another dispatch loop took over; this one stops",
                extra={"event": "driver_replaced"},
            )
            return self._sync_counts_from_db(execution_id)
        state = self._execution_state(execution_id)
        if state != ExecutionState.RUNNING:
            # Paused (or settled elsewhere): stop sending. In-flight jobs still finish;
            # POST /executions/{id}/resume starts a new loop.
            logger.info(
                "Execution is %s; dispatch loop stopping",
                state,
                extra={"event": "dispatch_stopped", "state": state},
            )
            return self._sync_counts_from_db(execution_id)

        now = time.time()
        if now - track["lease_checked_at"] >= LEASE_CHECK_INTERVAL:
            self._reclaim_expired_leases(execution_id, pipeline_name)
            track["lease_checked_at"] = now

        counts = self.job_manager.get_job_counts(execution_id)
        in_flight = counts[JobState.DISPATCHED] + counts[JobState.RUNNING]
        if counts[JobState.PENDING] == 0 and in_flight == 0:
            return self._finalize_execution_state(
                execution_id, counts["total"], self._sync_counts_from_db(execution_id)
            )

        finished = _finished_count(
            counts[JobState.COMPLETED], counts[JobState.FAILED], counts[JobState.DEDUPLICATED]
        )
        if finished != track["finished"]:
            track["finished"], track["progress_at"] = finished, now
        elif now - max(track["progress_at"], track["warned_at"]) >= STALL_WARN_SECONDS:
            # No batch timeout fails the run any more, so say so when it's stuck.
            retrying = self.job_manager.count_scheduled_retries(execution_id)
            logger.warning(
                "No job finished for %.0f min with %d in flight "
                "and %d pending (%d waiting to be retried) — are workers running?",
                (now - track["progress_at"]) / 60,
                in_flight,
                counts[JobState.PENDING],
                retrying,
                extra={
                    "event": "dispatch_stalled",
                    "stalled_minutes": round((now - track["progress_at"]) / 60),
                    "jobs_in_flight": in_flight,
                    "jobs_pending": counts[JobState.PENDING],
                    "jobs_retry_scheduled": retrying,
                },
            )
            track["warned_at"] = now

        free_slots = MAX_IN_FLIGHT_JOBS - in_flight
        sent = 0
        if counts[JobState.PENDING] and free_slots > 0:
            jobs = self.job_manager.get_pending_jobs(
                execution_id, limit=min(free_slots, DISPATCH_FETCH_LIMIT)
            )
            sent = await self._send_jobs(
                execution_id, rate_limit_key, effective_rate_limit, driver_id, jobs
            )
            self._sync_counts_from_db(execution_id)
        if not sent:
            # Window full, nothing pending, or the send failed: wait, but return the
            # pooled connection first (the count query opened a transaction; holding it
            # across the sleep pins a connection per execution and exhausts the pool
            # when schedules overlap).
            self.job_manager.db.commit()
            await asyncio.sleep(CHECKPOINT_POLL_INTERVAL)
        return None

    async def _send_jobs(
        self,
        execution_id: str,
        rate_limit_key: str,
        effective_rate_limit: Optional[float],
        driver_id: str,
        jobs: List[Job],
    ) -> int:
        """Send jobs in order, marking each dispatched right after its send.

        Hands control back after DISPATCH_CHUNK_SECONDS, and stops within about a
        second of the execution being paused.
        """
        started = time.time()
        # Before every send of this call: a retry a worker schedules after one of them
        # has a later next_attempt_at, so mark_job_dispatched leaves it pending.
        sent_after = datetime.now(timezone.utc).replace(tzinfo=None)
        last_check = 0.0
        stop = False

        def should_stop(chunk: bool = True) -> bool:
            # chunk=False: a token is already spent, so only a pause or a lost lease
            # may stop the send; the chunk running out must not drop the job.
            nonlocal last_check, stop
            now = time.time()
            if chunk and now - started >= DISPATCH_CHUNK_SECONDS:
                return True
            if now - last_check >= 1.0:
                last_check = now
                # Heartbeat here too: a chunk can outlast many drive-loop passes.
                stop = (
                    not self.execution_manager.heartbeat_driver(execution_id, driver_id)
                    or self._execution_state(execution_id) != ExecutionState.RUNNING
                )
            return stop

        def on_sent(payload: Dict[str, Any]) -> None:
            self.job_manager.mark_job_dispatched(payload["job_id"], sent_after=sent_after)

        def on_unsendable(payload: Dict[str, Any], reason: str) -> None:
            self.job_manager.update_job_state(
                payload["job_id"], JobState.FAILED, error_message=reason
            )
            metrics.count_job(payload.get("pipeline_name", "unknown"), metrics.JobOutcome.FAILED)

        sent: int = await self.dispatcher.dispatch_jobs_batch(
            jobs=[job.job_payload for job in jobs],
            pipeline_name=rate_limit_key,
            rate_limit=effective_rate_limit,
            on_sent=on_sent,
            should_stop=should_stop,
            on_unsendable=on_unsendable,
        )
        return sent

    @bind_log_context("execution_id", "pipeline_name")
    def _reclaim_expired_leases(self, execution_id: str, pipeline_name: str) -> None:
        """Requeue jobs whose worker or message was lost; fail ones out of attempts."""
        result = self.job_manager.reclaim_expired_leases(
            execution_id,
            running_lease_seconds=JOB_RUNNING_LEASE_SECONDS,
            dispatch_lease_seconds=JOB_DISPATCH_LEASE_SECONDS,
            max_attempts=JOB_MAX_ATTEMPTS,
        )
        if any(result.values()):
            logger.warning(
                "Lease expired — requeued %d lost-worker and %d "
                "lost-message jobs, failed %d out of attempts",
                result["requeued_running"],
                result["requeued_dispatched"],
                result[JobState.FAILED],
                extra={
                    "event": "lease_expired",
                    "jobs_requeued_running": result["requeued_running"],
                    "jobs_requeued_dispatched": result["requeued_dispatched"],
                    "jobs_failed": result[JobState.FAILED],
                },
            )
        if result[JobState.FAILED]:
            metrics.count_job(
                pipeline_name, metrics.JobOutcome.LEASE_EXPIRED, result[JobState.FAILED]
            )

    def _execution_state(self, execution_id: str) -> Optional[str]:
        """Fresh execution state (pause/resume land from other sessions)."""
        self.execution_manager.db.expire_all()
        execution = self.execution_manager.get_execution(execution_id)
        return execution.state if execution else None

    @bind_log_context("execution_id")
    def _finalize_execution_state(
        self,
        execution_id: str,
        expected_total: int,
        fallback_counts: Tuple[int, int, int],
    ) -> Optional[Tuple[int, int, int]]:
        """
        Sync final counts from the DB and set the terminal execution state.

        None when the execution is still running with unfinished jobs (``/retry-failed``
        requeued some after they were counted): the drive loop goes on.

        Args:
            execution_id: Execution identifier
            expected_total: Number of jobs that should have finished
            fallback_counts: (dispatched, completed, failed) to use if the DB sync fails

        Returns:
            The (dispatched, completed, failed) counts, or None (see above).
        """
        try:
            dispatched, completed, failed = self._sync_counts_from_db(execution_id)
            counts = self.job_manager.get_job_counts(execution_id)
            total_finished = _finished_count(
                counts.get(JobState.COMPLETED, 0),
                counts.get(JobState.FAILED, 0),
                counts.get(JobState.DEDUPLICATED, 0),
            )
        except Exception:
            logger.warning(
                "Could not sync counts; using in-memory totals",
                exc_info=True,
                extra={"event": "sync_counts_error"},
            )
            dispatched, completed, failed = fallback_counts
            # completed from the loop already includes any deduplicated jobs.
            total_finished = completed + failed

        if total_finished == expected_total:
            final_state = ExecutionState.COMPLETED if failed == 0 else ExecutionState.FAILED
        else:
            final_state = ExecutionState.FAILED
            logger.warning(
                "Only %d of %d jobs finished",
                total_finished,
                expected_total,
                extra={
                    "event": "jobs_missing",
                    "jobs_finished": total_finished,
                    "jobs_total": expected_total,
                },
            )

        # Compare-and-set, and only while no job is unfinished: a /cancel or a force-fail
        # since the caller saw `running`, or jobs /retry-failed requeued since it counted
        # them, win over this. It used to overwrite them.
        settled = self.execution_manager.update_execution_state(
            execution_id, final_state, unless_jobs_unfinished=True
        )
        if settled is None:
            # Logged by update_execution_state (execution_transition_refused).
            if self._execution_state(execution_id) == ExecutionState.RUNNING:
                return None  # jobs were requeued meanwhile: keep driving
            return dispatched, completed, failed
        logger.info(
            "Execution %s: %d completed, %d failed of %d jobs",
            final_state,
            completed,
            failed,
            expected_total,
            extra={
                "event": "execution_finished",
                "state": final_state,
                "jobs_total": expected_total,
                "jobs_completed": completed,
                "jobs_failed": failed,
            },
        )
        return dispatched, completed, failed

    def _set_total_jobs(self, execution_id: str, total: int) -> None:
        self.execution_manager.set_total_jobs(execution_id, total)

    def _sync_counts_from_db(self, execution_id: str) -> Tuple[int, int, int]:
        """Copy the jobs table's counts onto the execution row; returns (dispatched,
        finished, failed), deduplicated jobs counting as finished."""
        return self.execution_manager.store_job_counts(
            execution_id, self.job_manager.get_job_counts(execution_id)
        )
