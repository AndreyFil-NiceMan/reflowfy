"""Resuming an execution: after a crash, ``/resume`` or ``/retry-failed``.

``resume_execution`` re-enters the dispatch loop (``drive_loop``) for an execution
whose jobs are already planned; ``_recover_plan`` deals with one whose planning never
finished (it cannot be continued, so it is failed once its planner is gone).
"""

import logging
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional


from reflowfy.observability.context import bind_log_context
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.store.models import Job
from reflowfy.store.states import JobState, ExecutionState

from reflowfy.reflow_manager import drive_loop
from reflowfy.reflow_manager.drive_loop import DriveLoop
from reflowfy.reflow_manager.rate_limiter import resolve_rate_limit

logger = logging.getLogger(__name__)


class Recovery(DriveLoop):
    """Resume and planning-crash recovery on top of the dispatch loop."""

    @bind_log_context("execution_id")
    def resume_execution(
        self,
        execution_id: str,
        rate_limit_override: Optional[float] = None,
    ) -> None:
        """
        Resume an interrupted execution from where it left off.

        Skips Phase 1 (jobs already in DB) and re-enters the Phase 2 drive loop,
        which picks up pending jobs and leaves in-flight ones to their leases.
        Used for crash recovery and by POST /executions/{id}/resume.

        Args:
            execution_id: Execution identifier to resume
            rate_limit_override: Optional override for jobs per minute
        """
        from reflowfy.core.registry import pipeline_registry

        execution = self.execution_manager.get_execution(execution_id)
        if not execution:
            logger.warning(
                "Execution not found, skipping resume", extra={"event": "resume_skipped"}
            )
            return
        if execution.state in ExecutionManager.TERMINAL_STATES:
            # Settled while this resume was queued (e.g. /cancel after /resume):
            # driving it would finalize it again and overwrite "cancelled".
            logger.info(
                "Execution is already %s; not resuming",
                execution.state,
                extra={"event": "resume_skipped", "state": execution.state},
            )
            return

        pipeline_name = execution.pipeline_name

        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            logger.warning(
                "Pipeline not in registry, marking execution as failed",
                extra={"event": "resume_pipeline_missing", "pipeline_name": pipeline_name},
            )
            self.execution_manager.update_execution_state(
                execution_id,
                ExecutionState.FAILED,
                error_message=(
                    f"Pipeline '{pipeline_name}' not found in registry during recovery"
                    f"{pipeline_registry.describe_missing(pipeline_name)}"
                ),
            )
            return

        if not execution.plan_complete and not self._recover_plan(execution_id):
            return
        self.execution_manager.db.refresh(execution)

        # Nothing unfinished: settle the final state from the job rows.
        counts = self.job_manager.get_job_counts(execution_id)
        unfinished = (
            counts[JobState.PENDING] + counts[JobState.DISPATCHED] + counts[JobState.RUNNING]
        )
        if unfinished == 0:
            logger.info(
                "Execution has no unfinished jobs, syncing state",
                extra={"event": "resume_nothing_to_do", "jobs_total": counts["total"]},
            )
            self._finalize_execution_state(execution_id, counts["total"], (0, 0, 0))
            return

        # The rate the run resolved is persisted on the execution; without it a restart
        # silently fell back to the class rate_limit. Executions from before it was
        # persisted resolve it again from the pipeline.
        params: Dict[str, Any] = dict(execution.runtime_params or {})
        if hasattr(pipeline, "apply_defaults"):
            params = pipeline.apply_defaults(params)
        effective_rate_limit = resolve_rate_limit(
            pipeline, params, override=rate_limit_override, persisted=execution.rate_limit
        )

        # Local mode runs jobs in-process, so whatever was dispatched or running died
        # with the restart. Requeue it now instead of waiting out the leases. In
        # distributed mode those jobs may still be alive on a worker; the leases decide.
        if isinstance(self.dispatcher, LocalDispatcher):
            orphaned = (
                self.job_manager.db.query(Job)
                .filter(
                    Job.execution_id == execution_id,
                    Job.state.in_(JobState.IN_FLIGHT),
                )
                .update({"state": JobState.PENDING}, synchronize_session=False)
            )
            self.job_manager.db.commit()
            if orphaned:
                logger.warning(
                    "Local mode: requeued %d orphaned jobs",
                    orphaned,
                    extra={"event": "orphaned_jobs_requeued", "jobs_requeued": orphaned},
                )

        logger.info(
            "Resuming execution: %d pending, %d in flight",
            counts[JobState.PENDING],
            counts[JobState.DISPATCHED] + counts[JobState.RUNNING],
            extra={
                "event": "execution_resume",
                "jobs_pending": counts[JobState.PENDING],
                "jobs_in_flight": counts[JobState.DISPATCHED] + counts[JobState.RUNNING],
            },
        )
        dispatched, completed, failed = self._drive_execution(
            execution_id, pipeline_name, effective_rate_limit
        )
        logger.info(
            "Resumed execution drive ended: %d dispatched, %d completed, %d failed",
            dispatched,
            completed,
            failed,
            extra={
                "event": "execution_drive_end",
                "jobs_dispatched": dispatched,
                "jobs_completed": completed,
                "jobs_failed": failed,
            },
        )

    @bind_log_context("execution_id")
    def _recover_plan(self, execution_id: str) -> bool:
        """Deal with an execution whose planning had not finished when we looked.

        The jobs saved so far are a fragment of the plan and the source's position
        cannot be recovered, so a plan can't be continued. But a manager that is still
        planning (this pod's neighbour, or the old pod during a rollout) looks the same
        until its lease says otherwise. So: wait while the planning lease is fresh, and
        once it is gone — and the execution is older than one lease, so a record
        created a moment ago is not mistaken for an orphan — fail the execution with a
        message saying so, instead of dispatching the fragment.

        Returns:
            True if planning finished while we waited (carry on resuming the execution),
            False if there is nothing left to resume (failed here, or settled elsewhere).
        """
        recovery_id = f"recover:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        announced = False
        while True:
            self.execution_manager.db.expire_all()
            execution = self.execution_manager.get_execution(execution_id)
            if execution is None or execution.state not in (
                ExecutionState.PENDING,
                ExecutionState.RUNNING,
            ):
                return False
            if execution.plan_complete:
                return True
            age = (
                datetime.now(timezone.utc).replace(tzinfo=None) - execution.created_at
            ).total_seconds()
            if age >= drive_loop.DRIVER_LEASE_SECONDS and self.execution_manager.claim_driver(
                execution_id, recovery_id, drive_loop.DRIVER_LEASE_SECONDS
            ):
                saved = self.job_manager.get_job_counts(execution_id)["total"]
                self.execution_manager.update_execution_state(
                    execution_id,
                    ExecutionState.FAILED,
                    error_message=(
                        f"The manager stopped while planning this execution ({saved} of its "
                        "jobs had been saved, none dispatched). A plan cannot be continued; "
                        "run it again."
                    ),
                )
                self.execution_manager.release_driver(execution_id, recovery_id)
                logger.warning(
                    "Execution was abandoned mid-plan (%d jobs saved); marked failed",
                    saved,
                    extra={"event": "plan_abandoned", "jobs_total": saved},
                )
                return False
            if not announced:
                logger.info(
                    "Execution is still being planned (or was just created); "
                    "waiting to see whether its planner is alive",
                    extra={"event": "plan_waiting"},
                )
                announced = True
            time.sleep(drive_loop.DRIVER_RETRY_SECONDS)
