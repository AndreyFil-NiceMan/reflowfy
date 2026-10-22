"""Pipeline cron scheduler for ReflowManager.

Background scheduler that polls pipeline_schedules and fires executions
when a pipeline's next_run_at has elapsed.
"""

import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Set, Tuple

from croniter import croniter
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.models import PipelineSchedule
from reflowfy.reflow_manager.database import SessionLocal

logger = logging.getLogger(__name__)


PIPELINE_SCHEDULER_POLL_INTERVAL = int(os.getenv("PIPELINE_SCHEDULER_POLL_INTERVAL_SECONDS", "30"))


class PipelineScheduler:
    """
    Background scheduler for cron-based pipeline execution.

    Polls the database at regular intervals for pipeline_schedules rows
    whose next_run_at has elapsed, then fires a new execution for each.
    """

    def __init__(
        self,
        poll_interval: int = PIPELINE_SCHEDULER_POLL_INTERVAL,
        pipeline_runner_factory: Optional[Callable[..., Any]] = None,
    ):
        self.poll_interval = poll_interval
        self.pipeline_runner_factory = pipeline_runner_factory
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the background polling thread."""
        if self._running:
            logger.warning(
                "Pipeline Scheduler already running", extra={"event": "scheduler_already_running"}
            )
            return

        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Pipeline Scheduler started (polling every %ss)",
            self.poll_interval,
            extra={"event": "scheduler_start", "poll_interval_s": self.poll_interval},
        )

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if not self._running:
            return

        logger.info("Stopping Pipeline Scheduler", extra={"event": "scheduler_stopping"})
        self._running = False
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

        logger.info("Pipeline Scheduler stopped", extra={"event": "scheduler_stopped"})

    def _run_loop(self) -> None:
        """Main polling loop."""
        while self._running:
            try:
                self._poll_and_trigger()
            except Exception:
                logger.error(
                    "Pipeline Scheduler error", exc_info=True, extra={"event": "scheduler_error"}
                )

            self._stop_event.wait(timeout=self.poll_interval)

    def _poll_and_trigger(self) -> None:
        """Poll for due scheduled pipelines and trigger executions."""
        db = SessionLocal()
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            due = (
                db.query(PipelineSchedule)
                .filter(
                    PipelineSchedule.enabled == "true",
                    PipelineSchedule.next_run_at <= now,
                )
                .with_for_update(skip_locked=True)
                .all()
            )

            if not due:
                return

            logger.info(
                "Pipeline Scheduler found %d due pipeline(s)",
                len(due),
                extra={"event": "schedule_due", "schedules_due": len(due)},
            )

            for schedule in due:
                self._trigger_pipeline(db, schedule, now)

            db.commit()

        except Exception:
            db.rollback()
            logger.error(
                "Pipeline Scheduler poll error",
                exc_info=True,
                extra={"event": "scheduler_poll_error"},
            )
            raise
        finally:
            db.close()

    def _trigger_pipeline(self, db: Session, schedule: PipelineSchedule, now: datetime) -> None:
        """Fire an execution for a due scheduled pipeline and advance next_run_at.

        The execution record is created synchronously so it exists immediately
        and the schedule timer can advance, but the actual job split/dispatch
        runs on a background thread. This keeps the poll loop from blocking on
        a long-running (or hung) execution, so the cron cadence never stalls.
        Overlap is allowed: a new tick fires even if a previous run of the same
        pipeline is still in flight.
        """
        pipeline_name = schedule.pipeline_name
        schedule_name = schedule.schedule_name
        runtime_params = schedule.runtime_params
        execution_id = f"sched-{schedule_name}-{uuid.uuid4().hex[:8]}"

        log_ctx = {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "schedule_name": schedule_name,
        }
        logger.info(
            "Triggering scheduled pipeline: %s/%s (execution=%s)",
            pipeline_name,
            schedule_name,
            execution_id,
            extra={**log_ctx, "event": "schedule_trigger", "cron": schedule.cron_expression},
        )

        try:
            if not self.pipeline_runner_factory:
                raise RuntimeError("Pipeline runner factory not configured")

            self._create_execution(pipeline_name, execution_id, runtime_params)
            self._dispatch_async(pipeline_name, execution_id, runtime_params)

            schedule.last_execution_id = execution_id
            logger.info(
                "Scheduled execution created and dispatched: %s",
                execution_id,
                extra={**log_ctx, "event": "schedule_dispatched"},
            )

        except Exception:
            logger.error(
                "Failed to trigger scheduled pipeline '%s'",
                pipeline_name,
                exc_info=True,
                extra={**log_ctx, "event": "schedule_trigger_error"},
            )
            # Still advance the schedule to avoid a tight retry loop on persistent errors

        finally:
            # Always advance the timer regardless of trigger success
            schedule.last_triggered_at = now
            schedule.next_run_at = self._compute_next_run(schedule.cron_expression, now)
            db.flush()

    def _create_execution(
        self, pipeline_name: str, execution_id: str, runtime_params: Dict[str, Any]
    ) -> None:
        """Create the execution record synchronously, in its own session."""
        factory = self.pipeline_runner_factory
        if factory is None:
            raise RuntimeError("Pipeline runner factory not configured")
        runner = factory()
        try:
            runner.execution_manager.create_execution(
                execution_id=execution_id,
                pipeline_name=pipeline_name,
                runtime_params=runtime_params,
            )
            runner.execution_manager.db.commit()
        finally:
            runner.execution_manager.db.close()

    def _dispatch_async(
        self, pipeline_name: str, execution_id: str, runtime_params: Dict[str, Any]
    ) -> None:
        """Run the pipeline's jobs on a background daemon thread.

        Mirrors the API's background dispatch: a fresh runner (own DB session)
        runs the existing execution; on failure the execution is marked failed.
        """
        factory = self.pipeline_runner_factory
        if factory is None:
            raise RuntimeError("Pipeline runner factory not configured")

        def _run() -> None:
            runner = factory()
            try:
                runner.run_pipeline_jobs(
                    execution_id=execution_id,
                    pipeline_name=pipeline_name,
                    runtime_params=runtime_params,
                )
            except Exception as e:
                logger.error(
                    "Scheduled dispatch failed for %s",
                    execution_id,
                    exc_info=True,
                    extra={
                        "event": "schedule_dispatch_error",
                        "execution_id": execution_id,
                        "pipeline_name": pipeline_name,
                    },
                )
                try:
                    runner.execution_manager.update_execution_state(
                        execution_id, "failed", error_message=str(e)
                    )
                except Exception:
                    pass
            finally:
                try:
                    runner.execution_manager.db.close()
                except Exception:
                    pass

        threading.Thread(target=_run, daemon=True).start()

    @staticmethod
    def _compute_next_run(cron_expression: str, after: datetime) -> datetime:
        """Return the next scheduled datetime after `after` for the given cron expression."""
        cron = croniter(cron_expression, after)
        return cron.get_next(datetime)

    def sync_schedules_from_registry(self, db: Session) -> None:
        """
        Upsert pipeline_schedules rows from the current pipeline registry.

        Called once on startup after pipelines are loaded. Ensures the DB
        reflects the current set of named schedules across all pipelines:
        - New (pipeline, schedule name) → INSERT with next_run_at from now
        - Changed cron expression → UPDATE expression, recalculate next_run_at
        - Changed params (cron unchanged) → UPDATE params, leave next_run_at
        - Unchanged → leave next_run_at as-is (preserves timer)
        - (pipeline, schedule name) no longer declared → soft-disable
        """
        from reflowfy.core.registry import pipeline_registry

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        all_pipelines = pipeline_registry.list_all()
        scheduled_keys: Set[Tuple[str, str]] = set()

        for pipeline in all_pipelines:
            for run in getattr(pipeline, "schedules", []):
                scheduled_keys.add((pipeline.name, run.name))

                existing = (
                    db.query(PipelineSchedule)
                    .filter(
                        PipelineSchedule.pipeline_name == pipeline.name,
                        PipelineSchedule.schedule_name == run.name,
                    )
                    .first()
                )

                if existing is None:
                    row = PipelineSchedule(
                        pipeline_name=pipeline.name,
                        schedule_name=run.name,
                        cron_expression=run.cron,
                        runtime_params=run.params,
                        next_run_at=self._compute_next_run(run.cron, now),
                        enabled="true",
                    )
                    db.add(row)
                    logger.info(
                        "Registered schedule '%s' for '%s' (%s)",
                        run.name,
                        pipeline.name,
                        run.cron,
                        extra={
                            "event": "schedule_registered",
                            "schedule_name": run.name,
                            "pipeline_name": pipeline.name,
                            "cron": run.cron,
                        },
                    )

                else:
                    # Re-enable if it was previously disabled
                    existing.enabled = "true"

                    if existing.cron_expression != run.cron:
                        # Cron expression changed — recalculate next fire time
                        existing.cron_expression = run.cron
                        existing.next_run_at = self._compute_next_run(run.cron, now)
                        logger.info(
                            "Updated schedule '%s' for '%s' (%s)",
                            run.name,
                            pipeline.name,
                            run.cron,
                            extra={
                                "event": "schedule_updated",
                                "schedule_name": run.name,
                                "pipeline_name": pipeline.name,
                                "cron": run.cron,
                            },
                        )
                    # Else: leave next_run_at unchanged (mid-interval restart case)

                    if existing.runtime_params != run.params:
                        existing.runtime_params = run.params

        # Soft-disable schedules that are no longer declared by any pipeline
        all_schedule_rows = (
            db.query(PipelineSchedule).filter(PipelineSchedule.enabled == "true").all()
        )

        for row in all_schedule_rows:
            if (row.pipeline_name, row.schedule_name) not in scheduled_keys:
                row.enabled = "false"
                logger.info(
                    "Disabled schedule '%s' for '%s' (no longer scheduled)",
                    row.schedule_name,
                    row.pipeline_name,
                    extra={
                        "event": "schedule_disabled",
                        "schedule_name": row.schedule_name,
                        "pipeline_name": row.pipeline_name,
                    },
                )

    def reset_schedule(self, db: Session, pipeline_name: str, triggered_at: datetime) -> None:
        """
        Recalculate next_run_at from triggered_at after a manual trigger.

        A manual trigger isn't tied to one particular named schedule, so this
        pushes out every schedule belonging to the pipeline — preventing the
        scheduler from immediately re-running any of them right after a
        manual run via the API.
        The caller is responsible for committing the session.
        """
        schedules = (
            db.query(PipelineSchedule)
            .filter(
                PipelineSchedule.pipeline_name == pipeline_name,
                PipelineSchedule.enabled == "true",
            )
            .all()
        )

        for schedule in schedules:
            schedule.last_triggered_at = triggered_at
            schedule.next_run_at = self._compute_next_run(schedule.cron_expression, triggered_at)


# Module-level singleton (mirrors dlq_scheduler.py pattern)
_scheduler: Optional[PipelineScheduler] = None


def get_pipeline_scheduler() -> Optional[PipelineScheduler]:
    """Get the global PipelineScheduler instance."""
    return _scheduler


def init_pipeline_scheduler(pipeline_runner_factory: Callable[..., Any]) -> PipelineScheduler:
    """Initialize and start the global PipelineScheduler."""
    global _scheduler
    _scheduler = PipelineScheduler(pipeline_runner_factory=pipeline_runner_factory)
    _scheduler.start()
    return _scheduler


def stop_pipeline_scheduler() -> None:
    """Stop the global PipelineScheduler."""
    global _scheduler
    if _scheduler:
        _scheduler.stop()
        _scheduler = None
