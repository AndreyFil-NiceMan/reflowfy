"""Pipeline cron scheduler for ReflowManager.

Background scheduler that polls pipeline_schedules and fires executions
when a pipeline's next_run_at has elapsed.
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from croniter import croniter
from sqlalchemy.orm import Session

from reflowfy.store.models import PipelineSchedule
from reflowfy.store.states import ScheduleEnabled
from reflowfy.reflow_manager.database import SessionLocal
from reflowfy.reflow_manager.start import stage_execution, start_in_background
from reflowfy.reflow_manager.periodic import PeriodicTask

logger = logging.getLogger(__name__)


PIPELINE_SCHEDULER_POLL_INTERVAL = int(os.getenv("PIPELINE_SCHEDULER_POLL_INTERVAL_SECONDS", "30"))


class PipelineScheduler(PeriodicTask):
    """
    Background scheduler for cron-based pipeline execution.

    Polls the database at regular intervals for pipeline_schedules rows
    whose next_run_at has elapsed, then fires a new execution for each.
    """

    label = "Pipeline Scheduler"

    def __init__(
        self,
        poll_interval: int = PIPELINE_SCHEDULER_POLL_INTERVAL,
        pipeline_runner_factory: Optional[Callable[..., Any]] = None,
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.pipeline_runner_factory = pipeline_runner_factory

    @property
    def interval_seconds(self) -> float:
        return self.poll_interval

    def tick(self) -> None:
        self._poll_and_trigger()

    def _poll_and_trigger(self) -> None:
        """Poll for due scheduled pipelines and trigger executions."""
        db = SessionLocal()
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            due = (
                db.query(PipelineSchedule)
                .filter(
                    PipelineSchedule.enabled == ScheduleEnabled.TRUE,
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

            staged: List[Tuple[str, str, Dict[str, Any]]] = []
            for schedule in due:
                run = self._trigger_pipeline(db, schedule, now)
                if run is not None:
                    staged.append(run)

            # The executions and the advanced timers are one commit; the runs start
            # after it, so a crash or failed commit in between cannot leave a run going
            # while its schedule still looks due (a second run on the next poll).
            db.commit()
            factory = self.pipeline_runner_factory
            if factory is not None:
                for execution_id, pipeline_name, runtime_params in staged:
                    start_in_background(
                        factory, execution_id, pipeline_name, runtime_params, log=logger
                    )

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

    def _trigger_pipeline(
        self, db: Session, schedule: PipelineSchedule, now: datetime
    ) -> Optional[Tuple[str, str, Dict[str, Any]]]:
        """Stage an execution for a due scheduled pipeline and advance next_run_at.

        Returns ``(execution_id, pipeline_name, runtime_params)`` for the caller to
        start once the poll has committed, or None when the run could not be staged.
        The run itself goes on a background thread, so a long-running (or hung)
        execution never stalls the cron cadence. Overlap is allowed: a new tick fires
        even if a previous run of the same pipeline is still in flight.
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

        run: Optional[Tuple[str, str, Dict[str, Any]]] = None
        try:
            if not self.pipeline_runner_factory:
                raise RuntimeError("Pipeline runner factory not configured")

            # What can never run is refused here, as /run does, not left to fail later.
            stage_execution(db, execution_id, pipeline_name, runtime_params)
            run = (execution_id, pipeline_name, runtime_params)

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
        return run

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
                        enabled=ScheduleEnabled.TRUE,
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
                    existing.enabled = ScheduleEnabled.TRUE

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
            db.query(PipelineSchedule).filter(PipelineSchedule.enabled == ScheduleEnabled.TRUE).all()
        )

        for row in all_schedule_rows:
            if (row.pipeline_name, row.schedule_name) not in scheduled_keys:
                row.enabled = ScheduleEnabled.FALSE
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
                PipelineSchedule.enabled == ScheduleEnabled.TRUE,
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
