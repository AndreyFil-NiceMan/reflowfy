"""Pipeline cron scheduler for ReflowManager.

Background scheduler that polls pipeline_schedules and fires executions
when a pipeline's next_run_at has elapsed.
"""

import os
import threading
import traceback
import uuid
from datetime import datetime
from typing import Optional

from croniter import croniter
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.models import PipelineSchedule
from reflowfy.reflow_manager.database import SessionLocal


PIPELINE_SCHEDULER_POLL_INTERVAL = int(
    os.getenv("PIPELINE_SCHEDULER_POLL_INTERVAL_SECONDS", "30")
)


class PipelineScheduler:
    """
    Background scheduler for cron-based pipeline execution.

    Polls the database at regular intervals for pipeline_schedules rows
    whose next_run_at has elapsed, then fires a new execution for each.
    """

    def __init__(
        self,
        poll_interval: int = PIPELINE_SCHEDULER_POLL_INTERVAL,
        pipeline_runner_factory: Optional[callable] = None,
    ):
        self.poll_interval = poll_interval
        self.pipeline_runner_factory = pipeline_runner_factory
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the background polling thread."""
        if self._running:
            print("⚠️ Pipeline Scheduler already running")
            return

        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"✅ Pipeline Scheduler started (polling every {self.poll_interval}s)")

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if not self._running:
            return

        print("🛑 Stopping Pipeline Scheduler...")
        self._running = False
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

        print("✅ Pipeline Scheduler stopped")

    def _run_loop(self) -> None:
        """Main polling loop."""
        while self._running:
            try:
                self._poll_and_trigger()
            except Exception as e:
                print(f"❌ Pipeline Scheduler error: {e}")

            self._stop_event.wait(timeout=self.poll_interval)

    def _poll_and_trigger(self) -> None:
        """Poll for due scheduled pipelines and trigger executions."""
        db = SessionLocal()
        try:
            now = datetime.utcnow()
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

            print(f"⏰ Pipeline Scheduler found {len(due)} due pipeline(s)")

            for schedule in due:
                self._trigger_pipeline(db, schedule, now)

            db.commit()

        except Exception as e:
            db.rollback()
            print(f"❌ Pipeline Scheduler poll error: {e}")
            raise
        finally:
            db.close()

    def _trigger_pipeline(self, db: Session, schedule: PipelineSchedule, now: datetime) -> None:
        """Fire an execution for a due scheduled pipeline and advance next_run_at."""
        pipeline_name = schedule.pipeline_name
        execution_id = f"sched-{uuid.uuid4().hex[:12]}"

        print(f"🚀 Triggering scheduled pipeline: {pipeline_name} (execution={execution_id})")

        try:
            if not self.pipeline_runner_factory:
                raise RuntimeError("Pipeline runner factory not configured")

            runner = self.pipeline_runner_factory()
            runner.run_pipeline(
                pipeline_name=pipeline_name,
                runtime_params={},
                execution_id=execution_id,
            )

            schedule.last_execution_id = execution_id
            print(f"✅ Scheduled execution created: {execution_id}")

        except Exception as e:
            print(f"❌ Failed to trigger scheduled pipeline '{pipeline_name}': {e}")
            traceback.print_exc()
            # Still advance the schedule to avoid a tight retry loop on persistent errors

        finally:
            # Always advance the timer regardless of execution success
            schedule.last_triggered_at = now
            schedule.next_run_at = self._compute_next_run(schedule.cron_expression, now)
            db.flush()

    @staticmethod
    def _compute_next_run(cron_expression: str, after: datetime) -> datetime:
        """Return the next scheduled datetime after `after` for the given cron expression."""
        cron = croniter(cron_expression, after)
        return cron.get_next(datetime)

    def sync_schedules_from_registry(self, db: Session) -> None:
        """
        Upsert pipeline_schedules rows from the current pipeline registry.

        Called once on startup after pipelines are loaded. Ensures the DB
        reflects the current set of scheduled pipelines:
        - New scheduled pipelines → INSERT with next_run_at calculated from now
        - Changed cron expression → UPDATE expression, recalculate next_run_at
        - Unchanged cron expression → leave next_run_at as-is (preserves timer)
        - Pipeline no longer scheduled → soft-disable (enabled = 'false')
        """
        from reflowfy.core.registry import pipeline_registry

        now = datetime.utcnow()
        all_pipelines = pipeline_registry.list_all()
        scheduled_names = set()

        for pipeline in all_pipelines:
            if not getattr(pipeline, "is_scheduled", False):
                continue

            scheduled_names.add(pipeline.name)
            expr = pipeline.schedule

            existing = db.query(PipelineSchedule).filter(
                PipelineSchedule.pipeline_name == pipeline.name
            ).first()

            if existing is None:
                row = PipelineSchedule(
                    pipeline_name=pipeline.name,
                    cron_expression=expr,
                    next_run_at=self._compute_next_run(expr, now),
                    enabled="true",
                )
                db.add(row)
                print(f"  ✓ Registered schedule for '{pipeline.name}' ({expr})")

            else:
                # Re-enable if it was previously disabled
                existing.enabled = "true"

                if existing.cron_expression != expr:
                    # Cron expression changed — recalculate next fire time
                    existing.cron_expression = expr
                    existing.next_run_at = self._compute_next_run(expr, now)
                    print(f"  ✓ Updated schedule for '{pipeline.name}' ({expr})")
                # Else: leave next_run_at unchanged (mid-interval restart case)

        # Soft-disable schedules for pipelines that no longer have schedule set
        all_schedule_rows = db.query(PipelineSchedule).filter(
            PipelineSchedule.enabled == "true"
        ).all()

        for row in all_schedule_rows:
            if row.pipeline_name not in scheduled_names:
                row.enabled = "false"
                print(f"  ✓ Disabled schedule for '{row.pipeline_name}' (no longer scheduled)")

    def reset_schedule(self, db: Session, pipeline_name: str, triggered_at: datetime) -> None:
        """
        Recalculate next_run_at from triggered_at after a manual trigger.

        Prevents the scheduler from immediately re-running a pipeline that
        was just triggered manually via the API.
        The caller is responsible for committing the session.
        """
        schedule = db.query(PipelineSchedule).filter(
            PipelineSchedule.pipeline_name == pipeline_name
        ).first()

        if schedule and schedule.enabled == "true":
            schedule.last_triggered_at = triggered_at
            schedule.next_run_at = self._compute_next_run(schedule.cron_expression, triggered_at)


# Module-level singleton (mirrors dlq_scheduler.py pattern)
_scheduler: Optional[PipelineScheduler] = None


def get_pipeline_scheduler() -> Optional[PipelineScheduler]:
    """Get the global PipelineScheduler instance."""
    return _scheduler


def init_pipeline_scheduler(pipeline_runner_factory: callable) -> PipelineScheduler:
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
