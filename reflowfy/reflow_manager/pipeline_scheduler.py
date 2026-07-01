"""Pipeline cron scheduler for ReflowManager.

Background scheduler that polls pipeline_schedules and fires executions
when a pipeline's next_run_at has elapsed.
"""

import os
import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

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
        """Fire an execution for a due scheduled pipeline and advance next_run_at.

        The execution record is created synchronously so it exists immediately
        and the schedule timer can advance, but the actual job split/dispatch
        runs on a background thread. This keeps the poll loop from blocking on
        a long-running (or hung) execution, so the cron cadence never stalls.
        Overlap is allowed: a new tick fires even if a previous run of the same
        pipeline is still in flight.
        """
        pipeline_name = schedule.pipeline_name
        execution_id = f"sched-{uuid.uuid4().hex[:12]}"

        print(f"🚀 Triggering scheduled pipeline: {pipeline_name} (execution={execution_id})")

        try:
            if not self.pipeline_runner_factory:
                raise RuntimeError("Pipeline runner factory not configured")

            self._create_execution(pipeline_name, execution_id)
            self._dispatch_async(pipeline_name, execution_id)

            schedule.last_execution_id = execution_id
            print(f"✅ Scheduled execution created and dispatched: {execution_id}")

        except Exception as e:
            print(f"❌ Failed to trigger scheduled pipeline '{pipeline_name}': {e}")
            traceback.print_exc()
            # Still advance the schedule to avoid a tight retry loop on persistent errors

        finally:
            # Always advance the timer regardless of trigger success
            schedule.last_triggered_at = now
            schedule.next_run_at = self._compute_next_run(schedule.cron_expression, now)
            db.flush()

    def _create_execution(self, pipeline_name: str, execution_id: str) -> None:
        """Create the execution record synchronously, in its own session."""
        factory = self.pipeline_runner_factory
        if factory is None:
            raise RuntimeError("Pipeline runner factory not configured")
        runner = factory()
        try:
            runner.execution_manager.create_execution(
                execution_id=execution_id,
                pipeline_name=pipeline_name,
                runtime_params={},
            )
            runner.execution_manager.db.commit()
        finally:
            runner.execution_manager.db.close()

    def _dispatch_async(self, pipeline_name: str, execution_id: str) -> None:
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
                runner._run_pipeline_jobs(
                    execution_id=execution_id,
                    pipeline_name=pipeline_name,
                    runtime_params={},
                )
            except Exception as e:
                print(f"❌ Scheduled dispatch failed for {execution_id}: {e}")
                traceback.print_exc()
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
        reflects the current set of scheduled pipelines:
        - New scheduled pipelines → INSERT with next_run_at calculated from now
        - Changed cron expression → UPDATE expression, recalculate next_run_at
        - Unchanged cron expression → leave next_run_at as-is (preserves timer)
        - Pipeline no longer scheduled → soft-disable (enabled = 'false')
        """
        from reflowfy.core.registry import pipeline_registry

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        all_pipelines = pipeline_registry.list_all()
        scheduled_names = set()

        for pipeline in all_pipelines:
            if not getattr(pipeline, "is_scheduled", False):
                continue

            scheduled_names.add(pipeline.name)
            expr = pipeline.schedule
            assert expr is not None  # guaranteed by is_scheduled guard above

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
