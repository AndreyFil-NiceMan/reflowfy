"""DLQ (Dead Letter Queue) Scheduler for ReflowManager.

Background scheduler that polls for due DLQ jobs and processes them.
"""

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import defaultdict

from sqlalchemy.orm import Session

from reflowfy.observability import metrics
from reflowfy.store.models import DLQJob, DLQJobArchive
from reflowfy.store.states import DLQStatus
from reflowfy.reflow_manager.database import SessionLocal
from reflowfy.reflow_manager.periodic import PeriodicTask
from reflowfy.reflow_manager.start import stage_execution, start_in_background

logger = logging.getLogger(__name__)

# A run staged in the current transaction: (execution_id, pipeline_name, runtime_params).
# Started only once that transaction has committed.
Staged = Tuple[str, str, Dict[str, Any]]

# Configuration from environment variables
DLQ_POLL_INTERVAL_SECONDS = int(os.getenv("DLQ_POLL_INTERVAL_SECONDS", "900"))  # 15 minutes
DLQ_DEFAULT_DELAY_MINUTES = int(os.getenv("DLQ_DEFAULT_DELAY_MINUTES", "60"))  # 1 hour
DLQ_MAX_RETRIES = int(os.getenv("DLQ_MAX_RETRIES", "5"))


class DLQScheduler(PeriodicTask):
    """
    Background scheduler for processing DLQ jobs.

    Polls the database at regular intervals for jobs whose scheduled_at
    time has passed, groups them by pipeline, and creates executions.
    """

    label = "DLQ Scheduler"

    def __init__(
        self,
        poll_interval: int = DLQ_POLL_INTERVAL_SECONDS,
        pipeline_runner_factory: Optional[Callable[..., Any]] = None,
    ):
        """
        Initialize DLQ scheduler.

        Args:
            poll_interval: Seconds between polling cycles
            pipeline_runner_factory: Callable that returns a PipelineRunner instance
        """
        super().__init__()
        self.poll_interval = poll_interval
        self.pipeline_runner_factory = pipeline_runner_factory

    @property
    def interval_seconds(self) -> float:
        return self.poll_interval

    def tick(self) -> None:
        self._poll_and_process()

    def _poll_and_process(self) -> None:
        """Poll for due jobs and process them."""
        db = SessionLocal()
        try:
            # Publish current DLQ depth (all pending jobs) for dashboards.
            metrics.dlq_depth.set(db.query(DLQJob).filter(DLQJob.status == DLQStatus.PENDING).count())

            # Find all pending jobs whose scheduled_at has passed
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            due_jobs = (
                db.query(DLQJob)
                .filter(DLQJob.status == DLQStatus.PENDING, DLQJob.scheduled_at <= now)
                .with_for_update(skip_locked=True)
                .all()
            )

            if not due_jobs:
                return

            logger.info(
                "DLQ Scheduler found %d due jobs",
                len(due_jobs),
                extra={"event": "dlq_due", "jobs_due": len(due_jobs)},
            )
            for j in due_jobs:
                logger.debug(
                    "Job %s: pipeline=%s, scheduled_at=%s, retry=%s",
                    j.id,
                    j.pipeline_name,
                    j.scheduled_at,
                    j.retry_count,
                    extra={
                        "event": "dlq_job_due",
                        "dlq_job_id": j.id,
                        "pipeline_name": j.pipeline_name,
                    },
                )

            jobs_by_pipeline: Dict[str, List[DLQJob]] = defaultdict(list)
            for job in due_jobs:
                jobs_by_pipeline[job.pipeline_name].append(job)

            staged: List[Staged] = []
            for pipeline_name, jobs in jobs_by_pipeline.items():
                staged += self._process_pipeline_jobs(db, pipeline_name, jobs)

            # The executions and the DLQ jobs marked completed are one commit; the runs
            # start after it (see start.stage_execution).
            db.commit()
            self._start(staged)

        except Exception:
            db.rollback()
            logger.error("DLQ poll error", exc_info=True, extra={"event": "dlq_poll_error"})
            raise
        finally:
            db.close()

    def _process_pipeline_jobs(
        self, db: Session, pipeline_name: str, jobs: List[DLQJob]
    ) -> List[Staged]:
        """
        Stage the runs for a batch of DLQ jobs of one pipeline (started after commit).

        Jobs scheduled with the same params share one execution; each distinct set
        of params gets its own, so no job is marked completed without having run
        with its own params.
        """
        logger.info(
            "Processing %d DLQ jobs for pipeline: %s",
            len(jobs),
            pipeline_name,
            extra={
                "event": "dlq_process",
                "pipeline_name": pipeline_name,
                "jobs_due": len(jobs),
            },
        )

        staged: List[Staged] = []
        for group in _group_by_params(jobs):
            try:
                run = self._stage_run(db, pipeline_name, group)
            except Exception as e:
                error_msg = str(e)
                logger.error(
                    "Failed to dispatch DLQ jobs for %s: %s",
                    pipeline_name,
                    error_msg,
                    exc_info=True,
                    extra={
                        "event": "dlq_dispatch_error",
                        "pipeline_name": pipeline_name,
                        "jobs_due": len(group),
                        "trigger": "scheduler",
                    },
                )
                for job in group:
                    self._handle_job_failure(db, job, error_msg)
                continue

            staged.append(run)
            execution_id = run[0]
            _mark_completed(group, execution_id)
            logger.info(
                "DLQ jobs dispatched to execution: %s",
                execution_id,
                extra={
                    "event": "dlq_dispatched",
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "jobs_due": len(group),
                    "trigger": "scheduler",
                },
            )
        return staged

    def _stage_run(self, db: Session, pipeline_name: str, jobs: List[DLQJob]) -> Staged:
        """
        Add the execution for a group of DLQ jobs to ``db``'s transaction.

        A missing pipeline or bad params raise here, so the jobs take the
        retry/archive path. Nothing runs until the caller commits and calls
        :meth:`_start`.
        """
        if not self.pipeline_runner_factory:
            raise RuntimeError("Pipeline runner factory not configured")

        execution_id = f"dlq-{uuid.uuid4().hex[:12]}"

        # For DLQ, the job_payload IS the runtime_params. Callers pass a group whose
        # jobs all have the same payload (_group_by_params).
        runtime_params = jobs[0].job_payload if jobs else {}

        # Add DLQ metadata to runtime params
        runtime_params = {
            **runtime_params,
            "_dlq_source": True,
            "_dlq_job_ids": [job.id for job in jobs],
        }

        stage_execution(db, execution_id, pipeline_name, runtime_params)
        return execution_id, pipeline_name, runtime_params

    def _start(self, staged: List[Staged]) -> None:
        """Start committed runs, each on a thread of its own: driven inline a run would
        hold the poll for as long as it ran. A run that fails later ends its execution
        ``failed``; the DLQ job has done its work (started it)."""
        factory = self.pipeline_runner_factory
        if factory is None:  # _stage_run refused already; nothing was staged
            return
        for execution_id, pipeline_name, runtime_params in staged:
            start_in_background(factory, execution_id, pipeline_name, runtime_params, log=logger)

    def _handle_job_failure(self, db: Session, job: DLQJob, error_msg: str) -> None:
        """Handle a failed DLQ job - retry or archive."""
        job.retry_count += 1
        job.error_message = error_msg

        if job.retry_count >= job.max_retries:
            self._archive_job(db, job)
            logger.info(
                "DLQ job %s archived after %d retries",
                job.id,
                job.retry_count,
                extra={
                    "event": "dlq_archived",
                    "dlq_job_id": job.id,
                    "pipeline_name": job.pipeline_name,
                    "retry_count": job.retry_count,
                },
            )
        else:
            # Reschedule with exponential backoff
            backoff_minutes = self._calculate_backoff(job.retry_count)
            job.scheduled_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
                minutes=backoff_minutes
            )
            job.status = DLQStatus.PENDING
            logger.info(
                "DLQ job %s rescheduled (retry %d/%d)",
                job.id,
                job.retry_count,
                job.max_retries,
                extra={
                    "event": "dlq_rescheduled",
                    "dlq_job_id": job.id,
                    "pipeline_name": job.pipeline_name,
                    "retry_count": job.retry_count,
                    "backoff_minutes": backoff_minutes,
                },
            )

    def _calculate_backoff(self, retry_count: int) -> int:
        """
        Calculate exponential backoff in minutes.

        Backoff: 5, 10, 20, 40, 80 minutes for retries 1-5
        """
        return int(5 * 2 ** (retry_count - 1))

    def _archive_job(self, db: Session, job: DLQJob) -> None:
        """Move a permanently failed job to the archive table."""
        archive = DLQJobArchive(
            id=job.id,
            job_payload=job.job_payload,
            pipeline_name=job.pipeline_name,
            delay_minutes=job.delay_minutes,
            retry_count=job.retry_count,
            max_retries=job.max_retries,
            error_message=job.error_message,
            created_at=job.created_at,
            archived_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        db.add(archive)
        db.delete(job)

    def process_job_immediately(self, db: Session, job_id: int) -> Optional[str]:
        """
        Process a specific DLQ job immediately (on-demand dispatch).

        Args:
            db: Database session
            job_id: ID of the DLQ job to dispatch

        Returns:
            execution_id if successful, None otherwise
        """
        # Lock the row to prevent concurrent processing by another RM pod
        job = db.query(DLQJob).filter(DLQJob.id == job_id).with_for_update().first()
        if not job:
            return None

        if job.status != DLQStatus.PENDING:
            raise ValueError(f"Job {job_id} is not pending (status: {job.status})")

        pipeline_name = job.pipeline_name
        try:
            run = self._stage_run(db, pipeline_name, [job])
            _mark_completed([job], run[0])
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(
                "Failed to dispatch DLQ job: %s",
                e,
                exc_info=True,
                extra={
                    "event": "dlq_dispatch_error",
                    "pipeline_name": pipeline_name,
                    "dlq_job_id": job_id,
                    "trigger": "manual",
                },
            )
            # The rollback released the row: take it again before charging a retry.
            locked = db.query(DLQJob).filter(DLQJob.id == job_id).with_for_update().first()
            if locked is not None and locked.status == DLQStatus.PENDING:
                self._handle_job_failure(db, locked, str(e))
            db.commit()
            raise
        self._start([run])
        execution_id = run[0]
        logger.info(
            "DLQ job dispatched to execution: %s",
            execution_id,
            extra={
                "event": "dlq_dispatched",
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "dlq_job_id": job_id,
                "jobs_due": 1,
                "trigger": "manual",
            },
        )
        return execution_id

    def process_pipeline_immediately(
        self, db: Session, pipeline_name: str
    ) -> tuple[int, List[str]]:
        """
        Process all pending DLQ jobs for a pipeline immediately.

        Args:
            db: Database session
            pipeline_name: Pipeline to dispatch jobs for

        Returns:
            Tuple of (count of jobs dispatched, execution_ids) — one execution per
            distinct set of params. A group that fails goes to the retry path and
            the rest still run; the error is raised only if no group ran.
        """
        # Lock rows to prevent concurrent processing by another RM pod
        jobs = (
            db.query(DLQJob)
            .filter(DLQJob.pipeline_name == pipeline_name, DLQJob.status == DLQStatus.PENDING)
            .with_for_update(skip_locked=True)
            .all()
        )

        if not jobs:
            return 0, []

        execution_ids: List[str] = []
        staged: List[Staged] = []
        last_error: Optional[Exception] = None
        for group in _group_by_params(jobs):
            try:
                run = self._stage_run(db, pipeline_name, group)
            except Exception as e:
                # Only this group's jobs go to the retry path; the others still run,
                # as in _process_pipeline_jobs.
                last_error = e
                logger.error(
                    "Failed to dispatch DLQ jobs for %s: %s",
                    pipeline_name,
                    e,
                    exc_info=True,
                    extra={
                        "event": "dlq_dispatch_error",
                        "pipeline_name": pipeline_name,
                        "jobs_due": len(group),
                        "trigger": "manual",
                    },
                )
                for job in group:
                    self._handle_job_failure(db, job, str(e))
                continue
            staged.append(run)
            execution_id = run[0]
            _mark_completed(group, execution_id)
            execution_ids.append(execution_id)
            logger.info(
                "DLQ jobs dispatched to execution: %s",
                execution_id,
                extra={
                    "event": "dlq_dispatched",
                    "execution_id": execution_id,
                    "pipeline_name": pipeline_name,
                    "jobs_due": len(group),
                    "trigger": "manual",
                },
            )
        db.commit()
        self._start(staged)
        if last_error is not None and not execution_ids:
            raise last_error
        dispatched = sum(1 for job in jobs if job.status == DLQStatus.COMPLETED)
        return dispatched, execution_ids


def _group_by_params(jobs: List[DLQJob]) -> List[List[DLQJob]]:
    """Jobs with identical params, in first-seen order."""
    groups: Dict[str, List[DLQJob]] = {}
    for job in jobs:
        key = json.dumps(job.job_payload or {}, sort_keys=True, default=str)
        groups.setdefault(key, []).append(job)
    return list(groups.values())


def _mark_completed(jobs: List[DLQJob], execution_id: str) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for job in jobs:
        job.status = DLQStatus.COMPLETED
        job.processed_at = now
        job.execution_id = execution_id


# Global scheduler instance
_scheduler: Optional[DLQScheduler] = None


def get_dlq_scheduler() -> Optional[DLQScheduler]:
    """Get the global DLQ scheduler instance."""
    return _scheduler


def init_dlq_scheduler(pipeline_runner_factory: Callable[..., Any]) -> DLQScheduler:
    """Initialize and start the global DLQ scheduler."""
    global _scheduler
    _scheduler = DLQScheduler(pipeline_runner_factory=pipeline_runner_factory)
    _scheduler.start()
    return _scheduler


def stop_dlq_scheduler() -> None:
    """Stop the global DLQ scheduler."""
    global _scheduler
    if _scheduler:
        _scheduler.stop()
        _scheduler = None
