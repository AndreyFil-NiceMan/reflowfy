"""DLQ (Dead Letter Queue) Scheduler for ReflowManager.

Background scheduler that polls for due DLQ jobs and processes them.
"""

import os
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict

from sqlalchemy.orm import Session

from reflowfy.reflow_manager.models import DLQJob, DLQJobArchive
from reflowfy.reflow_manager.database import SessionLocal


# Configuration from environment variables
DLQ_POLL_INTERVAL_SECONDS = int(os.getenv("DLQ_POLL_INTERVAL_SECONDS", "900"))  # 15 minutes
DLQ_DEFAULT_DELAY_MINUTES = int(os.getenv("DLQ_DEFAULT_DELAY_MINUTES", "60"))  # 1 hour
DLQ_MAX_RETRIES = int(os.getenv("DLQ_MAX_RETRIES", "5"))


class DLQScheduler:
    """
    Background scheduler for processing DLQ jobs.
    
    Polls the database at regular intervals for jobs whose scheduled_at
    time has passed, groups them by pipeline, and creates executions.
    """
    
    def __init__(
        self,
        poll_interval: int = DLQ_POLL_INTERVAL_SECONDS,
        pipeline_runner_factory: Optional[callable] = None,
    ):
        """
        Initialize DLQ scheduler.
        
        Args:
            poll_interval: Seconds between polling cycles
            pipeline_runner_factory: Callable that returns a PipelineRunner instance
        """
        self.poll_interval = poll_interval
        self.pipeline_runner_factory = pipeline_runner_factory
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
    
    def start(self):
        """Start the background polling thread."""
        if self._running:
            print("⚠️ DLQ Scheduler already running")
            return
        
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"✅ DLQ Scheduler started (polling every {self.poll_interval}s)")
    
    def stop(self):
        """Stop the scheduler gracefully."""
        if not self._running:
            return
        
        print("🛑 Stopping DLQ Scheduler...")
        self._running = False
        self._stop_event.set()
        
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None
        
        print("✅ DLQ Scheduler stopped")
    
    def _run_loop(self):
        """Main polling loop."""
        while self._running:
            try:
                self._poll_and_process()
            except Exception as e:
                print(f"❌ DLQ Scheduler error: {e}")
            
            # Wait for poll interval or stop event
            self._stop_event.wait(timeout=self.poll_interval)
    
    def _poll_and_process(self):
        """Poll for due jobs and process them."""
        db = SessionLocal()
        try:
            # Find all pending jobs whose scheduled_at has passed
            # Use FOR UPDATE SKIP LOCKED to prevent multiple RM pods from
            # processing the same job (allows concurrent processing of different jobs)
            now = datetime.utcnow()
            due_jobs = db.query(DLQJob).filter(
                DLQJob.status == "pending",
                DLQJob.scheduled_at <= now
            ).with_for_update(skip_locked=True).all()
            
            if not due_jobs:
                return
            
            print(f"📋 DLQ Scheduler found {len(due_jobs)} due jobs")
            
            # Group jobs by pipeline
            jobs_by_pipeline: Dict[str, List[DLQJob]] = defaultdict(list)
            for job in due_jobs:
                jobs_by_pipeline[job.pipeline_name].append(job)
            
            # Process each pipeline group
            for pipeline_name, jobs in jobs_by_pipeline.items():
                self._process_pipeline_jobs(db, pipeline_name, jobs)
            
            db.commit()
            
        except Exception as e:
            db.rollback()
            print(f"❌ DLQ poll error: {e}")
            raise
        finally:
            db.close()
    
    def _process_pipeline_jobs(
        self,
        db: Session,
        pipeline_name: str,
        jobs: List[DLQJob]
    ):
        """
        Process a batch of DLQ jobs for a single pipeline.
        
        Creates a new execution and dispatches all jobs.
        """
        print(f"🚀 Processing {len(jobs)} DLQ jobs for pipeline: {pipeline_name}")
        
        # Mark jobs as processing
        job_ids = [job.id for job in jobs]
        for job in jobs:
            job.status = "processing"
        db.flush()
        
        try:
            # Create execution and dispatch jobs
            execution_id = self._dispatch_jobs(db, pipeline_name, jobs)
            
            # Mark jobs as completed
            now = datetime.utcnow()
            for job in jobs:
                job.status = "completed"
                job.processed_at = now
                job.execution_id = execution_id
            
            print(f"✅ DLQ jobs dispatched to execution: {execution_id}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Failed to dispatch DLQ jobs: {error_msg}")
            
            # Handle retries
            for job in jobs:
                self._handle_job_failure(db, job, error_msg)
    
    def _dispatch_jobs(
        self,
        db: Session,
        pipeline_name: str,
        jobs: List[DLQJob]
    ) -> str:
        """
        Dispatch DLQ jobs by creating an execution.
        
        Returns:
            execution_id of the created execution
        """
        if not self.pipeline_runner_factory:
            raise RuntimeError("Pipeline runner factory not configured")
        
        # Generate execution ID
        execution_id = f"dlq-{uuid.uuid4().hex[:12]}"
        
        # For DLQ, the job_payload IS the runtime_params
        # For single-job dispatch (most common), use that job's payload
        # For batch dispatch, use the first job's payload (they should have compatible params)
        runtime_params = jobs[0].job_payload if jobs else {}
        
        # Add DLQ metadata to runtime params
        runtime_params = {
            **runtime_params,
            "_dlq_source": True,
            "_dlq_job_ids": [job.id for job in jobs],
        }
        
        # Get pipeline runner and run
        runner = self.pipeline_runner_factory()
        runner.run_pipeline(
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            execution_id=execution_id,
        )
        
        return execution_id
    
    def _handle_job_failure(self, db: Session, job: DLQJob, error_msg: str):
        """Handle a failed DLQ job - retry or archive."""
        job.retry_count += 1
        job.error_message = error_msg
        
        if job.retry_count >= job.max_retries:
            # Move to archive
            self._archive_job(db, job)
            print(f"📦 DLQ job {job.id} archived after {job.retry_count} retries")
        else:
            # Reschedule with exponential backoff
            backoff_minutes = self._calculate_backoff(job.retry_count)
            job.scheduled_at = datetime.utcnow() + timedelta(minutes=backoff_minutes)
            job.status = "pending"
            print(f"🔄 DLQ job {job.id} rescheduled (retry {job.retry_count}/{job.max_retries})")
    
    def _calculate_backoff(self, retry_count: int) -> int:
        """
        Calculate exponential backoff in minutes.
        
        Backoff: 5, 10, 20, 40, 80 minutes for retries 1-5
        """
        return 5 * (2 ** (retry_count - 1))
    
    def _archive_job(self, db: Session, job: DLQJob):
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
            archived_at=datetime.utcnow(),
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
        
        if job.status != "pending":
            raise ValueError(f"Job {job_id} is not pending (status: {job.status})")
        
        job.status = "processing"
        db.flush()
        
        try:
            execution_id = self._dispatch_jobs(db, job.pipeline_name, [job])
            job.status = "completed"
            job.processed_at = datetime.utcnow()
            job.execution_id = execution_id
            db.commit()
            return execution_id
        except Exception as e:
            db.rollback()
            self._handle_job_failure(db, job, str(e))
            db.commit()
            raise
    
    def process_pipeline_immediately(
        self,
        db: Session,
        pipeline_name: str
    ) -> tuple[int, Optional[str]]:
        """
        Process all pending DLQ jobs for a pipeline immediately.
        
        Args:
            db: Database session
            pipeline_name: Pipeline to dispatch jobs for
            
        Returns:
            Tuple of (count of jobs dispatched, execution_id)
        """
        # Lock rows to prevent concurrent processing by another RM pod
        jobs = db.query(DLQJob).filter(
            DLQJob.pipeline_name == pipeline_name,
            DLQJob.status == "pending"
        ).with_for_update(skip_locked=True).all()
        
        if not jobs:
            return 0, None
        
        for job in jobs:
            job.status = "processing"
        db.flush()
        
        try:
            execution_id = self._dispatch_jobs(db, pipeline_name, jobs)
            now = datetime.utcnow()
            for job in jobs:
                job.status = "completed"
                job.processed_at = now
                job.execution_id = execution_id
            db.commit()
            return len(jobs), execution_id
        except Exception as e:
            db.rollback()
            for job in jobs:
                self._handle_job_failure(db, job, str(e))
            db.commit()
            raise


# Global scheduler instance
_scheduler: Optional[DLQScheduler] = None


def get_dlq_scheduler() -> Optional[DLQScheduler]:
    """Get the global DLQ scheduler instance."""
    return _scheduler


def init_dlq_scheduler(pipeline_runner_factory: callable) -> DLQScheduler:
    """Initialize and start the global DLQ scheduler."""
    global _scheduler
    _scheduler = DLQScheduler(pipeline_runner_factory=pipeline_runner_factory)
    _scheduler.start()
    return _scheduler


def stop_dlq_scheduler():
    """Stop the global DLQ scheduler."""
    global _scheduler
    if _scheduler:
        _scheduler.stop()
        _scheduler = None
