"""Job management for ReflowManager."""

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, insert
from reflowfy.reflow_manager.models import Job


class JobManager:
    """Manages job records for pipeline executions."""

    def __init__(self, db_session: Session):
        self.db = db_session

    def create_jobs(self, rows: List[Dict[str, Any]]) -> None:
        """
        Insert pending job records in one statement.

        Args:
            rows: One dict per job with keys ``execution_id``, ``job_id``,
                ``job_payload`` and ``batch_number``. ``state`` and the
                timestamps are filled in here.

        # ponytail: one INSERT + one commit per call. Inserting jobs one at a
        # time cost a round trip each, which dominated planning time once an
        # execution had more jobs than the network could commit per second.
        """
        if not rows:
            return

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.db.execute(
            insert(Job),
            [{**row, "state": "pending", "created_at": now, "updated_at": now} for row in rows],
        )
        self.db.commit()

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by job_id."""
        return self.db.query(Job).filter(Job.job_id == job_id).first()

    def get_pending_jobs(self, execution_id: str, limit: int) -> List[Job]:
        """Next pending jobs to dispatch, oldest planned first."""
        return (
            self.db.query(Job)
            .filter(Job.execution_id == execution_id, Job.state == "pending")
            .order_by(Job.batch_number, Job.created_at)
            .limit(limit)
            .all()
        )

    def get_jobs(
        self,
        execution_id: str,
        state: Optional[str] = None,
    ) -> List[Job]:
        """
        Get jobs for an execution.

        Args:
            execution_id: Execution identifier
            state: Optional state filter

        Returns:
            List of Job objects
        """
        query = self.db.query(Job).filter(
            Job.execution_id == execution_id
        )

        if state:
            query = query.filter(Job.state == state)

        return query.order_by(Job.created_at).all()

    def mark_job_dispatched(self, job_id: str) -> None:
        """Record that one job's message was sent, right after the send.

        Only a pending job becomes dispatched: a fast worker may already have
        claimed it (running) or finished it. dispatched_at is the send time of the
        latest dispatch, which the dispatch lease is measured from.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.db.query(Job).filter(Job.job_id == job_id).update(
            {"dispatched_at": now}, synchronize_session=False
        )
        self.db.query(Job).filter(Job.job_id == job_id, Job.state == "pending").update(
            {"state": "dispatched", "updated_at": now}, synchronize_session=False
        )
        self.db.commit()

    def reclaim_expired_leases(
        self,
        execution_id: str,
        running_lease_seconds: float,
        dispatch_lease_seconds: float,
        max_attempts: int,
    ) -> Dict[str, int]:
        """Put jobs whose worker or message went missing back in the queue.

        - running for longer than the running lease: the worker died mid-job
          (killed, OOM). Back to pending, or failed once it has been started
          ``max_attempts`` times.
        - dispatched for longer than the dispatch lease: no worker ever picked
          the message up (lost). Back to pending to be sent again.

        A duplicate message this creates is harmless: the worker claims a job
        (pending/dispatched -> running) before running it and skips any other.

        Returns:
            Counts of jobs ``failed``, ``requeued_running`` and ``requeued_dispatched``.
        """
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        running_cutoff = now - timedelta(seconds=running_lease_seconds)
        dispatch_cutoff = now - timedelta(seconds=dispatch_lease_seconds)
        stale_running = (
            Job.execution_id == execution_id,
            Job.state == "running",
            Job.updated_at < running_cutoff,
        )

        failed = (
            self.db.query(Job)
            .filter(*stale_running, Job.attempts >= max_attempts)
            .update(
                {
                    "state": "failed",
                    "error_message": (
                        f"Worker lost: no result within {running_lease_seconds:.0f}s, "
                        f"{max_attempts} attempt(s)"
                    ),
                    "completed_at": now,
                    "updated_at": now,
                },
                synchronize_session=False,
            )
        )
        requeued_running = (
            self.db.query(Job)
            .filter(*stale_running)
            .update({"state": "pending", "updated_at": now}, synchronize_session=False)
        )
        requeued_dispatched = (
            self.db.query(Job)
            .filter(
                Job.execution_id == execution_id,
                Job.state == "dispatched",
                Job.dispatched_at < dispatch_cutoff,
            )
            .update({"state": "pending", "updated_at": now}, synchronize_session=False)
        )
        self.db.commit()
        return {
            "failed": failed,
            "requeued_running": requeued_running,
            "requeued_dispatched": requeued_dispatched,
        }

    def update_job_state(
        self,
        job_id: str,
        state: str,
        processed_records: Optional[int] = None,
        error_message: Optional[str] = None,
        stats: Optional[Dict[str, Any]] = None,
    ) -> Optional[Job]:
        """
        Update job state.

        Commits immediately to ensure the update is persisted.
        """
        update_data: Dict[str, Any] = {
            "state": state,
            "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
        }

        if state in ["completed", "failed"]:
            update_data["completed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        if processed_records is not None:
            update_data["processed_records"] = processed_records
        if error_message:
            update_data["error_message"] = error_message
        if stats:
            update_data["stats"] = stats

        updated = self.db.query(Job).filter(
            Job.job_id == job_id
        ).update(update_data, synchronize_session=False)  # pyright: ignore[reportArgumentType]

        self.db.commit()

        if updated == 0:
            return None

        return self.get_job(job_id)

    def get_job_states(self, job_ids: List[str]) -> Dict[str, str]:
        """
        Get states for multiple jobs efficiently.

        Note: Expires session cache first to see external updates from workers
        who update jobs from their own database sessions.

        Args:
            job_ids: List of job identifiers

        Returns:
            Dictionary mapping job_id to state
        """
        # Expire cached objects to see external updates from workers
        self.db.expire_all()

        results = self.db.query(Job.job_id, Job.state).filter(
            Job.job_id.in_(job_ids)
        ).all()

        return {job_id: state for job_id, state in results}

    def get_job_counts(self, execution_id: str) -> Dict[str, int]:
        """
        Get job counts by state for an execution.

        Note: Expires session cache first to see external updates from workers.

        Returns:
            Dictionary with total, pending, dispatched, running, completed, failed,
            deduplicated counts
        """
        # Expire cached objects to see external updates from workers
        self.db.expire_all()

        rows = self.db.query(Job.state, func.count(Job.job_id)).filter(
            Job.execution_id == execution_id
        ).group_by(Job.state).all()

        counts = {
            "total": 0,
            "pending": 0,
            "dispatched": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "deduplicated": 0,
        }

        total = 0
        for state, count in rows:
            if state in counts:
                counts[state] = count
            total += count

        counts["total"] = total
        return counts

    def bulk_set_total_batches(self, execution_id: str, total_batches: int) -> None:
        """Update total_batches in every job's metadata payload for an execution.

        Casts job_payload to jsonb before calling jsonb_set because SQLAlchemy maps
        Column(JSON) to PostgreSQL's json type, and jsonb_set only accepts jsonb.
        """
        from sqlalchemy import text
        self.db.execute(
            text(
                "UPDATE jobs "
                "SET job_payload = jsonb_set(job_payload::jsonb, '{metadata,total_batches}', CAST(:val AS jsonb)) "
                "WHERE execution_id = :eid"
            ),
            {"val": str(total_batches), "eid": execution_id},
        )
        self.db.commit()
