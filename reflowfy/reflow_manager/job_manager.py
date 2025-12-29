"""Job management for ReflowManager."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from reflowfy.reflow_manager.models import Job


class JobManager:
    """Manages job records for pipeline executions."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_job(
        self,
        execution_id: str,
        job_id: str,
        job_payload: Dict[str, Any],
        batch_number: Optional[int] = None,
    ) -> Job:
        """
        Create a new job record.
        
        Args:
            execution_id: Execution identifier
            job_id: Unique job identifier
            job_payload: Full job data
            batch_number: Batch number for grouping
        
        Returns:
            Created Job object
        """
        job = Job(
            execution_id=execution_id,
            job_id=job_id,
            job_payload=job_payload,
            state="pending",
            batch_number=batch_number,
        )
        
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        
        return job
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by job_id."""
        return self.db.query(Job).filter(Job.job_id == job_id).first()
    
    def get_pending_jobs_by_batch_number(
        self,
        execution_id: str,
        batch_number: int,
    ) -> List[Job]:
        """Get all pending jobs for a specific batch number."""
        return self.db.query(Job).filter(
            Job.execution_id == execution_id,
            Job.batch_number == batch_number,
            Job.state == "pending",
        ).all()
    
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
    
    def mark_jobs_dispatched(self, job_ids: List[str]) -> int:
        """
        Mark multiple jobs as dispatched.
        
        Only updates jobs that are still 'pending'.
        
        Args:
            job_ids: List of job IDs to mark
        
        Returns:
            Number of jobs updated
        """
        updated = self.db.query(Job).filter(
            Job.job_id.in_(job_ids),
            Job.state == "pending",
        ).update(
            {"state": "dispatched", "dispatched_at": datetime.utcnow()},
            synchronize_session=False,
        )
        self.db.commit()
        return updated
    
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
        update_data = {
            "state": state,
            "updated_at": datetime.utcnow(),
        }
        
        if state in ["completed", "failed"]:
            update_data["completed_at"] = datetime.utcnow()
        if processed_records is not None:
            update_data["processed_records"] = processed_records
        if error_message:
            update_data["error_message"] = error_message
        if stats:
            update_data["stats"] = stats
        
        updated = self.db.query(Job).filter(
            Job.job_id == job_id
        ).update(update_data, synchronize_session=False)
        
        self.db.commit()
        
        if updated == 0:
            return None
        
        return self.get_job(job_id)
    
    def get_job_states(self, job_ids: List[str]) -> Dict[str, str]:
        """
        Get states for multiple jobs efficiently.
        
        Args:
            job_ids: List of job identifiers
        
        Returns:
            Dictionary mapping job_id to state
        """
        results = self.db.query(Job.job_id, Job.state).filter(
            Job.job_id.in_(job_ids)
        ).all()
        
        return {job_id: state for job_id, state in results}
    
    def get_job_counts(self, execution_id: str) -> Dict[str, int]:
        """
        Get job counts by state for an execution.
        
        Returns:
            Dictionary with total, pending, dispatched, completed, failed counts
        """
        rows = self.db.query(Job.state, func.count(Job.job_id)).filter(
            Job.execution_id == execution_id
        ).group_by(Job.state).all()
        
        counts = {
            "total": 0,
            "pending": 0,
            "dispatched": 0,
            "completed": 0,
            "failed": 0,
        }
        
        total = 0
        for state, count in rows:
            if state in counts:
                counts[state] = count
            total += count
        
        counts["total"] = total
        return counts

