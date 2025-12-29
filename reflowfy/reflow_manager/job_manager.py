"""Job management for ReflowManager."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from reflowfy.reflow_manager.models import Job


class JobManager:
    """Manages job records for pipeline executions (includes checkpoint functionality)."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_job(
        self,
        execution_id: str,
        batch_id: str,
        job_payload: Dict[str, Any],
        batch_number: Optional[int] = None,
        offset_data: Optional[Dict[str, Any]] = None,
    ) -> Job:
        """
        Create a new job record.
        
        Args:
            execution_id: Execution identifier
            batch_id: Unique batch identifier
            job_payload: Full job data
            batch_number: Checkpoint batch number
            offset_data: Source-specific offset/cursor data
        
        Returns:
            Created Job object
        """
        job = Job(
            execution_id=execution_id,
            batch_id=batch_id,
            job_payload=job_payload,
            state="pending",
            batch_number=batch_number,
            offset_data=offset_data or {},
        )
        
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        
        return job
    
    def get_job_by_batch_id(self, batch_id: str) -> Optional[Job]:
        """Get job by batch_id."""
        return self.db.query(Job).filter(Job.batch_id == batch_id).first()
    
    # Alias for backward compatibility
    get_checkpoint_by_batch = get_job_by_batch_id
    
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
    
    # Alias for backward compatibility
    get_checkpoints = get_jobs
    
    def mark_jobs_dispatched(self, batch_ids: List[str]) -> int:
        """
        Mark multiple jobs as dispatched.
        
        Only updates jobs that are still 'pending' - won't overwrite
        jobs that have already been completed/failed by workers.
        
        Args:
            batch_ids: List of batch IDs to mark
        
        Returns:
            Number of jobs updated
        """
        updated = self.db.query(Job).filter(
            Job.batch_id.in_(batch_ids),
            Job.state == "pending",
        ).update(
            {"state": "dispatched", "dispatched_at": datetime.utcnow()},
            synchronize_session=False,
        )
        self.db.commit()
        return updated
    
    def update_job_state(
        self,
        batch_id: str,
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
            Job.batch_id == batch_id
        ).update(update_data, synchronize_session=False)
        
        self.db.commit()
        
        if updated == 0:
            return None
        
        return self.get_job_by_batch_id(batch_id)
    
    def get_batch_states(self, batch_ids: List[str]) -> Dict[str, str]:
        """
        Get states for multiple jobs efficiently.
        Returns fresh data by querying columns directly.
        
        Args:
            batch_ids: List of batch identifiers
        
        Returns:
            Dictionary mapping batch_id to state
        """
        results = self.db.query(Job.batch_id, Job.state).filter(
            Job.batch_id.in_(batch_ids)
        ).all()
        
        return {batch_id: state for batch_id, state in results}
    
    def get_job_counts(self, execution_id: str) -> Dict[str, int]:
        """
        Get job counts by state for an execution (using aggregation).
        
        Returns:
            Dictionary with total, pending, dispatched, completed, failed counts
        """
        rows = self.db.query(Job.state, func.count(Job.id)).filter(
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
