"""Job management for ReflowManager."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from reflowfy.reflow_manager.models import Job


class JobManager:
    """Manages job records for pipeline executions."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_job(
        self,
        execution_id: str,
        batch_id: str,
        job_payload: Dict[str, Any],
        batch_number: Optional[int] = None,
    ) -> Job:
        """
        Create a new job record.
        
        Args:
            execution_id: Execution identifier
            batch_id: Unique batch identifier
            job_payload: Full job data
            batch_number: Checkpoint batch number
        
        Returns:
            Created Job object
        """
        job = Job(
            execution_id=execution_id,
            batch_id=batch_id,
            job_payload=job_payload,
            state="pending",
            batch_number=batch_number,
        )
        
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        
        return job
    
    def get_job_by_batch_id(self, batch_id: str) -> Optional[Job]:
        """Get job by batch_id."""
        return self.db.query(Job).filter(Job.batch_id == batch_id).first()
    
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
    
    def mark_jobs_dispatched(self, batch_ids: List[str]) -> int:
        """
        Mark multiple jobs as dispatched.
        
        Args:
            batch_ids: List of batch IDs to mark
        
        Returns:
            Number of jobs updated
        """
        updated = self.db.query(Job).filter(
            Job.batch_id.in_(batch_ids)
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
    ) -> Optional[Job]:
        """
        Update job state.
        
        Note: Does NOT commit - caller should commit after all updates.
        """
        job = self.get_job_by_batch_id(batch_id)
        if not job:
            return None
        
        job.state = state
        if state == "completed" or state == "failed":
            job.completed_at = datetime.utcnow()
        
        # Don't commit here - caller will commit
        return job
    
    def get_job_counts(self, execution_id: str) -> Dict[str, int]:
        """
        Get job counts by state for an execution.
        
        Returns:
            Dictionary with total, pending, dispatched, completed, failed counts
        """
        jobs = self.db.query(Job).filter(Job.execution_id == execution_id).all()
        
        counts = {
            "total": len(jobs),
            "pending": 0,
            "dispatched": 0,
            "completed": 0,
            "failed": 0,
        }
        
        for job in jobs:
            if job.state in counts:
                counts[job.state] += 1
        
        return counts
    
    def get_next_batch_number(self, execution_id: str) -> int:
        """Get the next batch number for an execution."""
        result = self.db.query(Job.batch_number).filter(
            Job.execution_id == execution_id
        ).order_by(Job.batch_number.desc()).first()
        
        if result and result[0] is not None:
            return result[0] + 1
        return 1
    
    def sync_states_from_checkpoints(self, execution_id: str) -> Dict[str, int]:
        """
        Sync job states from checkpoint states.
        
        Use this for executions that ran before job state updates were added.
        
        Returns:
            Dictionary with synced counts
        """
        from reflowfy.reflow_manager.models import Checkpoint
        
        # Get all checkpoints with completed/failed states
        checkpoints = self.db.query(Checkpoint).filter(
            Checkpoint.execution_id == execution_id,
            Checkpoint.state.in_(["completed", "failed"])
        ).all()
        
        synced_completed = 0
        synced_failed = 0
        
        for cp in checkpoints:
            job = self.db.query(Job).filter(
                Job.batch_id == cp.batch_id
            ).first()
            
            if job and job.state != cp.state:
                job.state = cp.state
                if cp.state == "completed":
                    synced_completed += 1
                elif cp.state == "failed":
                    synced_failed += 1
        
        self.db.commit()
        
        return {
            "synced_completed": synced_completed,
            "synced_failed": synced_failed,
            "total_synced": synced_completed + synced_failed,
        }
