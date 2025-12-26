"""Checkpoint management for ReflowManager."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from reflowfy.reflow_manager.models import Checkpoint


class CheckpointManager:
    """Manages job checkpoints for tracking and resume capability."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_checkpoint(
        self,
        execution_id: str,
        batch_id: str,
        offset_data: Optional[Dict[str, Any]] = None,
        processed_records: int = 0,
    ) -> Checkpoint:
        """
        Create checkpoint for a batch.
        
        Args:
            execution_id: Execution identifier
            batch_id: Batch identifier
            offset_data: Source-specific offset/cursor data
            processed_records: Number of records processed
        
        Returns:
            Created Checkpoint object
        """
        checkpoint = Checkpoint(
            execution_id=execution_id,
            batch_id=batch_id,
            offset_data=offset_data or {},
            processed_records=processed_records,
            state="pending",
        )
        
        self.db.add(checkpoint)
        self.db.commit()
        self.db.refresh(checkpoint)
        
        return checkpoint
    
    def update_checkpoint_state(
        self,
        batch_id: str,
        state: str,
        processed_records: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> Optional[Checkpoint]:
        """
        Update checkpoint state.
        
        Args:
            batch_id: Batch identifier
            state: New state (pending, processing, completed, failed)
            processed_records: Optional updated record count
            error_message: Optional error message
        
        Returns:
            Updated Checkpoint object or None if not found
        """
        checkpoint = self.db.query(Checkpoint).filter(
            Checkpoint.batch_id == batch_id
        ).first()
        
        if not checkpoint:
            return None
        
        checkpoint.state = state
        if processed_records is not None:
            checkpoint.processed_records = processed_records
        if error_message:
            checkpoint.error_message = error_message
        
        self.db.commit()
        self.db.refresh(checkpoint)
        
        return checkpoint
    
    def get_checkpoints(
        self,
        execution_id: str,
        state: Optional[str] = None,
    ) -> List[Checkpoint]:
        """
        Get checkpoints for an execution.
        
        Args:
            execution_id: Execution identifier
            state: Optional state filter
        
        Returns:
            List of Checkpoint objects
        """
        query = self.db.query(Checkpoint).filter(
            Checkpoint.execution_id == execution_id
        )
        
        if state:
            query = query.filter(Checkpoint.state == state)
        
        return query.order_by(Checkpoint.created_at).all()
    
    def get_checkpoint_stats(self, execution_id: str) -> Dict[str, int]:
        """Get checkpoint statistics for an execution."""
        total = self.db.query(Checkpoint).filter(
            Checkpoint.execution_id == execution_id
        ).count()
        
        completed = self.db.query(Checkpoint).filter(
            Checkpoint.execution_id == execution_id,
            Checkpoint.state == "completed"
        ).count()
        
        return {
            "total": total,
            "completed": completed,
            "pending": total - completed,
        }
