"""Execution management for ReflowManager."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from reflowfy.reflow_manager.models import Execution


class ExecutionManager:
    """Manages pipeline execution records."""

    def __init__(self, db_session: Session):
        self.db = db_session

    def create_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        runtime_params: Optional[Dict[str, Any]] = None,
    ) -> Execution:
        """
        Create new execution record.

        Args:
            execution_id: Unique execution identifier
            pipeline_name: Name of the pipeline
            runtime_params: Runtime parameters for the pipeline

        Returns:
            Created Execution object
        """
        execution = Execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            state="pending",
            runtime_params=runtime_params or {},
        )

        self.db.add(execution)
        self.db.commit()
        self.db.refresh(execution)

        return execution

    def get_execution(self, execution_id: str, for_update: bool = False) -> Optional[Execution]:
        """
        Get execution by ID.

        Args:
            execution_id: Execution identifier
            for_update: If True, lock the row for update (prevents concurrent modifications)

        Returns:
            Execution object or None if not found
        """
        query = self.db.query(Execution).filter(
            Execution.execution_id == execution_id
        )
        if for_update:
            query = query.with_for_update()
        return query.first()

    def update_execution_state(
        self,
        execution_id: str,
        state: str,
        error_message: Optional[str] = None,
    ) -> Optional[Execution]:
        """
        Update execution state.

        Args:
            execution_id: Execution identifier
            state: New state (pending, running, paused, completed, failed)
            error_message: Optional error message if failed

        Returns:
            Updated Execution object or None if not found
        """
        execution = self.get_execution(execution_id)
        if not execution:
            return None

        execution.state = state
        if error_message:
            execution.error_message = error_message

        if state in ["completed", "failed"]:
            execution.completed_at = datetime.utcnow()

        self.db.commit()
        self.db.refresh(execution)

        return execution

    def update_job_counts(
        self,
        execution_id: str,
        jobs_dispatched: Optional[int] = None,
        jobs_completed: Optional[int] = None,
        jobs_failed: Optional[int] = None,
    ) -> Optional[Execution]:
        """
        Update job counts for an execution.

        Note: Does NOT commit - caller should commit after all updates.

        Args:
            execution_id: Execution identifier
            jobs_dispatched: Increment jobs dispatched
            jobs_completed: Increment jobs completed
            jobs_failed: Increment jobs failed

        Returns:
            Updated Execution object or None if not found
        """
        execution = self.get_execution(execution_id)
        if not execution:
            return None

        if jobs_dispatched is not None:
            execution.jobs_dispatched += jobs_dispatched
        if jobs_completed is not None:
            execution.jobs_completed += jobs_completed
        if jobs_failed is not None:
            execution.jobs_failed += jobs_failed

        # Don't commit here - caller will commit
        return execution

    def pause_execution(self, execution_id: str) -> Optional[Execution]:
        """Pause an execution."""
        return self.update_execution_state(execution_id, "paused")

    def resume_execution(self, execution_id: str) -> Optional[Execution]:
        """Resume a paused execution."""
        execution = self.get_execution(execution_id)
        if not execution or execution.state != "paused":
            return None

        return self.update_execution_state(execution_id, "running")

    def get_interrupted_executions(self) -> List[Execution]:
        """
        Find executions that were running when the service crashed.

        These are executions in 'running' state that need to be resumed.

        Returns:
            List of Execution objects in 'running' state
        """
        return self.db.query(Execution).filter(
            Execution.state == "running"
        ).all()

