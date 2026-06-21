"""Execution tracking and status management."""

import threading
from typing import Dict, Optional
from reflowfy.execution.base import ExecutionStatus


class ExecutionTracker:
    """
    Thread-safe in-memory execution tracker.

    In production, this should be backed by Redis or a database.
    """

    def __init__(self):
        self._executions: Dict[str, ExecutionStatus] = {}
        self._lock = threading.RLock()

    def track(self, status: ExecutionStatus) -> None:
        """
        Track or update an execution.

        Args:
            status: ExecutionStatus to track
        """
        with self._lock:
            self._executions[status.execution_id] = status

    def get_status(self, execution_id: str) -> Optional[ExecutionStatus]:
        """
        Get execution status by ID.

        Args:
            execution_id: Execution ID

        Returns:
            ExecutionStatus or None if not found
        """
        with self._lock:
            return self._executions.get(execution_id)

    def update_job_completion(
        self,
        execution_id: str,
        success: bool,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Update job completion status.

        Called by workers when a job completes.

        Args:
            execution_id: Execution ID
            success: Whether job succeeded
            error_message: Optional error message
        """
        with self._lock:
            status = self._executions.get(execution_id)

            if status is None:
                # Create new status if not found
                from reflowfy.execution.base import ExecutionState

                status = ExecutionStatus(
                    execution_id=execution_id,
                    pipeline_name="unknown",
                    state=ExecutionState.RUNNING,
                    total_jobs=1,
                )
                self._executions[execution_id] = status

            if success:
                status.completed_jobs += 1
            else:
                status.failed_jobs += 1
                if error_message:
                    status.error_message = error_message

            # Update state
            from reflowfy.execution.base import ExecutionState

            if status.completed_jobs + status.failed_jobs >= status.total_jobs:
                if status.failed_jobs == 0:
                    status.state = ExecutionState.COMPLETED
                elif status.failed_jobs == status.total_jobs:
                    status.state = ExecutionState.FAILED
                else:
                    status.state = ExecutionState.PARTIALLY_FAILED

    def list_all(self) -> list:
        """Get all tracked executions."""
        with self._lock:
            return list(self._executions.values())

    def clear(self) -> None:
        """Clear all tracked executions (for testing)."""
        with self._lock:
            self._executions.clear()


# Global tracker instance
execution_tracker = ExecutionTracker()
