"""Base executor interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class ExecutionState(str, Enum):
    """Execution state machine."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_FAILED = "partially_failed"


@dataclass
class ExecutionStatus:
    """Execution status tracking."""

    execution_id: str
    pipeline_name: str
    state: ExecutionState
    total_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    retry_count: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_jobs == 0:
            return 0.0
        return self.completed_jobs / self.total_jobs

    @property
    def is_complete(self) -> bool:
        """Check if execution is complete."""
        return self.state in [ExecutionState.COMPLETED, ExecutionState.FAILED, ExecutionState.PARTIALLY_FAILED]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize status."""
        return {
            "execution_id": self.execution_id,
            "pipeline_name": self.pipeline_name,
            "state": self.state.value,
            "total_jobs": self.total_jobs,
            "completed_jobs": self.completed_jobs,
            "failed_jobs": self.failed_jobs,
            "retry_count": self.retry_count,
            "success_rate": self.success_rate,
            "error_message": self.error_message,
            "metadata": self.metadata,
        }


class BaseExecutor(ABC):
    """
    Base class for execution engines.

    Executors are responsible for:
    1. Executing pipelines
    2. Tracking execution status
    3. Error handling
    """

    @abstractmethod
    def execute(
        self,
        pipeline: Any,
        runtime_params: Dict[str, Any],
        execution_id: Optional[str] = None,
    ) -> ExecutionStatus:
        """
        Execute a pipeline.

        Args:
            pipeline: Pipeline instance to execute
            runtime_params: Runtime parameters
            execution_id: Optional execution ID (generated if not provided)

        Returns:
            ExecutionStatus
        """
        pass
