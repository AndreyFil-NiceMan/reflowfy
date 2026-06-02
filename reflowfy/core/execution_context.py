"""Execution context for passing runtime state through the pipeline."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from jinja2 import BaseLoader, Environment, TemplateSyntaxError, UndefinedError


@dataclass
class ExecutionContext:
    """
    Runtime execution context passed through pipeline execution.

    Contains:
    - Execution tracking IDs
    - Runtime parameters provided by user
    - Metadata (timestamps, batch info, etc.)

    Used by:
    - API to track executions
    - Workers to execute jobs
    - Sources to resolve runtime parameters
    """

    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    batch_id: Optional[str] = None
    pipeline_name: str = ""
    runtime_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    batch_number: int = 0
    total_batches: int = 0
    retry_count: int = 0
    is_retry: bool = False

    def __post_init__(self):
        """Initialize batch_id if not provided."""
        if self.batch_id is None:
            self.batch_id = str(uuid.uuid4())

    def to_dict(self) -> Dict[str, Any]:
        """Serialize context for job metadata."""
        return {
            "execution_id": self.execution_id,
            "batch_id": self.batch_id,
            "pipeline_name": self.pipeline_name,
            "runtime_params": self.runtime_params,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "batch_number": self.batch_number,
            "total_batches": self.total_batches,
            "retry_count": self.retry_count,
            "is_retry": self.is_retry,
        }


def build_flat_runtime_params(
    base_params: Dict[str, Any],
    *,
    execution_id: str,
    batch_id: str,
    pipeline_name: str,
    created_at: str,
    batch_number: int = 0,
    total_batches: int = 0,
    retry_count: int = 0,
    is_retry: bool = False,
    current_ids: Optional[list[Any]] = None,
) -> Dict[str, Any]:
    """
    Build a flat mutable runtime_params dict shared by a transformation chain.

    User/runtime values are used as the base. Reserved execution-context keys are
    merged on top so they cannot be shadowed by user input.

    Args:
        base_params: Base runtime parameters (user params + optional enrichments)
        execution_id: Execution identifier
        batch_id: Execution batch identifier
        pipeline_name: Pipeline name
        created_at: ISO timestamp
        batch_number: Current batch number
        total_batches: Total batch count (if known)
        retry_count: Retry attempt count
        is_retry: Whether this is a retry run
        current_ids: Optional ID list for IdBasedPipeline jobs

    Returns:
        Flat mutable dict for transformations and destination metadata
    """
    runtime_params = dict(base_params)
    runtime_params.update(
        {
            "execution_id": execution_id,
            "batch_id": batch_id,
            "pipeline_name": pipeline_name,
            "created_at": created_at,
            "batch_number": batch_number,
            "total_batches": total_batches,
            "retry_count": retry_count,
            "is_retry": is_retry,
        }
    )
    if current_ids is not None:
        runtime_params["current_ids"] = current_ids
    return runtime_params


def build_flat_runtime_params_from_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build flat runtime_params from serialized job metadata.

    Args:
        metadata: Job metadata payload created by PipelineRunner

    Returns:
        Flat mutable dict for worker transformation execution
    """
    return build_flat_runtime_params(
        metadata.get("runtime_params", {}) or {},
        execution_id=metadata.get("execution_id", ""),
        batch_id=metadata.get("batch_id", ""),
        pipeline_name=metadata.get("pipeline_name", ""),
        created_at=metadata.get("created_at", ""),
        batch_number=metadata.get("batch_number", 0),
        total_batches=metadata.get("total_batches", 0),
        retry_count=metadata.get("retry_count", 0),
        is_retry=metadata.get("is_retry", False),
        current_ids=metadata.get("current_ids") if "current_ids" in metadata else None,
    )


class ParameterResolver:
    """
    Resolves Jinja2 template parameters in source configurations.

    Example:
        >>> resolver = ParameterResolver()
        >>> query = {"range": {"timestamp": {"gte": "{{ start_time }}"}}}
        >>> params = {"start_time": "2024-01-01"}
        >>> resolved = resolver.resolve(query, params)
        >>> # Result: {"range": {"timestamp": {"gte": "2024-01-01"}}}
    """

    def __init__(self):
        self.env = Environment(loader=BaseLoader())

    def resolve(self, obj: Any, params: Dict[str, Any]) -> Any:
        """
        Recursively resolve Jinja2 templates in nested objects.

        Args:
            obj: Object to resolve (dict, list, str, or primitive)
            params: Runtime parameters to inject

        Returns:
            Resolved object with template values replaced

        Raises:
            ValueError: If template syntax is invalid or parameter is missing
        """
        if isinstance(obj, dict):
            return {k: self.resolve(v, params) for k, v in obj.items()}

        elif isinstance(obj, list):
            return [self.resolve(item, params) for item in obj]

        elif isinstance(obj, str):
            # Check if string contains Jinja2 template syntax
            if "{{" in obj or "{%" in obj:
                try:
                    template = self.env.from_string(obj)
                    return template.render(**params)
                except TemplateSyntaxError as e:
                    raise ValueError(f"Invalid template syntax: {e}")
                except UndefinedError as e:
                    raise ValueError(f"Missing required parameter: {e}")

        # Return primitives as-is
        return obj

    def extract_parameters(self, obj: Any) -> set[str]:
        """
        Extract all parameter names from Jinja2 templates.

        Args:
            obj: Object to scan for parameters

        Returns:
            Set of parameter names used in templates
        """
        params = set()

        if isinstance(obj, dict):
            for v in obj.values():
                params.update(self.extract_parameters(v))

        elif isinstance(obj, list):
            for item in obj:
                params.update(self.extract_parameters(item))

        elif isinstance(obj, str):
            if "{{" in obj:
                # Simple regex-based extraction
                import re

                matches = re.findall(r"\{\{\s*(\w+)\s*\}\}", obj)
                params.update(matches)

        return params


# Global resolver instance
parameter_resolver = ParameterResolver()
