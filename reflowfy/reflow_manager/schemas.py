"""Pydantic schemas for ReflowManager API."""

from typing import Dict, Any, Optional

from pydantic import BaseModel


class UpdateExecutionStateRequest(BaseModel):
    """Request to update execution state."""

    state: str
    error_message: Optional[str] = None


class RunPipelineRequest(BaseModel):
    """Request to run a pipeline."""

    pipeline_name: str
    runtime_params: Optional[Dict[str, Any]] = None
    rate_limit: Optional[float] = None  # jobs per minute
    execution_id: Optional[str] = None
    mode: Optional[str] = None  # "local" or "distributed" (defaults to EXECUTION_MODE env var)
    enable_duplicate_jobs: Optional[bool] = (
        None  # None = use pipeline default; True = allow duplicates; False = each unique job runs at most once
    )
