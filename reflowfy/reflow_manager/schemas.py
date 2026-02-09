"""Pydantic schemas for ReflowManager API."""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel


class CreateExecutionRequest(BaseModel):
    """Request to create a new execution."""
    execution_id: str
    pipeline_name: str
    runtime_params: Optional[Dict[str, Any]] = None


class UpdateExecutionStateRequest(BaseModel):
    """Request to update execution state."""
    state: str
    error_message: Optional[str] = None


class DispatchJobsRequest(BaseModel):
    """Request to dispatch jobs."""
    execution_id: str
    pipeline_name: str
    jobs: List[Dict[str, Any]]
    rate_limit: Optional[float] = None



class CheckpointRequest(BaseModel):
    """Request to create a checkpoint."""
    execution_id: str
    job_id: str
    processed_records: int = 0

class RunPipelineRequest(BaseModel):
    """Request to run a pipeline."""
    pipeline_name: str
    runtime_params: Optional[Dict[str, Any]] = None
    rate_limit: Optional[float] = None
    execution_id: Optional[str] = None


class DestinationConfig(BaseModel):
    """Destination configuration in job payload."""
    type: str
    config: Dict[str, Any] = {}


class JobPayload(BaseModel):
    """Validated job payload sent to workers via Kafka.
    
    This schema ensures job messages have the required structure
    before being dispatched to workers.
    """
    execution_id: str
    job_id: str
    pipeline_name: str
    records: List[Dict[str, Any]]
    transformations: List[str] = []
    destination: DestinationConfig
    metadata: Dict[str, Any] = {}
    
    class Config:
        extra = "allow"  # Allow additional fields for extensibility
