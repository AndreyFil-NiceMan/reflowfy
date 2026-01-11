"""Pydantic schemas for DLQ (Dead Letter Queue) API."""

from datetime import datetime
from typing import Dict, Any, List, Optional
from pydantic import BaseModel


class ScheduleDLQJobRequest(BaseModel):
    """Request to schedule a job in the DLQ."""
    job_payload: Dict[str, Any]
    pipeline_name: str
    delay_minutes: Optional[int] = None  # Uses DLQ_DEFAULT_DELAY_MINUTES if not provided


class DLQJobResponse(BaseModel):
    """Response containing DLQ job details."""
    id: int
    job_payload: Dict[str, Any]
    pipeline_name: str
    delay_minutes: int
    scheduled_at: datetime
    status: str
    retry_count: int
    max_retries: int
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None
    execution_id: Optional[str] = None

    class Config:
        from_attributes = True


class DLQJobListResponse(BaseModel):
    """Response containing a list of DLQ jobs."""
    jobs: List[DLQJobResponse]
    total: int


class DispatchDLQResponse(BaseModel):
    """Response after dispatching DLQ jobs."""
    dispatched_count: int
    execution_id: Optional[str] = None
    message: str
