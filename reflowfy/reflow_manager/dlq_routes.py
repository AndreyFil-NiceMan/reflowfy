"""DLQ (Dead Letter Queue) API Routes for ReflowManager."""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.models import DLQJob
from reflowfy.reflow_manager.dlq_schemas import (
    ScheduleDLQJobRequest,
    DLQJobResponse,
    DLQJobListResponse,
    DispatchDLQResponse,
)
from reflowfy.reflow_manager.dlq_scheduler import (
    get_dlq_scheduler,
    DLQ_DEFAULT_DELAY_MINUTES,
    DLQ_MAX_RETRIES,
)

# Create router with /dlq prefix
router = APIRouter(prefix="/dlq", tags=["DLQ"])


@router.post("/schedule", status_code=status.HTTP_201_CREATED, response_model=DLQJobResponse)
async def schedule_dlq_job(
    request: ScheduleDLQJobRequest,
    db: Session = Depends(get_db),
):
    """
    Schedule a job for DLQ processing.

    The job will be processed after the specified delay (or default delay).
    """
    from reflowfy.core.registry import pipeline_registry

    # Reject now, as /run does, rather than failing every retry when the job comes due.
    if not pipeline_registry.get(request.pipeline_name):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Pipeline '{request.pipeline_name}' not found in registry"
                f"{pipeline_registry.describe_missing(request.pipeline_name)}"
            ),
        )

    delay_minutes = (
        request.delay_minutes if request.delay_minutes is not None else DLQ_DEFAULT_DELAY_MINUTES
    )
    scheduled_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
        minutes=delay_minutes
    )

    dlq_job = DLQJob(
        job_payload=request.job_payload,
        pipeline_name=request.pipeline_name,
        delay_minutes=delay_minutes,
        scheduled_at=scheduled_at,
        status="pending",
        retry_count=0,
        max_retries=DLQ_MAX_RETRIES,
    )

    db.add(dlq_job)
    db.commit()
    db.refresh(dlq_job)

    return dlq_job.to_dict()


@router.get("/jobs", response_model=DLQJobListResponse)
async def list_dlq_jobs(
    pipeline_name: Optional[str] = None,
    status_filter: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    """List DLQ jobs with optional filters."""
    query = db.query(DLQJob)

    if pipeline_name:
        query = query.filter(DLQJob.pipeline_name == pipeline_name)
    if status_filter:
        query = query.filter(DLQJob.status == status_filter)

    total = query.count()
    jobs = query.order_by(DLQJob.created_at.desc()).offset(offset).limit(limit).all()

    return {
        "jobs": [job.to_dict() for job in jobs],
        "total": total,
    }


@router.get("/jobs/{job_id}", response_model=DLQJobResponse)
async def get_dlq_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """Get a specific DLQ job by ID."""
    job = db.query(DLQJob).filter(DLQJob.id == job_id).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DLQ job {job_id} not found",
        )

    return job.to_dict()


@router.delete("/jobs/{job_id}")
async def cancel_dlq_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """Cancel a pending DLQ job."""
    job = db.query(DLQJob).filter(DLQJob.id == job_id).first()

    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DLQ job {job_id} not found",
        )

    if job.status != "pending":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot cancel job with status '{job.status}'",
        )

    job.status = "cancelled"
    db.commit()

    return {"message": f"DLQ job {job_id} cancelled"}


# Plain def, not async: run_pipeline blocks until the run is planned and driven, so
# FastAPI runs these in its threadpool instead of on the event loop.
@router.post("/jobs/{job_id}/dispatch", response_model=DispatchDLQResponse)
def dispatch_dlq_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    """Dispatch a specific DLQ job immediately (bypass scheduler)."""
    scheduler = get_dlq_scheduler()

    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DLQ scheduler not initialized",
        )

    try:
        execution_id = scheduler.process_job_immediately(db, job_id)

        if not execution_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"DLQ job {job_id} not found",
            )

        return {
            "dispatched_count": 1,
            "execution_id": execution_id,
            "message": f"DLQ job {job_id} dispatched successfully",
        }

    except HTTPException:
        raise  # e.g. the 404 above: not a dispatch failure
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to dispatch job: {str(e)}",
        )


@router.post("/pipelines/{pipeline_name}/dispatch", response_model=DispatchDLQResponse)
def dispatch_pipeline_dlq_jobs(
    pipeline_name: str,
    db: Session = Depends(get_db),
):
    """Dispatch all pending DLQ jobs for a pipeline immediately."""
    scheduler = get_dlq_scheduler()

    if not scheduler:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DLQ scheduler not initialized",
        )

    try:
        count, execution_ids = scheduler.process_pipeline_immediately(db, pipeline_name)

        if count == 0:
            return {
                "dispatched_count": 0,
                "execution_id": None,
                "execution_ids": execution_ids,  # empty
                "message": f"No pending DLQ jobs found for pipeline '{pipeline_name}'",
            }

        return {
            "dispatched_count": count,
            # One execution per distinct set of params; execution_id is the first.
            "execution_id": execution_ids[0],
            "execution_ids": execution_ids,
            "message": f"Dispatched {count} DLQ job(s) for pipeline '{pipeline_name}'",
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to dispatch jobs: {str(e)}",
        )
