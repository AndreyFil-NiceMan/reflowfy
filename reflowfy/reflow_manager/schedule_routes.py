"""Schedule management routes for ReflowManager."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.models import DLQJobArchive, Execution, PipelineSchedule

router = APIRouter()


@router.get("/schedules")
def list_schedules(db: Session = Depends(get_db)):
    """List all pipeline schedule entries."""
    rows = (
        db.query(PipelineSchedule)
        .order_by(PipelineSchedule.pipeline_name)
        .all()
    )
    return {"schedules": [r.to_dict() for r in rows], "total": len(rows)}


@router.get("/schedules/{pipeline_name}")
def get_schedule(pipeline_name: str, db: Session = Depends(get_db)):
    """Get a single pipeline schedule with last-execution enrichment."""
    row = db.query(PipelineSchedule).filter(
        PipelineSchedule.pipeline_name == pipeline_name
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Schedule '{pipeline_name}' not found")

    data = row.to_dict()
    if row.last_execution_id:
        exec_row = db.query(Execution).filter(
            Execution.execution_id == row.last_execution_id
        ).first()
        if exec_row:
            data["last_execution_state"] = exec_row.state
            data["last_execution_jobs_completed"] = exec_row.jobs_completed
            data["last_execution_jobs_failed"] = exec_row.jobs_failed
    return data


@router.get("/schedules/{pipeline_name}/stats")
def get_schedule_stats(pipeline_name: str, db: Session = Depends(get_db)):
    """Execution history stats for a scheduled pipeline."""
    row = db.query(PipelineSchedule).filter(
        PipelineSchedule.pipeline_name == pipeline_name
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Schedule '{pipeline_name}' not found")

    counts = (
        db.query(Execution.state, func.count(Execution.execution_id))
        .filter(Execution.pipeline_name == pipeline_name)
        .group_by(Execution.state)
        .all()
    )
    by_state = {state: cnt for state, cnt in counts}
    total = sum(by_state.values())

    last_exec_state = None
    if row.last_execution_id:
        exec_row = db.query(Execution).filter(
            Execution.execution_id == row.last_execution_id
        ).first()
        last_exec_state = exec_row.state if exec_row else None

    return {
        "pipeline_name": pipeline_name,
        "cron_expression": row.cron_expression,
        "next_run_at": row.next_run_at.isoformat() if row.next_run_at else None,
        "enabled": row.enabled == "true",
        "total_executions": total,
        "completed_executions": by_state.get("completed", 0),
        "failed_executions": by_state.get("failed", 0),
        "running_executions": by_state.get("running", 0),
        "pending_executions": by_state.get("pending", 0),
        "last_execution_id": row.last_execution_id,
        "last_execution_state": last_exec_state,
        "last_triggered_at": row.last_triggered_at.isoformat() if row.last_triggered_at else None,
    }


@router.get("/archive/jobs")
def list_archived_jobs(
    pipeline_name: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    """List permanently failed DLQ jobs from the archive (exceeded max_retries)."""
    limit = min(limit, 500)
    query = db.query(DLQJobArchive)
    if pipeline_name:
        query = query.filter(DLQJobArchive.pipeline_name == pipeline_name)
    total = query.count()
    jobs = query.order_by(DLQJobArchive.archived_at.desc()).offset(offset).limit(limit).all()
    return {
        "jobs": [j.to_dict() for j in jobs],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/archive/jobs/{job_id}")
def get_archived_job(job_id: int, db: Session = Depends(get_db)):
    """Get a single archived (permanently failed) DLQ job by ID."""
    job = db.query(DLQJobArchive).filter(DLQJobArchive.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=f"Archived job {job_id} not found")
    return job.to_dict()
