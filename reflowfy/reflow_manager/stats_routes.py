"""Statistics API routes for ReflowManager."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func, case, desc

from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.models import Execution, Job, DLQJob, DLQJobArchive

router = APIRouter(prefix="/stats", tags=["Stats"])


# ===== 1. Global Overview =====


@router.get("/overview")
async def get_overview_stats(db: Session = Depends(get_db)):
    """
    Global dashboard statistics across all pipelines.

    Returns total executions, jobs by state, success rate, and DLQ summary.
    """
    # Execution counts by state
    exec_rows = (
        db.query(
            Execution.state,
            func.count(Execution.execution_id),
        )
        .group_by(Execution.state)
        .all()
    )

    executions_by_state = {}
    total_executions = 0
    for state, count in exec_rows:
        executions_by_state[state] = count
        total_executions += count

    # Job counts by state
    job_rows = (
        db.query(
            Job.state,
            func.count(Job.job_id),
        )
        .group_by(Job.state)
        .all()
    )

    jobs_by_state = {}
    total_jobs = 0
    for state, count in job_rows:
        jobs_by_state[state] = count
        total_jobs += count

    # Success rate
    completed_jobs = jobs_by_state.get("completed", 0)
    failed_jobs = jobs_by_state.get("failed", 0)
    finished = completed_jobs + failed_jobs
    success_rate = round((completed_jobs / finished) * 100, 2) if finished > 0 else 100.0

    # DLQ summary
    dlq_rows = (
        db.query(
            DLQJob.status,
            func.count(DLQJob.id),
        )
        .group_by(DLQJob.status)
        .all()
    )

    dlq_by_status = {}
    for dlq_status, count in dlq_rows:
        dlq_by_status[dlq_status] = count

    return {
        "total_executions": total_executions,
        "executions_by_state": executions_by_state,
        "total_jobs": total_jobs,
        "jobs_by_state": jobs_by_state,
        "success_rate": success_rate,
        "dlq": dlq_by_status,
    }


# ===== 2. Per-Pipeline Breakdown =====


@router.get("/pipelines")
async def get_pipeline_stats(db: Session = Depends(get_db)):
    """
    Statistics grouped by pipeline name.

    Returns execution counts, job totals, and success rate per pipeline.
    """
    # Execution counts per pipeline
    exec_rows = (
        db.query(
            Execution.pipeline_name,
            Execution.state,
            func.count(Execution.execution_id),
            func.max(Execution.created_at).label("last_execution_at"),
        )
        .group_by(
            Execution.pipeline_name,
            Execution.state,
        )
        .all()
    )

    # Build per-pipeline map
    pipelines = {}
    for pipeline_name, state, count, last_at in exec_rows:
        if pipeline_name not in pipelines:
            pipelines[pipeline_name] = {
                "pipeline_name": pipeline_name,
                "total_executions": 0,
                "last_execution_at": None,
                "executions_by_state": {},
            }
        p = pipelines[pipeline_name]
        p["total_executions"] += count
        p["executions_by_state"][state] = count
        if last_at and (p["last_execution_at"] is None or last_at > p["last_execution_at"]):
            p["last_execution_at"] = last_at

    # Job counts per pipeline (via join)
    job_rows = (
        db.query(
            Execution.pipeline_name,
            func.count(Job.job_id).label("total_jobs"),
            func.sum(case((Job.state == "completed", 1), else_=0)).label("jobs_completed"),
            func.sum(case((Job.state == "failed", 1), else_=0)).label("jobs_failed"),
        )
        .join(
            Job,
            Job.execution_id == Execution.execution_id,
        )
        .group_by(Execution.pipeline_name)
        .all()
    )

    for pipeline_name, total_jobs, completed, failed in job_rows:
        p = pipelines.get(pipeline_name)
        if p:
            completed_int = int(completed or 0)
            failed_int = int(failed or 0)
            finished = completed_int + failed_int
            p["total_jobs"] = total_jobs
            p["jobs_completed"] = completed_int
            p["jobs_failed"] = failed_int
            p["success_rate"] = (
                round((completed_int / finished) * 100, 2) if finished > 0 else 100.0
            )

    # Ensure all pipelines have job fields
    for p in pipelines.values():
        p.setdefault("total_jobs", 0)
        p.setdefault("jobs_completed", 0)
        p.setdefault("jobs_failed", 0)
        p.setdefault("success_rate", 100.0)
        if p["last_execution_at"]:
            p["last_execution_at"] = p["last_execution_at"].isoformat()

    return {"pipelines": list(pipelines.values())}


# ===== 3. Single Pipeline Detail =====


@router.get("/pipelines/{pipeline_name}")
async def get_pipeline_detail(
    pipeline_name: str,
    limit: int = 10,
    db: Session = Depends(get_db),
):
    """
    Detailed statistics for a single pipeline, including recent executions.

    Args:
        pipeline_name: Name of the pipeline
        limit: Number of recent executions to return (default 10)
    """
    # Check pipeline exists in execution history
    exec_count = (
        db.query(func.count(Execution.execution_id))
        .filter(
            Execution.pipeline_name == pipeline_name,
        )
        .scalar()
    )

    if exec_count == 0:
        # No executions yet — try the pipeline registry for static metadata
        from reflowfy.core.registry import pipeline_registry

        pipeline_obj = pipeline_registry.get(pipeline_name)
        if not pipeline_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Pipeline '{pipeline_name}' not found in registry or execution history",
            )
        pipeline_meta = pipeline_obj.to_dict()
        return {
            "pipeline_name": pipeline_name,
            "total_executions": 0,
            "executions_by_state": {},
            "total_jobs": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "success_rate": 100.0,
            "recent_executions": [],
            **{k: v for k, v in pipeline_meta.items() if k != "name"},
        }

    # Execution counts by state
    exec_rows = (
        db.query(
            Execution.state,
            func.count(Execution.execution_id),
        )
        .filter(
            Execution.pipeline_name == pipeline_name,
        )
        .group_by(Execution.state)
        .all()
    )

    executions_by_state = {}
    total_executions = 0
    for state, count in exec_rows:
        executions_by_state[state] = count
        total_executions += count

    # Job counts
    job_row = (
        db.query(
            func.count(Job.job_id).label("total_jobs"),
            func.sum(case((Job.state == "completed", 1), else_=0)).label("jobs_completed"),
            func.sum(case((Job.state == "failed", 1), else_=0)).label("jobs_failed"),
        )
        .join(
            Execution,
            Execution.execution_id == Job.execution_id,
        )
        .filter(
            Execution.pipeline_name == pipeline_name,
        )
        .first()
    )

    total_jobs = job_row.total_jobs if job_row else 0
    jobs_completed = int(job_row.jobs_completed or 0) if job_row else 0
    jobs_failed = int(job_row.jobs_failed or 0) if job_row else 0
    finished = jobs_completed + jobs_failed
    success_rate = round((jobs_completed / finished) * 100, 2) if finished > 0 else 100.0

    # Recent executions
    recent = (
        db.query(Execution)
        .filter(
            Execution.pipeline_name == pipeline_name,
        )
        .order_by(desc(Execution.created_at))
        .limit(limit)
        .all()
    )

    recent_executions = []
    for ex in recent:
        recent_executions.append(
            {
                "execution_id": ex.execution_id,
                "state": ex.state,
                "total_jobs": ex.total_jobs,
                "jobs_completed": ex.jobs_completed,
                "jobs_failed": ex.jobs_failed,
                "created_at": ex.created_at.isoformat() if ex.created_at else None,
                "completed_at": ex.completed_at.isoformat() if ex.completed_at else None,
                "error_message": ex.error_message,
            }
        )

    return {
        "pipeline_name": pipeline_name,
        "total_executions": total_executions,
        "executions_by_state": executions_by_state,
        "total_jobs": total_jobs,
        "jobs_completed": jobs_completed,
        "jobs_failed": jobs_failed,
        "success_rate": success_rate,
        "recent_executions": recent_executions,
    }


# ===== 4. Failure Summary =====


@router.get("/failures")
async def get_failure_stats(
    pipeline_name: Optional[str] = None,
    limit: int = 20,
    db: Session = Depends(get_db),
):
    """
    Recent failures across all (or a specific) pipeline.

    Args:
        pipeline_name: Optional filter by pipeline
        limit: Number of failed executions to return (default 20)
    """
    # Failed executions query
    query = db.query(Execution).filter(Execution.state == "failed")
    if pipeline_name:
        query = query.filter(Execution.pipeline_name == pipeline_name)

    total_failed_executions = query.count()

    failed_execs = query.order_by(desc(Execution.completed_at)).limit(limit).all()

    failed_executions = []
    for ex in failed_execs:
        failed_executions.append(
            {
                "execution_id": ex.execution_id,
                "pipeline_name": ex.pipeline_name,
                "error_message": ex.error_message,
                "total_jobs": ex.total_jobs,
                "jobs_failed": ex.jobs_failed,
                "created_at": ex.created_at.isoformat() if ex.created_at else None,
                "completed_at": ex.completed_at.isoformat() if ex.completed_at else None,
            }
        )

    # Total failed jobs across all (or filtered) executions
    jobs_query = db.query(func.count(Job.job_id)).filter(Job.state == "failed")
    if pipeline_name:
        jobs_query = jobs_query.join(
            Execution,
            Execution.execution_id == Job.execution_id,
        ).filter(Execution.pipeline_name == pipeline_name)

    total_failed_jobs = jobs_query.scalar() or 0

    return {
        "failed_executions": failed_executions,
        "total_failed_executions": total_failed_executions,
        "total_failed_jobs": total_failed_jobs,
    }


# ===== 5. DLQ Statistics =====


@router.get("/dlq")
async def get_dlq_stats(
    pipeline_name: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    DLQ statistics with per-pipeline breakdown.

    Args:
        pipeline_name: Optional filter to a single pipeline
    """
    # Global DLQ counts by status
    dlq_query = db.query(
        DLQJob.status,
        func.count(DLQJob.id),
    )
    if pipeline_name:
        dlq_query = dlq_query.filter(DLQJob.pipeline_name == pipeline_name)

    dlq_rows = dlq_query.group_by(DLQJob.status).all()

    jobs_by_status = {}
    total_dlq_jobs = 0
    for dlq_status, count in dlq_rows:
        jobs_by_status[dlq_status] = count
        total_dlq_jobs += count

    # Archived count
    archive_query = db.query(func.count(DLQJobArchive.id))
    if pipeline_name:
        archive_query = archive_query.filter(DLQJobArchive.pipeline_name == pipeline_name)
    total_archived = archive_query.scalar() or 0

    # Per-pipeline breakdown
    pipeline_query = db.query(
        DLQJob.pipeline_name,
        DLQJob.status,
        func.count(DLQJob.id).label("count"),
        func.avg(DLQJob.retry_count).label("avg_retry_count"),
        func.min(case((DLQJob.status == "pending", DLQJob.created_at), else_=None)).label(
            "oldest_pending_at"
        ),
    )
    if pipeline_name:
        pipeline_query = pipeline_query.filter(DLQJob.pipeline_name == pipeline_name)

    pipeline_rows = pipeline_query.group_by(
        DLQJob.pipeline_name,
        DLQJob.status,
    ).all()

    # Build per-pipeline map
    pipeline_map = {}
    for pname, dlq_status, count, avg_retry, oldest in pipeline_rows:
        if pname not in pipeline_map:
            pipeline_map[pname] = {
                "pipeline_name": pname,
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
                "archived": 0,
                "avg_retry_count": 0.0,
                "oldest_pending_at": None,
            }
        p = pipeline_map[pname]
        if dlq_status in p:
            p[dlq_status] = count
        if avg_retry is not None:
            p["avg_retry_count"] = round(float(avg_retry), 1)
        if dlq_status == "pending" and oldest:
            p["oldest_pending_at"] = oldest.isoformat()

    # Add archived counts per pipeline
    archive_pipeline_rows = db.query(
        DLQJobArchive.pipeline_name,
        func.count(DLQJobArchive.id),
    )
    if pipeline_name:
        archive_pipeline_rows = archive_pipeline_rows.filter(
            DLQJobArchive.pipeline_name == pipeline_name,
        )
    archive_pipeline_rows = archive_pipeline_rows.group_by(
        DLQJobArchive.pipeline_name,
    ).all()

    for pname, count in archive_pipeline_rows:
        if pname in pipeline_map:
            pipeline_map[pname]["archived"] = count
        else:
            pipeline_map[pname] = {
                "pipeline_name": pname,
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
                "archived": count,
                "avg_retry_count": 0.0,
                "oldest_pending_at": None,
            }

    return {
        "total_dlq_jobs": total_dlq_jobs,
        "jobs_by_status": jobs_by_status,
        "total_archived": total_archived,
        "pipelines": list(pipeline_map.values()),
    }
