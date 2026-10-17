"""FastAPI application for ReflowManager service."""

import logging
import os
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

from reflowfy import __version__  # noqa: E402

from reflowfy.observability.logging import setup_logging  # noqa: E402
from reflowfy.reflow_manager.database import get_db, init_db, SessionLocal  # noqa: E402
from reflowfy.reflow_manager.manager import ReflowManager  # noqa: E402
from reflowfy.reflow_manager.execution import ExecutionManager  # noqa: E402
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner  # noqa: E402
from reflowfy.reflow_manager.schemas import (  # noqa: E402
    UpdateExecutionStateRequest,
    RunPipelineRequest,
)
from reflowfy.reflow_manager.dlq_routes import router as dlq_router  # noqa: E402
from reflowfy.reflow_manager.stats_routes import router as stats_router  # noqa: E402
from reflowfy.reflow_manager.schedule_routes import router as schedule_router  # noqa: E402
from reflowfy.reflow_manager.dlq_scheduler import (  # noqa: E402
    init_dlq_scheduler,
    stop_dlq_scheduler,
)
from reflowfy.reflow_manager.pipeline_scheduler import (  # noqa: E402
    init_pipeline_scheduler,
    stop_pipeline_scheduler,
    get_pipeline_scheduler,
)
from reflowfy.reflow_manager.content_dedup_scheduler import (  # noqa: E402
    init_content_dedup_scheduler,
    stop_content_dedup_scheduler,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Run startup initialization and graceful shutdown around the app's lifetime."""
    await _startup()
    yield
    await _shutdown()


# Create FastAPI app
app = FastAPI(
    title="ReflowManager",
    description="Pipeline state management and rate limiting service",
    version=__version__,
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dlq_router)
app.include_router(stats_router)
app.include_router(schedule_router)

# Observability: expose Prometheus /metrics and instrument tracing.
from fastapi import Response  # noqa: E402
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # noqa: E402
from reflowfy.observability.tracing import init_tracing, instrument_fastapi  # noqa: E402


@app.get("/metrics")
def metrics_endpoint() -> Response:
    """Prometheus scrape endpoint (served directly — no mount redirect)."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


init_tracing(service_name="reflow-manager")
instrument_fastapi(app)


def _get_kafka_config() -> Dict[str, Any]:
    """Get Kafka/execution configuration from environment variables (including SASL)."""
    return {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "reflow.jobs"),
        "max_jobs_per_minute": float(os.getenv("MAX_JOBS_PER_MINUTE", "6000")),
        # Execution mode: "distributed" (Kafka) or "local" (in-process)
        "execution_mode": os.getenv("EXECUTION_MODE", "distributed"),
        # SASL Authentication (set by Helm chart)
        "kafka_security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
        "kafka_sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "kafka_sasl_username": os.getenv("KAFKA_SASL_USERNAME"),
        "kafka_sasl_password": os.getenv("KAFKA_SASL_PASSWORD"),
    }


def _make_manager(db: Session, mode: Optional[str] = None) -> ReflowManager:
    """Build a ReflowManager from environment config, optionally overriding execution mode."""
    config = _get_kafka_config()
    if mode:
        config["execution_mode"] = mode
    return ReflowManager(db_session=db, **config)


def _pipeline_runner_factory() -> PipelineRunner:
    """Build a PipelineRunner on a fresh session for background schedulers."""
    return _make_manager(SessionLocal()).pipeline_runner


# Dependency to get ReflowManager instance
def get_reflow_manager(db: Session = Depends(get_db)) -> ReflowManager:
    """Get ReflowManager instance with database session."""
    return _make_manager(db)


# ===== Health Check =====

_db_ready: bool = False  # set to True after successful init_db()


@app.get("/health")
async def health_check() -> Any:
    """Health check endpoint. Returns 503 until the DB is initialized."""
    if not _db_ready:
        return JSONResponse(
            status_code=503,
            content={"status": "starting", "service": "reflow-manager", "version": __version__},
        )
    return {
        "status": "healthy",
        "service": "reflow-manager",
        "version": __version__,
    }


# ===== Pipeline Registry =====


@app.get("/pipelines/{pipeline_name}")
async def get_pipeline(pipeline_name: str) -> Dict[str, Any]:
    """Get metadata for a registered pipeline by name."""
    from reflowfy.core.registry import pipeline_registry

    pipeline = pipeline_registry.get(pipeline_name)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Pipeline '{pipeline_name}' not found in registry"
                f"{pipeline_registry.describe_missing(pipeline_name)}"
            ),
        )
    return pipeline.to_dict()


@app.get("/pipelines")
async def list_pipelines() -> List[Dict[str, Any]]:
    """List all registered pipelines."""
    from reflowfy.core.registry import pipeline_registry

    return [p.to_dict() for p in pipeline_registry.list_all()]


# ===== Execution Management =====


@app.patch("/executions/{execution_id}/state")
async def update_execution_state(
    execution_id: str,
    request: UpdateExecutionStateRequest,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> Dict[str, Any]:
    """Update execution state."""
    execution = manager.update_execution_state(
        execution_id=execution_id,
        state=request.state,
        error_message=request.error_message,
    )

    if not execution:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution '{execution_id}' not found",
        )

    return execution.to_dict()


@app.post("/executions/{execution_id}/pause")
async def pause_execution(
    execution_id: str,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> Dict[str, Any]:
    """Pause an execution: no new jobs are sent (within about a second, or one
    rate-limit token wait); jobs already in flight still finish."""
    if not manager.get_execution(execution_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution '{execution_id}' not found",
        )
    execution = manager.pause_execution(execution_id)
    if not execution:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Execution '{execution_id}' is not running",
        )

    return {"message": "Execution paused", "execution": execution.to_dict()}


@app.post("/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str,
    background_tasks: BackgroundTasks,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> Dict[str, Any]:
    """Resume a paused execution: mark it running and restart its dispatch loop.

    The loop of a paused execution exits within about a second of the pause, so
    the state change alone would leave nothing sending. If the old loop is still
    winding down, the new one stands by on the execution's dispatch lease instead of
    doubling the rate.
    """
    execution = manager.resume_execution(execution_id)

    if not execution:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Execution '{execution_id}' not found or not paused",
        )

    background_tasks.add_task(_resume_execution_sync, execution_id)
    return {"message": "Execution resumed", "execution": execution.to_dict()}


@app.post("/executions/{execution_id}/retry-failed")
async def retry_failed_jobs(
    execution_id: str,
    background_tasks: BackgroundTasks,
    error_contains: Optional[str] = None,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> Dict[str, Any]:
    """Re-run an execution's failed jobs (optionally only those whose error contains
    ``error_contains``) and resume dispatching.

    Retried jobs run again from scratch: a job that had already written part of its
    records to the destination before failing writes them a second time, unless the
    execution used ``enable_duplicate_jobs=false`` (content dedup).
    """
    execution = manager.get_execution(execution_id)
    if not execution:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution '{execution_id}' not found",
        )
    if execution.state == "paused":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Execution '{execution_id}' is paused; resume it first",
        )

    requeued = manager.job_manager.requeue_failed(execution_id, error_contains)
    if requeued:
        was_running = execution.state == "running"
        manager.execution_manager.reopen_execution(execution_id)
        if not was_running:
            # A running execution already has a loop that will pick the jobs up.
            background_tasks.add_task(_resume_execution_sync, execution_id)
    return {"execution_id": execution_id, "requeued": requeued}


# ===== Jobs =====


@app.get("/executions/{execution_id}/jobs")
async def get_jobs(
    execution_id: str,
    state: Optional[str] = None,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> List[Dict[str, Any]]:
    """Get jobs for an execution."""
    jobs = manager.job_manager.get_jobs(execution_id, state)
    return [job.to_dict() for job in jobs]


@app.get("/executions/{execution_id}/errors")
async def get_execution_errors(
    execution_id: str,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> List[Dict[str, Any]]:
    """Get all failed jobs with their error messages and tracebacks."""
    failed_jobs = manager.job_manager.get_jobs(execution_id, state="failed")
    if not failed_jobs:
        execution = manager.get_execution(execution_id)
        if not execution:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution '{execution_id}' not found",
            )
    return [
        {
            "job_id": job.job_id,
            "batch_number": job.batch_number,
            "error_message": job.error_message,
            "error_traceback": job.error_traceback,
            "failed_at": job.completed_at.isoformat() if job.completed_at else None,
        }
        for job in failed_jobs
    ]


@app.get("/executions/{execution_id}/checkpoints")
async def get_checkpoints(
    execution_id: str,
    state: Optional[str] = None,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> JSONResponse:
    """Deprecated: use GET /executions/{id}/jobs instead."""
    jobs = manager.job_manager.get_jobs(execution_id, state)
    return JSONResponse(
        content=[job.to_dict() for job in jobs],
        headers={
            "Deprecation": "true",
            "Link": f'/executions/{execution_id}/jobs; rel="successor-version"',
        },
    )


# ===== Pipeline Execution (New Simplified API) =====


@app.post("/run", status_code=status.HTTP_202_ACCEPTED)
async def run_pipeline(
    request: RunPipelineRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Run a pipeline (async endpoint).

    Returns immediately with 202 Accepted and execution details.
    Job splitting and dispatching happens in the background.

    Use GET /executions/{execution_id} to check progress.
    """
    import uuid
    from reflowfy.core.registry import pipeline_registry

    try:
        # Generate execution ID if not provided
        execution_id = request.execution_id or str(uuid.uuid4())

        # Validate pipeline exists before accepting
        pipeline = pipeline_registry.get(request.pipeline_name)
        if not pipeline:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=(
                    f"Pipeline '{request.pipeline_name}' not found in registry"
                    f"{pipeline_registry.describe_missing(request.pipeline_name)}"
                ),
            )

        # Create initial execution record (pending state)
        manager = _make_manager(db)
        manager.create_execution(
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            runtime_params=request.runtime_params or {},
            rate_limit=request.rate_limit,
        )

        # Reset cron schedule timer if this pipeline is scheduled,
        # so the next auto-run is one interval after this manual trigger.
        _reset_pipeline_schedule_if_needed(db, request.pipeline_name)
        db.commit()

        # Schedule job dispatching in background
        background_tasks.add_task(
            _dispatch_pipeline_jobs,
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            runtime_params=request.runtime_params or {},
            rate_limit_override=request.rate_limit,
            mode=request.mode,
            enable_duplicate_jobs=request.enable_duplicate_jobs,
        )

        # Return immediately with execution details
        return {
            "execution_id": execution_id,
            "pipeline_name": request.pipeline_name,
            "state": "pending",  # Will become "running" once background task starts
            "message": "Pipeline execution accepted. Jobs are being dispatched in background.",
            "status_url": f"/executions/{execution_id}",
        }

    except HTTPException:
        raise

    except Exception as e:
        logger.exception("Failed to start pipeline %s", request.pipeline_name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start pipeline: {str(e)}",
        )


def _reset_pipeline_schedule_if_needed(db: Session, pipeline_name: str) -> None:
    """Recalculate next_run_at after a manual trigger for a scheduled pipeline."""
    from reflowfy.core.registry import pipeline_registry

    scheduler = get_pipeline_scheduler()
    if scheduler is None:
        return
    pipeline = pipeline_registry.get(pipeline_name)
    if pipeline is None or not getattr(pipeline, "is_scheduled", False):
        return
    scheduler.reset_schedule(
        db, pipeline_name, triggered_at=datetime.now(timezone.utc).replace(tzinfo=None)
    )


def _dispatch_pipeline_jobs(
    execution_id: str,
    pipeline_name: str,
    runtime_params: Dict[str, Any],
    rate_limit_override: Optional[float] = None,
    mode: Optional[str] = None,
    enable_duplicate_jobs: Optional[bool] = None,
) -> None:
    """Background task to dispatch pipeline jobs."""
    # Create a new database session for background task
    db = SessionLocal()

    try:
        manager = _make_manager(db, mode=mode)

        # Run the pipeline (this updates the existing execution record)
        manager.run_pipeline_jobs(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            rate_limit_override=rate_limit_override,
            enable_duplicate_jobs=enable_duplicate_jobs,
        )

    except Exception as e:
        logger.exception("Background job dispatch failed for %s", execution_id)

        # Update execution state to failed. Use a fresh ExecutionManager so this
        # works even when ReflowManager construction above failed (manager unbound).
        try:
            ExecutionManager(db).update_execution_state(
                execution_id, "failed", error_message=str(e)
            )
        except Exception:
            logger.exception("Failed to mark execution %s as failed", execution_id)

    finally:
        db.close()


# ===== Statistics =====


@app.get("/executions/{execution_id}/stats")
async def get_execution_stats(
    execution_id: str,
    manager: ReflowManager = Depends(get_reflow_manager),
) -> Dict[str, Any]:
    """Get detailed execution statistics."""
    try:
        stats = manager.get_execution_stats(execution_id)

        if not stats:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Execution '{execution_id}' not found",
            )

        return stats
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error fetching execution stats for %s", execution_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching execution stats: {str(e)}",
        )


async def _startup() -> None:
    """Initialize database and load pipelines on startup."""
    setup_logging(service_name="reflow-manager")
    logger.info("Starting ReflowManager service (version %s)", __version__)

    # Initialize database (retries internally until DB is ready)
    global _db_ready
    logger.info("Initializing database...")
    init_db()
    _db_ready = True
    logger.info("Database initialized")

    # Load pipelines using global discovery (module from PIPELINE_MODULE env)
    from reflowfy.core.pipeline_discovery import discover_and_load_pipelines

    discover_and_load_pipelines()

    logger.info(
        "Kafka=%s topic=%s rate_limit=%s jobs/min",
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        os.getenv("KAFKA_TOPIC", "reflow.jobs"),
        os.getenv("MAX_JOBS_PER_MINUTE", "6000"),
    )

    # Check for interrupted executions and recover
    logger.info("Checking for interrupted executions...")
    await _recover_interrupted_executions()

    # Initialize DLQ scheduler
    try:
        init_dlq_scheduler(_pipeline_runner_factory)
        logger.info("DLQ Scheduler initialized")
    except Exception:
        logger.exception("Failed to initialize DLQ scheduler")

    # Initialize Pipeline Scheduler (cron-based)
    try:
        scheduler = init_pipeline_scheduler(_pipeline_runner_factory)

        sync_db = SessionLocal()
        try:
            scheduler.sync_schedules_from_registry(sync_db)
            sync_db.commit()
            logger.info("Pipeline Scheduler initialized and schedules synced")
        finally:
            sync_db.close()
    except Exception:
        logger.exception("Failed to initialize Pipeline Scheduler")

    # Initialize Content Dedup Sweeper
    try:
        init_content_dedup_scheduler()
        logger.info("Content Dedup Sweeper initialized")
    except Exception:
        logger.exception("Failed to initialize Content Dedup Sweeper")


async def _recover_interrupted_executions() -> None:
    """Find and resume any executions that were interrupted by a crash."""
    import asyncio

    db = SessionLocal()
    try:
        manager = _make_manager(db)

        # Dispatch leases held by an earlier process on this host died with it (a
        # container restart keeps the hostname). Free them so recovery resumes now
        # instead of standing by until the dead loop's heartbeat goes stale.
        released = manager.execution_manager.release_drivers_on_host(socket.gethostname())
        if released:
            logger.info("Released %d dispatch lease(s) left by a previous process", released)

        # Find interrupted executions
        interrupted = manager.execution_manager.get_interrupted_executions()

        if not interrupted:
            logger.info("No interrupted executions found")
            return

        logger.info("Found %d interrupted execution(s)", len(interrupted))

        # Resume each in a background thread (don't block startup)
        for execution in interrupted:
            logger.info("Scheduling resume for: %s", execution.execution_id)
            # Use run_in_executor to avoid blocking the event loop
            asyncio.get_event_loop().run_in_executor(
                None,  # Default executor
                _resume_execution_sync,
                execution.execution_id,
            )

        logger.info("Scheduled %d execution(s) for recovery", len(interrupted))

    finally:
        db.close()


def _resume_execution_sync(execution_id: str) -> None:
    """Synchronously resume an execution (runs in thread pool)."""
    db = SessionLocal()
    try:
        manager = _make_manager(db)
        manager.pipeline_runner.resume_execution(execution_id)
    except Exception:
        logger.exception("Failed to resume execution %s", execution_id)
    finally:
        db.close()


async def _shutdown() -> None:
    """Gracefully stop background schedulers on service shutdown."""
    logger.info("Shutting down ReflowManager service...")
    stop_pipeline_scheduler()
    stop_dlq_scheduler()
    stop_content_dedup_scheduler()
    logger.info("Shutdown complete")


# Main entry point
def main() -> None:
    """Run the FastAPI application."""
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8001"))

    logger.info("ReflowManager service starting on http://%s:%d", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
