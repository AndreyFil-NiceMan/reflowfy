"""FastAPI application for ReflowManager service."""

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator, Optional
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

from reflowfy import __version__  # noqa: E402

from reflowfy.reflow_manager.database import get_db, init_db  # noqa: E402
from reflowfy.reflow_manager.manager import ReflowManager  # noqa: E402
from reflowfy.reflow_manager.execution import ExecutionManager  # noqa: E402
from reflowfy.reflow_manager.schemas import (  # noqa: E402
    UpdateExecutionStateRequest,
    CheckpointRequest,
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

# Include DLQ router
app.include_router(dlq_router)

# Include Stats router
app.include_router(stats_router)

# Include Schedule router
app.include_router(schedule_router)


# Helper to get Kafka configuration from environment (including SASL)
def _get_kafka_config() -> dict:
    """Get Kafka configuration from environment variables."""
    return {
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "kafka_topic": os.getenv("KAFKA_TOPIC", "reflow.jobs"),
        "max_jobs_per_second": float(os.getenv("MAX_JOBS_PER_SECOND", "100")),
        # Execution mode: "distributed" (Kafka) or "local" (in-process)
        "execution_mode": os.getenv("EXECUTION_MODE", "distributed"),
        # SASL Authentication (set by Helm chart)
        "kafka_security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
        "kafka_sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "kafka_sasl_username": os.getenv("KAFKA_SASL_USERNAME"),
        "kafka_sasl_password": os.getenv("KAFKA_SASL_PASSWORD"),
    }


# Dependency to get ReflowManager instance
def get_reflow_manager(db: Session = Depends(get_db)) -> ReflowManager:
    """Get ReflowManager instance with database session."""
    config = _get_kafka_config()

    return ReflowManager(
        db_session=db,
        **config,
    )


# ===== Health Check =====

_db_ready: bool = False  # set to True after successful init_db()


@app.get("/health")
async def health_check():
    """Health check endpoint. Returns 503 until the DB is initialized."""
    if not _db_ready:
        from fastapi.responses import JSONResponse

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
async def get_pipeline(pipeline_name: str):
    """Get metadata for a registered pipeline by name."""
    from reflowfy.core.registry import pipeline_registry

    pipeline = pipeline_registry.get(pipeline_name)
    if not pipeline:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pipeline '{pipeline_name}' not found in registry",
        )
    return pipeline.to_dict()


@app.get("/pipelines")
async def list_pipelines():
    """List all registered pipelines."""
    from reflowfy.core.registry import pipeline_registry

    return [p.to_dict() for p in pipeline_registry.list_all()]


# ===== Execution Management =====


@app.patch("/executions/{execution_id}/state")
async def update_execution_state(
    execution_id: str,
    request: UpdateExecutionStateRequest,
    manager: ReflowManager = Depends(get_reflow_manager),
):
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
):
    """Pause an execution."""
    execution = manager.pause_execution(execution_id)

    if not execution:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution '{execution_id}' not found",
        )

    return {"message": "Execution paused", "execution": execution.to_dict()}


@app.post("/executions/{execution_id}/resume")
async def resume_execution(
    execution_id: str,
    manager: ReflowManager = Depends(get_reflow_manager),
):
    """Resume a paused execution."""
    execution = manager.resume_execution(execution_id)

    if not execution:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Execution '{execution_id}' not found or not paused",
        )

    return {"message": "Execution resumed", "execution": execution.to_dict()}


# ===== Checkpointing =====


@app.post("/checkpoints", status_code=status.HTTP_201_CREATED)
async def create_checkpoint(
    request: CheckpointRequest,
    manager: ReflowManager = Depends(get_reflow_manager),
):
    """Create a checkpoint."""
    try:
        checkpoint = manager.create_checkpoint(
            execution_id=request.execution_id,
            job_id=request.job_id,
            offset_data=request.offset_data,
            processed_records=request.processed_records,
        )
        return checkpoint.to_dict()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create checkpoint: {str(e)}",
        )


@app.get("/executions/{execution_id}/jobs")
async def get_jobs(
    execution_id: str,
    state: Optional[str] = None,
    manager: ReflowManager = Depends(get_reflow_manager),
):
    """Get jobs for an execution."""
    jobs = manager.job_manager.get_jobs(execution_id, state)
    return [job.to_dict() for job in jobs]


@app.get("/executions/{execution_id}/errors")
async def get_execution_errors(
    execution_id: str,
    manager: ReflowManager = Depends(get_reflow_manager),
):
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
):
    """Deprecated: use GET /executions/{id}/jobs instead."""
    from fastapi.responses import JSONResponse

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
):
    """
    Run a pipeline (async endpoint).

    Returns immediately with 202 Accepted and execution details.
    Job splitting and dispatching happens in the background.

    Use GET /executions/{execution_id} to check progress.

    Set dry_run=true to preview jobs without executing.
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
                detail=f"Pipeline '{request.pipeline_name}' not found in registry",
            )

        # Get ReflowManager config from environment
        config = _get_kafka_config()

        # Create initial execution record (pending state)
        manager = ReflowManager(
            db_session=db,
            **config,
        )

        manager.create_execution(
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            runtime_params=request.runtime_params or {},
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
        import traceback

        traceback.print_exc()
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
    runtime_params: dict,
    rate_limit_override: Optional[float] = None,
    mode: Optional[str] = None,
    enable_duplicate_jobs: Optional[bool] = None,
):
    """Background task to dispatch pipeline jobs."""
    from reflowfy.reflow_manager.database import SessionLocal

    # Create a new database session for background task
    db = SessionLocal()

    try:
        config = _get_kafka_config()

        # Override execution_mode if mode was specified in the request
        if mode:
            config["execution_mode"] = mode

        manager = ReflowManager(
            db_session=db,
            **config,
        )

        # Run the pipeline (this updates the existing execution record)
        manager._run_pipeline_jobs(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            rate_limit_override=rate_limit_override,
            enable_duplicate_jobs=enable_duplicate_jobs,
        )

    except Exception as e:
        import traceback

        print(f"❌ Background job dispatch failed: {e}")
        traceback.print_exc()

        # Update execution state to failed. Use a fresh ExecutionManager so this
        # works even when ReflowManager construction above failed (manager unbound).
        try:
            ExecutionManager(db).update_execution_state(
                execution_id, "failed", error_message=str(e)
            )
        except Exception:
            pass

    finally:
        db.close()


# ===== Statistics =====


@app.get("/executions/{execution_id}/stats")
async def get_execution_stats(
    execution_id: str,
    manager: ReflowManager = Depends(get_reflow_manager),
):
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
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching execution stats: {str(e)}",
        )


async def _startup() -> None:
    """Initialize database and load pipelines on startup."""

    print("=" * 60)
    print("🚀 Starting ReflowManager service...")
    print(f"📦 Version: {__version__}")
    print("=" * 60)

    # Initialize database (retries internally until DB is ready)
    global _db_ready
    print("Initializing database...")
    init_db()
    _db_ready = True
    print("✓ Database initialized")

    # Load pipelines using global discovery
    from reflowfy.core.pipeline_discovery import discover_and_load_pipelines

    pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
    print(f"\n📂 Loading pipelines from '{pipeline_module}'...")
    discover_and_load_pipelines(pipeline_module)

    print(f"\n✓ Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    print(f"✓ Topic: {os.getenv('KAFKA_TOPIC', 'reflow.jobs')}")
    print(f"✓ Rate limit: {os.getenv('MAX_JOBS_PER_SECOND', '100')} jobs/sec")

    # Check for interrupted executions and recover
    print("\n🔄 Checking for interrupted executions...")
    await _recover_interrupted_executions()

    # Initialize DLQ scheduler
    print("\n📬 Initializing DLQ scheduler...")
    try:

        def pipeline_runner_factory():
            from reflowfy.reflow_manager.database import SessionLocal

            db = SessionLocal()
            config = _get_kafka_config()
            manager = ReflowManager(
                db_session=db,
                **config,
            )
            return manager.pipeline_runner

        init_dlq_scheduler(pipeline_runner_factory)
        print("✅ DLQ Scheduler initialized")
    except Exception as e:
        print(f"⚠️ Failed to initialize DLQ scheduler: {e}")

    # Initialize Pipeline Scheduler (cron-based)
    print("\n⏰ Initializing Pipeline Scheduler...")
    try:

        def pipeline_scheduler_factory():
            from reflowfy.reflow_manager.database import SessionLocal

            db = SessionLocal()
            config = _get_kafka_config()
            manager = ReflowManager(
                db_session=db,
                **config,
            )
            return manager.pipeline_runner

        scheduler = init_pipeline_scheduler(pipeline_scheduler_factory)

        from reflowfy.reflow_manager.database import SessionLocal as _SL

        sync_db = _SL()
        try:
            scheduler.sync_schedules_from_registry(sync_db)
            sync_db.commit()
            print("✅ Pipeline Scheduler initialized and schedules synced")
        finally:
            sync_db.close()

    except Exception as e:
        print(f"⚠️  Failed to initialize Pipeline Scheduler: {e}")

    # Initialize Content Dedup Sweeper
    print("\n🧹 Initializing Content Dedup Sweeper...")
    try:
        init_content_dedup_scheduler()
        print("✅ Content Dedup Sweeper initialized")
    except Exception as e:
        print(f"⚠️ Failed to initialize Content Dedup Sweeper: {e}")


async def _recover_interrupted_executions():
    """Find and resume any executions that were interrupted by a crash."""
    import asyncio
    from reflowfy.reflow_manager.database import SessionLocal

    db = SessionLocal()
    try:
        config = _get_kafka_config()

        manager = ReflowManager(
            db_session=db,
            **config,
        )

        # Find interrupted executions
        interrupted = manager.execution_manager.get_interrupted_executions()

        if not interrupted:
            print("  ✓ No interrupted executions found")
            return

        print(f"  Found {len(interrupted)} interrupted execution(s)")

        # Resume each in a background thread (don't block startup)
        for execution in interrupted:
            print(f"  → Scheduling resume for: {execution.execution_id}")
            # Use run_in_executor to avoid blocking the event loop
            asyncio.get_event_loop().run_in_executor(
                None,  # Default executor
                _resume_execution_sync,
                execution.execution_id,
            )

        print(f"  ✓ Scheduled {len(interrupted)} execution(s) for recovery")

    finally:
        db.close()


def _resume_execution_sync(execution_id: str):
    """Synchronously resume an execution (runs in thread pool)."""
    from reflowfy.reflow_manager.database import SessionLocal

    db = SessionLocal()
    try:
        config = _get_kafka_config()

        manager = ReflowManager(
            db_session=db,
            **config,
        )

        manager.pipeline_runner.resume_execution(execution_id)

    except Exception as e:
        import traceback

        print(f"❌ Failed to resume execution {execution_id}: {e}")
        traceback.print_exc()


async def _shutdown() -> None:
    """Gracefully stop background schedulers on service shutdown."""
    print("\n🛑 Shutting down ReflowManager service...")
    stop_pipeline_scheduler()
    stop_dlq_scheduler()
    stop_content_dedup_scheduler()
    print("✅ Shutdown complete")


# Main entry point
def main():
    """Run the FastAPI application."""
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8001"))

    print("=" * 60)
    print("🔧 ReflowManager Service")
    print("=" * 60)
    print(f"🌐 Server starting on http://{host}:{port}")
    print(f"📖 API docs at http://localhost:{port}/docs\n")

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
