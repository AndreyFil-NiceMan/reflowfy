"""FastAPI application for ReflowManager service."""

import os
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.database import get_db, init_db
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.reflow_manager.schemas import (
    CreateExecutionRequest,
    UpdateExecutionStateRequest,
    DispatchJobsRequest,
    UpdateJobStatusRequest,
    CheckpointRequest,
    RunPipelineRequest,
)


# Create FastAPI app
app = FastAPI(
    title="ReflowManager",
    description="Pipeline state management and rate limiting service",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency to get ReflowManager instance
def get_reflow_manager(db: Session = Depends(get_db)) -> ReflowManager:
    """Get ReflowManager instance with database session."""
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "reflow.jobs")
    max_jobs_per_second = float(os.getenv("MAX_JOBS_PER_SECOND", "100"))
    
    return ReflowManager(
        db_session=db,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=kafka_topic,
        max_jobs_per_second=max_jobs_per_second,
    )


# ===== Health Check =====

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "reflow-manager",
    }


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


@app.get("/executions/{execution_id}/checkpoints")
async def get_checkpoints(
    execution_id: str,
    state: Optional[str] = None,
    manager: ReflowManager = Depends(get_reflow_manager),
):
    """Get jobs (checkpoints) for an execution."""
    jobs = manager.job_manager.get_jobs(execution_id, state)
    return [job.to_dict() for job in jobs]


@app.patch("/checkpoints/{job_id}")
async def update_checkpoint(
    job_id: str,
    request: UpdateJobStatusRequest,
    manager: ReflowManager = Depends(get_reflow_manager),
):
    """
    Update job/checkpoint status (called by workers after processing).
    
    Updates job state and execution counts in a single transaction.
    """
    # Get the job to update
    job = manager.job_manager.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job for batch '{job_id}' not found",
        )
    
    execution_id = job.execution_id
    old_state = job.state
    
    # Update job state with all checkpoint fields
    manager.job_manager.update_job_state(
        job_id,
        request.state,
        processed_records=request.processed_records,
        error_message=request.error_message,
        stats=request.stats,
    )
    
    # Update execution counts if state changed to completed/failed
    if old_state not in ["completed", "failed"]:
        if request.state == "completed":
            manager.update_job_counts(execution_id, jobs_completed=1)
        elif request.state == "failed":
            manager.update_job_counts(execution_id, jobs_failed=1)
    
    # Return updated job
    job = manager.job_manager.get_job(job_id)
    return job.to_dict()



    """Dispatch jobs to Kafka with rate limiting."""
    try:
        # Update execution state to running
        manager.update_execution_state(request.execution_id, "running")
        
        # Create checkpoints for all jobs
        for job in request.jobs:
            manager.create_checkpoint(
                execution_id=request.execution_id,
                job_id=job.get("job_id", ""),
            )
        
        # Dispatch jobs with rate limiting
        dispatched = manager.dispatch_jobs_batch(
            jobs=request.jobs,
            pipeline_name=request.pipeline_name,
            rate_limit=request.rate_limit,
        )
        
        # Update job counts
        manager.update_job_counts(request.execution_id, jobs_dispatched=dispatched)
        
        return {
            "execution_id": request.execution_id,
            "total_jobs": len(request.jobs),
            "dispatched": dispatched,
            "rate_limited": len(request.jobs) - dispatched,
        }
    
    except Exception as e:
        # Mark execution as failed
        manager.update_execution_state(
            request.execution_id,
            "failed",
            error_message=str(e),
        )
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to dispatch jobs: {str(e)}",
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
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        kafka_topic = os.getenv("KAFKA_TOPIC", "reflow.jobs")
        max_jobs_per_second = float(os.getenv("MAX_JOBS_PER_SECOND", "100"))
        
        # Create initial execution record (pending state)
        manager = ReflowManager(
            db_session=db,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            max_jobs_per_second=max_jobs_per_second,
        )
        
        execution = manager.create_execution(
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            runtime_params=request.runtime_params or {},
        )
        
        # Schedule job dispatching in background
        background_tasks.add_task(
            _dispatch_pipeline_jobs,
            execution_id=execution_id,
            pipeline_name=request.pipeline_name,
            runtime_params=request.runtime_params or {},
            rate_limit_override=request.rate_limit,
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


def _dispatch_pipeline_jobs(
    execution_id: str,
    pipeline_name: str,
    runtime_params: dict,
    rate_limit_override: Optional[float] = None,
):
    """Background task to dispatch pipeline jobs."""
    from reflowfy.reflow_manager.database import SessionLocal
    
    # Create a new database session for background task
    db = SessionLocal()
    
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        kafka_topic = os.getenv("KAFKA_TOPIC", "reflow.jobs")
        max_jobs_per_second = float(os.getenv("MAX_JOBS_PER_SECOND", "100"))
        
        manager = ReflowManager(
            db_session=db,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            max_jobs_per_second=max_jobs_per_second,
        )
        
        # Run the pipeline (this updates the existing execution record)
        manager._run_pipeline_jobs(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            rate_limit_override=rate_limit_override,
        )
        
    except Exception as e:
        import traceback
        print(f"❌ Background job dispatch failed: {e}")
        traceback.print_exc()
        
        # Update execution state to failed
        try:
            manager.update_execution_state(execution_id, "failed", error_message=str(e))
        except:
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
    stats = manager.get_execution_stats(execution_id)
    
    if not stats:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Execution '{execution_id}' not found",
        )
    
    return stats



# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize database and load pipelines on startup."""
    import importlib
    import pkgutil
    from pathlib import Path
    
    print("🚀 Starting ReflowManager service...")
    
    # Initialize database
    print("Initializing database...")
    init_db()
    print("✓ Database initialized")
    
    # Load pipelines
    pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
    print(f"\n📂 Discovering pipelines in '{pipeline_module}'...")
    
    try:
        pipelines_package = importlib.import_module(pipeline_module)
        package_path = Path(pipelines_package.__file__).parent
        
        loaded_count = 0
        for _, module_name, is_pkg in pkgutil.iter_modules([str(package_path)]):
            if not is_pkg:
                try:
                    full_module = f"{pipeline_module}.{module_name}"
                    importlib.import_module(full_module)
                    print(f"  ✓ Loaded {module_name}.py")
                    loaded_count += 1
                except Exception as e:
                    print(f"  ✗ Failed to load {module_name}.py: {e}")
        
        if loaded_count == 0:
            print(f"  ⚠️  No pipeline files found in '{pipeline_module}'")
        else:
            print(f"  ✓ Loaded {loaded_count} pipeline file(s)")
    except ImportError:
        print(f"  ⚠️  Module '{pipeline_module}' not found - no pipelines loaded")
    
    print(f"\n✓ Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    print(f"✓ Topic: {os.getenv('KAFKA_TOPIC', 'reflow.jobs')}")
    print(f"✓ Rate limit: {os.getenv('MAX_JOBS_PER_SECOND', '100')} jobs/sec")




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
