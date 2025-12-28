"""
ReflowManager - Central coordinator for pipeline execution.

This is a slim coordinator that composes the following modules:
- ExecutionManager: Execution records management
- CheckpointManager: Job checkpoint tracking
- JobManager: Job payload persistence
- RateLimiter: Token bucket rate limiting
- JobDispatcher: Kafka job dispatch
- PipelineRunner: Pipeline execution
"""

from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import Integer

from reflowfy.reflow_manager.models import Execution, Checkpoint, RateLimitState, Job
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.checkpoint import CheckpointManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.rate_limiter import RateLimiter
from reflowfy.reflow_manager.dispatcher import JobDispatcher
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner


class ReflowManager:
    """
    Central manager for pipeline execution state and rate limiting.
    
    This is a coordinator class that composes specialized managers
    for execution, checkpointing, rate limiting, dispatch, and pipeline running.
    """
    
    def __init__(
        self,
        db_session: Session,
        kafka_bootstrap_servers: str = "localhost:9092",
        kafka_topic: str = "reflow.jobs",
        max_jobs_per_second: float = 100.0,
    ):
        """
        Initialize ReflowManager.
        
        Args:
            db_session: SQLAlchemy database session
            kafka_bootstrap_servers: Kafka broker addresses
            kafka_topic: Topic for job dispatch
            max_jobs_per_second: Default global rate limit
        """
        self.db = db_session
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.max_jobs_per_second = max_jobs_per_second
        
        # Initialize component managers
        self.execution_manager = ExecutionManager(db_session)
        self.checkpoint_manager = CheckpointManager(db_session)
        self.job_manager = JobManager(db_session)
        self.rate_limiter = RateLimiter(db_session, max_jobs_per_second)
        self.dispatcher = JobDispatcher(
            kafka_bootstrap_servers,
            kafka_topic,
            self.rate_limiter,
        )
        self.pipeline_runner = PipelineRunner(
            self.execution_manager,
            self.checkpoint_manager,
            self.job_manager,
            self.dispatcher,
        )
    
    # ===== Execution Management (delegated) =====
    
    def create_execution(self, execution_id: str, pipeline_name: str, 
                        runtime_params: Optional[Dict[str, Any]] = None) -> Execution:
        return self.execution_manager.create_execution(execution_id, pipeline_name, runtime_params)
    
    def get_execution(self, execution_id: str) -> Optional[Execution]:
        return self.execution_manager.get_execution(execution_id)
    
    def update_execution_state(self, execution_id: str, state: str, 
                              error_message: Optional[str] = None) -> Optional[Execution]:
        return self.execution_manager.update_execution_state(execution_id, state, error_message)
    
    def update_job_counts(self, execution_id: str, jobs_dispatched: Optional[int] = None,
                         jobs_completed: Optional[int] = None, jobs_failed: Optional[int] = None):
        return self.execution_manager.update_job_counts(execution_id, jobs_dispatched, jobs_completed, jobs_failed)
    
    def pause_execution(self, execution_id: str) -> Optional[Execution]:
        return self.execution_manager.pause_execution(execution_id)
    
    def resume_execution(self, execution_id: str) -> Optional[Execution]:
        return self.execution_manager.resume_execution(execution_id)
    
    # ===== Checkpoint Management (delegated) =====
    
    def create_checkpoint(self, execution_id: str, batch_id: str,
                         offset_data: Optional[Dict[str, Any]] = None,
                         processed_records: int = 0) -> Checkpoint:
        return self.checkpoint_manager.create_checkpoint(execution_id, batch_id, offset_data, processed_records)
    
    def update_checkpoint_state(self, batch_id: str, state: str,
                               processed_records: Optional[int] = None,
                               error_message: Optional[str] = None) -> Optional[Checkpoint]:
        return self.checkpoint_manager.update_checkpoint_state(batch_id, state, processed_records, error_message)
    
    def get_checkpoints(self, execution_id: str, state: Optional[str] = None) -> List[Checkpoint]:
        return self.checkpoint_manager.get_checkpoints(execution_id, state)
    
    # ===== Rate Limiting (delegated) =====
    
    def can_dispatch(self, pipeline_name: str, count: int = 1, rate_limit: Optional[float] = None) -> bool:
        return self.rate_limiter.can_dispatch(pipeline_name, count, rate_limit)
    
    def consume_tokens(self, pipeline_name: str, count: int = 1, rate_limit: Optional[float] = None) -> bool:
        return self.rate_limiter.consume_tokens(pipeline_name, count, rate_limit)
    
    def acquire_token(self, pipeline_name: str, rate_limit: Optional[float] = None, max_wait: float = 1.0) -> bool:
        return self.rate_limiter.acquire_token(pipeline_name, rate_limit, max_wait)
    
    # ===== Job Dispatch (delegated) =====
    
    def dispatch_job(self, job_payload: Dict[str, Any], pipeline_name: str, 
                    rate_limit: Optional[float] = None) -> bool:
        return self.dispatcher.dispatch_job(job_payload, pipeline_name, rate_limit)
    
    def dispatch_jobs_batch(self, jobs: List[Dict[str, Any]], pipeline_name: str,
                           rate_limit: Optional[float] = None) -> int:
        return self.dispatcher.dispatch_jobs_batch(jobs, pipeline_name, rate_limit)
    
    # ===== Pipeline Execution (delegated) =====
    
    def run_pipeline(self, pipeline_name: str, runtime_params: Dict[str, Any],
                    execution_id: str, rate_limit_override: Optional[float] = None) -> Dict[str, Any]:
        return self.pipeline_runner.run_pipeline(pipeline_name, runtime_params, execution_id, rate_limit_override)
    
    def _run_pipeline_jobs(self, execution_id: str, pipeline_name: str,
                          runtime_params: Dict[str, Any], 
                          rate_limit_override: Optional[float] = None) -> None:
        return self.pipeline_runner._run_pipeline_jobs(execution_id, pipeline_name, runtime_params, rate_limit_override)
    
    # ===== Statistics =====
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get global statistics."""
        active = self.db.query(Execution).filter(
            Execution.state.in_(["pending", "running", "paused"])
        ).count()
        
        total_dispatched = self.db.query(Execution).with_entities(
            Execution.jobs_dispatched
        ).all()
        total_completed = self.db.query(Execution).with_entities(
            Execution.jobs_completed
        ).all()
        total_failed = self.db.query(Execution).with_entities(
            Execution.jobs_failed
        ).all()
        
        return {
            "active_executions": active,
            "total_jobs_dispatched": sum(e[0] for e in total_dispatched),
            "total_jobs_completed": sum(e[0] for e in total_completed),
            "total_jobs_failed": sum(e[0] for e in total_failed),
        }
    
    def get_execution_stats(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed execution statistics."""
        execution = self.get_execution(execution_id)
        if not execution:
            return None
        
        # Get total job counts from jobs table
        job_counts = self.job_manager.get_job_counts(execution_id)
        
        # Get checkpoint batch stats
        checkpoints = self._get_batch_checkpoint_stats(execution_id)
        
        # Find current checkpoint (first non-completed batch)
        current_checkpoint = None
        for cp in checkpoints:
            if cp["state"] != "completed":
                current_checkpoint = cp["batch_number"]
                break
        
        # If all completed, current_checkpoint is the last one
        if current_checkpoint is None and checkpoints:
            current_checkpoint = len(checkpoints)
        
        return {
            "execution_id": execution.execution_id,
            "pipeline_name": execution.pipeline_name,
            "state": execution.state,
            "total_jobs": execution.total_jobs,
            "jobs_dispatched": job_counts.get("dispatched", 0) + job_counts.get("completed", 0) + job_counts.get("failed", 0),
            "jobs_pending": job_counts.get("pending", 0),
            "jobs_completed": job_counts.get("completed", 0),
            "jobs_failed": job_counts.get("failed", 0),
            "current_checkpoint": current_checkpoint,
            "created_at": execution.created_at.isoformat() if execution.created_at else None,
            "updated_at": execution.updated_at.isoformat() if execution.updated_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "error_message": execution.error_message,
            "runtime_params": execution.runtime_params,
            "checkpoints": checkpoints,
        }
    
    def _get_batch_checkpoint_stats(self, execution_id: str) -> List[Dict[str, Any]]:
        """Get checkpoint stats grouped by batch_number (uses checkpoint states as source of truth)."""
        from reflowfy.reflow_manager.models import Job, Checkpoint
        from sqlalchemy import func, case
        
        # Join jobs and checkpoints to get accurate states from checkpoints
        results = self.db.query(
            Job.batch_number,
            func.count(Job.id).label("total_jobs"),
            func.sum(case((Checkpoint.state == "pending", 1), else_=0)).label("pending"),
            func.sum(case((Checkpoint.state == "completed", 1), else_=0)).label("completed"),
            func.sum(case((Checkpoint.state == "failed", 1), else_=0)).label("failed"),
        ).join(
            Checkpoint, Job.batch_id == Checkpoint.batch_id
        ).filter(
            Job.execution_id == execution_id,
            Job.batch_number.isnot(None)
        ).group_by(Job.batch_number).order_by(Job.batch_number).all()
        
        batches = []
        for row in results:
            # Determine batch state from checkpoint states
            if row.failed and row.failed > 0:
                state = "failed"
            elif row.completed == row.total_jobs:
                state = "completed"
            elif row.completed and row.completed > 0:
                state = "in_progress"
            else:
                state = "pending"
            
            batches.append({
                "batch_number": row.batch_number,
                "total_jobs": row.total_jobs,
                "pending": row.pending or 0,
                "completed": row.completed or 0,
                "failed": row.failed or 0,
                "state": state,
            })
        
        return batches
    
    # ===== Cleanup =====
    
    def close(self):
        """Close connections."""
        self.dispatcher.close()
