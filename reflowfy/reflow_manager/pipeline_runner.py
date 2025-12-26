"""Pipeline execution runner for ReflowManager."""

import uuid
from typing import Dict, Any, Optional

from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.checkpoint import CheckpointManager
from reflowfy.reflow_manager.dispatcher import JobDispatcher


class PipelineRunner:
    """
    Executes pipelines by splitting jobs and dispatching to Kafka.
    
    Coordinates between:
    - ExecutionManager for execution records
    - CheckpointManager for job tracking
    - JobDispatcher for Kafka dispatch
    """
    
    def __init__(
        self,
        execution_manager: ExecutionManager,
        checkpoint_manager: CheckpointManager,
        dispatcher: JobDispatcher,
    ):
        """
        Initialize pipeline runner.
        
        Args:
            execution_manager: ExecutionManager instance
            checkpoint_manager: CheckpointManager instance
            dispatcher: JobDispatcher instance
        """
        self.execution_manager = execution_manager
        self.checkpoint_manager = checkpoint_manager
        self.dispatcher = dispatcher
    
    def run_pipeline(
        self,
        pipeline_name: str,
        runtime_params: Dict[str, Any],
        execution_id: str,
        rate_limit_override: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Execute a pipeline by splitting jobs and dispatching to Kafka.
        
        This creates a new execution record, then runs the pipeline.
        
        Args:
            pipeline_name: Name of the registered pipeline
            runtime_params: Runtime parameters for the pipeline
            execution_id: Unique execution identifier
            rate_limit_override: Optional override for jobs per second
        
        Returns:
            Dictionary with execution details and job counts
        """
        from reflowfy.core.registry import pipeline_registry
        from reflowfy.core.execution_context import ExecutionContext
        
        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found in registry")
        
        print(f"🚀 Running pipeline: {pipeline_name}")
        print(f"📊 Execution ID: {execution_id}")
        
        # Create execution record
        execution = self.execution_manager.create_execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
        )
        
        # Run the job dispatch
        self._run_pipeline_jobs(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            rate_limit_override=rate_limit_override,
        )
        
        # Refresh execution to get final counts
        execution = self.execution_manager.get_execution(execution_id)
        
        return {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "state": execution.state,
            "jobs_dispatched": execution.jobs_dispatched,
            "rate_limit": rate_limit_override,
        }
    
    def _run_pipeline_jobs(
        self,
        execution_id: str,
        pipeline_name: str,
        runtime_params: Dict[str, Any],
        rate_limit_override: Optional[float] = None,
    ) -> None:
        """
        Dispatch pipeline jobs for an existing execution.
        
        Used by background tasks when execution already exists.
        
        Args:
            execution_id: Existing execution identifier
            pipeline_name: Name of the registered pipeline
            runtime_params: Runtime parameters for the pipeline
            rate_limit_override: Optional override for jobs per second
        """
        from reflowfy.core.registry import pipeline_registry
        from reflowfy.core.execution_context import ExecutionContext
        
        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found in registry")
        
        print(f"🚀 Job dispatch starting: {pipeline_name}")
        print(f"📊 Execution ID: {execution_id}")
        
        # Update state to running
        self.execution_manager.update_execution_state(execution_id, "running")
        
        # Create execution context
        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
        )
        
        # Determine effective rate limit
        effective_rate_limit = rate_limit_override
        if effective_rate_limit is None and pipeline.rate_limit:
            effective_rate_limit = pipeline.rate_limit.get("jobs_per_second")
        
        print(f"🔄 Splitting source data into jobs (rate: {effective_rate_limit}/sec)...")
        
        # Split source into jobs
        jobs = []
        for source_job in pipeline.source.split_jobs(runtime_params):
            batch_id = str(uuid.uuid4())
            
            # Create job payload
            job_payload = {
                "execution_id": execution_id,
                "batch_id": batch_id,
                "pipeline_name": pipeline_name,
                "transformations": pipeline.get_transformation_names(),
                "destination": {
                    "type": pipeline.destination.__class__.__name__,
                    "config": pipeline.destination.config,
                },
                "rate_limit": pipeline.rate_limit,
                "records": source_job.records,
                "metadata": {
                    **context.to_dict(),
                    "source_metadata": source_job.metadata,
                },
            }
            
            # Serialize to handle non-JSON-serializable objects
            job_payload = self._serialize_for_json(job_payload)
            
            # Create checkpoint for this job
            self.checkpoint_manager.create_checkpoint(
                execution_id=execution_id,
                batch_id=batch_id,
                offset_data=source_job.metadata,
            )
            
            jobs.append(job_payload)
            
            # Dispatch in batches to avoid memory issues
            if len(jobs) >= 50:
                dispatched = self.dispatcher.dispatch_jobs_batch(
                    jobs=jobs,
                    pipeline_name=pipeline_name,
                    rate_limit=effective_rate_limit,
                )
                self.execution_manager.update_job_counts(execution_id, jobs_dispatched=dispatched)
                print(f"  ✓ Dispatched {dispatched} jobs...")
                jobs = []
        
        # Dispatch remaining jobs
        if jobs:
            dispatched = self.dispatcher.dispatch_jobs_batch(
                jobs=jobs,
                pipeline_name=pipeline_name,
                rate_limit=effective_rate_limit,
            )
            self.execution_manager.update_job_counts(execution_id, jobs_dispatched=dispatched)
            print(f"  ✓ Dispatched {dispatched} jobs...")
        
        # Refresh execution to get final counts
        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            print(f"✓ Dispatch complete: {execution.jobs_dispatched} jobs dispatched")
    
    def _serialize_for_json(self, obj: Any) -> Any:
        """Recursively convert objects to JSON-serializable form."""
        if isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        elif isinstance(obj, dict):
            return {k: self._serialize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._serialize_for_json(item) for item in obj]
        elif hasattr(obj, 'to_dict'):
            return self._serialize_for_json(obj.to_dict())
        elif hasattr(obj, '__dict__'):
            return self._serialize_for_json(obj.__dict__)
        else:
            return str(obj)
