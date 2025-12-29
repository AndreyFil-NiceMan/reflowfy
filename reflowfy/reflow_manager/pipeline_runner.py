"""Pipeline execution runner for ReflowManager."""

import time
import uuid
from typing import Dict, Any, Optional, List, Tuple

from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.dispatcher import JobDispatcher


# Checkpoint batch configuration
CHECKPOINT_BATCH_SIZE = 25  # Jobs per checkpoint batch
CHECKPOINT_BATCH_TIMEOUT = 300  # 5 minutes timeout per batch
CHECKPOINT_POLL_INTERVAL = 2.0  # Poll every 2 seconds



class PipelineRunner:
    """
    Executes pipelines by splitting jobs and dispatching to Kafka.
    
    Coordinates between:
    - ExecutionManager for execution records
    - JobManager for job payload and checkpoint tracking (unified)
    - JobDispatcher for Kafka dispatch
    """
    
    def __init__(
        self,
        execution_manager: ExecutionManager,
        job_manager: JobManager,
        dispatcher: JobDispatcher,
    ):
        """
        Initialize pipeline runner.
        
        Args:
            execution_manager: ExecutionManager instance
            job_manager: JobManager instance (includes checkpoint functionality)
            dispatcher: JobDispatcher instance
        """
        self.execution_manager = execution_manager
        self.job_manager = job_manager
        self.dispatcher = dispatcher
        
        # Backward compatibility alias
        self.checkpoint_manager = self.job_manager
    
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
        
        print(f"Splitting source data into jobs (rate: {effective_rate_limit}/sec)...")
        
        # Phase 1: Stream all jobs to database (not RAM)
        print("  Phase 1: Saving jobs to database...")
        batch_number = 1
        job_count = 0
        current_batch_ids = []
        
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
            
            # Save job to database (includes checkpoint fields)
            self.job_manager.create_job(
                execution_id=execution_id,
                batch_id=batch_id,
                job_payload=job_payload,
                batch_number=batch_number,
                offset_data=source_job.metadata,
            )
            
            current_batch_ids.append(batch_id)
            job_count += 1
            
            # When batch is full, increment batch number
            if len(current_batch_ids) >= CHECKPOINT_BATCH_SIZE:
                batch_number += 1
                current_batch_ids = []
        
        # Set total_jobs correctly (once, after all jobs saved)
        self._set_total_jobs(execution_id, job_count)
        print(f"  Saved {job_count} jobs to database in {batch_number} batches")
        
        # Phase 2: Dispatch and wait for each batch
        print("  Phase 2: Dispatching batches...")
        total_dispatched = 0
        total_completed = 0
        total_failed = 0
        
        for current_batch_num in range(1, batch_number + 1):
            # Load jobs for this batch from database
            jobs = self.job_manager.get_pending_jobs_by_batch_number(execution_id, current_batch_num)
            
            if not jobs:
                continue
            
            batch_ids = [job.batch_id for job in jobs]
            job_payloads = [job.job_payload for job in jobs]
            
            print(f"    Dispatching batch {current_batch_num} ({len(jobs)} jobs)...")
            
            # Dispatch to Kafka
            dispatched = self.dispatcher.dispatch_jobs_batch(
                jobs=job_payloads,
                pipeline_name=pipeline_name,
                rate_limit=effective_rate_limit,
            )
            
            # Mark jobs as dispatched in database
            self.job_manager.mark_jobs_dispatched(batch_ids)
            total_dispatched += dispatched
            
            # Update execution counts (SET, not +=)
            self._set_job_counts(execution_id, dispatched=total_dispatched)
            
            # Wait for this batch to complete
            print(f"    Waiting for batch {current_batch_num}...")
            completed, failed = self._wait_for_batch_completion(
                batch_ids=batch_ids,
                timeout=CHECKPOINT_BATCH_TIMEOUT,
                poll_interval=CHECKPOINT_POLL_INTERVAL,
            )
            
            total_completed += completed
            total_failed += failed
            
            # Update completion counts
            self._set_job_counts(
                execution_id,
                dispatched=total_dispatched,
                completed=total_completed,
                failed=total_failed,
            )
            
            print(f"    Batch {current_batch_num}: {completed} completed, {failed} failed")
        
        # Final state update - sync actual counts from database
        dispatched, completed, failed = self._sync_counts_from_db(execution_id)
        
        # Determine final state based on actual DB counts
        total_finished = completed + failed
        if total_finished == job_count:
            final_state = "completed" if failed == 0 else "failed"
        else:
            # Some jobs may not have completed properly
            final_state = "failed"
            print(f"  Warning: Only {total_finished} of {job_count} jobs finished")
        
        self.execution_manager.update_execution_state(execution_id, final_state)
        
        print(f"Execution {final_state}: {dispatched} dispatched, {completed} completed, {failed} failed")
    
    def _set_total_jobs(self, execution_id: str, total: int) -> None:
        """Set total_jobs for an execution (uses SET, not +=)."""
        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            execution.total_jobs = total
            self.execution_manager.db.commit()
    
    def _set_job_counts(
        self,
        execution_id: str,
        dispatched: Optional[int] = None,
        completed: Optional[int] = None,
        failed: Optional[int] = None,
    ) -> None:
        """Set job counts for an execution (uses SET, not +=)."""
        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            if dispatched is not None:
                execution.jobs_dispatched = dispatched
            if completed is not None:
                execution.jobs_completed = completed
            if failed is not None:
                execution.jobs_failed = failed
            self.execution_manager.db.commit()
    
    def _sync_counts_from_db(self, execution_id: str) -> Tuple[int, int, int]:
        """
        Sync job counts from actual job states in database.
        
        Returns:
            Tuple of (dispatched, completed, failed)
        """
        from reflowfy.reflow_manager.models import Job
        
        # Get counts directly from jobs table (now the source of truth)
        job_counts = self.job_manager.get_job_counts(execution_id)
        
        completed = job_counts.get("completed", 0)
        failed = job_counts.get("failed", 0)
        dispatched = job_counts.get("dispatched", 0) + completed + failed
        
        # Update execution with real counts
        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            execution.jobs_dispatched = dispatched
            execution.jobs_completed = completed
            execution.jobs_failed = failed
            self.execution_manager.db.commit()
        
        return (dispatched, completed, failed)
    
    def _wait_for_batch_completion(
        self,
        batch_ids: List[str],
        timeout: float = CHECKPOINT_BATCH_TIMEOUT,
        poll_interval: float = CHECKPOINT_POLL_INTERVAL,
    ) -> Tuple[int, int]:
        """
        Wait for all jobs in a checkpoint batch to complete.
        
        Args:
            batch_ids: List of batch IDs to wait for
            timeout: Maximum time to wait in seconds
            poll_interval: How often to poll for completion
        
        Returns:
            Tuple of (completed_count, failed_count)
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Get states for all checkpoints in this batch
            states = self.checkpoint_manager.get_batch_states(batch_ids)
            
            completed = 0
            failed = 0
            pending = 0
            
            for batch_id in batch_ids:
                state = states.get(batch_id, "pending")
                if state == "completed":
                    completed += 1
                elif state == "failed":
                    failed += 1
                else:
                    pending += 1
            
            # Check if all jobs are done (completed or failed)
            if pending == 0:
                return (completed, failed)
            
            # Wait before polling again
            time.sleep(poll_interval)
        
        # Timeout reached - return current counts
        print(f"  Warning: Batch timeout after {timeout}s, some jobs may not have completed")
        states = self.checkpoint_manager.get_batch_states(batch_ids)
        completed = sum(1 for s in states.values() if s == "completed")
        failed = sum(1 for s in states.values() if s == "failed")
        
        return (completed, failed)
    
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
