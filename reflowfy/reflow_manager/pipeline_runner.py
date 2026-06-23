"""Pipeline execution runner for ReflowManager."""

import asyncio
import copy
import hashlib
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from reflowfy.reflow_manager.dispatcher import JobDispatcher
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager

# Checkpoint batch configuration
CHECKPOINT_BATCH_SIZE = 25  # Jobs per checkpoint batch
CHECKPOINT_BATCH_TIMEOUT = 300  # 5 minutes timeout per batch
CHECKPOINT_POLL_INTERVAL = 2.0  # Poll every 2 seconds


def _chunk(lst: List, size: int) -> List[List]:
    """Split a list into consecutive chunks of at most `size` elements."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


# Keys that indicate date/time values — stripped from source_metadata before hashing
# so the job ID stays stable across runs even when these fields change.
_DATE_KEY_PATTERNS = ("date", "time", "timestamp", "created_at", "updated_at")


def _filter_volatile_keys(d: dict) -> dict:
    """Remove date/time keys from a dict to keep only stable content."""
    return {k: v for k, v in d.items() if not any(pat in k.lower() for pat in _DATE_KEY_PATTERNS)}


def generate_job_id(
    pipeline_name: str,
    source: Dict[str, Any],
    current_ids: Optional[list] = None,
) -> str:
    """Return a deterministic SHA256 job ID derived from the source slice.

    Used when enable_duplicate_jobs=False. The narrowed source ``config``
    encodes the slice (id-range, scroll/PIT id, key list, or — for
    StaticSource — the records themselves), so identical slices produce the
    same ID across runs. Volatile date/time keys are stripped from config.
    """
    stable = {
        "pipeline_name": pipeline_name,
        "source": {
            "type": source.get("type"),
            "config": _filter_volatile_keys(source.get("config", {}) or {}),
        },
        "current_ids": current_ids,
    }
    content = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()


def _run_async(coro):
    """Run async code in sync context, handling nested event loops."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop — safe to call asyncio.run directly.
        return asyncio.run(coro)
    else:
        # A loop is already running (e.g. Jupyter, pytest-asyncio).
        # Use nest_asyncio if available for clean re-entry; otherwise
        # fall back to a separate thread to avoid deadlocks.
        try:
            import nest_asyncio

            nest_asyncio.apply()
            return asyncio.get_event_loop().run_until_complete(coro)
        except ImportError:
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, coro).result()


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

        # Run the job dispatch; always leave execution in a terminal state
        try:
            self._run_pipeline_jobs(
                execution_id=execution_id,
                pipeline_name=pipeline_name,
                runtime_params=runtime_params,
                rate_limit_override=rate_limit_override,
            )
        except Exception as exc:
            try:
                self.execution_manager.update_execution_state(
                    execution_id, "failed", error_message=str(exc)
                )
            except Exception:
                pass
            raise

        # Refresh execution to get final counts
        execution = self.execution_manager.get_execution(execution_id)
        assert execution is not None

        return {
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "state": execution.state,
            "jobs_dispatched": execution.jobs_dispatched,
            "rate_limit": rate_limit_override,
        }

    def resume_execution(
        self,
        execution_id: str,
        rate_limit_override: Optional[float] = None,
    ) -> None:
        """
        Resume an interrupted execution from where it left off.

        Skips Phase 1 (jobs already in DB) and resumes Phase 2
        from the first incomplete batch. Used for crash recovery.

        Args:
            execution_id: Execution identifier to resume
            rate_limit_override: Optional override for jobs per second
        """
        from reflowfy.core.registry import pipeline_registry

        # Get execution record
        execution = self.execution_manager.get_execution(execution_id)
        if not execution:
            print(f"⚠️ Execution {execution_id} not found, skipping resume")
            return

        pipeline_name = execution.pipeline_name

        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            print(f"⚠️ Pipeline '{pipeline_name}' not in registry, marking execution as failed")
            self.execution_manager.update_execution_state(
                execution_id,
                "failed",
                error_message=f"Pipeline '{pipeline_name}' not found in registry during recovery",
            )
            return

        # Find first incomplete batch
        first_incomplete_batch = self.job_manager.get_first_incomplete_batch(execution_id)
        if first_incomplete_batch is None:
            # All batches complete - just update final state
            print(f"✓ Execution {execution_id} has no incomplete batches, syncing state")
            self._sync_counts_from_db(execution_id)
            counts = self.job_manager.get_job_counts(execution_id)
            final_state = "completed" if counts.get("failed", 0) == 0 else "failed"
            self.execution_manager.update_execution_state(execution_id, final_state)
            return

        print(f"🔄 Resuming execution {execution_id} from batch {first_incomplete_batch}")

        # Determine effective rate limit
        effective_rate_limit = rate_limit_override
        if effective_rate_limit is None and pipeline.rate_limit:
            effective_rate_limit = pipeline.rate_limit

        # Check if we're in local execution mode
        # In local mode, dispatched jobs that weren't completed are orphaned
        # (the executor that was processing them died with the restart)
        from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher

        is_local_mode = isinstance(self.dispatcher, LocalDispatcher)

        if is_local_mode:
            # Reset any "dispatched" jobs back to "pending" - they need re-dispatch
            # because the local executor that was processing them is gone
            from reflowfy.reflow_manager.models import Job

            orphaned = (
                self.job_manager.db.query(Job)
                .filter(
                    Job.execution_id == execution_id,
                    Job.state == "dispatched",
                )
                .all()
            )
            if orphaned:
                print(
                    f"  ⚠️ Local mode: Resetting {len(orphaned)} orphaned dispatched jobs to pending"
                )
                for job in orphaned:
                    job.state = "pending"
                self.job_manager.db.commit()

        # Get total jobs and max batch number from DB
        job_counts = self.job_manager.get_job_counts(execution_id)
        total_jobs = job_counts.get("total", 0)

        # Find max batch number
        from sqlalchemy import func

        from reflowfy.reflow_manager.models import Job

        max_batch_result = (
            self.job_manager.db.query(func.max(Job.batch_number))
            .filter(Job.execution_id == execution_id)
            .scalar()
        )
        max_batch = max_batch_result or 0

        # Sync current counts
        total_dispatched, total_completed, total_failed = self._sync_counts_from_db(execution_id)

        # Resume from first incomplete batch
        for current_batch_num in range(first_incomplete_batch, max_batch + 1):
            # Load pending jobs for this batch
            jobs = self.job_manager.get_pending_jobs_by_batch_number(
                execution_id, current_batch_num
            )

            if not jobs:
                # Batch might already be complete, check dispatched jobs
                dispatched_jobs = (
                    self.job_manager.db.query(Job)
                    .filter(
                        Job.execution_id == execution_id,
                        Job.batch_number == current_batch_num,
                        Job.state == "dispatched",
                    )
                    .all()
                )

                if dispatched_jobs:
                    # In distributed mode, wait for already-dispatched jobs
                    # (workers may still be processing them via Kafka)
                    job_ids = [job.job_id for job in dispatched_jobs]
                    print(
                        f"    Waiting for {len(dispatched_jobs)} dispatched jobs in batch {current_batch_num}..."
                    )
                    completed, failed = self._wait_for_batch_completion(
                        job_ids=job_ids,
                        timeout=CHECKPOINT_BATCH_TIMEOUT,
                        poll_interval=CHECKPOINT_POLL_INTERVAL,
                    )
                    total_completed += completed
                    total_failed += failed
                continue

            job_ids = [job.job_id for job in jobs]
            job_payloads = [job.job_payload for job in jobs]

            print(f"    Dispatching batch {current_batch_num} ({len(jobs)} jobs)...")

            # Dispatch to Kafka (async method, run in sync context)
            # Use execution-scoped key for rate limiting to prevent
            # concurrent executions of the same pipeline from contending
            # on the same rate limiter bucket
            rate_limit_key = f"{pipeline_name}:{execution_id}"
            dispatched = _run_async(
                self.dispatcher.dispatch_jobs_batch(
                    jobs=job_payloads,
                    pipeline_name=rate_limit_key,
                    rate_limit=effective_rate_limit,
                )
            )

            # Mark jobs as dispatched
            self.job_manager.mark_jobs_dispatched(job_ids)
            total_dispatched += dispatched

            self._set_job_counts(execution_id, dispatched=total_dispatched)

            # Wait for batch completion
            print(f"    Waiting for batch {current_batch_num}...")
            completed, failed = self._wait_for_batch_completion(
                job_ids=job_ids,
                timeout=CHECKPOINT_BATCH_TIMEOUT,
                poll_interval=CHECKPOINT_POLL_INTERVAL,
            )

            total_completed += completed
            total_failed += failed

            self._set_job_counts(
                execution_id,
                dispatched=total_dispatched,
                completed=total_completed,
                failed=total_failed,
            )

            print(f"    Batch {current_batch_num}: {completed} completed, {failed} failed")

        # Final state update
        dispatched, completed, failed = self._sync_counts_from_db(execution_id)

        total_finished = completed + failed
        if total_finished == total_jobs:
            final_state = "completed" if failed == 0 else "failed"
        else:
            final_state = "failed"
            print(f"  Warning: Only {total_finished} of {total_jobs} jobs finished")

        self.execution_manager.update_execution_state(execution_id, final_state)
        print(
            f"✓ Resumed execution {final_state}: {dispatched} dispatched, {completed} completed, {failed} failed"
        )

    def _run_pipeline_jobs(
        self,
        execution_id: str,
        pipeline_name: str,
        runtime_params: Dict[str, Any],
        rate_limit_override: Optional[float] = None,
        enable_duplicate_jobs: Optional[bool] = None,
    ) -> None:
        """
        Dispatch pipeline jobs for an existing execution.

        Used by background tasks when execution already exists.

        Args:
            execution_id: Existing execution identifier
            pipeline_name: Name of the registered pipeline
            runtime_params: Runtime parameters for the pipeline
            rate_limit_override: Optional override for jobs per second
            enable_duplicate_jobs: True = jobs may run multiple times (default);
                False = each unique job (by content hash) runs at most once.
                None falls back to the pipeline's own enable_duplicate_jobs setting.
        """
        from reflowfy.core.execution_context import ExecutionContext
        from reflowfy.core.id_based_pipeline import IdBasedPipeline
        from reflowfy.core.registry import pipeline_registry

        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found in registry")

        # Resolve effective duplicate setting: API override > pipeline default
        if enable_duplicate_jobs is None:
            enable_duplicate_jobs = getattr(pipeline, "enable_duplicate_jobs", True)

        # Check if this is an IdBasedPipeline — use per-ID execution flow
        if isinstance(pipeline, IdBasedPipeline):
            self._run_id_based_pipeline_jobs(
                execution_id=execution_id,
                pipeline=pipeline,
                pipeline_name=pipeline_name,
                runtime_params=runtime_params,
                rate_limit_override=rate_limit_override,
                enable_duplicate_jobs=enable_duplicate_jobs,
            )
            return

        # Resolve pipeline with runtime params (for AbstractPipeline).
        # _resolved_params includes defaults + any keys added by define_source.
        if hasattr(pipeline, "resolve"):
            pipeline.resolve(runtime_params)

        print(f"🚀 Job dispatch starting: {pipeline_name}")
        print(f"📊 Execution ID: {execution_id}")

        # Update state to running
        self.execution_manager.update_execution_state(execution_id, "running")

        # Use enriched params (define_source may have added keys) for the context
        # so workers receive them in job metadata.
        enriched_params = getattr(pipeline, "_resolved_params", runtime_params)

        # Create execution context
        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=enriched_params,
        )

        # Determine effective rate limit
        effective_rate_limit = rate_limit_override
        if effective_rate_limit is None and pipeline.rate_limit:
            effective_rate_limit = pipeline.rate_limit

        print(f"Splitting source data into jobs (rate: {effective_rate_limit}/sec)...")

        # Phase 1: Stream all jobs to database (not RAM)
        print("  Phase 1: Saving jobs to database...")
        batch_number = 1
        job_count = 0
        dedup_count = 0
        current_job_ids = []

        for source_job in pipeline.source.split_jobs(enriched_params):
            # Use per-job params so in-place enrichment does not bleed across jobs.
            job_params = dict(enriched_params)
            if source_job.metadata:
                for key, value in source_job.metadata.items():
                    if key not in job_params:
                        job_params[key] = value

            # Resolve transformation chain and destination for this source job.
            resolved_transformations = list(
                pipeline.define_transformations(source_job.records, job_params)
            )
            transformed_preview = copy.deepcopy(source_job.records)
            for t in resolved_transformations:
                transformed_preview = t.apply(transformed_preview, job_params)

            destination = pipeline.define_destination(transformed_preview, job_params)
            transformation_names = [t.name for t in resolved_transformations]
            transformation_specs = [
                {
                    "name": t.name,
                    "module": t.__class__.__module__,
                    "class_name": t.__class__.__name__,
                }
                for t in resolved_transformations
            ]

            if enable_duplicate_jobs:
                job_id = str(uuid.uuid4())
            else:
                job_id = generate_job_id(
                    pipeline_name=pipeline_name,
                    transformations=transformation_names,
                    records=source_job.records,
                    source_metadata=source_job.metadata or {},
                )
                if self.job_manager.get_job(job_id):
                    print(f"  [no-dup] Skipping job {job_id[:12]}... (already exists)")
                    dedup_count += 1
                    continue

            # Set current batch number on context before embedding in payload
            context.batch_number = batch_number

            # Ensure workers receive per-job enriched runtime params.
            context_dict = context.to_dict()
            context_dict["runtime_params"] = dict(job_params)

            # Create job payload
            job_payload = {
                "execution_id": execution_id,
                "job_id": job_id,
                "pipeline_name": pipeline_name,
                "transformations": transformation_names,
                "transformation_specs": transformation_specs,
                "destination": {
                    "type": destination.__class__.__name__,
                    "config": destination.config,
                },
                "rate_limit": pipeline.rate_limit,
                "records": source_job.records,
                "metadata": {
                    **context_dict,
                    "source_metadata": source_job.metadata,
                },
            }

            # Serialize to handle non-JSON-serializable objects
            job_payload = self._serialize_for_json(job_payload)

            # Save job to database
            self.job_manager.create_job(
                execution_id=execution_id,
                job_id=job_id,
                job_payload=job_payload,
                batch_number=batch_number,
            )

            current_job_ids.append(job_id)
            job_count += 1

            # When batch is full, increment batch number
            if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                batch_number += 1
                current_job_ids = []

        # Set total_jobs correctly (once, after all jobs saved)
        self._set_total_jobs(execution_id, job_count)

        # Persist dedup count so it shows in stats
        if dedup_count > 0:
            self.execution_manager.update_deduplicated_count(execution_id, dedup_count)
            print(f"  Deduplicated {dedup_count} jobs (content hash match)")

        # Back-fill total_batches into every job's metadata payload (best-effort)
        try:
            self.job_manager.bulk_set_total_batches(execution_id, batch_number)
        except Exception as _e:
            self.job_manager.db.rollback()
            print(f"  Warning: could not back-fill total_batches ({_e}); continuing")

        print(f"  Saved {job_count} jobs to database in {batch_number} batches")

        # Phase 2: Dispatch and wait for each batch
        # Check if we're in local mode (no need to poll for completion)
        from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher

        is_local_mode = isinstance(self.dispatcher, LocalDispatcher)

        if is_local_mode:
            print("  Phase 2: Executing batches locally (in-process)...")
        else:
            print("  Phase 2: Dispatching batches...")

        # Pre-dispatch destination lag health check (Kafka only, opt-in).
        # Runs in both local and distributed mode — high lag should block
        # dispatch regardless of how jobs are executed.
        if job_count > 0:
            lag_ok = _run_async(
                self._check_destination_lag_health(execution_id, pipeline_name, runtime_params)
            )
            if not lag_ok:
                return  # execution already marked failed; DLQ entry inserted

        total_dispatched = 0
        total_completed = 0
        total_failed = 0

        for current_batch_num in range(1, batch_number + 1):
            # Load jobs for this batch from database
            jobs = self.job_manager.get_pending_jobs_by_batch_number(
                execution_id, current_batch_num
            )

            if not jobs:
                continue

            job_ids = [job.job_id for job in jobs]
            job_payloads = [job.job_payload for job in jobs]

            print(
                f"    {'Executing' if is_local_mode else 'Dispatching'} batch {current_batch_num} ({len(jobs)} jobs)..."
            )

            # Dispatch (in local mode, this executes jobs synchronously)
            # Use execution-scoped key for rate limiting to prevent
            # concurrent executions of the same pipeline from contending
            # on the same rate limiter bucket
            rate_limit_key = f"{pipeline_name}:{execution_id}"
            dispatched = _run_async(
                self.dispatcher.dispatch_jobs_batch(
                    jobs=job_payloads,
                    pipeline_name=rate_limit_key,
                    rate_limit=effective_rate_limit,
                )
            )

            # Mark jobs as dispatched in database
            self.job_manager.mark_jobs_dispatched(job_ids)
            total_dispatched += dispatched

            # Update execution counts (SET, not +=)
            self._set_job_counts(execution_id, dispatched=total_dispatched)

            if is_local_mode:
                # Local mode: jobs already executed synchronously by LocalDispatcher.
                # WorkerExecutor updated job states directly in DB.
                # No need to poll — just count results from dispatch.
                completed = dispatched
                failed = len(jobs) - dispatched
            else:
                # Distributed mode: jobs sent to Kafka, wait for workers to complete
                print(f"    Waiting for batch {current_batch_num}...")
                completed, failed = self._wait_for_batch_completion(
                    job_ids=job_ids,
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
        try:
            dispatched, completed, failed = self._sync_counts_from_db(execution_id)
        except Exception:
            dispatched, completed, failed = total_dispatched, total_completed, total_failed

        # Determine final state based on actual DB counts
        total_finished = completed + failed
        if total_finished == job_count:
            final_state = "completed" if failed == 0 else "failed"
        else:
            # Some jobs may not have completed properly
            final_state = "failed"
            print(f"  Warning: Only {total_finished} of {job_count} jobs finished")

        self.execution_manager.update_execution_state(execution_id, final_state)

        print(
            f"Execution {final_state}: {dispatched} dispatched, {completed} completed, {failed} failed"
        )

    def _run_id_based_pipeline_jobs(
        self,
        execution_id: str,
        pipeline: Any,
        pipeline_name: str,
        runtime_params: Dict[str, Any],
        rate_limit_override: Optional[float] = None,
        enable_duplicate_jobs: Optional[bool] = None,
    ) -> None:
        """
        Dispatch jobs for an IdBasedPipeline.

        Iterates over each ID in runtime_params['ids'], resolves the source
        per-ID, and creates jobs from each ID's source. All jobs belong to
        the same execution.

        Args:
            execution_id: Existing execution identifier
            pipeline: IdBasedPipeline instance
            pipeline_name: Name of the pipeline
            runtime_params: Runtime parameters (must include 'ids')
            rate_limit_override: Optional override for jobs per second
            enable_duplicate_jobs: True = jobs may run multiple times (default);
                False = each unique job (by content hash) runs at most once.
        """
        from reflowfy.core.execution_context import ExecutionContext

        # Resolve effective duplicate setting: caller override > pipeline default
        if enable_duplicate_jobs is None:
            enable_duplicate_jobs = getattr(pipeline, "enable_duplicate_jobs", True)

        # Validate parameters
        pipeline.resolve(runtime_params)
        params = pipeline.apply_defaults(runtime_params)

        ids = params.get("ids", [])
        ids_batch_size = getattr(pipeline, "ids_batch_size", 1)

        print(f"🚀 IdBasedPipeline dispatch starting: {pipeline_name}")
        print(f"📊 Execution ID: {execution_id}")
        print(f"🔑 Processing {len(ids)} IDs (batch_size={ids_batch_size}): {ids}")

        # Update state to running
        self.execution_manager.update_execution_state(execution_id, "running")

        # Create execution context
        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=params,
        )

        # Determine effective rate limit
        effective_rate_limit = rate_limit_override
        if effective_rate_limit is None and pipeline.rate_limit:
            effective_rate_limit = pipeline.rate_limit

        # Phase 1: For each ID-batch, resolve source and save jobs to database
        id_batches = _chunk(ids, ids_batch_size)
        print(
            f"  Phase 1: Saving jobs to database for {len(ids)} IDs in {len(id_batches)} batches..."
        )
        batch_number = 1
        job_count = 0
        dedup_count = 0
        current_job_ids = []

        for ids_batch in id_batches:
            print(f"    Processing ID batch: {ids_batch}")

            # Resolve source for this batch of IDs.
            # batch_params is a per-batch copy of params enriched by define_source.
            resolved = pipeline.resolve_for_ids(params, ids_batch)
            source = resolved["source"]
            batch_params = resolved.get("batch_params", params)

            # Split this batch's source into jobs
            for source_job in source.split_jobs(batch_params):
                # Per-job params start from batch params and include source metadata.
                job_params = dict(batch_params)
                if source_job.metadata:
                    for key, value in source_job.metadata.items():
                        if key not in job_params:
                            job_params[key] = value

                transformations = list(
                    pipeline.define_transformations(source_job.records, job_params)
                )
                transformed_preview = copy.deepcopy(source_job.records)
                for t in transformations:
                    transformed_preview = t.apply(transformed_preview, job_params)

                destination = pipeline.define_destination(transformed_preview, job_params)
                transformation_names = [t.name for t in transformations]
                transformation_specs = [
                    {
                        "name": t.name,
                        "module": t.__class__.__module__,
                        "class_name": t.__class__.__name__,
                    }
                    for t in transformations
                ]
                if enable_duplicate_jobs:
                    job_id = str(uuid.uuid4())
                else:
                    job_id = generate_job_id(
                        pipeline_name=pipeline_name,
                        transformations=transformation_names,
                        records=source_job.records,
                        source_metadata=source_job.metadata or {},
                    )
                    if self.job_manager.get_job(job_id):
                        print(f"  [no-dup] Skipping job {job_id[:12]}... (already exists)")
                        dedup_count += 1
                        continue

                # Set current batch number on context before embedding in payload
                context.batch_number = batch_number

                # Create job payload with current_ids list in metadata.
                # runtime_params in metadata is the per-job enriched version so
                # workers receive any keys added by define_source and job metadata.
                context_dict = context.to_dict()
                context_dict["runtime_params"] = dict(job_params)
                job_payload = {
                    "execution_id": execution_id,
                    "job_id": job_id,
                    "pipeline_name": pipeline_name,
                    "transformations": transformation_names,
                    "transformation_specs": transformation_specs,
                    "destination": {
                        "type": destination.__class__.__name__,
                        "config": destination.config,
                    },
                    "rate_limit": pipeline.rate_limit,
                    "records": source_job.records,
                    "metadata": {
                        **context_dict,
                        "current_ids": ids_batch,
                        "source_metadata": source_job.metadata,
                    },
                }

                # Serialize to handle non-JSON-serializable objects
                job_payload = self._serialize_for_json(job_payload)

                # Save job to database
                self.job_manager.create_job(
                    execution_id=execution_id,
                    job_id=job_id,
                    job_payload=job_payload,
                    batch_number=batch_number,
                )

                current_job_ids.append(job_id)
                job_count += 1

                # When batch is full, increment batch number
                if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                    batch_number += 1
                    current_job_ids = []

        # Set total_jobs correctly (once, after all jobs saved)
        self._set_total_jobs(execution_id, job_count)

        # Persist dedup count so it shows in stats
        if dedup_count > 0:
            self.execution_manager.update_deduplicated_count(execution_id, dedup_count)
            print(f"  Deduplicated {dedup_count} jobs (content hash match)")

        # Back-fill total_batches into every job's metadata payload (best-effort)
        try:
            self.job_manager.bulk_set_total_batches(execution_id, batch_number)
        except Exception as _e:
            self.job_manager.db.rollback()
            print(f"  Warning: could not back-fill total_batches ({_e}); continuing")

        print(
            f"  Saved {job_count} jobs to database in {batch_number} batches (from {len(ids)} IDs)"
        )

        # Phase 2: Dispatch and wait for each batch (reuses existing logic)
        # Check if we're in local mode (no need to poll for completion)
        from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher

        is_local_mode = isinstance(self.dispatcher, LocalDispatcher)

        if is_local_mode:
            print("  Phase 2: Executing batches locally (in-process)...")
        else:
            print("  Phase 2: Dispatching batches...")

        total_dispatched = 0
        total_completed = 0
        total_failed = 0

        for current_batch_num in range(1, batch_number + 1):
            # Load jobs for this batch from database
            jobs = self.job_manager.get_pending_jobs_by_batch_number(
                execution_id, current_batch_num
            )

            if not jobs:
                continue

            job_ids = [job.job_id for job in jobs]
            job_payloads = [job.job_payload for job in jobs]

            print(
                f"    {'Executing' if is_local_mode else 'Dispatching'} batch {current_batch_num} ({len(jobs)} jobs)..."
            )

            # Dispatch (in local mode, this executes jobs synchronously)
            # Use execution-scoped key for rate limiting
            rate_limit_key = f"{pipeline_name}:{execution_id}"
            dispatched = _run_async(
                self.dispatcher.dispatch_jobs_batch(
                    jobs=job_payloads,
                    pipeline_name=rate_limit_key,
                    rate_limit=effective_rate_limit,
                )
            )

            # Mark jobs as dispatched in database
            self.job_manager.mark_jobs_dispatched(job_ids)
            total_dispatched += dispatched

            # Update execution counts (SET, not +=)
            self._set_job_counts(execution_id, dispatched=total_dispatched)

            if is_local_mode:
                # Local mode: jobs already executed synchronously by LocalDispatcher.
                # WorkerExecutor updated job states directly in DB.
                # No need to poll — just count results from dispatch.
                completed = dispatched
                failed = len(jobs) - dispatched
            else:
                # Distributed mode: jobs sent to Kafka, wait for workers to complete
                print(f"    Waiting for batch {current_batch_num}...")
                completed, failed = self._wait_for_batch_completion(
                    job_ids=job_ids,
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
        try:
            dispatched, completed, failed = self._sync_counts_from_db(execution_id)
        except Exception:
            dispatched, completed, failed = total_dispatched, total_completed, total_failed

        # Determine final state based on actual DB counts
        total_finished = completed + failed
        if total_finished == job_count:
            final_state = "completed" if failed == 0 else "failed"
        else:
            final_state = "failed"
            print(f"  Warning: Only {total_finished} of {job_count} jobs finished")

        self.execution_manager.update_execution_state(execution_id, final_state)

        print(
            f"IdBasedPipeline {final_state}: {dispatched} dispatched, {completed} completed, {failed} failed ({len(ids)} IDs)"
        )

    async def _check_destination_lag_health(
        self,
        execution_id: str,
        pipeline_name: str,
        runtime_params: Dict[str, Any],
    ) -> bool:
        """
        Run a lag health check on the Kafka destination before dispatching.

        Looks at the first job in batch 1 to discover the destination config.
        If the destination is KafkaDestination with lag_health_check_enabled=True
        and the measured lag exceeds lag_threshold, the execution is immediately
        failed and a DLQJob is inserted for later retry.

        Returns True if dispatch should proceed, False if it was blocked.
        Fails open on any unexpected error (returns True).
        """
        try:
            first_jobs = self.job_manager.get_pending_jobs_by_batch_number(execution_id, 1)
            if not first_jobs:
                return True

            job_payload = first_jobs[0].job_payload
            dest_info = job_payload.get("destination", {})
            if dest_info.get("type") != "KafkaDestination":
                return True

            dest_kwargs = dest_info.get("config", {})
            if not dest_kwargs.get("lag_health_check_enabled"):
                return True

            from reflowfy.destinations.kafka import KafkaDestination
            destination = KafkaDestination(**dest_kwargs)

            try:
                is_healthy = await destination.health_check()
            finally:
                await destination.close()

        except Exception as exc:
            print(f"⚠️ Lag health check error (fail open): {exc}")
            return True

        if not is_healthy:
            print(f"🚫 Kafka lag threshold exceeded for '{pipeline_name}' — rescheduling via DLQ")

            from datetime import datetime, timedelta, timezone
            from reflowfy.reflow_manager.models import DLQJob

            delay_minutes = 5
            dlq_job = DLQJob(
                job_payload=runtime_params,
                pipeline_name=pipeline_name,
                scheduled_at=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=delay_minutes),
                delay_minutes=delay_minutes,
                status="pending",
                retry_count=0,
                max_retries=5,
            )
            try:
                self.execution_manager.db.add(dlq_job)
                self.execution_manager.db.commit()
            except Exception as dlq_exc:
                print(f"⚠️ Failed to insert DLQ entry: {dlq_exc}")
                try:
                    self.execution_manager.db.rollback()
                except Exception:
                    pass

            self.execution_manager.update_execution_state(
                execution_id,
                "failed",
                error_message=(
                    "Kafka destination lag threshold exceeded — "
                    "job rescheduled via DLQ"
                ),
            )
            return False

        return True

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
        job_ids: List[str],
        timeout: float = CHECKPOINT_BATCH_TIMEOUT,
        poll_interval: float = CHECKPOINT_POLL_INTERVAL,
    ) -> Tuple[int, int]:
        """
        Wait for all jobs in a checkpoint batch to complete.

        Args:
            job_ids: List of batch IDs to wait for
            timeout: Maximum time to wait in seconds
            poll_interval: How often to poll for completion

        Returns:
            Tuple of (completed_count, failed_count)
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Get states for all checkpoints in this batch
            states = self.checkpoint_manager.get_job_states(job_ids)

            completed = 0
            failed = 0
            pending = 0

            for job_id in job_ids:
                state = states.get(job_id, "pending")
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
        states = self.checkpoint_manager.get_job_states(job_ids)
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
        elif hasattr(obj, "to_dict"):
            return self._serialize_for_json(obj.to_dict())
        elif hasattr(obj, "__dict__"):
            return self._serialize_for_json(obj.__dict__)
        else:
            return str(obj)
