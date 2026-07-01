"""Pipeline execution runner for ReflowManager."""

import asyncio
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from sqlalchemy import func

from reflowfy.core.serialization import to_json_safe
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.reflow_manager.dispatcher import JobDispatcher
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.reflow_manager.models import Job

if TYPE_CHECKING:
    from reflowfy.core.id_based_pipeline import IdBasedPipeline

logger = logging.getLogger(__name__)

# Checkpoint batch configuration
CHECKPOINT_BATCH_SIZE = 25  # Jobs per checkpoint batch
CHECKPOINT_BATCH_TIMEOUT = 300  # 5 minutes timeout per batch
CHECKPOINT_POLL_INTERVAL = 2.0  # Poll every 2 seconds

# Worker job message schema version
JOB_SCHEMA_VERSION = 2


def _chunk(lst: List[Any], size: int) -> List[List[Any]]:
    """Split a list into consecutive chunks of at most `size` elements."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def _finished_count(completed: int, failed: int, deduplicated: int) -> int:
    """Jobs in a terminal state. Deduplicated jobs are a success outcome."""
    return completed + failed + deduplicated


def _run_async(coro: Any) -> Any:
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


def build_job_payload(
    execution_id: str,
    job_id: str,
    pipeline_name: str,
    sub_source: Any,
    metadata: Dict[str, Any],
    dedup_check: bool = False,
) -> Dict[str, Any]:
    """Assemble the v2 worker job message for one narrowed sub-source."""
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "execution_id": execution_id,
        "job_id": job_id,
        "pipeline_name": pipeline_name,
        "source": SourceFactory.serialize(sub_source),
        "dedup_check": dedup_check,
        "metadata": metadata,
    }


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

        logger.info("Running pipeline: %s (execution %s)", pipeline_name, execution_id)

        # Create execution record
        self.execution_manager.create_execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
        )

        # Run the job dispatch; always leave execution in a terminal state
        try:
            self.run_pipeline_jobs(
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
                logger.exception("Failed to mark execution %s as failed", execution_id)
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
            logger.warning("Execution %s not found, skipping resume", execution_id)
            return

        pipeline_name = execution.pipeline_name

        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            logger.warning(
                "Pipeline '%s' not in registry, marking execution %s as failed",
                pipeline_name,
                execution_id,
            )
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
            logger.info("Execution %s has no incomplete batches, syncing state", execution_id)
            self._sync_counts_from_db(execution_id)
            counts = self.job_manager.get_job_counts(execution_id)
            final_state = "completed" if counts.get("failed", 0) == 0 else "failed"
            self.execution_manager.update_execution_state(execution_id, final_state)
            return

        logger.info("Resuming execution %s from batch %d", execution_id, first_incomplete_batch)

        # Determine effective rate limit
        effective_rate_limit = rate_limit_override
        if effective_rate_limit is None and pipeline.rate_limit:
            effective_rate_limit = pipeline.rate_limit

        # In local mode, dispatched jobs that weren't completed are orphaned
        # (the executor that was processing them died with the restart), so
        # reset them to pending for re-dispatch.
        is_local_mode = isinstance(self.dispatcher, LocalDispatcher)
        if is_local_mode:
            orphaned = (
                self.job_manager.db.query(Job)
                .filter(
                    Job.execution_id == execution_id,
                    Job.state == "dispatched",
                )
                .all()
            )
            if orphaned:
                logger.warning(
                    "Local mode: resetting %d orphaned dispatched jobs to pending", len(orphaned)
                )
                for job in orphaned:
                    job.state = "pending"
                self.job_manager.db.commit()

        # Get total jobs and max batch number from DB
        job_counts = self.job_manager.get_job_counts(execution_id)
        total_jobs = job_counts.get("total", 0)

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
            jobs = self.job_manager.get_pending_jobs_by_batch_number(
                execution_id, current_batch_num
            )

            if not jobs:
                # No pending jobs. In distributed mode, workers may still be
                # processing jobs already dispatched to Kafka — wait for them.
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
                    job_ids = [job.job_id for job in dispatched_jobs]
                    logger.info(
                        "Waiting for %d dispatched jobs in batch %d...",
                        len(dispatched_jobs),
                        current_batch_num,
                    )
                    completed, failed = self._wait_for_batch_completion(
                        job_ids=job_ids,
                        timeout=CHECKPOINT_BATCH_TIMEOUT,
                        poll_interval=CHECKPOINT_POLL_INTERVAL,
                    )
                    total_completed += completed
                    total_failed += failed
                continue

            logger.info("Dispatching batch %d (%d jobs)...", current_batch_num, len(jobs))
            dispatched, completed, failed = self._dispatch_one_batch(
                execution_id,
                pipeline_name,
                jobs,
                effective_rate_limit,
                is_local_mode,
                prior_dispatched=total_dispatched,
            )
            total_dispatched += dispatched
            total_completed += completed
            total_failed += failed

            self._set_job_counts(
                execution_id,
                dispatched=total_dispatched,
                completed=total_completed,
                failed=total_failed,
            )
            logger.info("Batch %d: %d completed, %d failed", current_batch_num, completed, failed)

        # Final state update
        dispatched, completed, failed = self._finalize_execution_state(
            execution_id, total_jobs, (total_dispatched, total_completed, total_failed)
        )
        logger.info(
            "Resumed execution %s: %d dispatched, %d completed, %d failed",
            execution_id,
            dispatched,
            completed,
            failed,
        )

    def run_pipeline_jobs(
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

        logger.info("Job dispatch starting: %s (execution %s)", pipeline_name, execution_id)

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

        logger.info("Splitting source data into jobs (rate: %s/sec)...", effective_rate_limit)

        # Phase 1: Stream all jobs to database (not RAM)
        logger.info("Phase 1: Saving jobs to database...")
        batch_number = 1
        job_count = 0
        current_job_ids: List[str] = []

        base_source = pipeline.source
        for sub_source in base_source.split(enriched_params):
            context.batch_number = batch_number
            context_dict = context.to_dict()
            context_dict["runtime_params"] = dict(enriched_params)
            metadata = {**context_dict, "source_metadata": None}

            dedup_check = not enable_duplicate_jobs
            job_id = str(uuid.uuid4())

            job_payload = build_job_payload(
                execution_id, job_id, pipeline_name, sub_source, metadata, dedup_check=dedup_check
            )
            job_payload = self._serialize_for_json(job_payload)

            self.job_manager.create_job(
                execution_id=execution_id,
                job_id=job_id,
                job_payload=job_payload,
                batch_number=batch_number,
            )
            current_job_ids.append(job_id)
            job_count += 1
            if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                batch_number += 1
                current_job_ids = []

        # Set total_jobs correctly (once, after all jobs saved)
        self._set_total_jobs(execution_id, job_count)
        self._backfill_total_batches(execution_id, batch_number)
        logger.info("Saved %d jobs to database in %d batches", job_count, batch_number)

        # Phase 2: dispatch and wait for each batch
        dispatched, completed, failed = self._dispatch_and_wait_batches(
            execution_id, pipeline_name, batch_number, job_count, effective_rate_limit
        )
        logger.info(
            "Execution %s: %d dispatched, %d completed, %d failed",
            execution_id,
            dispatched,
            completed,
            failed,
        )

    def _run_id_based_pipeline_jobs(
        self,
        execution_id: str,
        pipeline: "IdBasedPipeline",
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

        logger.info(
            "IdBasedPipeline dispatch starting: %s (execution %s)", pipeline_name, execution_id
        )
        logger.info("Processing %d IDs (batch_size=%d): %s", len(ids), ids_batch_size, ids)

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
        logger.info(
            "Phase 1: Saving jobs to database for %d IDs in %d batches...",
            len(ids),
            len(id_batches),
        )
        batch_number = 1
        job_count = 0
        current_job_ids: List[str] = []

        for ids_batch in id_batches:
            logger.info("Processing ID batch: %s", ids_batch)

            resolved = pipeline.resolve_for_ids(params, ids_batch)
            source = resolved["source"]
            batch_params = resolved.get("batch_params", params)

            for sub_source in source.split(batch_params):
                context.batch_number = batch_number
                context_dict = context.to_dict()
                context_dict["runtime_params"] = dict(batch_params)
                metadata = {**context_dict, "current_ids": ids_batch, "source_metadata": None}

                dedup_check = not enable_duplicate_jobs
                job_id = str(uuid.uuid4())

                job_payload = build_job_payload(
                    execution_id,
                    job_id,
                    pipeline_name,
                    sub_source,
                    metadata,
                    dedup_check=dedup_check,
                )
                job_payload = self._serialize_for_json(job_payload)

                self.job_manager.create_job(
                    execution_id=execution_id,
                    job_id=job_id,
                    job_payload=job_payload,
                    batch_number=batch_number,
                )
                current_job_ids.append(job_id)
                job_count += 1
                if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                    batch_number += 1
                    current_job_ids = []

        # Set total_jobs correctly (once, after all jobs saved)
        self._set_total_jobs(execution_id, job_count)
        self._backfill_total_batches(execution_id, batch_number)
        logger.info(
            "Saved %d jobs to database in %d batches (from %d IDs)",
            job_count,
            batch_number,
            len(ids),
        )

        # Phase 2: dispatch and wait for each batch
        dispatched, completed, failed = self._dispatch_and_wait_batches(
            execution_id, pipeline_name, batch_number, job_count, effective_rate_limit
        )
        logger.info(
            "IdBasedPipeline %s: %d dispatched, %d completed, %d failed (%d IDs)",
            execution_id,
            dispatched,
            completed,
            failed,
            len(ids),
        )

    def _backfill_total_batches(self, execution_id: str, total_batches: int) -> None:
        """Back-fill total_batches into every job's metadata payload (best-effort)."""
        try:
            self.job_manager.bulk_set_total_batches(execution_id, total_batches)
        except Exception:
            self.job_manager.db.rollback()
            logger.warning(
                "Could not back-fill total_batches for %s; continuing",
                execution_id,
                exc_info=True,
            )

    def _dispatch_one_batch(
        self,
        execution_id: str,
        pipeline_name: str,
        jobs: List[Job],
        effective_rate_limit: Optional[float],
        is_local_mode: bool,
        prior_dispatched: int = 0,
    ) -> Tuple[int, int, int]:
        """
        Dispatch a single batch of jobs and report its counts.

        Returns:
            Tuple of (dispatched, completed, failed) for this batch only.
        """
        job_ids = [job.job_id for job in jobs]
        job_payloads = [job.job_payload for job in jobs]

        # Use an execution-scoped rate-limit key so concurrent executions of the
        # same pipeline don't contend on the same rate limiter bucket.
        rate_limit_key = f"{pipeline_name}:{execution_id}"
        dispatched = _run_async(
            self.dispatcher.dispatch_jobs_batch(
                jobs=job_payloads,
                pipeline_name=rate_limit_key,
                rate_limit=effective_rate_limit,
            )
        )

        # Mark jobs as dispatched and surface progress before the (possibly long) wait.
        self.job_manager.mark_jobs_dispatched(job_ids)
        self._set_job_counts(execution_id, dispatched=prior_dispatched + dispatched)

        if is_local_mode:
            # LocalDispatcher executed jobs synchronously and wrote their states
            # directly to the DB, so no polling is needed.
            completed = dispatched
            failed = len(jobs) - dispatched
        else:
            # Distributed mode: jobs sent to Kafka, wait for workers to complete.
            logger.info("Waiting for batch completion...")
            completed, failed = self._wait_for_batch_completion(
                job_ids=job_ids,
                timeout=CHECKPOINT_BATCH_TIMEOUT,
                poll_interval=CHECKPOINT_POLL_INTERVAL,
            )

        return dispatched, completed, failed

    def _dispatch_and_wait_batches(
        self,
        execution_id: str,
        pipeline_name: str,
        batch_number: int,
        job_count: int,
        effective_rate_limit: Optional[float],
    ) -> Tuple[int, int, int]:
        """
        Phase 2: dispatch every batch in order, then drive the execution to a terminal state.

        Shared by the standard and ID-based pipeline flows.

        Returns:
            Final (dispatched, completed, failed) counts synced from the database.
        """
        is_local_mode = isinstance(self.dispatcher, LocalDispatcher)
        if is_local_mode:
            logger.info("Phase 2: Executing batches locally (in-process)...")
        else:
            logger.info("Phase 2: Dispatching batches...")

        total_dispatched = 0
        total_completed = 0
        total_failed = 0

        for current_batch_num in range(1, batch_number + 1):
            jobs = self.job_manager.get_pending_jobs_by_batch_number(
                execution_id, current_batch_num
            )
            if not jobs:
                continue

            logger.info(
                "%s batch %d (%d jobs)...",
                "Executing" if is_local_mode else "Dispatching",
                current_batch_num,
                len(jobs),
            )

            dispatched, completed, failed = self._dispatch_one_batch(
                execution_id,
                pipeline_name,
                jobs,
                effective_rate_limit,
                is_local_mode,
                prior_dispatched=total_dispatched,
            )
            total_dispatched += dispatched
            total_completed += completed
            total_failed += failed

            self._set_job_counts(
                execution_id,
                dispatched=total_dispatched,
                completed=total_completed,
                failed=total_failed,
            )
            logger.info("Batch %d: %d completed, %d failed", current_batch_num, completed, failed)

        return self._finalize_execution_state(
            execution_id, job_count, (total_dispatched, total_completed, total_failed)
        )

    def _finalize_execution_state(
        self,
        execution_id: str,
        expected_total: int,
        fallback_counts: Tuple[int, int, int],
    ) -> Tuple[int, int, int]:
        """
        Sync final counts from the DB and set the terminal execution state.

        Args:
            execution_id: Execution identifier
            expected_total: Number of jobs that should have finished
            fallback_counts: (dispatched, completed, failed) to use if the DB sync fails

        Returns:
            The (dispatched, completed, failed) counts used for the final state.
        """
        try:
            dispatched, completed, failed = self._sync_counts_from_db(execution_id)
            counts = self.job_manager.get_job_counts(execution_id)
            total_finished = _finished_count(
                counts.get("completed", 0),
                counts.get("failed", 0),
                counts.get("deduplicated", 0),
            )
        except Exception:
            logger.warning(
                "Could not sync counts for %s; using in-memory totals",
                execution_id,
                exc_info=True,
            )
            dispatched, completed, failed = fallback_counts
            # completed from the loop already includes any deduplicated jobs.
            total_finished = completed + failed

        if total_finished == expected_total:
            final_state = "completed" if failed == 0 else "failed"
        else:
            final_state = "failed"
            logger.warning(
                "Only %d of %d jobs finished for %s", total_finished, expected_total, execution_id
            )

        self.execution_manager.update_execution_state(execution_id, final_state)
        return dispatched, completed, failed

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
        deduplicated = job_counts.get("deduplicated", 0)
        dispatched = job_counts.get("dispatched", 0) + completed + failed + deduplicated

        # Update execution with real counts
        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            execution.jobs_dispatched = dispatched
            execution.jobs_completed = completed + deduplicated
            execution.jobs_failed = failed
            execution.deduplicated_jobs = deduplicated
            self.execution_manager.db.commit()

        return (dispatched, completed + deduplicated, failed)

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
            states = self.job_manager.get_job_states(job_ids)

            completed = 0
            failed = 0
            pending = 0

            for job_id in job_ids:
                state = states.get(job_id, "pending")
                if state in ("completed", "deduplicated"):
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
        logger.warning("Batch timeout after %ss, some jobs may not have completed", timeout)
        states = self.job_manager.get_job_states(job_ids)
        completed = sum(1 for s in states.values() if s in ("completed", "deduplicated"))
        failed = sum(1 for s in states.values() if s == "failed")

        return (completed, failed)

    def _serialize_for_json(self, obj: Any) -> Any:
        """Recursively convert objects to JSON-serializable form."""
        return to_json_safe(obj)
