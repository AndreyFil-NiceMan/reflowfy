"""Pipeline execution runner for ReflowManager."""

import asyncio
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple


from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session

from reflowfy.core.exceptions import pipeline_step
from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.job_runner import plan_slices
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.observability import metrics
from reflowfy.observability.context import bind_log_context
from reflowfy.observability.tracing import inject_trace_context
from reflowfy.reflow_manager.dispatcher import JobDispatcher
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.reflow_manager.models import Job

logger = logging.getLogger(__name__)

# Planning granularity. Phase 1 writes jobs to the database this many at a time
# (planning stays streaming) and tags each with a batch_number, which the stats
# endpoint reports as "checkpoints". It no longer gates dispatch.
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "1000"))
CHECKPOINT_POLL_INTERVAL = 2.0  # seconds between drive-loop polls when nothing can be sent

# Phase 2 is a continuous loop, not a batch barrier: it keeps up to this many jobs
# in flight (dispatched or running) and tops the window up as jobs finish. This is
# the throughput lever the batch size used to be — enough queued work for KEDA to
# see lag and scale, bounded so a stalled worker pool can't absorb the whole plan.
MAX_IN_FLIGHT_JOBS = int(os.getenv("MAX_IN_FLIGHT_JOBS", "1000"))

# Job leases. A job a worker claimed (running) with no result after the running
# lease lost its worker (killed, OOM) and goes back to pending — or fails after
# JOB_MAX_ATTEMPTS claims. A job dispatched but never claimed after the dispatch
# lease lost its message and is sent again. The worker-side claim makes the
# duplicate message harmless.
# The worker refreshes a running job's updated_at every JOB_HEARTBEAT_SECONDS (30),
# so the running lease only has to exceed a few missed beats, not the longest job.
JOB_RUNNING_LEASE_SECONDS = float(os.getenv("JOB_RUNNING_LEASE_SECONDS", "300"))
JOB_DISPATCH_LEASE_SECONDS = float(os.getenv("JOB_DISPATCH_LEASE_SECONDS", "3600"))
JOB_MAX_ATTEMPTS = int(os.getenv("JOB_MAX_ATTEMPTS", "3"))
LEASE_CHECK_INTERVAL = 60.0  # seconds between lease sweeps

# The dispatcher hands control back to the drive loop this often, so lease sweeps
# and the in-flight cap are re-evaluated even at very low rate limits.
DISPATCH_CHUNK_SECONDS = 30.0
DISPATCH_FETCH_LIMIT = 500  # job payloads loaded per drive-loop iteration
STALL_WARN_SECONDS = 600.0  # warn when jobs are in flight but none finished this long

# One dispatch loop per execution, by a lease on the execution row: the owner
# heartbeats about once a second; another loop (a restarted manager, a second pod,
# a resume racing a pausing loop) stands by and takes over once the heartbeat is
# older than this. Must exceed the longest gap between heartbeats — a rate-limit
# token wait (TOKEN_WAIT_SECONDS) plus one chunk, or one in-process job in local mode.
DRIVER_LEASE_SECONDS = float(os.getenv("DRIVER_LEASE_SECONDS", "60"))
DRIVER_RETRY_SECONDS = 15.0


def _iter_plan(pipeline_name: str, slices: Iterator[Any]) -> Iterator[Any]:
    """Yield planned jobs, naming ``define_jobs`` when the plan itself raises.

    A ``define_jobs`` that ``yield``s runs its body here, during Phase 1, not at
    ``resolve()`` time — so the ``pipeline_step`` wrapper around the call in
    :meth:`AbstractPipeline.resolve` never sees the user's code fail. Only the
    step that advances the plan is wrapped: the caller's loop body writes to the
    database, and those errors are not the user's planning code.
    """
    iterator = iter(slices)
    while True:
        with pipeline_step("define_jobs", pipeline_name):
            try:
                item = next(iterator)
            except StopIteration:
                return
        yield item


# Worker job message schema version
JOB_SCHEMA_VERSION = 2


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

            nest_asyncio.apply()  # pyright: ignore[reportUnknownMemberType]  # no stubs
        except (ImportError, ValueError):
            # ValueError: nest_asyncio can't patch uvloop, which uvicorn runs — the
            # case for the synchronous DLQ "dispatch now" endpoint. Use the thread.
            pass
        else:
            return asyncio.get_event_loop().run_until_complete(coro)
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
    job_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the v2 worker job message for one narrowed sub-source.

    The current span's W3C traceparent is injected into metadata so the worker
    can continue the trace across the Kafka hop (additive; no schema bump).

    ``job_params`` is what the plan attached to this job alone (see
    :func:`reflowfy.job`), carried separately from the merged runtime_params in
    metadata because content deduplication has to tell two jobs apart by it —
    the merged view also holds execution-wide params, which are identical
    across jobs (additive; no schema bump).
    """
    enriched_metadata = dict(metadata)
    inject_trace_context(enriched_metadata)
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "execution_id": execution_id,
        "job_id": job_id,
        "pipeline_name": pipeline_name,
        "source": SourceFactory.serialize(sub_source),
        "dedup_check": dedup_check,
        "job_params": job_params or {},
        "metadata": enriched_metadata,
    }


PLAN_HEARTBEAT_SECONDS = 10.0


class _PlanLease:
    """The planning lease of one execution, kept alive from a thread.

    Planning blocks the manager for as long as the source needs to enumerate its
    result set (about 40 s for 2M Elasticsearch docs, minutes for much larger ones),
    so the heartbeat cannot come from the planner itself. While the lease is fresh
    no other manager treats the execution as abandoned; once the planner dies the
    heartbeat stops, and recovery can tell (see :meth:`PipelineRunner._recover_plan`).
    """

    def __init__(self, bind: Any, execution_id: str):
        self._bind = bind
        self._execution_id = execution_id
        self._owner = f"plan:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._stopped = False

    def start(self) -> None:
        """Claim the lease and start heartbeating. Best effort: planning goes ahead
        unprotected rather than not at all if the database refuses."""
        try:
            with Session(bind=self._bind) as session:
                ExecutionManager(session).claim_driver(
                    self._execution_id, self._owner, DRIVER_LEASE_SECONDS
                )
        except Exception:
            logger.warning(
                "Could not take the planning lease",
                exc_info=True,
                extra={"event": "plan_lease_error", "execution_id": self._execution_id},
            )
            return
        self._thread = threading.Thread(
            target=self._beat, name=f"plan-lease-{self._execution_id[:8]}", daemon=True
        )
        self._thread.start()

    def _beat(self) -> None:
        while not self._stop.wait(PLAN_HEARTBEAT_SECONDS):
            try:
                with Session(bind=self._bind) as session:
                    ExecutionManager(session).heartbeat_driver(self._execution_id, self._owner)
            except Exception:
                logger.warning(
                    "Planning heartbeat failed; will retry",
                    exc_info=True,
                    extra={"event": "plan_heartbeat_error", "execution_id": self._execution_id},
                )

    def stop(self) -> None:
        """Stop heartbeating and release the lease (the drive loop takes its own)."""
        if self._stopped:
            return
        self._stopped = True
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            try:
                with Session(bind=self._bind) as session:
                    ExecutionManager(session).release_driver(self._execution_id, self._owner)
            except Exception:
                logger.warning(
                    "Could not release the planning lease",
                    exc_info=True,
                    extra={"event": "plan_lease_error", "execution_id": self._execution_id},
                )


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

    @bind_log_context("execution_id", "pipeline_name")
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
            rate_limit_override: Optional override for jobs per minute

        Returns:
            Dictionary with execution details and job counts
        """
        from reflowfy.core.registry import pipeline_registry

        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            raise ValueError(
                f"Pipeline '{pipeline_name}' not found in registry"
                f"{pipeline_registry.describe_missing(pipeline_name)}"
            )

        logger.info("Running pipeline", extra={"event": "execution_start"})

        # Create execution record
        self.execution_manager.create_execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            rate_limit=rate_limit_override,
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
                logger.exception(
                    "Failed to mark execution as failed",
                    extra={"event": "execution_mark_failed_error"},
                )
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

    @bind_log_context("execution_id")
    def resume_execution(
        self,
        execution_id: str,
        rate_limit_override: Optional[float] = None,
    ) -> None:
        """
        Resume an interrupted execution from where it left off.

        Skips Phase 1 (jobs already in DB) and re-enters the Phase 2 drive loop,
        which picks up pending jobs and leaves in-flight ones to their leases.
        Used for crash recovery and by POST /executions/{id}/resume.

        Args:
            execution_id: Execution identifier to resume
            rate_limit_override: Optional override for jobs per minute
        """
        from reflowfy.core.registry import pipeline_registry

        # Get execution record
        execution = self.execution_manager.get_execution(execution_id)
        if not execution:
            logger.warning(
                "Execution not found, skipping resume", extra={"event": "resume_skipped"}
            )
            return
        if execution.state in ExecutionManager.TERMINAL_STATES:
            # Settled while this resume was queued (e.g. /cancel after /resume):
            # driving it would finalize it again and overwrite "cancelled".
            logger.info(
                "Execution is already %s; not resuming",
                execution.state,
                extra={"event": "resume_skipped", "state": execution.state},
            )
            return

        pipeline_name = execution.pipeline_name

        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            logger.warning(
                "Pipeline not in registry, marking execution as failed",
                extra={"event": "resume_pipeline_missing", "pipeline_name": pipeline_name},
            )
            self.execution_manager.update_execution_state(
                execution_id,
                "failed",
                error_message=(
                    f"Pipeline '{pipeline_name}' not found in registry during recovery"
                    f"{pipeline_registry.describe_missing(pipeline_name)}"
                ),
            )
            return

        if execution.plan_complete is False and not self._recover_plan(execution_id):
            return
        self.execution_manager.db.refresh(execution)

        # Nothing unfinished: settle the final state from the job rows.
        counts = self.job_manager.get_job_counts(execution_id)
        unfinished = counts["pending"] + counts["dispatched"] + counts["running"]
        if unfinished == 0:
            logger.info(
                "Execution has no unfinished jobs, syncing state",
                extra={"event": "resume_nothing_to_do", "jobs_total": counts["total"]},
            )
            self._finalize_execution_state(execution_id, counts["total"], (0, 0, 0))
            return

        # Determine effective rate limit. The original /run override is persisted on the
        # execution; without it a restart silently fell back to the class rate_limit.
        effective_rate_limit = rate_limit_override
        if effective_rate_limit is None:
            effective_rate_limit = execution.rate_limit
        if effective_rate_limit is None and pipeline.rate_limit:
            effective_rate_limit = pipeline.rate_limit

        # Local mode runs jobs in-process, so whatever was dispatched or running died
        # with the restart. Requeue it now instead of waiting out the leases. In
        # distributed mode those jobs may still be alive on a worker; the leases decide.
        if isinstance(self.dispatcher, LocalDispatcher):
            orphaned = (
                self.job_manager.db.query(Job)
                .filter(
                    Job.execution_id == execution_id,
                    Job.state.in_(["dispatched", "running"]),
                )
                .update({"state": "pending"}, synchronize_session=False)
            )
            self.job_manager.db.commit()
            if orphaned:
                logger.warning(
                    "Local mode: requeued %d orphaned jobs",
                    orphaned,
                    extra={"event": "orphaned_jobs_requeued", "jobs_requeued": orphaned},
                )

        logger.info(
            "Resuming execution: %d pending, %d in flight",
            counts["pending"],
            counts["dispatched"] + counts["running"],
            extra={
                "event": "execution_resume",
                "jobs_pending": counts["pending"],
                "jobs_in_flight": counts["dispatched"] + counts["running"],
            },
        )
        dispatched, completed, failed = self._drive_execution(
            execution_id, pipeline_name, effective_rate_limit
        )
        logger.info(
            "Resumed execution drive ended: %d dispatched, %d completed, %d failed",
            dispatched,
            completed,
            failed,
            extra={
                "event": "execution_drive_end",
                "jobs_dispatched": dispatched,
                "jobs_completed": completed,
                "jobs_failed": failed,
            },
        )

    @bind_log_context("execution_id", "pipeline_name")
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
            rate_limit_override: Optional override for jobs per minute
            enable_duplicate_jobs: True = jobs may run multiple times (default);
                False = each unique job (by content hash) runs at most once.
                None falls back to the pipeline's own enable_duplicate_jobs setting.

        Planning (resolving the source and saving every job) holds a heartbeated lease
        on the execution; ``executions.plan_complete`` flips to true once the last job
        is saved. A manager killed in between leaves ``plan_complete`` false, and
        recovery then fails the execution instead of dispatching a fragment of its plan.
        """
        lease = _PlanLease(self.execution_manager.db.get_bind(), execution_id)
        lease.start()
        try:
            self._plan_and_dispatch(
                execution_id,
                pipeline_name,
                runtime_params,
                rate_limit_override,
                enable_duplicate_jobs,
                lease,
            )
        finally:
            lease.stop()

    @bind_log_context("execution_id", "pipeline_name")
    def _plan_and_dispatch(
        self,
        execution_id: str,
        pipeline_name: str,
        runtime_params: Dict[str, Any],
        rate_limit_override: Optional[float],
        enable_duplicate_jobs: Optional[bool],
        plan_lease: "_PlanLease",
    ) -> None:
        """Phase 1 (plan: save every job) then Phase 2 (dispatch until done)."""
        from reflowfy.core.execution_context import ExecutionContext
        from reflowfy.core.id_based_pipeline import IdBasedPipeline
        from reflowfy.core.registry import pipeline_registry

        # Load pipeline from registry
        pipeline = pipeline_registry.get(pipeline_name)
        if not pipeline:
            raise ValueError(
                f"Pipeline '{pipeline_name}' not found in registry"
                f"{pipeline_registry.describe_missing(pipeline_name)}"
            )

        # Count the execution here (the convergence point for both the API /run
        # path and run_pipeline / scheduler paths).
        mode = "local" if isinstance(self.dispatcher, LocalDispatcher) else "distributed"
        metrics.pipeline_executions_total.labels(pipeline=pipeline_name, mode=mode).inc()

        # Resolve effective duplicate setting: API override > pipeline default
        if enable_duplicate_jobs is None:
            enable_duplicate_jobs = getattr(pipeline, "enable_duplicate_jobs", True)

        # Resolve pipeline with runtime params.
        # _resolved_params includes defaults + any keys added by define_source.
        # On a private copy: resolve() stores the plan on the instance, and the registry's
        # instance is shared by every execution of this pipeline running in this process.
        if hasattr(pipeline, "fork"):
            pipeline = pipeline.fork()
        if hasattr(pipeline, "resolve"):
            pipeline.resolve(runtime_params)

        logger.info("Job dispatch starting", extra={"event": "plan_start"})

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

        logger.info(
            "Splitting source data into jobs (rate: %s/min)",
            effective_rate_limit,
            extra={"event": "plan_split", "rate_limit": effective_rate_limit},
        )

        # Phase 1: Stream all jobs to database (not RAM)
        logger.debug("Phase 1: saving jobs to database", extra={"event": "plan_save"})
        batch_number = 1
        job_count = 0
        current_job_ids: List[str] = []
        job_rows: List[Dict[str, Any]] = []

        # Params for jobs the plan didn't narrow itself. IdBasedPipeline.define_jobs
        # sets job_params per job, but a pipeline that *overrides* define_jobs plans
        # its own sources and sets nothing — and then every job would carry the whole
        # `input_ids` list. Drop it for the ID-based path as define_jobs does.
        default_job_params = enriched_params
        if isinstance(pipeline, IdBasedPipeline):
            default_job_params = {k: v for k, v in enriched_params.items() if k != "input_ids"}

        base_source = pipeline.source
        plan = _iter_plan(pipeline_name, plan_slices(base_source, enriched_params))
        last_state_check = 0.0
        for sub_source in plan:
            # A cancelled (or force-failed) execution must not keep planning: a plan can take
            # minutes to hours, and its jobs would just be cancelled again as they are saved.
            # The check cannot interrupt a source call that blocks (the enumeration before the
            # first slice); it takes effect as soon as that returns.
            if time.time() - last_state_check >= 1.0:
                last_state_check = time.time()
                if self._execution_state(execution_id) in ("cancelled", "failed"):
                    cancelled_jobs = self.job_manager.cancel_unstarted_jobs(execution_id)
                    logger.info(
                        "Execution was stopped while planning (%d jobs cancelled); planning aborted",
                        cancelled_jobs,
                        extra={"event": "plan_aborted", "jobs_cancelled": cancelled_jobs},
                    )
                    return
            context.batch_number = batch_number
            context_dict = context.to_dict()
            # A planned source may narrow the params its own job receives — an
            # IdBasedPipeline gives each job only the IDs that job handles,
            # instead of every ID in the execution. Everyone else gets the
            # execution's params unchanged.
            # Merged, not replaced: reflowfy.job() attaches only the keys that
            # identify one job (e.g. current_id), and the rest of the
            # execution's params still have to reach it.
            own_params = getattr(sub_source, "job_params", None)
            job_params = {**default_job_params, **own_params} if own_params else default_job_params
            context_dict["runtime_params"] = dict(job_params)
            metadata = {**context_dict, "source_metadata": None}
            if "current_ids" in job_params:
                metadata["current_ids"] = job_params["current_ids"]

            dedup_check = not enable_duplicate_jobs
            job_id = str(uuid.uuid4())

            job_payload = build_job_payload(
                execution_id,
                job_id,
                pipeline_name,
                sub_source,
                metadata,
                dedup_check=dedup_check,
                job_params=own_params,
            )
            job_payload = self._serialize_for_json(job_payload)

            job_rows.append(
                {
                    "execution_id": execution_id,
                    "job_id": job_id,
                    "job_payload": job_payload,
                    "batch_number": batch_number,
                }
            )
            current_job_ids.append(job_id)
            job_count += 1
            if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                # Flush per checkpoint batch so planning stays streaming: the
                # buffer never holds more than one batch of job payloads.
                self.job_manager.create_jobs(job_rows)
                job_rows = []
                batch_number += 1
                current_job_ids = []

        self.job_manager.create_jobs(job_rows)

        # Set total_jobs correctly (once, after all jobs saved)
        self._set_total_jobs(execution_id, job_count)
        # The plan is whole: from here on a restarted manager may safely dispatch it.
        self.execution_manager.mark_plan_complete(execution_id)
        plan_lease.stop()
        self._backfill_total_batches(execution_id, batch_number)
        logger.info(
            "Saved %d jobs to database in %d batches",
            job_count,
            batch_number,
            extra={"event": "plan_complete", "jobs_total": job_count, "batches": batch_number},
        )

        # Phase 2: keep the in-flight window full until every job has finished
        dispatched, completed, failed = self._drive_execution(
            execution_id, pipeline_name, effective_rate_limit
        )
        logger.info(
            "Execution drive ended: %d dispatched, %d completed, %d failed",
            dispatched,
            completed,
            failed,
            extra={
                "event": "execution_drive_end",
                "jobs_dispatched": dispatched,
                "jobs_completed": completed,
                "jobs_failed": failed,
            },
        )

    def _backfill_total_batches(self, execution_id: str, total_batches: int) -> None:
        """Back-fill total_batches into every job's metadata payload (best-effort)."""
        try:
            self.job_manager.bulk_set_total_batches(execution_id, total_batches)
        except Exception:
            self.job_manager.db.rollback()
            logger.warning(
                "Could not back-fill total_batches; continuing",
                exc_info=True,
                extra={"event": "backfill_batches_error"},
            )

    @bind_log_context("execution_id", "pipeline_name")
    def _drive_execution(
        self,
        execution_id: str,
        pipeline_name: str,
        effective_rate_limit: Optional[float],
    ) -> Tuple[int, int, int]:
        """Phase 2: dispatch jobs until every one has finished, then set the final state.

        Only one loop may drive an execution — a second one would double the dispatch
        rate. Ownership is a lease on the execution row (see DRIVER_LEASE_SECONDS).

        Returns:
            Final (dispatched, completed, failed) counts.
        """
        driver_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        try:
            result: Tuple[int, int, int] = _run_async(
                self._drive_async(execution_id, pipeline_name, effective_rate_limit, driver_id)
            )
            return result
        finally:
            try:
                self.execution_manager.release_driver(execution_id, driver_id)
            except Exception:
                logger.warning(
                    "Could not release dispatch lease",
                    exc_info=True,
                    extra={"event": "driver_release_error"},
                )

    @bind_log_context("execution_id")
    async def _wait_for_driver_lease(self, execution_id: str, driver_id: str) -> bool:
        """Claim the dispatch lease, standing by while a live loop holds it.

        Returns False if the execution stops running while waiting.
        """
        announced = False
        while True:
            try:
                if self.execution_manager.claim_driver(
                    execution_id, driver_id, DRIVER_LEASE_SECONDS
                ):
                    return True
                if self._execution_state(execution_id) != "running":
                    return False
            except DBAPIError:
                logger.warning(
                    "Database error claiming the dispatch lease; retrying",
                    exc_info=True,
                    extra={"event": "driver_claim_db_error"},
                )
                self.job_manager.db.rollback()
            if not announced:
                logger.info(
                    "Execution is being dispatched by another loop; standing by to "
                    "take over if it stops",
                    extra={"event": "driver_standby"},
                )
                announced = True
            await asyncio.sleep(DRIVER_RETRY_SECONDS)

    @bind_log_context("execution_id", "pipeline_name")
    async def _drive_async(
        self,
        execution_id: str,
        pipeline_name: str,
        effective_rate_limit: Optional[float],
        driver_id: str,
    ) -> Tuple[int, int, int]:
        """The drive loop. One event loop for the whole execution, so the Kafka
        producer is created once and closed at the end."""
        if not await self._wait_for_driver_lease(execution_id, driver_id):
            logger.info(
                "Execution is no longer running; not dispatching",
                extra={"event": "dispatch_skipped"},
            )
            return self._sync_counts_from_db(execution_id)
        # Execution-scoped key: concurrent executions of one pipeline don't share a bucket.
        rate_limit_key = f"{pipeline_name}:{execution_id}"
        started = time.time()
        # Loop bookkeeping carried between steps.
        track: Dict[str, float] = {
            "finished": -1.0,
            "progress_at": started,
            "warned_at": started,
            "lease_checked_at": 0.0,
        }
        db_backoff = 0.0
        try:
            while True:
                try:
                    result = await self._drive_step(
                        execution_id,
                        pipeline_name,
                        effective_rate_limit,
                        rate_limit_key,
                        driver_id,
                        track,
                    )
                except DBAPIError:
                    # Postgres went away (restart, failover, idled namespace). The jobs and
                    # their states are safe in the database; keep retrying instead of
                    # ending the loop, which left the execution stuck until a manager restart.
                    db_backoff = min(max(db_backoff * 2, 5.0), 60.0)
                    logger.warning(
                        "Database error in dispatch loop; retrying in %.0fs",
                        db_backoff,
                        exc_info=True,
                        extra={"event": "dispatch_db_error", "retry_in_s": db_backoff},
                    )
                    try:
                        self.job_manager.db.rollback()
                    except Exception:
                        pass
                    await asyncio.sleep(db_backoff)
                    continue
                db_backoff = 0.0
                if result is not None:
                    return result
        finally:
            try:
                await self.dispatcher.close()
            except Exception:
                logger.warning(
                    "Could not close dispatcher",
                    exc_info=True,
                    extra={"event": "dispatcher_close_error"},
                )

    async def _drive_step(
        self,
        execution_id: str,
        pipeline_name: str,
        effective_rate_limit: Optional[float],
        rate_limit_key: str,
        driver_id: str,
        track: Dict[str, float],
    ) -> Optional[Tuple[int, int, int]]:
        """One pass of the drive loop. Returns the final counts when the loop should end."""
        if not self.execution_manager.heartbeat_driver(execution_id, driver_id):
            # Our heartbeat went stale (a long stall) and another loop took over.
            logger.warning(
                "Another dispatch loop took over; this one stops",
                extra={"event": "driver_replaced"},
            )
            return self._sync_counts_from_db(execution_id)
        state = self._execution_state(execution_id)
        if state != "running":
            # Paused (or settled elsewhere): stop sending. In-flight jobs still finish;
            # POST /executions/{id}/resume starts a new loop.
            logger.info(
                "Execution is %s; dispatch loop stopping",
                state,
                extra={"event": "dispatch_stopped", "state": state},
            )
            return self._sync_counts_from_db(execution_id)

        now = time.time()
        if now - track["lease_checked_at"] >= LEASE_CHECK_INTERVAL:
            self._reclaim_expired_leases(execution_id, pipeline_name)
            track["lease_checked_at"] = now

        counts = self.job_manager.get_job_counts(execution_id)
        in_flight = counts["dispatched"] + counts["running"]
        if counts["pending"] == 0 and in_flight == 0:
            return self._finalize_execution_state(
                execution_id, counts["total"], self._sync_counts_from_db(execution_id)
            )

        finished = _finished_count(counts["completed"], counts["failed"], counts["deduplicated"])
        if finished != track["finished"]:
            track["finished"], track["progress_at"] = finished, now
        elif now - max(track["progress_at"], track["warned_at"]) >= STALL_WARN_SECONDS:
            # No batch timeout fails the run any more, so say so when it's stuck.
            logger.warning(
                "No job finished for %.0f min with %d in flight "
                "and %d pending — are workers running?",
                (now - track["progress_at"]) / 60,
                in_flight,
                counts["pending"],
                extra={
                    "event": "dispatch_stalled",
                    "stalled_minutes": round((now - track["progress_at"]) / 60),
                    "jobs_in_flight": in_flight,
                    "jobs_pending": counts["pending"],
                },
            )
            track["warned_at"] = now

        free_slots = MAX_IN_FLIGHT_JOBS - in_flight
        sent = 0
        if counts["pending"] and free_slots > 0:
            jobs = self.job_manager.get_pending_jobs(
                execution_id, limit=min(free_slots, DISPATCH_FETCH_LIMIT)
            )
            sent = await self._send_jobs(
                execution_id, rate_limit_key, effective_rate_limit, driver_id, jobs
            )
            self._sync_counts_from_db(execution_id)
        if not sent:
            # Window full, nothing pending, or the send failed: wait, but return the
            # pooled connection first (the count query opened a transaction; holding it
            # across the sleep pins a connection per execution and exhausts the pool
            # when schedules overlap).
            self.job_manager.db.commit()
            await asyncio.sleep(CHECKPOINT_POLL_INTERVAL)
        return None

    async def _send_jobs(
        self,
        execution_id: str,
        rate_limit_key: str,
        effective_rate_limit: Optional[float],
        driver_id: str,
        jobs: List[Job],
    ) -> int:
        """Send jobs in order, marking each dispatched right after its send.

        Hands control back after DISPATCH_CHUNK_SECONDS, and stops within about a
        second of the execution being paused.
        """
        started = time.time()
        last_check = 0.0
        stop = False

        def should_stop(chunk: bool = True) -> bool:
            # chunk=False: a token is already spent, so only a pause or a lost lease
            # may stop the send; the chunk running out must not drop the job.
            nonlocal last_check, stop
            now = time.time()
            if chunk and now - started >= DISPATCH_CHUNK_SECONDS:
                return True
            if now - last_check >= 1.0:
                last_check = now
                # Heartbeat here too: a chunk can outlast many drive-loop passes.
                stop = (
                    not self.execution_manager.heartbeat_driver(execution_id, driver_id)
                    or self._execution_state(execution_id) != "running"
                )
            return stop

        def on_sent(payload: Dict[str, Any]) -> None:
            self.job_manager.mark_job_dispatched(payload["job_id"])

        def on_unsendable(payload: Dict[str, Any], reason: str) -> None:
            self.job_manager.update_job_state(payload["job_id"], "failed", error_message=reason)
            metrics.jobs_processed_total.labels(
                pipeline=payload.get("pipeline_name", "unknown"), status="failed"
            ).inc()

        sent: int = await self.dispatcher.dispatch_jobs_batch(
            jobs=[job.job_payload for job in jobs],
            pipeline_name=rate_limit_key,
            rate_limit=effective_rate_limit,
            on_sent=on_sent,
            should_stop=should_stop,
            on_unsendable=on_unsendable,
        )
        return sent

    @bind_log_context("execution_id", "pipeline_name")
    def _reclaim_expired_leases(self, execution_id: str, pipeline_name: str) -> None:
        """Requeue jobs whose worker or message was lost; fail ones out of attempts."""
        result = self.job_manager.reclaim_expired_leases(
            execution_id,
            running_lease_seconds=JOB_RUNNING_LEASE_SECONDS,
            dispatch_lease_seconds=JOB_DISPATCH_LEASE_SECONDS,
            max_attempts=JOB_MAX_ATTEMPTS,
        )
        if any(result.values()):
            logger.warning(
                "Lease expired — requeued %d lost-worker and %d "
                "lost-message jobs, failed %d out of attempts",
                result["requeued_running"],
                result["requeued_dispatched"],
                result["failed"],
                extra={
                    "event": "lease_expired",
                    "jobs_requeued_running": result["requeued_running"],
                    "jobs_requeued_dispatched": result["requeued_dispatched"],
                    "jobs_failed": result["failed"],
                },
            )
        if result["failed"]:
            metrics.jobs_processed_total.labels(pipeline=pipeline_name, status="lease_expired").inc(
                result["failed"]
            )

    @bind_log_context("execution_id")
    def _recover_plan(self, execution_id: str) -> bool:
        """Deal with an execution whose planning had not finished when we looked.

        The jobs saved so far are a fragment of the plan and the source's position
        cannot be recovered, so a plan can't be continued. But a manager that is still
        planning (this pod's neighbour, or the old pod during a rollout) looks the same
        until its lease says otherwise. So: wait while the planning lease is fresh, and
        once it is gone — and the execution is older than one lease, so a record
        created a moment ago is not mistaken for an orphan — fail the execution with a
        message saying so, instead of dispatching the fragment.

        Returns:
            True if planning finished while we waited (carry on resuming the execution),
            False if there is nothing left to resume (failed here, or settled elsewhere).
        """
        recovery_id = f"recover:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        announced = False
        while True:
            self.execution_manager.db.expire_all()
            execution = self.execution_manager.get_execution(execution_id)
            if execution is None or execution.state not in ("pending", "running"):
                return False
            if execution.plan_complete is not False:
                return True
            age = (
                datetime.now(timezone.utc).replace(tzinfo=None) - execution.created_at
            ).total_seconds()
            if age >= DRIVER_LEASE_SECONDS and self.execution_manager.claim_driver(
                execution_id, recovery_id, DRIVER_LEASE_SECONDS
            ):
                saved = self.job_manager.get_job_counts(execution_id)["total"]
                self.execution_manager.update_execution_state(
                    execution_id,
                    "failed",
                    error_message=(
                        f"The manager stopped while planning this execution ({saved} of its "
                        "jobs had been saved, none dispatched). A plan cannot be continued; "
                        "run it again."
                    ),
                )
                self.execution_manager.release_driver(execution_id, recovery_id)
                logger.warning(
                    "Execution was abandoned mid-plan (%d jobs saved); marked failed",
                    saved,
                    extra={"event": "plan_abandoned", "jobs_total": saved},
                )
                return False
            if not announced:
                logger.info(
                    "Execution is still being planned (or was just created); "
                    "waiting to see whether its planner is alive",
                    extra={"event": "plan_waiting"},
                )
                announced = True
            time.sleep(DRIVER_RETRY_SECONDS)

    def _execution_state(self, execution_id: str) -> Optional[str]:
        """Fresh execution state (pause/resume land from other sessions)."""
        self.execution_manager.db.expire_all()
        execution = self.execution_manager.get_execution(execution_id)
        return execution.state if execution else None

    @bind_log_context("execution_id")
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
                "Could not sync counts; using in-memory totals",
                exc_info=True,
                extra={"event": "sync_counts_error"},
            )
            dispatched, completed, failed = fallback_counts
            # completed from the loop already includes any deduplicated jobs.
            total_finished = completed + failed

        if total_finished == expected_total:
            final_state = "completed" if failed == 0 else "failed"
        else:
            final_state = "failed"
            logger.warning(
                "Only %d of %d jobs finished",
                total_finished,
                expected_total,
                extra={
                    "event": "jobs_missing",
                    "jobs_finished": total_finished,
                    "jobs_total": expected_total,
                },
            )

        self.execution_manager.update_execution_state(execution_id, final_state)
        logger.info(
            "Execution %s: %d completed, %d failed of %d jobs",
            final_state,
            completed,
            failed,
            expected_total,
            extra={
                "event": "execution_finished",
                "state": final_state,
                "jobs_total": expected_total,
                "jobs_completed": completed,
                "jobs_failed": failed,
            },
        )
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
        dispatched = (
            job_counts.get("dispatched", 0)
            + job_counts.get("running", 0)
            + completed
            + failed
            + deduplicated
        )

        # Update execution with real counts
        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            execution.jobs_dispatched = dispatched
            execution.jobs_completed = completed + deduplicated
            execution.jobs_failed = failed
            execution.deduplicated_jobs = deduplicated
            self.execution_manager.db.commit()

        return (dispatched, completed + deduplicated, failed)

    def _serialize_for_json(self, obj: Any) -> Any:
        """Recursively convert objects to JSON-serializable form."""
        return to_json_safe(obj)
