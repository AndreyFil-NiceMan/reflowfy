"""Pipeline execution runner for ReflowManager: planning (Phase 1) and the class.

``PipelineRunner`` = planning (here) + resume/recovery (``recovery``) + the dispatch
loop (``drive_loop``), one class per concern stacked by inheritance.
"""

import logging
import os
import socket
import threading
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional


from sqlalchemy.orm import Session

from reflowfy.core.exceptions import pipeline_step
from reflowfy.core.serialization import to_json_safe
from reflowfy.core.planning import ResolvedPlan
from reflowfy.execution.job_runner import plan_slices
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.observability import metrics
from reflowfy.observability.context import bind_log_context
from reflowfy.observability.tracing import inject_trace_context
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.store.states import ExecutionState

from reflowfy.reflow_manager import drive_loop
from reflowfy.reflow_manager.rate_limiter import resolve_rate_limit
from reflowfy.reflow_manager.recovery import Recovery

logger = logging.getLogger(__name__)

# Planning granularity. Phase 1 writes jobs to the database this many at a time
# (planning stays streaming) and tags each with a batch_number, which the stats
# endpoint reports as "checkpoints". It no longer gates dispatch.
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "1000"))


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


JOB_SCHEMA_VERSION = 2


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
        "source": SourceFactory.serialize(sub_source, pipeline_name=pipeline_name),
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
                    self._execution_id, self._owner, drive_loop.DRIVER_LEASE_SECONDS
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


class PipelineRunner(Recovery):
    """Executes pipelines by splitting jobs and dispatching to Kafka.

    Coordinates between:
    - ExecutionManager for execution records
    - JobManager for job payload and checkpoint tracking (unified)
    - BaseDispatcher (Kafka or local) for dispatch
    """

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
        from reflowfy.core.registry import pipeline_registry

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

        # Defaults applied and validated; define_source may add keys to plan.params.
        resolved: ResolvedPlan = pipeline.resolve(runtime_params)

        logger.info("Job dispatch starting", extra={"event": "plan_start"})

        # pending -> running, compare-and-set: a /cancel (or PATCH failed) that landed
        # while the execution was still pending used to be overwritten here, and the
        # cancelled run went ahead in full. A pause is kept: planning goes on, nothing is
        # dispatched until /resume (the drive loop only sends while running).
        if (
            self.execution_manager.update_execution_state(
                execution_id, ExecutionState.RUNNING, from_states=(ExecutionState.PENDING,)
            )
            is None
        ):
            state = self._execution_state(execution_id)
            if state != ExecutionState.PAUSED:
                logger.info(
                    "Execution is %s before planning started; not planning it",
                    state,
                    extra={"event": "plan_aborted", "state": state, "jobs_cancelled": 0},
                )
                return
            logger.info(
                "Execution was paused before planning started; planning it, dispatch waits "
                "for /resume",
                extra={"event": "plan_paused"},
            )

        enriched_params = resolved.params

        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=enriched_params,
        )

        # The rate it runs at, persisted so a resume (manager restart, /resume) reuses it.
        # (What is persisted at this point is only the /run override, passed in here.)
        effective_rate_limit = resolve_rate_limit(
            pipeline, enriched_params, override=rate_limit_override
        )
        self.execution_manager.set_rate_limit(execution_id, effective_rate_limit)

        # Automatic retries travel in each job's metadata: the worker that runs the job
        # decides, without reading the pipeline class again.
        job_retry = getattr(pipeline, "job_retry", None)
        job_retry_dict = job_retry.to_dict() if job_retry is not None else None

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

        plan = _iter_plan(pipeline_name, plan_slices(resolved.jobs, enriched_params))
        last_state_check = 0.0
        for planned in plan:
            # A cancelled (or force-failed) execution must not keep planning: a plan can take
            # minutes to hours, and its jobs would just be cancelled again as they are saved.
            # The check cannot interrupt a source call that blocks (the enumeration before the
            # first slice); it takes effect as soon as that returns.
            if time.time() - last_state_check >= 1.0:
                last_state_check = time.time()
                if self._execution_state(execution_id) in (
                    ExecutionState.CANCELLED,
                    ExecutionState.FAILED,
                ):
                    cancelled_jobs = self.job_manager.cancel_unstarted_jobs(execution_id)
                    logger.info(
                        "Execution was stopped while planning (%d jobs cancelled); planning aborted",
                        cancelled_jobs,
                        extra={"event": "plan_aborted", "jobs_cancelled": cancelled_jobs},
                    )
                    return
            context.batch_number = batch_number
            context_dict = context.to_dict()
            # The execution's params (minus execution-only ones such as input_ids)
            # with this job's own merged over them.
            job_params = resolved.job_params(planned)
            context_dict["runtime_params"] = job_params
            metadata = {**context_dict, "source_metadata": None}
            if job_retry_dict is not None:
                metadata["job_retry"] = job_retry_dict
            if "current_ids" in job_params:
                metadata["current_ids"] = job_params["current_ids"]

            dedup_check = not enable_duplicate_jobs
            job_id = str(uuid.uuid4())

            job_payload = build_job_payload(
                execution_id,
                job_id,
                pipeline_name,
                planned.source,
                metadata,
                dedup_check=dedup_check,
                job_params=planned.params,
            )
            job_payload = to_json_safe(job_payload)

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
