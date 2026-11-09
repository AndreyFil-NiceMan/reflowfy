"""Worker job executor with async support."""

import asyncio
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from reflowfy.core.exceptions import pipeline_step
from reflowfy.core.execution_context import build_flat_runtime_params_from_metadata
from reflowfy.core.job_retry import ERROR_HISTORY_MAX, JobRetryConfig, describe_error
from reflowfy.core.registry import pipeline_registry
from reflowfy.execution.content_dedup import (
    claim_record_hashes,
    compute_record_hash,
    destination_identity,
    record_identity,
    release_record_hashes,
)
from reflowfy.destinations.base import DestinationError
from reflowfy.destinations.record import RecordDestination
from reflowfy.execution.job_runner import run_job_records_async
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.observability import metrics
from reflowfy.observability.metrics import JobOutcome
from reflowfy.observability.context import log_context
from reflowfy.observability.logging import get_logger
from reflowfy.observability.tracing import extract_and_attach
from reflowfy.reflow_manager.db_settings import async_connect_args
from reflowfy.reflow_manager.db_settings import database_url as env_database_url
from reflowfy.reflow_manager.models import Execution, Job
from reflowfy.core.record_steps import count_skip_reason, top_skip_reasons

logger = get_logger("worker.executor")


def _carries_job_retry(job_payload: Dict[str, Any]) -> bool:
    """Whether the job message carries a ``job_retry`` config (not whether it parses)."""
    raw: Any = job_payload.get("metadata")
    return isinstance(raw, dict) and isinstance(cast(Dict[str, Any], raw).get("job_retry"), dict)


class JobStats:
    """Statistics for a job execution."""

    def __init__(self):
        """Initialize job statistics."""
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.records_input = 0
        self.records_output = 0
        self.transformation_times: Dict[str, float] = {}
        self.destination_write_time: float = 0.0
        self.error: Optional[str] = None
        self.error_type: Optional[str] = None
        self.error_traceback: Optional[str] = None
        self.success = False
        self.deduplicated = False
        # Records dropped before the send because another job already sent them.
        self.records_deduplicated = 0
        self.skip_reason: Optional[str] = None
        # Records dropped with SkipRecord, and why (reason -> count, bounded).
        self.records_skipped = 0
        self.record_skip_reasons: Dict[str, int] = {}
        # Another delivery of this job already claimed it; this message was skipped.
        self.redelivered = False
        # The execution was cancelled before this job started; the message was skipped.
        self.cancelled = False
        # Distinct destinations a @record_destination (or fan_out) sent this job's
        # records to, and how many of its optional ones failed without failing the job.
        self.destinations: Optional[int] = None
        self.destinations_failed_optional: Optional[int] = None
        # job_retry (only for a pipeline that sets it): retries scheduled so far, and the
        # failures that led to them (type + short message, the last ERROR_HISTORY_MAX).
        self.retry_count: Optional[int] = None
        self.error_history: Optional[List[Dict[str, str]]] = None
        # This failure was not final: the job went back to pending to be retried.
        self.retry_scheduled = False
        self.retry_in_seconds: Optional[float] = None
        # The job message carries a job_retry config: a run that ends any other way than
        # a failure keeps the retry count earlier runs left (else a job that needed
        # retries looks, after the fact, like one that worked first time).
        self.job_retry_enabled = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        duration = self.end_time - self.start_time if self.end_time else 0

        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(duration, 3),
            "records_input": self.records_input,
            "records_output": self.records_output,
            "records_deduplicated": self.records_deduplicated,
            "throughput_records_per_second": (
                round(self.records_output / duration, 2) if duration > 0 else 0
            ),
            "transformation_times": self.transformation_times,
            "destination_write_time": round(self.destination_write_time, 3),
            "error": self.error,
            "skip_reason": self.skip_reason,
            "records_skipped": self.records_skipped,
            "record_skip_reasons": self.record_skip_reasons,
            "success": self.success,
            # The exception class of a failed job (never its message): what tells a
            # RecordValidationError or PermanentDestinationError apart from other failures.
            **({"error_type": self.error_type} if self.error_type else {}),
            # Only for a @record_destination, so other jobs' stats keep their shape.
            **({"destinations": self.destinations} if self.destinations is not None else {}),
            **(
                {"destinations_failed_optional": self.destinations_failed_optional}
                if self.destinations_failed_optional is not None
                else {}
            ),
            # Only with job_retry, so other jobs' stats keep their shape.
            **({"retry_count": self.retry_count} if self.retry_count is not None else {}),
            **({"error_history": self.error_history} if self.error_history is not None else {}),
            **(
                {"retry_scheduled": True, "retry_in_seconds": self.retry_in_seconds}
                if self.retry_scheduled
                else {}
            ),
        }


class WorkerExecutor:
    """
    Executes jobs on worker nodes with async I/O.

    Responsibilities:
    1. Load transformations from registry
    2. Apply transformations to records
    3. Check destination health (async)
    4. Send to destination with retries (async)
    5. Rate limiting
    6. Update job status directly in PostgreSQL (async)
    """

    #: Waits between attempts at saving a job's outcome. A brief Postgres outage
    #: (restart, failover) should not make a finished job run again.
    _save_retry_delays: Tuple[float, ...] = (1.0, 2.0, 4.0)

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize worker executor.

        Args:
            database_url: PostgreSQL connection URL
        """
        sync_url = database_url or env_database_url()

        # Convert sync URL to async (postgresql:// -> postgresql+asyncpg://)
        if sync_url.startswith("postgresql://"):
            self.database_url = sync_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        else:
            self.database_url = sync_url

        # Create async database engine and session factory
        self._engine = create_async_engine(
            self.database_url,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
            connect_args=async_connect_args(self.database_url),
        )
        self._async_session = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    def record_job_metrics(
        self,
        pipeline: str,
        success: bool,
        deduplicated: bool,
        error_type: Optional[str],
        duration: float,
        records: int,
        skipped: bool = False,
        redelivered: bool = False,
        cancelled: bool = False,
        records_deduplicated: int = 0,
        retry_scheduled: bool = False,
    ) -> None:
        """Emit Prometheus metrics for one finished job.

        A failure that ``job_retry`` sent back to the queue is ``retry_scheduled``, not
        ``failed``: the job is not over, and ``jobs_failed_total`` counts only final ones.

        # ponytail: error_type = exception class name only — never the message (cardinality).
        """
        if cancelled:
            outcome = JobOutcome.CANCELLED
        elif retry_scheduled:
            outcome = JobOutcome.RETRY_SCHEDULED
        elif redelivered:
            outcome = JobOutcome.REDELIVERED
        elif deduplicated:
            outcome = JobOutcome.DEDUPLICATED
        elif skipped:
            outcome = JobOutcome.SKIPPED
        else:
            outcome = JobOutcome.COMPLETED if success else JobOutcome.FAILED
        metrics.count_job(pipeline, outcome)
        metrics.job_processing_duration_seconds.labels(pipeline=pipeline).observe(duration)
        # Only count records that actually made it through (success); a job that
        # failed at the destination has records_output set but nothing delivered.
        if success and records:
            metrics.records_processed_total.labels(pipeline=pipeline).inc(records)
        if success and records_deduplicated:
            metrics.records_deduplicated_total.labels(pipeline=pipeline).inc(records_deduplicated)
        if not success and error_type and not retry_scheduled:
            metrics.jobs_failed_total.labels(pipeline=pipeline, error_type=error_type).inc()

    async def execute_job(self, job_payload: Dict[str, Any]) -> bool:
        """Execute a job: bind log context, continue the trace, record metrics."""
        stats = JobStats()
        # Just "is there one": a malformed config is reported where a failure reads it.
        stats.job_retry_enabled = _carries_job_retry(job_payload)
        execution_id = job_payload.get("execution_id", "unknown")
        job_id = job_payload.get("job_id", "unknown")
        pipeline_name = job_payload.get("pipeline_name", "unknown")

        try:
            from opentelemetry import trace

            # Inside try so a malformed trace carrier can't skip metric recording.
            metadata: Dict[str, Any] = job_payload.get("metadata") or {}
            parent = extract_and_attach(metadata)
            # ponytail: log the count of current_ids, not the list — it can be huge.
            job_params: Dict[str, Any] = metadata.get("runtime_params") or {}
            current_ids: Any = metadata.get("current_ids")
            with log_context(
                execution_id=execution_id,
                job_id=job_id,
                pipeline_name=pipeline_name,
                current_id=job_params.get("current_id"),
                current_ids_count=(
                    len(cast(List[Any], current_ids)) if isinstance(current_ids, list) else None
                ),
            ):
                with trace.get_tracer("worker").start_as_current_span(
                    "process_job", context=parent
                ):
                    return await self._execute_job_inner(
                        job_payload, stats, execution_id, job_id, pipeline_name
                    )
        finally:
            duration = (stats.end_time or time.time()) - stats.start_time
            self.record_job_metrics(
                pipeline_name,
                success=stats.success,
                deduplicated=stats.deduplicated,
                skipped=stats.skip_reason is not None,
                redelivered=stats.redelivered,
                cancelled=stats.cancelled,
                error_type=stats.error_type,
                duration=duration,
                records=stats.records_output,
                records_deduplicated=stats.records_deduplicated,
                retry_scheduled=stats.retry_scheduled,
            )

    async def _execute_job_inner(
        self,
        job_payload: Dict[str, Any],
        stats: "JobStats",
        execution_id: str,
        job_id: str,
        _pipeline_name: str,
    ) -> bool:
        """Fetch the slice, transform, write to destination, report to Postgres."""
        # job_id/execution_id/pipeline_name are bound onto every record here by the
        # log_context() the caller entered — no need to pass them per call.
        claimed_record_hashes: List[bytes] = []
        destination: Any = None
        heartbeat: Optional["asyncio.Task[None]"] = None

        try:
            # Claim the job before doing any work. Kafka redelivers after a rebalance or
            # a crash before commit, and the manager re-sends jobs whose lease expired;
            # without the claim each delivery ran the job again and wrote it twice.
            blocking_state = await self._claim_job(job_id)
            if blocking_state is not None:
                if blocking_state == "cancelled":
                    logger.info(
                        "Execution was cancelled — skipping job", extra={"event": "job_cancelled"}
                    )
                    stats.cancelled = True
                else:
                    logger.info(
                        "Job already %s — skipping redelivered message",
                        blocking_state,
                        extra={"event": "job_redelivered", "state": blocking_state},
                    )
                    stats.redelivered = True
                stats.success = True
                stats.end_time = time.time()
                return True  # job row belongs to the delivery that claimed it

            # Prove liveness while the job runs, so the manager's running lease can be
            # short (a dead worker is noticed in minutes) without re-running a job that
            # is merely long.
            heartbeat = asyncio.create_task(self._heartbeat_job(job_id))

            metadata = job_payload.get("metadata", {})
            source_descriptor: Dict[str, Any] = job_payload.get("source") or {}

            runtime_params = build_flat_runtime_params_from_metadata(metadata)

            # Rebuild the planned source slice the manager assigned to this job.
            source = SourceFactory.create(source_descriptor["type"], source_descriptor["config"])

            # Pipeline is required to resolve transforms + destination dynamically.
            pipeline = pipeline_registry.get(_pipeline_name)
            if pipeline is None:
                raise RuntimeError(
                    f"Pipeline '{_pipeline_name}' not found in worker registry"
                    f"{pipeline_registry.describe_missing(_pipeline_name)}; "
                    "worker-side sourcing requires the pipeline to be discoverable."
                )

            # Shared v2 core: fetch this slice, normalize, transform, resolve destination.
            # Async transformations are awaited on this loop; the blocking parts (sync
            # Elasticsearch client, sync transformations) run in threads inside it. Run
            # inline they stall the event loop, starving the Kafka heartbeat so the broker
            # evicts the worker and the later commit fails with CommitFailedError.
            records, transformed_records, applied, destination = await run_job_records_async(
                source, pipeline, runtime_params
            )
            stats.records_input = len(records)

            if not records:
                # SkipJob leaves its reason on runtime_params; without one this is
                # just an empty slice.
                stats.skip_reason = runtime_params.get("skip_reason")
                if stats.skip_reason:
                    logger.info(
                        "Job dropped by pipeline (%s)",
                        stats.skip_reason,
                        extra={"event": "job_dropped", "skip_reason": stats.skip_reason},
                    )
                else:
                    logger.info("No records to process", extra={"event": "job_empty"})
                stats.success = True
                stats.records_output = 0
                stats.end_time = time.time()
                await self._update_job_in_db(execution_id, job_id, stats)
                return True

            logger.info(
                "Processing %d records",
                len(records),
                extra={"event": "job_processing", "records_in": len(records)},
            )
            for step in applied:
                stats.transformation_times[step.name] = round(step.duration, 3)
                logger.debug(
                    "Applied transformation %s (%s, %.3fs, %d -> %d records)",
                    step.name,
                    step.mode if step.concurrency is None else f"{step.mode} x{step.concurrency}",
                    step.duration,
                    step.records_in,
                    step.records_out,
                    extra={
                        "event": "transform_applied",
                        "transformation": step.name,
                        "mode": step.mode,
                        "concurrency": step.concurrency,
                        "records_in": step.records_in,
                        "records_out": step.records_out,
                    },
                )
                if step.records_skipped:
                    stats.records_skipped += step.records_skipped
                    for reason, times in (step.skip_reasons or {}).items():
                        count_skip_reason(stats.record_skip_reasons, reason, times)
                    # Reasons stay in the message: as a field their keys would be
                    # unbounded mapping names in the log index.
                    logger.info(
                        "%s skipped %d records (%s)",
                        step.name,
                        step.records_skipped,
                        top_skip_reasons(step.skip_reasons or {}),
                        extra={
                            "event": "records_skipped",
                            "transformation": step.name,
                            "records_skipped": step.records_skipped,
                        },
                    )
                    metrics.records_skipped_total.labels(
                        pipeline=_pipeline_name, transformation=step.name
                    ).inc(step.records_skipped)

            stats.records_output = len(transformed_records)

            # Worker-side per-record deduplication (enable_duplicate_jobs=False).
            # Nothing to send (transformations filtered everything) is not a duplicate.
            if job_payload.get("dedup_check", False) and transformed_records:
                job_params: Optional[Dict[str, Any]] = job_payload.get("job_params") or None
                kept, claimed_record_hashes = await self._claim_records(
                    pipeline,
                    _pipeline_name,
                    job_id,
                    transformed_records,
                    job_params,
                    destination,
                    runtime_params,
                )
                stats.records_deduplicated = len(transformed_records) - len(kept)
                if not kept:
                    logger.info(
                        "Content already processed — deduplicated",
                        extra={"event": "job_deduplicated"},
                    )
                    stats.deduplicated = True
                    stats.success = True
                    stats.records_output = 0
                    stats.end_time = time.time()
                    await self._update_job_in_db(execution_id, job_id, stats)
                    return True
                if stats.records_deduplicated:
                    logger.info(
                        "%d of %d records already processed — dropped",
                        stats.records_deduplicated,
                        len(transformed_records),
                        extra={
                            "event": "records_deduplicated",
                            "records_deduplicated": stats.records_deduplicated,
                        },
                    )
                    # define_destination saw every record, and a destination may have
                    # baked them in (an api_destination body is sent verbatim). Resolve
                    # it again against what is actually being sent. Not a record
                    # destination: each record's destination was built from that record
                    # alone, and routing again would report its skips twice.
                    if not isinstance(destination, RecordDestination):
                        await destination.close()
                        with pipeline_step("define_destination", _pipeline_name):
                            # User code: off the loop, like the first call (job_runner).
                            destination = await asyncio.to_thread(
                                pipeline.define_destination, kept, runtime_params
                            )
                transformed_records = kept
                stats.records_output = len(kept)

            if isinstance(destination, RecordDestination):
                stats.destinations = len(destination.destinations)

            # Health check (async)
            if not await destination.health_check():
                logger.error(
                    "Destination health check failed", extra={"event": "destination_unhealthy"}
                )
                stats.success = False
                stats.error = "Destination health check failed"
                stats.end_time = time.time()
                if claimed_record_hashes:
                    await self._release_claims(claimed_record_hashes, job_id)
                await self._finish_failed_job(
                    job_payload,
                    execution_id,
                    job_id,
                    stats,
                    DestinationError("health_check", "Destination health check failed"),
                )
                return False

            # Send to destination and track time (async)
            logger.debug(
                "Sending %d records to destination",
                len(transformed_records),
                extra={"event": "destination_send"},
            )
            dest_start = time.time()
            await destination.send_with_retry(transformed_records, runtime_params)
            stats.destination_write_time = time.time() - dest_start
            if isinstance(destination, RecordDestination):
                # Logged by the destination as each failed; kept on the job here.
                stats.destinations_failed_optional = len(destination.failed_optional)

            # Mark as successful
            stats.success = True
            stats.end_time = time.time()

            logger.info(
                "Job completed (%.2fs, %d records)",
                stats.end_time - stats.start_time,
                stats.records_output,
                extra={
                    "event": "job_completed",
                    "duration_ms": round((stats.end_time - stats.start_time) * 1000),
                    "records_in": stats.records_input,
                    "records_out": stats.records_output,
                    "destination_write_ms": round(stats.destination_write_time * 1000),
                },
            )

            # Update job status in PostgreSQL (async)
            await self._update_job_in_db(execution_id, job_id, stats)

            return True

        except Exception as e:
            tb_str = traceback.format_exc()
            logger.error(
                "Job failed: %s",
                e,
                exc_info=True,
                extra={"event": "job_failed", "error_type": type(e).__name__},
            )

            # Mark as failed — capture both summary and full traceback
            stats.success = False
            stats.error = str(e)
            stats.error_type = type(e).__name__
            stats.error_traceback = tb_str
            stats.end_time = time.time()

            if claimed_record_hashes:
                await self._release_claims(claimed_record_hashes, job_id)

            # Update job status in PostgreSQL (async): failed, or back to pending when
            # the pipeline's job_retry retries this error.
            await self._finish_failed_job(job_payload, execution_id, job_id, stats, e)

            return False

        except BaseException:
            # Shutdown mid-job (SIGTERM's SystemExit, or the task cancelled on a
            # scale-down/rollout) bypasses `except Exception`. Release the claim so the
            # redelivered message runs again; left held, the redelivery was marked
            # deduplicated and the unwritten rest of the job was silently lost.
            if claimed_record_hashes:
                # Shielded: a second cancel must not stop the release half way, and
                # must not replace the shutdown being re-raised below.
                try:
                    await asyncio.shield(self._release_claims(claimed_record_hashes, job_id))
                except BaseException:
                    pass  # the original shutdown below is what matters
            raise

        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                # Let it actually stop: cancelled mid-write, it still held a database
                # connection while the job (and close()) went on. asyncio.wait never
                # raises the task's CancelledError, and an outer cancel still propagates.
                await asyncio.wait({heartbeat})
            # Always release the per-job destination's resources (e.g. the
            # Kafka producer started during health_check/send). Without this,
            # every job leaks a broker connection and a background sender task.
            if destination is not None:
                try:
                    await destination.close()
                except Exception as close_err:
                    logger.warning(
                        "Failed to close destination: %s",
                        close_err,
                        extra={"event": "destination_close_error"},
                    )

    async def _claim_records(
        self,
        pipeline: Any,
        pipeline_name: str,
        job_id: str,
        transformed_records: List[Any],
        job_params: Optional[Dict[str, Any]],
        destination: Any,
        runtime_params: Any,
    ) -> Tuple[List[Any], List[bytes]]:
        """Claim a hash per (record, destination) about to be sent.

        Returns (records to send, claimed). A pair whose hash another job holds was
        already written there and is dropped, as is a second record with the same
        identity within this job. A record destination (``@record_destination``,
        ``fan_out``) writes each record somewhere of its own, maybe several places:
        each is hashed, so the same record sent to two topics is two writes, and a
        record already in one is still sent to the other (``keep_only``). A record
        left with nowhere to go is dropped.
        """
        dedup_key = getattr(pipeline, "dedup_key", None)
        identities: Dict[int, Dict[str, Any]] = {}

        def targets_of(record: Any) -> List[Any]:
            if isinstance(destination, RecordDestination):
                return list(destination.destinations_for(record))
            return [destination]

        def identity_of(target: Any) -> Dict[str, Any]:
            if id(target) not in identities:
                identities[id(target)] = destination_identity(target)
            return identities[id(target)]

        hashed: List[Tuple[Any, List[Tuple[Any, bytes]]]] = []
        for record in transformed_records:
            identity = record_identity(record, dedup_key, runtime_params)
            hashed.append(
                (
                    record,
                    [
                        (
                            target,
                            compute_record_hash(
                                pipeline_name, identity, identity_of(target), job_params
                            ),
                        )
                        for target in targets_of(record)
                    ],
                )
            )
        won = await claim_record_hashes(
            self._async_session, [h for _, pairs in hashed for _, h in pairs], job_id
        )
        kept: List[Any] = []
        seen: Set[bytes] = set()
        for record, pairs in hashed:
            new_targets = [target for target, h in pairs if h in won and h not in seen]
            seen.update(h for _, h in pairs)
            if not new_targets:
                continue
            if isinstance(destination, RecordDestination) and len(new_targets) < len(pairs):
                destination.keep_only(record, new_targets)
            kept.append(record)
        return kept, list(won)

    async def _finish_failed_job(
        self,
        job_payload: Dict[str, Any],
        execution_id: str,
        job_id: str,
        stats: JobStats,
        error: BaseException,
    ) -> None:
        """Record a failed job: ``failed``, or ``pending`` again when ``job_retry`` says so.

        Retried when the job message carries a ``job_retry`` config, the error matches it
        and retries are left: the job goes back to ``pending`` with ``next_attempt_at`` set
        after the backoff, and the drive loop sends it again then. The content-dedup
        claims were released before this, so the retry re-claims its records.
        """
        try:
            retry = JobRetryConfig.from_payload(job_payload)
        except Exception:
            logger.warning(
                "Ignoring an unreadable job_retry config",
                exc_info=True,
                extra={"event": "job_retry_config_error"},
            )
            retry = None
        if retry is None:
            await self._update_job_in_db(execution_id, job_id, stats)
            return

        previous = await self._previous_retries(job_id)
        if previous is None:
            logger.warning(
                "Job failed (%s), not retried: its earlier retries could not be read",
                type(error).__name__,
                extra={"event": "job_retry_exhausted", "error_type": type(error).__name__},
            )
            stats.error_history = [describe_error(error)]
            await self._update_job_in_db(execution_id, job_id, stats)
            return
        previous_count, history = previous
        stats.retry_count = previous_count
        stats.error_history = (history + [describe_error(error)])[-ERROR_HISTORY_MAX:]
        error_type = type(error).__name__
        if retry.should_retry(error) and previous_count < retry.max_retries:
            retry_number = previous_count + 1
            delay = retry.delay_for(retry_number, error)
            stats.retry_count = retry_number
            stats.retry_scheduled = True
            stats.retry_in_seconds = round(delay, 3)
            if await self._update_job_in_db(execution_id, job_id, stats, retry_in=delay):
                logger.info(
                    "Job failed (%s), retry %d/%d in %.0fs",
                    error_type,
                    retry_number,
                    retry.max_retries,
                    delay,
                    extra={
                        "event": "job_retry_scheduled",
                        "error_type": error_type,
                        "retry": retry_number,
                        "max_retries": retry.max_retries,
                        "retry_in_s": round(delay, 3),
                    },
                )
                return
            # The execution ended meanwhile (cancelled, force-failed): no retry.
            stats.retry_count = previous_count
            stats.retry_scheduled = False
            stats.retry_in_seconds = None
        else:
            reason = (
                f"{previous_count} retr{'y' if previous_count == 1 else 'ies'} used"
                if retry.should_retry(error)
                else "error is not retried"
            )
            logger.info(
                "Job failed (%s), not retried: %s",
                error_type,
                reason,
                extra={
                    "event": "job_retry_exhausted",
                    "error_type": error_type,
                    "retry_count": previous_count,
                },
            )
        await self._update_job_in_db(execution_id, job_id, stats)

    async def _release_claims(self, record_hashes: Any, job_id: str) -> None:
        """Give back this job's content-dedup claims; a failure is logged, not raised.

        Left held, the claims make a re-run of this job drop its records as already
        written until CONTENT_DEDUP_RETENTION_HOURS purges them.
        """
        try:
            await release_record_hashes(self._async_session, record_hashes, job_id)
        except Exception:
            logger.warning(
                "Could not release %d content-dedup claims",
                len(record_hashes),
                exc_info=True,
                extra={"event": "dedup_release_failed"},
            )

    async def _previous_retries(self, job_id: str) -> Optional[Tuple[int, List[Dict[str, str]]]]:
        """The retry count and error history earlier runs of this job left in its stats.

        ``None`` when they cannot be read: the caller must not guess. Counting from
        zero made every failure look like the first, so ``max_retries`` was never
        reached while the read kept failing.
        """
        from sqlalchemy import select

        try:
            async with self._async_session() as db:
                previous: Any = (
                    await db.execute(select(Job.stats).where(Job.job_id == job_id))
                ).scalar_one_or_none()
        except Exception:
            logger.warning(
                "Could not read the job's earlier retries",
                exc_info=True,
                extra={"event": "job_retry_read_error"},
            )
            return None
        if not isinstance(previous, dict):
            return 0, []
        stats_dict = cast(Dict[str, Any], previous)
        count: Any = stats_dict.get("retry_count")
        history: Any = stats_dict.get("error_history")
        return (
            count if isinstance(count, int) and not isinstance(count, bool) else 0,
            [cast(Dict[str, str], h) for h in cast(List[Any], history) if isinstance(h, dict)]
            if isinstance(history, list)
            else [],
        )

    async def _heartbeat_job(self, job_id: str) -> None:
        """Refresh the running job's ``updated_at`` every JOB_HEARTBEAT_SECONDS.

        The manager requeues a ``running`` job whose ``updated_at`` is older than its
        running lease; this keeps a live job from looking dead. A failed beat is
        logged and retried — never raised into the job.
        """
        from sqlalchemy import update

        interval = float(os.getenv("JOB_HEARTBEAT_SECONDS", "30"))
        while True:
            await asyncio.sleep(interval)
            try:
                async with self._async_session() as db:
                    await db.execute(
                        update(Job)
                        .where(Job.job_id == job_id, Job.state == "running")
                        .values(updated_at=datetime.now(timezone.utc).replace(tzinfo=None))
                    )
                    await db.commit()
            except Exception:
                logger.warning(
                    "Heartbeat failed; will retry",
                    exc_info=True,
                    extra={"event": "heartbeat_error"},
                )

    async def _claim_job(self, job_id: str) -> Optional[str]:
        """Claim a job for this delivery: pending/dispatched -> running, attempts + 1.

        Returns None when this delivery should run the job — it claimed it, or the
        job has no row (an untracked message). Otherwise returns the state that
        blocks it (running elsewhere, or already finished).

        Fails open: if the database is unreachable the job runs anyway, which is
        the at-least-once behaviour from before claims existed.
        """
        from sqlalchemy import select, update

        from sqlalchemy import or_

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            async with self._async_session() as db:
                # A pending job whose job_retry backoff has not passed is not claimed: an
                # old message delivered again must not cut the backoff short.
                result = await db.execute(
                    update(Job)
                    .where(
                        Job.job_id == job_id,
                        Job.state.in_(["pending", "dispatched"]),
                        or_(Job.next_attempt_at.is_(None), Job.next_attempt_at <= now),
                    )
                    .values(
                        state="running",
                        attempts=Job.attempts + 1,
                        updated_at=now,
                        next_attempt_at=None,
                    )
                )
                if (getattr(result, "rowcount", 0) or 0) == 1:
                    await db.commit()
                    return None
                state = (
                    await db.execute(select(Job.state).where(Job.job_id == job_id))
                ).scalar_one_or_none()
                await db.commit()
                return state
        except Exception:
            logger.warning(
                "Could not claim job (database error); running it anyway",
                exc_info=True,
                extra={"event": "job_claim_error"},
            )
            return None

    async def _update_job_in_db(
        self,
        execution_id: str,
        job_id: str,
        stats: JobStats,
        retry_in: Optional[float] = None,
    ) -> bool:
        """
        Update job status directly in PostgreSQL asynchronously.

        Note: Only updates the jobs table. The reflow manager syncs
        execution counts from the jobs table via _sync_counts_from_db().

        Args:
            execution_id: Execution ID
            job_id: Job ID
            stats: Job statistics
            retry_in: Schedule a job_retry retry this many seconds from now: the job goes
                back to ``pending`` with ``next_attempt_at`` set, ``attempts`` reset (each
                retry gets the full JOB_MAX_ATTEMPTS lease budget), and the error kept.
                Only while the execution has not ended; otherwise nothing is written.

        Returns:
            True if the row was written.
        """
        if stats.job_retry_enabled and stats.retry_count is None:
            # Not a failure (those set it): keep what earlier runs of this job recorded.
            previous = await self._previous_retries(job_id)
            if previous is not None and previous[0]:
                stats.retry_count, stats.error_history = previous
        for attempt, delay in enumerate((*self._save_retry_delays, None), start=1):
            try:
                return await self._write_job_row(execution_id, job_id, stats, retry_in)
            except Exception as e:
                if delay is None:
                    # Left `running`: once its heartbeat stops, the lease sweep puts it
                    # back to pending after JOB_RUNNING_LEASE_SECONDS and it runs again.
                    logger.error(
                        "Job outcome not saved after %d attempts: %s",
                        attempt,
                        e,
                        exc_info=True,
                        extra={"event": "job_outcome_not_saved", "attempts": attempt},
                    )
                    return False
                logger.warning(
                    "Failed to update database (attempt %d), retrying in %.0fs: %s",
                    attempt,
                    delay,
                    e,
                    extra={"event": "db_update_error", "attempt": attempt},
                )
                await asyncio.sleep(delay)
        return False  # unreachable: the last attempt returns or logs and returns

    async def _write_job_row(
        self,
        execution_id: str,
        job_id: str,
        stats: JobStats,
        retry_in: Optional[float],
    ) -> bool:
        """One attempt at the job row write. True if written, False if no row matched."""
        async with self._async_session() as db:
            try:
                from datetime import timedelta

                from sqlalchemy import select, update

                if retry_in is not None:
                    state = "pending"
                elif stats.deduplicated:
                    state = "deduplicated"
                elif stats.success:
                    state = "completed"
                else:
                    state = "failed"
                now = datetime.now(timezone.utc).replace(tzinfo=None)

                # Update job state
                update_data: Dict[str, Any] = {
                    "state": state,
                    "updated_at": now,
                    "completed_at": now,
                    "processed_records": stats.records_output,
                    "stats": stats.to_dict(),
                }
                if retry_in is not None:
                    update_data.update(
                        completed_at=None,
                        attempts=0,
                        next_attempt_at=now + timedelta(seconds=retry_in),
                    )

                if stats.error:
                    update_data["error_message"] = stats.error
                if stats.error_traceback:
                    update_data["error_traceback"] = stats.error_traceback

                # Update the job using async execute
                stmt = update(Job).where(Job.job_id == job_id)
                if retry_in is not None:
                    # A job of an execution that was cancelled or force-failed meanwhile
                    # is not put back in the queue (nothing would ever take it).
                    stmt = stmt.where(
                        Job.execution_id.in_(
                            select(Execution.execution_id).where(
                                Execution.execution_id == execution_id,
                                Execution.state.in_(["pending", "running", "paused"]),
                            )
                        )
                    )
                result = await db.execute(stmt.values(**update_data))

                rowcount = getattr(result, "rowcount", None)
                if rowcount == 0:
                    await db.rollback()
                    if retry_in is None:
                        logger.warning(
                            "Job not found in database", extra={"event": "job_row_missing"}
                        )
                    return False

                await db.commit()
                logger.debug("Status updated in database", extra={"event": "job_status_saved"})
                return True

            except Exception:
                await db.rollback()
                raise

    async def close(self):
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
