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
from reflowfy.core.registry import pipeline_registry
from reflowfy.execution.content_dedup import (
    claim_record_hashes,
    compute_content_hash,
    compute_record_hash,
    destination_identity,
    held_by_other_job,
    honor_legacy_hash,
    previous_destination_identity,
    record_identity,
    release_record_hashes,
)
from reflowfy.execution.job_runner import run_job_records_async
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.observability import metrics
from reflowfy.observability.context import log_context
from reflowfy.observability.logging import get_logger
from reflowfy.observability.tracing import extract_and_attach
from reflowfy.reflow_manager.db_settings import async_connect_args
from reflowfy.reflow_manager.models import Job
from reflowfy.transformations.base import count_skip_reason, top_skip_reasons

logger = get_logger("worker.executor")


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

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize worker executor.

        Args:
            database_url: PostgreSQL connection URL
        """
        sync_url = database_url or os.getenv(
            "DATABASE_URL", "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy"
        )

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
    ) -> None:
        """Emit Prometheus metrics for one finished job.

        # ponytail: error_type = exception class name only — never the message (cardinality).
        """
        if cancelled:
            status = "cancelled"
        elif redelivered:
            status = "redelivered"
        elif deduplicated:
            status = "deduplicated"
        elif skipped:
            status = "skipped"
        else:
            status = "completed" if success else "failed"
        metrics.jobs_processed_total.labels(pipeline=pipeline, status=status).inc()
        metrics.job_processing_duration_seconds.labels(pipeline=pipeline).observe(duration)
        # Only count records that actually made it through (success); a job that
        # failed at the destination has records_output set but nothing delivered.
        if success and records:
            metrics.records_processed_total.labels(pipeline=pipeline).inc(records)
        if success and records_deduplicated:
            metrics.records_deduplicated_total.labels(pipeline=pipeline).inc(records_deduplicated)
        if not success and error_type:
            metrics.jobs_failed_total.labels(pipeline=pipeline, error_type=error_type).inc()

    async def execute_job(self, job_payload: Dict[str, Any]) -> bool:
        """Execute a job: bind log context, continue the trace, record metrics."""
        stats = JobStats()
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
        destination = None
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
                    records,
                    transformed_records,
                    [step.name for step in applied],
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
                    # it again against what is actually being sent.
                    await destination.close()
                    with pipeline_step("define_destination", _pipeline_name):
                        destination = pipeline.define_destination(kept, runtime_params)
                transformed_records = kept
                stats.records_output = len(kept)

            # Health check (async)
            if not await destination.health_check():
                logger.error(
                    "Destination health check failed", extra={"event": "destination_unhealthy"}
                )
                stats.success = False
                stats.error = "Destination health check failed"
                stats.end_time = time.time()
                if claimed_record_hashes:
                    await release_record_hashes(self._async_session, claimed_record_hashes, job_id)
                await self._update_job_in_db(execution_id, job_id, stats)
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
                try:
                    await release_record_hashes(self._async_session, claimed_record_hashes, job_id)
                except Exception:
                    pass

            # Update job status in PostgreSQL (async)
            await self._update_job_in_db(execution_id, job_id, stats)

            return False

        except BaseException:
            # Shutdown mid-job (SIGTERM's SystemExit, or the task cancelled on a
            # scale-down/rollout) bypasses `except Exception`. Release the claim so the
            # redelivered message runs again; left held, the redelivery was marked
            # deduplicated and the unwritten rest of the job was silently lost.
            if claimed_record_hashes:
                try:
                    await release_record_hashes(self._async_session, claimed_record_hashes, job_id)
                except BaseException:
                    pass
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
        records: List[Any],
        transformed_records: List[Any],
        transformation_names: List[str],
        job_params: Optional[Dict[str, Any]],
        destination: Any,
        runtime_params: Any,
    ) -> Tuple[List[Any], List[bytes]]:
        """Claim a hash per record about to be sent. Returns (records to send, claimed).

        A record whose hash another job holds was already sent there and is dropped,
        as is a second record with the same identity within this job.
        """
        identity = destination_identity(destination)
        if honor_legacy_hash():
            # Upgrade guard: the previous release claimed one hash per job in
            # processed_content. A job it already wrote is a duplicate as a whole.
            # (Hashing is per record, so the previous identity only when it differs.)
            previous_identity = previous_destination_identity(destination)
            job_hashes = [
                compute_content_hash(
                    pipeline_name, transformation_names, records, job_params, identity
                ),
                compute_content_hash(pipeline_name, transformation_names, records, job_params),
            ]
            if previous_identity != identity:
                job_hashes.append(
                    compute_content_hash(
                        pipeline_name,
                        transformation_names,
                        records,
                        job_params,
                        previous_identity,
                    )
                )
            if await held_by_other_job(self._async_session, job_hashes, job_id):
                return [], []

        dedup_key = getattr(pipeline, "dedup_key", None)
        hashes = [
            compute_record_hash(
                pipeline_name,
                record_identity(record, dedup_key, runtime_params),
                identity,
                job_params,
            )
            for record in transformed_records
        ]
        won = await claim_record_hashes(self._async_session, hashes, job_id)
        kept: List[Any] = []
        seen: Set[bytes] = set()
        for record, record_hash in zip(transformed_records, hashes):
            if record_hash in won and record_hash not in seen:
                seen.add(record_hash)
                kept.append(record)
        return kept, list(won)

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

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            async with self._async_session() as db:
                result = await db.execute(
                    update(Job)
                    .where(Job.job_id == job_id, Job.state.in_(["pending", "dispatched"]))
                    .values(state="running", attempts=Job.attempts + 1, updated_at=now)
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

    async def _update_job_in_db(self, execution_id: str, job_id: str, stats: JobStats):
        """
        Update job status directly in PostgreSQL asynchronously.

        Note: Only updates the jobs table. The reflow manager syncs
        execution counts from the jobs table via _sync_counts_from_db().

        Args:
            execution_id: Execution ID
            job_id: Job ID
            stats: Job statistics
        """
        async with self._async_session() as db:
            try:
                from sqlalchemy import update

                if stats.deduplicated:
                    state = "deduplicated"
                elif stats.success:
                    state = "completed"
                else:
                    state = "failed"
                now = datetime.now(timezone.utc).replace(tzinfo=None)

                # Update job state
                update_data = {
                    "state": state,
                    "updated_at": now,
                    "completed_at": now,
                    "processed_records": stats.records_output,
                    "stats": stats.to_dict(),
                }

                if stats.error:
                    update_data["error_message"] = stats.error
                if stats.error_traceback:
                    update_data["error_traceback"] = stats.error_traceback

                # Update the job using async execute
                stmt = update(Job).where(Job.job_id == job_id).values(**update_data)
                result = await db.execute(stmt)

                rowcount = getattr(result, "rowcount", None)
                if rowcount == 0:
                    logger.warning("Job not found in database", extra={"event": "job_row_missing"})
                    await db.rollback()
                    return

                await db.commit()
                logger.debug("Status updated in database", extra={"event": "job_status_saved"})

            except Exception as e:
                logger.error(
                    "Failed to update database: %s",
                    e,
                    exc_info=True,
                    extra={"event": "db_update_error"},
                )
                await db.rollback()

    async def close(self):
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
