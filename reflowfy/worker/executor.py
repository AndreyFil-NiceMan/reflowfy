"""Worker job executor with async support."""

import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from reflowfy.core.execution_context import build_flat_runtime_params_from_metadata
from reflowfy.core.registry import pipeline_registry
from reflowfy.execution.content_dedup import (
    claim_content_hash,
    compute_content_hash,
    release_content_hash,
)
from reflowfy.execution.job_runner import run_job_records
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.observability import metrics
from reflowfy.observability.context import log_context
from reflowfy.observability.logging import get_logger
from reflowfy.observability.tracing import extract_and_attach
from reflowfy.reflow_manager.models import Job

logger = get_logger("worker.executor")


class JobStats:
    """Statistics for a job execution."""

    def __init__(self):
        """Initialize job statistics."""
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.records_input = 0
        self.records_output = 0
        self.transformation_times = {}
        self.destination_write_time: float = 0.0
        self.error: Optional[str] = None
        self.error_type: Optional[str] = None
        self.error_traceback: Optional[str] = None
        self.success = False
        self.deduplicated = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        duration = self.end_time - self.start_time if self.end_time else 0

        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(duration, 3),
            "records_input": self.records_input,
            "records_output": self.records_output,
            "throughput_records_per_second": (
                round(self.records_output / duration, 2) if duration > 0 else 0
            ),
            "transformation_times": self.transformation_times,
            "destination_write_time": round(self.destination_write_time, 3),
            "error": self.error,
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
    ) -> None:
        """Emit Prometheus metrics for one finished job.

        # ponytail: error_type = exception class name only — never the message (cardinality).
        """
        status = "deduplicated" if deduplicated else ("completed" if success else "failed")
        metrics.jobs_processed_total.labels(pipeline=pipeline, status=status).inc()
        metrics.job_processing_duration_seconds.labels(pipeline=pipeline).observe(duration)
        # Only count records that actually made it through (success); a job that
        # failed at the destination has records_output set but nothing delivered.
        if success and records:
            metrics.records_processed_total.labels(pipeline=pipeline).inc(records)
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
            parent = extract_and_attach(job_payload.get("metadata", {}))
            with log_context(execution_id=execution_id, job_id=job_id, pipeline_name=pipeline_name):
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
                error_type=stats.error_type,
                duration=duration,
                records=stats.records_output,
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
        claimed_content_hash = None
        destination = None

        try:
            metadata = job_payload.get("metadata", {})
            source_descriptor = job_payload.get("source") or {}

            runtime_params = build_flat_runtime_params_from_metadata(metadata)

            # Rebuild the planned source slice the manager assigned to this job.
            source = SourceFactory.create(source_descriptor["type"], source_descriptor["config"])

            # Pipeline is required to resolve transforms + destination dynamically.
            pipeline = pipeline_registry.get(_pipeline_name)
            if pipeline is None:
                raise RuntimeError(
                    f"Pipeline '{_pipeline_name}' not found in worker registry; "
                    "worker-side sourcing requires the pipeline to be discoverable."
                )

            # Shared v2 core: fetch this slice, normalize, transform, resolve destination.
            records, transformed_records, applied, destination = run_job_records(
                source, pipeline, runtime_params
            )
            stats.records_input = len(records)

            if not records:
                logger.info("Job %s: no records to process", job_id)
                stats.success = True
                stats.records_output = 0
                stats.end_time = time.time()
                await self._update_job_in_db(execution_id, job_id, stats)
                return True

            logger.info("Processing job %s: %d records", job_id, len(records))
            for name, duration in applied:
                stats.transformation_times[name] = round(duration, 3)
                logger.debug("Applied transformation %s (%.3fs)", name, duration)

            stats.records_output = len(transformed_records)

            # Worker-side content deduplication (enable_duplicate_jobs=False).
            dedup_check = bool(job_payload.get("dedup_check", False))
            if dedup_check:
                transformation_names = [name for name, _ in applied]
                content_hash = compute_content_hash(_pipeline_name, transformation_names, records)
                won = await claim_content_hash(
                    self._async_session, content_hash, _pipeline_name, job_id, execution_id
                )
                if not won:
                    logger.info("Job %s: content already processed — deduplicated", job_id)
                    stats.deduplicated = True
                    stats.success = True
                    stats.records_output = 0
                    stats.end_time = time.time()
                    await self._update_job_in_db(execution_id, job_id, stats)
                    return True
                claimed_content_hash = content_hash

            # Health check (async)
            if not await destination.health_check():
                logger.error("Job %s: destination health check failed", job_id)
                stats.success = False
                stats.error = "Destination health check failed"
                stats.end_time = time.time()
                if claimed_content_hash:
                    await release_content_hash(self._async_session, claimed_content_hash, job_id)
                await self._update_job_in_db(execution_id, job_id, stats)
                return False

            # Send to destination and track time (async)
            logger.debug(
                "Job %s: sending %d records to destination", job_id, len(transformed_records)
            )
            dest_start = time.time()
            await destination.send_with_retry(transformed_records, runtime_params)
            stats.destination_write_time = time.time() - dest_start

            # Mark as successful
            stats.success = True
            stats.end_time = time.time()

            logger.info(
                "Job %s completed (%.2fs, %d records)",
                job_id,
                stats.end_time - stats.start_time,
                stats.records_output,
            )

            # Update job status in PostgreSQL (async)
            await self._update_job_in_db(execution_id, job_id, stats)

            return True

        except Exception as e:
            tb_str = traceback.format_exc()
            logger.error("Job %s failed: %s", job_id, e, exc_info=True)

            # Mark as failed — capture both summary and full traceback
            stats.success = False
            stats.error = str(e)
            stats.error_type = type(e).__name__
            stats.error_traceback = tb_str
            stats.end_time = time.time()

            if claimed_content_hash:
                try:
                    await release_content_hash(self._async_session, claimed_content_hash, job_id)
                except Exception:
                    pass

            # Update job status in PostgreSQL (async)
            await self._update_job_in_db(execution_id, job_id, stats)

            return False

        finally:
            # Always release the per-job destination's resources (e.g. the
            # Kafka producer started during health_check/send). Without this,
            # every job leaks a broker connection and a background sender task.
            if destination is not None:
                try:
                    await destination.close()
                except Exception as close_err:
                    logger.warning("Job %s: failed to close destination: %s", job_id, close_err)

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
                    logger.warning("Job %s not found in database", job_id)
                    await db.rollback()
                    return

                await db.commit()
                logger.debug("Job %s: status updated in database", job_id)

            except Exception as e:
                logger.error("Job %s: failed to update database: %s", job_id, e, exc_info=True)
                await db.rollback()

    async def close(self):
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
