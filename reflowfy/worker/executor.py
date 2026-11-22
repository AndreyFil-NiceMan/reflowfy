"""Worker job executor: runs one job message end to end.

Claims the job, rebuilds its source, runs the per-job core (``execution.job_runner``),
deduplicates and sends, then records the outcome. The row writes and dedup claims are
``job_store.JobStore``; the stats it records, ``job_stats.JobStats``.
"""

import asyncio
import time
import traceback
from typing import Any, Dict, List, Optional, cast


from reflowfy.core.async_utils import call_hook
from reflowfy.core.exceptions import pipeline_step
from reflowfy.core.execution_context import build_flat_runtime_params_from_metadata
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.retry import count_retries
from reflowfy.core.exceptions import DestinationError
from reflowfy.destinations.fan_out import as_destination
from reflowfy.destinations.record import RecordDestination
from reflowfy.execution.job_runner import (
    log_job_dropped,
    run_job_records_async,
)
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.observability import metrics
from reflowfy.observability.context import log_context
from reflowfy.observability.logging import get_logger
from reflowfy.observability.tracing import extract_and_attach
from reflowfy.store.states import JobState

from reflowfy.worker.job_stats import JobStats, report_steps
from reflowfy.worker.job_store import JobStore

logger = get_logger("worker.executor")


def _carries_job_retry(job_payload: Dict[str, Any]) -> bool:
    """Whether the job message carries a ``job_retry`` config (not whether it parses)."""
    raw: Any = job_payload.get("metadata")
    return isinstance(raw, dict) and isinstance(cast(Dict[str, Any], raw).get("job_retry"), dict)


class WorkerExecutor(JobStore):
    """Executes jobs on worker nodes with async I/O.

    Responsibilities:
    1. Load transformations from registry
    2. Apply transformations to records
    3. Check destination health (async)
    4. Send to destination with retries (async)
    5. Rate limiting
    6. Update job status directly in PostgreSQL (async)
    """

    @staticmethod
    def record_job_metrics(pipeline: str, stats: JobStats) -> None:
        """Emit Prometheus metrics for one finished job delivery.

        A failure that ``job_retry`` sent back to the queue is ``retry_scheduled``, not
        ``failed``: the job is not over, and ``jobs_failed_total`` counts only final ones.

        # ponytail: error_type = exception class name only — never the message (cardinality).
        """
        metrics.count_job(pipeline, stats.outcome)
        duration = (stats.end_time or time.time()) - stats.start_time
        metrics.job_processing_duration_seconds.labels(pipeline=pipeline).observe(duration)
        # Only count records that actually made it through (success); a job that
        # failed at the destination has records_output set but nothing delivered.
        if stats.success and stats.records_output:
            metrics.records_processed_total.labels(pipeline=pipeline).inc(stats.records_output)
        if stats.success and stats.records_deduplicated:
            metrics.records_deduplicated_total.labels(pipeline=pipeline).inc(
                stats.records_deduplicated
            )
        for step, retries in stats.step_retries.items():
            metrics.step_retries_total.labels(pipeline=pipeline, step=step).inc(retries)
        if (
            not stats.success
            and stats.error_type
            and not stats.retry_scheduled
            and not stats.claim_lost
        ):
            metrics.jobs_failed_total.labels(pipeline=pipeline, error_type=stats.error_type).inc()

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
                    # Step retries made during the job land on its stats as they happen.
                    with count_retries(stats.step_retries):
                        return await self._execute_job_inner(
                            job_payload, stats, execution_id, job_id, pipeline_name
                        )
        finally:
            self.record_job_metrics(pipeline_name, stats)

    async def _execute_job_inner(
        self,
        job_payload: Dict[str, Any],
        stats: "JobStats",
        execution_id: str,
        job_id: str,
        pipeline_name: str,
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
            blocking_state = await self._claim_job(job_id, stats)
            if blocking_state is not None:
                if blocking_state == JobState.CANCELLED:
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
            heartbeat = asyncio.create_task(self._heartbeat_job(job_id, stats.claim_token))

            metadata = job_payload.get("metadata", {})
            source_descriptor: Dict[str, Any] = job_payload.get("source") or {}

            runtime_params = build_flat_runtime_params_from_metadata(metadata)

            # Rebuild the planned source slice the manager assigned to this job.
            source = SourceFactory.from_descriptor(source_descriptor)

            # Pipeline is required to resolve transforms + destination dynamically.
            pipeline = pipeline_registry.get(pipeline_name)
            if pipeline is None:
                raise RuntimeError(
                    f"Pipeline '{pipeline_name}' not found in worker registry"
                    f"{pipeline_registry.describe_missing(pipeline_name)}; "
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
                # just an empty slice. Steps that ran before a SkipJob (and the
                # records they skipped) are reported first, so both show.
                report_steps(applied, stats, pipeline_name)
                stats.skip_reason = runtime_params.get("skip_reason")
                if stats.skip_reason:
                    log_job_dropped(logger, stats.skip_reason, runtime_params.get("skip_extra"))
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
            report_steps(applied, stats, pipeline_name)

            stats.records_output = len(transformed_records)

            # Worker-side per-record deduplication (enable_duplicate_jobs=False).
            # Nothing to send (transformations filtered everything) is not a duplicate.
            if job_payload.get("dedup_check", False) and transformed_records:
                job_params: Optional[Dict[str, Any]] = job_payload.get("job_params") or None
                kept, claimed_record_hashes = await self._claim_records(
                    pipeline,
                    pipeline_name,
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
                        with pipeline_step("define_destination", pipeline_name):
                            # User code: off the loop, like the first call (job_runner).
                            destination = as_destination(
                                await call_hook(pipeline.define_destination, kept, runtime_params)
                            )
                transformed_records = kept
                stats.records_output = len(kept)

            if isinstance(destination, RecordDestination):
                stats.destinations = len(destination.destinations)

            # Last check before anything is written: a worker that stalled past its lease
            # (and whose job was requeued and claimed again, or cancelled) must not send
            # the job a second time. Its dedup claims are left alone: they are held by
            # job_id, which the delivery that owns the job now shares.
            if not await self._still_owns(job_id, stats):
                stats.end_time = time.time()
                return True

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

            # Both the summary and the full traceback are kept on the job.
            stats.success = False
            stats.error = str(e)
            stats.error_type = type(e).__name__
            stats.error_traceback = tb_str
            stats.end_time = time.time()

            if claimed_record_hashes:
                await self._release_claims(claimed_record_hashes, job_id)

            # Failed, or back to pending when the pipeline's job_retry retries this error.
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
