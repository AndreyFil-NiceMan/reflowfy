# aiokafka ships no stubs — see reflowfy/destinations/kafka.py for the rationale.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Async Kafka consumer for job processing."""

import asyncio
import json
import logging
import os
from typing import Any, List, Optional, Union

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError, KafkaError

from reflowfy.observability import metrics
from reflowfy.worker.executor import WorkerExecutor

logger = logging.getLogger(__name__)


class _InvalidPayload(Exception):
    """A message that can never be a job (not UTF-8, not JSON, not an object)."""


class KafkaJobConsumer:
    """
    Async Kafka consumer that processes Reflowfy jobs.

    Consumes jobs from the reflow.jobs topic and executes them asynchronously.
    """

    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topic: str,
        group_id: str = "reflowfy-workers",
        auto_offset_reset: str = "earliest",
        database_url: Optional[str] = None,
        # SASL Authentication
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
    ):
        """
        Initialize async Kafka consumer.

        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Topic to consume from
            group_id: Consumer group ID
            auto_offset_reset: Offset reset strategy
            database_url: PostgreSQL connection URL for job status updates
            security_protocol: Security protocol (e.g., SASL_PLAINTEXT)
            sasl_mechanism: SASL mechanism (e.g., SCRAM-SHA-256)
            sasl_username: SASL username
            sasl_password: SASL password
        """
        # Handle comma-separated string for bootstrap_servers
        if isinstance(bootstrap_servers, str) and "," in bootstrap_servers:
            self.bootstrap_servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
        else:
            self.bootstrap_servers = bootstrap_servers

        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset

        # SASL config
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password

        self.consumer: Optional[AIOKafkaConsumer] = None
        self.executor = WorkerExecutor(database_url=database_url)
        self._running = False

    async def start(self):
        """Start consuming and processing jobs asynchronously."""
        # Build consumer kwargs
        consumer_kwargs: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": self.group_id,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": False,  # Manual commit for reliability
            "retry_backoff_ms": 500,
            "metadata_max_age_ms": 30000,
            # A job is processed inline between polls, so one big job must fit in this
            # window or the broker evicts us from the group. Default 5 min is too short.
            "max_poll_interval_ms": int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "3600000")),
            "max_poll_records": 1,
        }

        # Add SASL config if credentials provided
        if self.sasl_username and self.sasl_password:
            consumer_kwargs.update(
                {
                    "security_protocol": self.security_protocol or "SASL_PLAINTEXT",
                    "sasl_mechanism": self.sasl_mechanism or "SCRAM-SHA-256",
                    "sasl_plain_username": self.sasl_username,
                    "sasl_plain_password": self.sasl_password,
                    "client_id": self.sasl_username,  # client_id = username
                }
            )

        self.consumer = AIOKafkaConsumer(self.topic, **consumer_kwargs)

        # Retry starting consumer with backoff (handle GroupCoordinatorNotAvailableError)
        max_retries = 10
        for attempt in range(max_retries):
            try:
                await self.consumer.start()
                logger.info("Kafka consumer connected", extra={"event": "consumer_connected"})
                break
            except KafkaError as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(
                        "Failed to start consumer (attempt %d/%d): %s; retrying in %ds",
                        attempt + 1,
                        max_retries,
                        e,
                        wait_time,
                        extra={
                            "event": "consumer_start_retry",
                            "attempt": attempt + 1,
                            "retry_in_s": wait_time,
                        },
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        "Failed to start consumer after %d attempts",
                        max_retries,
                        extra={"event": "consumer_start_failed"},
                    )
                    raise

        self._running = True

        try:
            await self._consume_until_stopped()
        finally:
            # Leaves the group cleanly, so the partitions move to another worker
            # right away instead of after the session timeout.
            await self.consumer.stop()
            await self.executor.close()

    async def _consume_until_stopped(self) -> None:
        """Process messages one at a time until :meth:`request_stop`.

        Polls with a 1s timeout so a stop request is noticed while idle, and only
        between messages, so a job in progress always finishes and commits first.
        """
        assert self.consumer is not None  # set by start()
        while self._running:
            batch = await self.consumer.getmany(timeout_ms=1000, max_records=1)
            for messages in batch.values():
                for msg in messages:
                    await self._process_message(msg)

    async def _process_message(self, msg: Any) -> None:
        """Decode, execute, and commit a single Kafka message."""
        assert self.consumer is not None  # only called from start() after connect
        try:
            if msg.value is None:
                # Null-value record (e.g. tombstone) carries no job; skip it.
                logger.warning(
                    "Received message with empty value, skipping",
                    extra={
                        "event": "message_empty",
                        "kafka_partition": getattr(msg, "partition", None),
                        "kafka_offset": getattr(msg, "offset", None),
                    },
                )
                await self.consumer.commit()
                return

            try:
                job_payload = json.loads(msg.value.decode("utf-8"))
            except UnicodeDecodeError as e:
                raise _InvalidPayload(f"not UTF-8: {e}") from e
            except json.JSONDecodeError as e:
                raise _InvalidPayload(str(e)) from e
            if not isinstance(job_payload, dict):
                raise _InvalidPayload(f"expected a JSON object, got {type(job_payload).__name__}")

            job_id = job_payload.get("job_id", "unknown")
            log_ctx = {
                "job_id": job_id,
                "execution_id": job_payload.get("execution_id"),
                "pipeline_name": job_payload.get("pipeline_name"),
            }

            version = job_payload.get("schema_version")
            if version != 2:
                logger.error(
                    "Unsupported job schema_version=%r; skipping",
                    version,
                    extra={
                        **log_ctx,
                        "event": "message_bad_schema",
                        "schema_version": str(version),
                    },
                )
                await self.consumer.commit()
                return

            logger.info(
                "Received job",
                extra={
                    **log_ctx,
                    "event": "job_received",
                    "kafka_partition": getattr(msg, "partition", None),
                    "kafka_offset": getattr(msg, "offset", None),
                },
            )

            # Execute job asynchronously. execute_job durably records the outcome
            # (completed/failed/deduplicated) in Postgres before returning.
            success = await self.executor.execute_job(job_payload)

            # Commit whether the job succeeded or failed. Kafka does not own
            # retries here — the failure is already recorded in Postgres and the
            # DLQ (POST /dlq/schedule) owns re-runs. Not committing would not
            # actually retry: arg-less commit() advances the whole partition, so
            # the next successful message commits past this offset regardless,
            # while risking a duplicate reprocess of a deterministic failure on
            # restart. A crash *before* this line leaves the offset uncommitted,
            # preserving at-least-once reprocessing.
            if not success:
                logger.warning(
                    "Job failed; recorded to DB", extra={**log_ctx, "event": "job_failed_recorded"}
                )
            await self.consumer.commit()

        except _InvalidPayload as e:
            # Malformed for good: redelivery cannot fix it, so commit past it — and
            # count it, so dropped messages show on the dashboards, not just in logs.
            logger.error(
                "Invalid job payload: %s",
                e,
                extra={
                    "event": "message_invalid",
                    "kafka_partition": getattr(msg, "partition", None),
                    "kafka_offset": getattr(msg, "offset", None),
                },
            )
            metrics.jobs_processed_total.labels(pipeline="unknown", status="invalid").inc()
            # Commit anyway to skip bad message
            await self.consumer.commit()

        except CommitFailedError:
            # Group rebalanced mid-job. The outcome is already in Postgres; the message
            # will be redelivered, so a duplicate run is possible (enable_duplicate_jobs=False
            # dedups by content).
            logger.warning(
                "Offset commit failed: group rebalanced; message will be redelivered",
                extra={"event": "commit_failed_rebalance"},
            )

        except Exception:
            logger.exception("Job processing error", extra={"event": "message_error"})
            # Don't commit - will retry

    def request_stop(self) -> None:
        """Ask the loop to exit after the current job (safe from a signal handler)."""
        self._running = False

    async def stop(self):
        """Stop consuming."""
        self.request_stop()
