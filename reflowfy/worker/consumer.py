"""Async Kafka consumer for job processing."""

import asyncio
import json
import logging
from typing import Any, List, Optional, Union

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from reflowfy.worker.executor import WorkerExecutor

logger = logging.getLogger(__name__)


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
                logger.info("Kafka consumer connected")
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
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Failed to start consumer after %d attempts", max_retries)
                    raise

        self._running = True

        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                await self._process_message(msg)

        finally:
            await self.consumer.stop()
            await self.executor.close()

    async def _process_message(self, msg: Any) -> None:
        """Decode, execute, and commit a single Kafka message."""
        assert self.consumer is not None  # only called from start() after connect
        try:
            if msg.value is None:
                # Null-value record (e.g. tombstone) carries no job; skip it.
                logger.warning("Received message with empty value, skipping")
                await self.consumer.commit()
                return

            job_payload = json.loads(msg.value.decode("utf-8"))

            job_id = job_payload.get("job_id", "unknown")
            log_ctx = {
                "job_id": job_id,
                "execution_id": job_payload.get("execution_id"),
                "pipeline_name": job_payload.get("pipeline_name"),
            }

            version = job_payload.get("schema_version")
            if version != 2:
                logger.error(
                    "Unsupported job schema_version=%r; skipping", version, extra=log_ctx
                )
                await self.consumer.commit()
                return

            logger.info("Received job %s", job_id, extra=log_ctx)

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
                logger.warning("Job %s failed; recorded to DB", job_id, extra=log_ctx)
            await self.consumer.commit()

        except json.JSONDecodeError as e:
            logger.error("Invalid job payload: %s", e)
            # Commit anyway to skip bad message
            await self.consumer.commit()

        except Exception:
            logger.exception("Job processing error")
            # Don't commit - will retry

    async def stop(self):
        """Stop consuming."""
        self._running = False
