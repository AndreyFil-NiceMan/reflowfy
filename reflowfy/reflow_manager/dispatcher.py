# aiokafka ships no stubs — see reflowfy/destinations/kafka.py for the rationale.
# pyright: reportUnknownMemberType=false
"""Job dispatchers for ReflowManager using aiokafka."""

import logging
import json
import asyncio
from abc import ABC, abstractmethod
from typing import Callable, Dict, Any, List, Optional, Union
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, MessageSizeTooLargeError

from reflowfy.reflow_manager.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Called with each job payload right after its message is sent.
OnSent = Callable[[Dict[str, Any]], None]
# Polled before each job; True ends the batch early (execution paused, chunk time up).
ShouldStop = Callable[..., bool]  # should_stop(chunk=False) ignores the chunk time limit
# Called with a job payload and the reason when that job can never be sent (so retrying it
# would loop forever); the caller fails the job, the dispatcher moves on to the next.
OnUnsendable = Callable[[Dict[str, Any], str], None]

# Longest wait for one rate-limit token before handing back to the drive loop, which
# simply calls again. Short so the loop keeps heartbeating its lease at low rates.
TOKEN_WAIT_SECONDS = 30.0


class BaseDispatcher(ABC):
    """Abstract base class for job dispatchers."""

    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter

    @abstractmethod
    async def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch a single job."""
        pass

    @abstractmethod
    async def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
        on_sent: Optional[OnSent] = None,
        should_stop: Optional[ShouldStop] = None,
        on_unsendable: Optional[OnUnsendable] = None,
    ) -> int:
        """Dispatch jobs in order; return how many were sent.

        Stops at a transient failure (broker down) or when ``should_stop`` returns True;
        a job that can never be sent is reported through ``on_unsendable`` and skipped.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close resources."""
        pass


class KafkaDispatcher(BaseDispatcher):
    """
    Dispatches jobs to Kafka with rate limiting using aiokafka.
    """

    def __init__(
        self,
        kafka_bootstrap_servers: Union[str, List[str]],
        kafka_topic: str,
        rate_limiter: RateLimiter,
        # SASL Authentication
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
    ):
        super().__init__(rate_limiter)

        # Handle comma-separated string
        if isinstance(kafka_bootstrap_servers, str) and "," in kafka_bootstrap_servers:
            self.kafka_bootstrap_servers = [
                s.strip() for s in kafka_bootstrap_servers.split(",") if s.strip()
            ]
        else:
            self.kafka_bootstrap_servers = kafka_bootstrap_servers

        self.kafka_topic = kafka_topic

        # SASL config
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password

        self._producer: Optional[AIOKafkaProducer] = None
        self._producer_loop: Optional[asyncio.AbstractEventLoop] = None
        self._started = False

    async def _get_producer(self) -> AIOKafkaProducer:
        """Get or create Kafka producer."""
        loop = asyncio.get_running_loop()

        # Check if existing producer is bound to a different or closed loop
        if self._producer and (
            self._producer_loop is None
            or self._producer_loop != loop
            or self._producer_loop.is_closed()
        ):
            logger.info(
                "Detected event loop change, resetting Kafka producer",
                extra={"event": "producer_reset"},
            )
            # We cannot strictly close() the old producer if its loop is closed, just discard it
            self._producer = None
            self._producer_loop = None
            self._started = False

        if self._producer is None or not self._started:
            # Build producer kwargs
            producer_kwargs: Dict[str, Any] = {
                "bootstrap_servers": self.kafka_bootstrap_servers,
                "compression_type": "gzip",
            }

            # Add SASL config if credentials provided
            if self.sasl_username and self.sasl_password:
                producer_kwargs.update(
                    {
                        "security_protocol": self.security_protocol or "SASL_PLAINTEXT",
                        "sasl_mechanism": self.sasl_mechanism or "SCRAM-SHA-256",
                        "sasl_plain_username": self.sasl_username,
                        "sasl_plain_password": self.sasl_password,
                        "client_id": self.sasl_username,  # client_id = username
                    }
                )

            self._producer = AIOKafkaProducer(**producer_kwargs)
            await self._producer.start()
            self._producer_loop = loop
            self._started = True

        return self._producer

    async def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch a single job to Kafka."""
        # Check and consume tokens
        if not self.rate_limiter.consume_tokens(pipeline_name, 1, rate_limit):
            return False

        # Send to Kafka
        producer = await self._get_producer()

        try:
            await producer.send_and_wait(
                topic=self.kafka_topic,
                value=json.dumps(job_payload).encode("utf-8"),
            )
            return True

        except KafkaError as e:
            logger.error(
                "Kafka error: %s",
                e,
                extra={
                    "event": "kafka_send_error",
                    "job_id": job_payload.get("job_id"),
                    "error_type": type(e).__name__,
                },
            )
            return False

    async def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
        on_sent: Optional[OnSent] = None,
        should_stop: Optional[ShouldStop] = None,
        on_unsendable: Optional[OnUnsendable] = None,
    ) -> int:
        """Dispatch jobs to Kafka in order; see :meth:`BaseDispatcher.dispatch_jobs_batch`."""
        dispatched = 0
        producer = await self._get_producer()

        for job in jobs:
            if should_stop and should_stop():
                break
            # Atomic token acquisition with rate limiting
            if not self.rate_limiter.acquire_token(
                pipeline_name, rate_limit, max_wait=TOKEN_WAIT_SECONDS
            ):
                # Normal below ~2 jobs/min: the drive loop calls again.
                logger.debug(
                    "No rate-limit token yet after %d jobs; returning",
                    dispatched,
                    extra={"event": "rate_limit_wait", "jobs_dispatched": dispatched},
                )
                break
            # Re-check: a pause may have landed during a long token wait. The chunk
            # limit is ignored here: at rates below ~2 jobs/min the token only arrives
            # after the chunk is spent, and stopping now would burn it without a send.
            if should_stop and should_stop(chunk=False):
                break

            # Dispatch
            try:
                await producer.send_and_wait(
                    topic=self.kafka_topic,
                    value=json.dumps(job).encode("utf-8"),
                )
                dispatched += 1
                if on_sent:
                    on_sent(job)

            except MessageSizeTooLargeError as e:
                # The records ride inside the job message, so this job can never be sent:
                # retrying it (as a transient error would be) loops forever and the
                # execution never ends. Fail the job and carry on with the rest.
                reason = (
                    f"The job message is too large for Kafka ({e}). Records travel inside the "
                    "message: use fewer records per job, or a source that re-fetches on the worker."
                )
                logger.error(
                    "Job cannot be sent: %s",
                    reason,
                    extra={"event": "job_unsendable", "job_id": job.get("job_id")},
                )
                if on_unsendable:
                    on_unsendable(job, reason)
                    continue
                break
            except KafkaError as e:
                logger.error(
                    "Kafka error: %s",
                    e,
                    extra={
                        "event": "kafka_send_error",
                        "job_id": job.get("job_id"),
                        "error_type": type(e).__name__,
                    },
                )
                break

        return dispatched

    async def close(self) -> None:
        """Close the producer connection."""
        if self._producer and self._started:
            await self._producer.stop()
            self._producer = None
            self._started = False


# Backward compatibility alias
JobDispatcher = BaseDispatcher
