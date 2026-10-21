# aiokafka ships no stubs, so every AIOKafkaProducer/Consumer member is Unknown and
# cascades through anything derived from one. Scoped here rather than repo-wide.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false
"""Kafka destination connector using aiokafka."""

import asyncio
import functools
import inspect
import json
import logging
import ssl
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def _producer_parameters() -> frozenset[str]:
    import aiokafka  # the library's class, even where this module's name is patched

    params = inspect.signature(aiokafka.AIOKafkaProducer.__init__).parameters
    return frozenset(params) - {"self"} - _MANAGED_PRODUCER_PARAMS


# AIOKafkaProducer parameters KafkaDestination sets itself: send() hands the producer
# bytes already, so a configured serializer would encode them a second time.
_MANAGED_PRODUCER_PARAMS = frozenset({"value_serializer", "key_serializer"})

# producer_config keys already warned about, so a pipeline keeping one does not log
# a warning per job (a destination is built per job).
_warned_producer_keys: Set[str] = set()


def _named_config_keys() -> frozenset[str]:
    """Config keys KafkaDestination.__init__ names; anything else is **producer_config."""
    params = inspect.signature(KafkaDestination.__init__).parameters
    return frozenset(params) - {"self", "retry_config", "producer_config"}


class KafkaDestination(BaseDestination):
    """
    Kafka destination connector using aiokafka.

    Supports:
    - SASL authentication (SCRAM-SHA-256)
    - Configurable serialization (JSON by default)
    - Compression
    - Error handling
    - Consumer lag health check (opt-in)
    """

    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topic: str,
        compression_type: str = "gzip",
        retry_config: Optional[RetryConfig] = None,
        # SASL Authentication
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        # Lag health check
        lag_health_check_enabled: bool = False,
        consumer_group_id: Optional[str] = None,
        lag_threshold: int = 10000,
        lag_check_timeout: float = 10.0,
        # Message key
        key_field: Optional[str] = None,
        **producer_config: Any,
    ):
        """
        Initialize Kafka destination.

        Args:
            bootstrap_servers: Kafka broker addresses (comma-separated)
            topic: Target topic
            compression_type: Compression algorithm (gzip, snappy, lz4)
            retry_config: Optional retry configuration
            security_protocol: Security protocol (e.g., SASL_PLAINTEXT)
            sasl_mechanism: SASL mechanism (e.g., SCRAM-SHA-256)
            sasl_username: SASL username
            sasl_password: SASL password
            lag_health_check_enabled: Enable consumer lag health check
            consumer_group_id: Consumer group to monitor for lag
            lag_threshold: Max allowed consumer lag (records) before health check fails
            lag_check_timeout: Timeout in seconds for the lag check
            key_field: Name of a field on each record. When set and present on a
                record, only that field's value is sent as the Kafka message
                (instead of the whole record). Records missing the field, or
                non-dict records, are sent unchanged.
            **producer_config: Additional producer configuration
        """
        config = {
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "compression_type": compression_type,
            "security_protocol": security_protocol,
            "sasl_mechanism": sasl_mechanism,
            "sasl_username": sasl_username,
            "sasl_password": sasl_password,
            "lag_health_check_enabled": lag_health_check_enabled,
            "consumer_group_id": consumer_group_id,
            "lag_threshold": lag_threshold,
            "lag_check_timeout": lag_check_timeout,
            "key_field": key_field,
            **producer_config,
        }

        # Parse bootstrap_servers if it's a comma-separated string
        if isinstance(bootstrap_servers, str) and "," in bootstrap_servers:
            config["bootstrap_servers"] = [
                s.strip() for s in bootstrap_servers.split(",") if s.strip()
            ]

        # An SSLContext is not config: it is not JSON, and its repr (an address)
        # would make destination_identity, and so the dedup hash, differ per process.
        self._ssl_context = config.pop("ssl_context", None)

        super().__init__(config, retry_config)
        self._producer: Optional[AIOKafkaProducer] = None
        self._started = False

    def _build_connection_kwargs(self) -> Dict[str, Any]:
        """Build shared connection kwargs for producer/consumer/admin."""
        kwargs: Dict[str, Any] = {
            "bootstrap_servers": self.config["bootstrap_servers"],
        }
        username = self.config.get("sasl_username")
        password = self.config.get("sasl_password")
        security_protocol = self.config.get("security_protocol")
        if username and password:
            kwargs.update(
                {
                    "security_protocol": security_protocol or "SASL_PLAINTEXT",
                    "sasl_mechanism": self.config.get("sasl_mechanism") or "SCRAM-SHA-256",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password,
                }
            )
        elif security_protocol:
            # TLS without SASL: dropping it would silently connect in plaintext.
            kwargs["security_protocol"] = security_protocol
        if kwargs.get("security_protocol") in ("SSL", "SASL_SSL"):
            # aiokafka refuses SSL without an explicit context; default to system CAs.
            kwargs["ssl_context"] = self._ssl_context or ssl.create_default_context()
        return kwargs

    async def _get_producer(self) -> AIOKafkaProducer:
        """Get or create async Kafka producer."""
        if self._producer is None or not self._started:
            conn_kwargs = self._build_connection_kwargs()
            producer_kwargs = {
                **conn_kwargs,
                "compression_type": self.config.get("compression_type", "gzip"),
            }
            username = self.config.get("sasl_username")
            if username:
                producer_kwargs["client_id"] = username
            # **producer_config (acks, linger_ms, ...) — everything the constructor
            # did not name. Explicit producer settings win over the defaults above.
            # A key AIOKafkaProducer does not take (e.g. the Java client's batch_size,
            # which aiokafka calls max_batch_size) was always ignored; keep ignoring
            # it, loudly, rather than failing every job with a TypeError.
            accepted = _producer_parameters()
            named = _named_config_keys()
            for key, value in self.config.items():
                if key in named:
                    continue
                if key in accepted:
                    producer_kwargs[key] = value
                elif key not in _warned_producer_keys:
                    _warned_producer_keys.add(key)
                    logger.warning(
                        "Kafka destination: ignoring producer setting %r "
                        "(not an AIOKafkaProducer parameter, or one it sets itself)",
                        key,
                    )

            self._producer = AIOKafkaProducer(**producer_kwargs)
            await self._producer.start()
            self._started = True

        return self._producer

    async def _get_consumer_lag(self, consumer_group_id: str) -> int:
        """
        Return total consumer lag for *consumer_group_id* on the destination topic.

        Uses a temporary consumer (no group) to fetch end offsets, and the
        admin client to fetch committed offsets for the target group.
        Fails open: returns 0 on any error so a misconfigured check never
        blocks dispatch.
        """
        from aiokafka import AIOKafkaConsumer
        from aiokafka.admin import AIOKafkaAdminClient
        from aiokafka.structs import TopicPartition

        topic = self.config["topic"]
        conn_kwargs = self._build_connection_kwargs()

        # --- end offsets via a temporary consumer (no group, no commits) ---
        consumer = AIOKafkaConsumer(
            enable_auto_commit=False,
            **conn_kwargs,
        )
        await consumer.start()
        try:
            partitions_set = consumer.partitions_for_topic(topic)
            if not partitions_set:
                # Give metadata a moment to propagate
                await asyncio.sleep(0.5)
                partitions_set = consumer.partitions_for_topic(topic)

            if not partitions_set:
                return 0

            tps = [TopicPartition(topic, p) for p in sorted(partitions_set)]
            consumer.assign(tps)
            end_offsets = await consumer.end_offsets(tps)
        finally:
            await consumer.stop()

        # --- committed offsets via admin client ---
        admin = AIOKafkaAdminClient(**conn_kwargs)
        await admin.start()
        try:
            committed = await admin.list_consumer_group_offsets(consumer_group_id)
        except Exception:
            # Group doesn't exist or has no committed offsets yet — treat as offset=0
            # so that lag correctly equals the full end_offset of the topic.
            committed = {}
        finally:
            await admin.close()

        # --- total lag ---
        total_lag = 0
        for tp in tps:
            end = end_offsets.get(tp, 0)
            committed_meta = committed.get(tp)
            committed_offset = committed_meta.offset if committed_meta else 0
            total_lag += max(0, end - committed_offset)

        return total_lag

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to Kafka topic.

        Args:
            records: List of records to send
            metadata: Optional metadata to include in message headers

        Raises:
            DestinationError: If send fails
        """
        producer = await self._get_producer()
        topic = self.config["topic"]
        key_field = self.config.get("key_field")

        try:
            for record in records:
                # If key_field is set and present, send just that field's value
                # instead of the whole record.
                payload = record
                if key_field and isinstance(record, dict) and key_field in record:
                    payload = record[key_field]
                value = json.dumps(payload).encode("utf-8")

                # Prepare headers
                headers: List[Tuple[str, bytes]] = []
                if metadata:
                    for k, v in metadata.items():
                        if isinstance(v, str):
                            headers.append((k, v.encode("utf-8")))

                # Send message
                await producer.send_and_wait(
                    topic=topic,
                    value=value,
                    headers=headers if headers else None,
                )

        except KafkaError as e:
            raise DestinationError("kafka", f"Failed to send to topic '{topic}': {e}", e)
        except Exception as e:
            raise DestinationError("kafka", f"Unexpected error: {e}", e)

    async def health_check(self) -> bool:
        """
        Check Kafka cluster connectivity and optionally consumer lag.

        When lag_health_check_enabled=True the check also measures the lag for
        consumer_group_id against the destination topic.  Returns False if lag
        exceeds lag_threshold, causing the calling pipeline to be rescheduled
        via the DLQ rather than dispatching jobs right now.
        """
        try:
            await self._get_producer()
        except Exception:
            return False

        if not self.config.get("lag_health_check_enabled"):
            return True

        consumer_group_id = self.config.get("consumer_group_id")
        if not consumer_group_id:
            return True

        timeout = float(self.config.get("lag_check_timeout", 10.0))
        threshold = int(self.config.get("lag_threshold", 10000))

        try:
            lag = await asyncio.wait_for(
                self._get_consumer_lag(consumer_group_id),
                timeout=timeout,
            )
        except Exception as e:
            # Fail open — a broken lag check must not block the pipeline
            logger.warning("Kafka lag check failed (fail open): %s", e)
            return True

        if lag > threshold:
            logger.warning(
                "Kafka lag %d exceeds threshold %d for group '%s'",
                lag,
                threshold,
                consumer_group_id,
            )
            return False

        return True

    async def close(self):
        """Close producer connection."""
        if self._producer and self._started:
            await self._producer.stop()
            self._producer = None
            self._started = False


def kafka_destination(
    bootstrap_servers: Union[str, List[str]],
    topic: str,
    compression_type: str = "gzip",
    retry_config: Optional[RetryConfig] = None,
    security_protocol: Optional[str] = None,
    sasl_mechanism: Optional[str] = None,
    sasl_username: Optional[str] = None,
    sasl_password: Optional[str] = None,
    lag_health_check_enabled: bool = False,
    consumer_group_id: Optional[str] = None,
    lag_threshold: int = 10000,
    lag_check_timeout: float = 10.0,
    key_field: Optional[str] = None,
    **producer_config: Any,
) -> KafkaDestination:
    """
    Factory function for Kafka destination.

    Example:
        >>> destination = kafka_destination(
        ...     bootstrap_servers="kafka:9092",
        ...     topic="processed-logs",
        ...     lag_health_check_enabled=True,
        ...     consumer_group_id="downstream-consumers",
        ...     lag_threshold=5000,
        ...     key_field="user_id",
        ... )
    """
    return KafkaDestination(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        compression_type=compression_type,
        retry_config=retry_config,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        lag_health_check_enabled=lag_health_check_enabled,
        consumer_group_id=consumer_group_id,
        lag_threshold=lag_threshold,
        lag_check_timeout=lag_check_timeout,
        key_field=key_field,
        **producer_config,
    )
