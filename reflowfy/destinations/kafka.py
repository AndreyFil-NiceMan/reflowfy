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
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple, Union, cast
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from reflowfy.core.record_templates import fill
from reflowfy.destinations.base import (
    BaseDestination,
    DestinationError,
    RetryConfig,
)

logger = logging.getLogger(__name__)

HeaderValue = Union[str, bytes, None]
"""A Kafka header value: str (sent UTF-8 encoded), raw bytes, or None (a null value)."""

HeadersFn = Callable[[Any], Optional[Mapping[str, HeaderValue]]]
"""Per-record headers: called with each record, returns that message's headers."""

KafkaHeaders = Union[Mapping[str, str], HeadersFn]
"""What ``kafka_destination(headers=...)`` takes: a static mapping or a function."""

KeyValue = Union[str, bytes, None]
"""A message key: str (sent UTF-8 encoded), raw bytes, or None (no key)."""

KeyFn = Callable[[Any], KeyValue]
"""Per-record message key: called with each record, returns that message's key."""

KafkaKey = Union[str, KeyFn]
"""What ``kafka_destination(key=...)`` takes: a ``{field}`` template or a function."""

_Headers = List[Tuple[str, Optional[bytes]]]


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
        # What each message holds, its key and its headers
        value_field: Optional[str] = None,
        key: Optional[KafkaKey] = None,
        headers: Optional[KafkaHeaders] = None,
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
            value_field: Name of a field on each record. When set and present on a
                record, only that field's value is sent as the Kafka message value
                (instead of the whole record). Records missing the field, or
                non-dict records, are sent unchanged.
            key: The message key, per record: a ``{field}`` template
                (``"{customer_id}"``, ``"{data.region}"``) or a function called with
                each record returning str / bytes / None. Messages with the same key
                go to the same partition, in order. Default: no key.
            headers: Kafka headers to set on every message. Either a mapping of
                header name to string value, where ``{field}`` placeholders are
                filled from each record (``{"tenant": "{tenant_id}"}``), or a
                function called with each record that returns that message's
                headers as a mapping of name to str / bytes / None (or None for
                no headers). Headers with the same name as a metadata header
                replace it. A record missing a placeholder field, or a function
                that raises, fails the job before anything is sent.
            **producer_config: Additional producer configuration
        """
        if "concurrency" in producer_config:
            raise TypeError(
                "kafka_destination: concurrency= was removed in 1.1.4 — every job's "
                "messages are now sent as one batch, which is faster than any setting "
                "of it. Remove the argument."
            )
        headers_fn, static_headers = _check_headers(headers)
        key_fn, static_key = _check_key(key)
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
            "value_field": value_field,
            **producer_config,
        }
        # Only when set, so a destination without headers keeps its config (and
        # repr) exactly as before. A function is not config: like ssl_context,
        # its repr is an address.
        if static_headers is not None:
            config["headers"] = static_headers
        if static_key is not None:
            config["key"] = static_key

        # Parse bootstrap_servers if it's a comma-separated string
        if isinstance(bootstrap_servers, str) and "," in bootstrap_servers:
            config["bootstrap_servers"] = [
                s.strip() for s in bootstrap_servers.split(",") if s.strip()
            ]

        # An SSLContext is not config: it is not JSON, and its repr (an address)
        # would make destination_identity, and so the dedup hash, differ per process.
        self._ssl_context = config.pop("ssl_context", None)

        super().__init__(config, retry_config)
        self._headers_fn = headers_fn
        self._key_fn = key_fn
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

    def _producer_kwargs(self) -> Dict[str, Any]:
        """Everything AIOKafkaProducer is built with: connection, compression, producer_config."""
        producer_kwargs = {
            **self._build_connection_kwargs(),
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
                    extra={"event": "producer_setting_ignored"},
                )
        return producer_kwargs

    def _connection_key(self) -> str:
        """Same key, same producer: the cluster, credentials and producer settings.

        Not the topic — a producer writes to any topic, so a job's topics on one
        cluster share one.
        """
        return "kafka:" + json.dumps(self._producer_kwargs(), sort_keys=True, default=_object_key)

    async def _get_producer(self) -> AIOKafkaProducer:
        """The producer: the pool's one for this cluster and settings, else this destination's."""
        pool = self._active_pool()
        if pool is not None:
            producer_kwargs = self._producer_kwargs()

            async def create() -> AIOKafkaProducer:
                producer = AIOKafkaProducer(**producer_kwargs)
                await producer.start()
                return producer

            async def stop(producer: AIOKafkaProducer) -> None:
                await producer.stop()

            return await self._pooled(pool, self._connection_key(), create, stop)

        if self._producer is None or not self._started:
            self._producer = AIOKafkaProducer(**self._producer_kwargs())
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
        Send records to the Kafka topic, one message per record, as one batch.

        Every message is handed to the producer first and the acks are awaited
        together, so aiokafka batches them. Order on a partition is the records'
        order: aiokafka keeps one batch in flight per partition.

        Args:
            records: List of records to send
            metadata: Optional metadata to include in message headers

        Raises:
            DestinationError: If a message fails (naming its position, not its contents)
            ValueError: If a record cannot get its headers or key (nothing is sent)
        """
        topic = self.config["topic"]
        total = len(records)
        if not total:
            return

        base_headers: _Headers = []
        if metadata:
            for k, v in metadata.items():
                if isinstance(v, str):
                    base_headers.append((k, v.encode("utf-8")))

        # Built for every record before the first send: a record that cannot get its
        # headers or key fails the job (a ValueError, not retried) with nothing written.
        per_record = self._build_headers(records, base_headers)
        keys = self._build_keys(records)
        values: List[bytes] = []
        for index, record in enumerate(records):
            try:
                values.append(json.dumps(self._payload(record)).encode("utf-8"))
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"kafka_destination: record {index + 1} of {total} is not JSON "
                    f"serializable: {type(e).__name__}"
                ) from e

        producer = await self._get_producer()
        futures: List["asyncio.Future[Any]"] = []
        try:
            try:
                for index, value in enumerate(values):
                    headers = per_record[index] if per_record is not None else base_headers
                    futures.append(
                        await producer.send(
                            topic,
                            value=value,
                            key=keys[index] if keys is not None else None,
                            headers=headers if headers else None,
                        )
                    )
            except KafkaError as e:
                await self._evict(producer)
                raise DestinationError(
                    "kafka",
                    f"Failed to send to topic '{topic}': record {len(futures) + 1} of {total}: {e}",
                    e,
                )
            finally:
                # Every message handed over is awaited, even after a failure, so no
                # ack is left unobserved.
                results = await asyncio.gather(*futures, return_exceptions=True)
            for index, result in enumerate(results):
                if isinstance(result, BaseException):
                    await self._evict(producer)
                    raise DestinationError(
                        "kafka",
                        f"Failed to send to topic '{topic}': record {index + 1} of {total}: "
                        f"{result}",
                        result if isinstance(result, Exception) else None,
                    )
        except DestinationError:
            raise
        except Exception as e:
            raise DestinationError("kafka", f"Unexpected error: {e}", e)
        logger.debug(
            "Sent %d messages to %s as one batch",
            total,
            topic,
            extra={"event": "kafka_batch_sent", "messages": total},
        )

    async def _evict(self, producer: AIOKafkaProducer) -> None:
        """A pooled producer that failed is replaced: the retry gets a fresh one."""
        if self._used_pool is not None:
            await self._used_pool.evict(self._connection_key(), producer)

    def _payload(self, record: Any) -> Any:
        """The message value: ``record[value_field]`` when set and present, else the record."""
        value_field = self.config.get("value_field")
        if value_field and isinstance(record, dict) and value_field in record:
            return cast(Dict[str, Any], record)[value_field]
        return record

    def _build_keys(self, records: List[Any]) -> Optional[List[Optional[bytes]]]:
        """Each record's message key, or None when messages have no key."""
        static: Optional[str] = self.config.get("key")
        if self._key_fn is None and static is None:
            return None
        total = len(records)
        keys: List[Optional[bytes]] = []
        for index, record in enumerate(records):
            if self._key_fn is not None:
                try:
                    produced: Any = self._key_fn(record)
                except Exception as e:
                    raise ValueError(
                        f"kafka_destination: key function failed on record "
                        f"{index + 1} of {total}: {type(e).__name__}"
                    ) from e
            else:
                assert static is not None
                produced = fill(
                    static,
                    record,
                    owner="kafka_destination",
                    what="key",
                    index=index,
                    total=total,
                )
            if produced is None or isinstance(produced, bytes):
                keys.append(produced)
            elif isinstance(produced, str):
                keys.append(produced.encode("utf-8"))
            else:
                raise ValueError(
                    f"kafka_destination: key for record {index + 1} of {total} is "
                    f"{type(produced).__name__}; keys must be str, bytes or None"
                )
        return keys

    def _build_headers(self, records: List[Any], base: _Headers) -> Optional[List[_Headers]]:
        """Each record's headers, or None when every message gets ``base`` unchanged."""
        static: Optional[Dict[str, str]] = self.config.get("headers")
        if self._headers_fn is None and not static:
            return None
        total = len(records)
        result: List[_Headers] = []
        for index, record in enumerate(records):
            if self._headers_fn is not None:
                try:
                    produced = self._headers_fn(record)
                except Exception as e:
                    raise ValueError(
                        f"kafka_destination: headers function failed on record "
                        f"{index + 1} of {total}: {type(e).__name__}"
                    ) from e
                own = _encode_headers(produced, index, total)
            else:
                assert static is not None
                own = [
                    (
                        name,
                        fill(
                            template,
                            record,
                            owner="kafka_destination",
                            what=f"header {name!r}",
                            index=index,
                            total=total,
                        ).encode("utf-8"),
                    )
                    for name, template in static.items()
                ]
            names = {name for name, _ in own}
            result.append([h for h in base if h[0] not in names] + own)
        return result

    def identity_config(self) -> Dict[str, Any]:
        """``config`` without ``headers`` or ``key``: they describe a message, not where it goes.

        So adding or changing them does not turn records already written into new work.
        """
        return {k: v for k, v in self.config.items() if k not in ("headers", "key")}

    def health_key(self) -> Optional[str]:
        """One check per cluster per job; per topic and group when it measures lag."""
        if self.config.get("lag_health_check_enabled") and self.config.get("consumer_group_id"):
            return (
                f"{self._connection_key()}|lag:{self.config['topic']}"
                f"|{self.config['consumer_group_id']}"
            )
        return self._connection_key()

    async def health_check(self) -> bool:
        """
        Check Kafka cluster connectivity and optionally consumer lag.

        When lag_health_check_enabled=True the check also measures the lag for
        consumer_group_id against the destination topic.  Returns False if lag
        exceeds lag_threshold, causing the calling pipeline to be rescheduled
        via the DLQ rather than dispatching jobs right now.

        Pooled (a worker), a healthy result is reused for 30 s per :meth:`health_key`.
        """
        return await self._cached_health(self._probe)

    async def _probe(self) -> bool:
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
            logger.warning(
                "Kafka lag check failed (fail open): %s",
                e,
                extra={"event": "kafka_lag_check_error", "error_type": type(e).__name__},
            )
            return True

        if lag > threshold:
            logger.warning(
                "Kafka lag %d exceeds threshold %d for group '%s'",
                lag,
                threshold,
                consumer_group_id,
                extra={"event": "kafka_lag_high", "lag": lag, "lag_threshold": threshold},
            )
            return False

        return True

    async def close(self) -> None:
        """Stop this destination's own producer; a pooled one is only released."""
        await self._release_pooled()
        if self._producer and self._started:
            await self._producer.stop()
            self._producer = None
            self._started = False


def _check_headers(headers: object) -> Tuple[Optional[HeadersFn], Optional[Dict[str, str]]]:
    """Split ``headers=`` into (function, static mapping), rejecting bad shapes early.

    Takes ``object``: the annotation on ``kafka_destination`` is not enforced for
    untyped callers, and a wrong shape should fail here, not on the first send.
    """
    if headers is None:
        return None, None
    if callable(headers):
        return cast(HeadersFn, headers), None
    if not isinstance(headers, Mapping):
        raise TypeError(
            "kafka_destination: headers must be a mapping of name to string value, "
            f"or a function of the record; got {type(headers).__name__}"
        )
    static: Dict[str, str] = {}
    for name, value in cast(Mapping[Any, Any], headers).items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError(
                "kafka_destination: static headers must map str to str; use a "
                "function of the record for bytes or null values"
            )
        static[name] = value
    return None, static


def _check_key(key: object) -> Tuple[Optional[KeyFn], Optional[str]]:
    """Split ``key=`` into (function, template), rejecting bad shapes early."""
    if key is None:
        return None, None
    if callable(key):
        return cast(KeyFn, key), None
    if isinstance(key, str):
        return None, key
    raise TypeError(
        "kafka_destination: key must be a {field} template string or a function of "
        f"the record; got {type(key).__name__}"
    )


def _object_key(value: Any) -> str:
    """JSON fallback for a connection key: an object (an ssl_context) is itself."""
    return f"{type(value).__name__}@{id(value)}"


def _encode_headers(produced: Any, index: int, total: int) -> _Headers:
    """A headers function's result as aiokafka's list of (name, bytes | None)."""
    if produced is None:
        return []
    if not isinstance(produced, Mapping):
        raise ValueError(
            f"kafka_destination: headers function returned {type(produced).__name__} "
            f"for record {index + 1} of {total}; expected a mapping or None"
        )
    encoded: _Headers = []
    for name, value in cast(Mapping[Any, Any], produced).items():
        if not isinstance(name, str):
            raise ValueError(
                f"kafka_destination: header name {type(name).__name__} on record "
                f"{index + 1} of {total}; header names must be str"
            )
        if isinstance(value, str):
            encoded.append((name, value.encode("utf-8")))
        elif value is None or isinstance(value, bytes):
            encoded.append((name, value))
        else:
            raise ValueError(
                f"kafka_destination: header {name!r} on record {index + 1} of {total} "
                f"is {type(value).__name__}; header values must be str, bytes or None"
            )
    return encoded


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
    value_field: Optional[str] = None,
    key: Optional[KafkaKey] = None,
    headers: Optional[KafkaHeaders] = None,
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
        ...     value_field="data",  # send record["data"] as the message value
        ...     key="{customer_id}",  # one customer's messages: one partition, in order
        ...     headers={"source": "reflowfy", "tenant": "{tenant_id}"},  # per record
        ... )

        Headers computed in code (bytes, null values, anything beyond a field):
        >>> destination = kafka_destination(
        ...     bootstrap_servers="kafka:9092",
        ...     topic="events",
        ...     headers=lambda record: {"type": record["type"], "trace": None},
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
        value_field=value_field,
        key=key,
        headers=headers,
        **producer_config,
    )
