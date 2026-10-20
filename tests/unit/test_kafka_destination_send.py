"""KafkaDestination: connection kwargs, send, health_check (lag) and close.

Fake aiokafka producer/consumer/admin classes record every call; no broker.
"""

import datetime as dt
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional

import aiokafka
import aiokafka.admin
import pytest
from aiokafka.errors import KafkaConnectionError, KafkaError
from aiokafka.structs import TopicPartition

import reflowfy.destinations.kafka as kafka_mod
from reflowfy.destinations.base import DestinationError, RetryConfig
from reflowfy.destinations.kafka import KafkaDestination, kafka_destination


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@contextmanager
def capture_logs(name: str) -> Iterator[List[str]]:
    """Collect a logger's messages directly (the 'reflowfy' logger may not propagate)."""
    target = logging.getLogger(name)
    handler = _ListHandler()
    old_level = target.level
    target.addHandler(handler)
    target.setLevel(logging.DEBUG)
    try:
        yield handler.messages
    finally:
        target.removeHandler(handler)
        target.setLevel(old_level)


# ---------------------------------------------------------------- fakes


class FakeProducer:
    """Records constructor kwargs and every send; can fail on start or on the Nth send."""

    instances: List["FakeProducer"] = []
    fail_start: Optional[BaseException] = None
    fail_on_send: Optional[int] = None  # 1-based send number that raises
    send_error: BaseException = KafkaError("broker gone")

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.started = False
        self.stopped = 0
        self.sent: List[Dict[str, Any]] = []
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        if FakeProducer.fail_start is not None:
            raise FakeProducer.fail_start
        self.started = True

    async def stop(self) -> None:
        self.stopped += 1

    async def send_and_wait(self, topic: str, value: bytes, headers: Any = None) -> None:
        if (
            FakeProducer.fail_on_send is not None
            and len(self.sent) + 1 == FakeProducer.fail_on_send
        ):
            raise FakeProducer.send_error
        self.sent.append({"topic": topic, "value": value, "headers": headers})


@pytest.fixture(autouse=True)
def fake_producer(monkeypatch):
    FakeProducer.instances = []
    FakeProducer.fail_start = None
    FakeProducer.fail_on_send = None
    FakeProducer.send_error = KafkaError("broker gone")
    monkeypatch.setattr(kafka_mod, "AIOKafkaProducer", FakeProducer)
    return FakeProducer


def _dest(**kwargs: Any) -> KafkaDestination:
    kwargs.setdefault("bootstrap_servers", "k:9092")
    kwargs.setdefault("topic", "out")
    # No retry back-off in unit tests.
    kwargs.setdefault("retry_config", RetryConfig(max_attempts=1, min_wait_seconds=0))
    return kafka_destination(**kwargs)


# ---------------------------------------------------------------- connection kwargs


class TestConnectionKwargs:
    def test_plaintext_has_only_bootstrap_servers(self):
        assert _dest()._build_connection_kwargs() == {"bootstrap_servers": "k:9092"}

    def test_comma_separated_servers_are_split(self):
        dest = _dest(bootstrap_servers="a:1, b:2,")
        assert dest._build_connection_kwargs()["bootstrap_servers"] == ["a:1", "b:2"]

    def test_sasl_defaults_when_only_credentials_given(self):
        kwargs = _dest(sasl_username="u", sasl_password="p")._build_connection_kwargs()
        assert kwargs == {
            "bootstrap_servers": "k:9092",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "u",
            "sasl_plain_password": "p",
        }

    def test_sasl_ssl_with_explicit_mechanism(self):
        kwargs = _dest(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="u",
            sasl_password="p",
        )._build_connection_kwargs()
        assert kwargs["security_protocol"] == "SASL_SSL"
        assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"

    def test_username_without_password_does_not_enable_sasl(self):
        assert _dest(sasl_username="u")._build_connection_kwargs() == {
            "bootstrap_servers": "k:9092"
        }

    def test_ssl_without_sasl_keeps_the_security_protocol(self):
        kwargs = _dest(security_protocol="SSL")._build_connection_kwargs()
        assert kwargs.get("security_protocol") == "SSL"


# ---------------------------------------------------------------- producer lifecycle


class TestProducer:
    async def test_producer_is_created_once_with_compression_and_client_id(self, fake_producer):
        dest = _dest(compression_type="lz4", sasl_username="svc", sasl_password="pw")

        await dest.send([{"a": 1}])
        await dest.send([{"a": 2}])

        assert len(fake_producer.instances) == 1
        kwargs = fake_producer.instances[0].kwargs
        assert kwargs["compression_type"] == "lz4"
        assert kwargs["client_id"] == "svc"
        assert kwargs["sasl_plain_password"] == "pw"

    async def test_no_client_id_without_sasl(self, fake_producer):
        await _dest().send([{"a": 1}])
        assert "client_id" not in fake_producer.instances[0].kwargs

    async def test_failed_start_is_retried_with_a_new_producer(self, fake_producer):
        dest = _dest()
        fake_producer.fail_start = KafkaConnectionError("down")
        with pytest.raises(KafkaConnectionError):
            await dest.send([{"a": 1}])

        fake_producer.fail_start = None
        await dest.send([{"a": 1}])

        assert len(fake_producer.instances) == 2
        assert fake_producer.instances[1].sent

    async def test_unknown_producer_setting_is_ignored_with_a_warning(
        self, fake_producer, caplog, monkeypatch
    ):
        monkeypatch.setattr(kafka_mod, "_warned_producer_keys", set())
        # The Java client's batch_size is not an AIOKafkaProducer parameter: passing
        # it through would fail every job with a TypeError.
        with caplog.at_level(logging.WARNING, logger="reflowfy.destinations.kafka"):
            await _dest(batch_size=16384, acks="all").send([{"a": 1}])
            # A destination is built per job: the second one does not warn again.
            await _dest(batch_size=16384).send([{"a": 1}])
        kwargs = fake_producer.instances[0].kwargs
        assert "batch_size" not in kwargs and kwargs["acks"] == "all"
        assert caplog.text.count("ignoring producer setting 'batch_size'") == 1

    async def test_serializers_are_never_forwarded(self, fake_producer, monkeypatch):
        # send() hands the producer bytes already; a serializer would encode them twice.
        monkeypatch.setattr(kafka_mod, "_warned_producer_keys", set())
        await _dest(value_serializer=lambda v: v, key_serializer=lambda k: k).send([{"a": 1}])
        kwargs = fake_producer.instances[0].kwargs
        assert "value_serializer" not in kwargs and "key_serializer" not in kwargs

    def test_ssl_context_is_used_but_kept_out_of_config(self):
        import ssl

        ctx = ssl.create_default_context()
        dest = _dest(security_protocol="SSL", ssl_context=ctx)
        # Not config: its repr is an address, which would change the dedup identity.
        assert "ssl_context" not in dest.config
        assert dest._build_connection_kwargs()["ssl_context"] is ctx

    async def test_extra_producer_config_reaches_the_producer(self, fake_producer):
        await _dest(acks="all", linger_ms=5).send([{"a": 1}])
        kwargs = fake_producer.instances[0].kwargs
        assert kwargs.get("acks") == "all"
        assert kwargs.get("linger_ms") == 5


# ---------------------------------------------------------------- send


class TestSend:
    async def test_each_record_is_one_message_in_order(self, fake_producer):
        records = [{"id": i} for i in range(5)]

        await _dest(topic="events").send(records)

        sent = fake_producer.instances[0].sent
        assert [m["topic"] for m in sent] == ["events"] * 5
        assert [m["value"] for m in sent] == [f'{{"id": {i}}}'.encode() for i in range(5)]
        assert all(m["headers"] is None for m in sent)

    async def test_empty_batch_sends_nothing(self, fake_producer):
        await _dest().send([])
        assert fake_producer.instances[0].sent == []

    async def test_non_dict_records_are_json_encoded(self, fake_producer):
        await _dest().send(["text", 7, None, [1, 2]])
        assert [m["value"] for m in fake_producer.instances[0].sent] == [
            b'"text"',
            b"7",
            b"null",
            b"[1, 2]",
        ]

    async def test_unicode_is_utf8_encoded(self, fake_producer):
        await _dest().send([{"name": "Zoë"}])
        value = fake_producer.instances[0].sent[0]["value"]
        assert value.decode("utf-8") == '{"name": "Zo\\u00eb"}'

    async def test_only_string_metadata_becomes_headers(self, fake_producer):
        await _dest().send(
            [{"a": 1}],
            metadata={"execution_id": "e1", "batch_number": 3, "flag": None, "job_id": "j"},
        )
        assert fake_producer.instances[0].sent[0]["headers"] == [
            ("execution_id", b"e1"),
            ("job_id", b"j"),
        ]

    async def test_metadata_with_no_string_values_sends_no_headers(self, fake_producer):
        await _dest().send([{"a": 1}], metadata={"n": 1})
        assert fake_producer.instances[0].sent[0]["headers"] is None

    async def test_non_json_value_raises_destination_error(self, fake_producer):
        with pytest.raises(DestinationError) as excinfo:
            await _dest().send([{"ok": 1}, {"when": dt.datetime(2026, 1, 1)}])

        assert excinfo.value.destination_type == "kafka"
        assert "Unexpected error" in str(excinfo.value)
        assert isinstance(excinfo.value.original_error, TypeError)
        # The record before the bad one was already sent.
        assert len(fake_producer.instances[0].sent) == 1

    async def test_kafka_error_raises_destination_error_naming_the_topic(self, fake_producer):
        fake_producer.fail_on_send = 2
        with pytest.raises(DestinationError) as excinfo:
            await _dest(topic="events").send([{"i": 1}, {"i": 2}, {"i": 3}])

        assert "Failed to send to topic 'events'" in str(excinfo.value)
        assert isinstance(excinfo.value.original_error, KafkaError)
        assert len(fake_producer.instances[0].sent) == 1  # stopped at the failure

    async def test_send_with_retry_retries_a_kafka_error(self, fake_producer):
        dest = _dest(retry_config=RetryConfig(max_attempts=3, min_wait_seconds=0, multiplier=0))
        producer = await dest._get_producer()
        real_send = producer.send_and_wait
        calls = {"n": 0}

        async def flaky(topic, value, headers=None):
            calls["n"] += 1
            if calls["n"] == 1:  # first send of the first attempt
                raise KafkaError("leader not available")
            await real_send(topic, value, headers)

        producer.send_and_wait = flaky

        await dest.send_with_retry([{"i": 1}])

        # Attempt 1 failed before sending anything; attempt 2 delivered.
        assert fake_producer.instances[0].sent == [
            {"topic": "out", "value": b'{"i": 1}', "headers": None}
        ]

    async def test_send_with_retry_gives_up_after_max_attempts(self, fake_producer):
        dest = _dest(retry_config=RetryConfig(max_attempts=2, min_wait_seconds=0, multiplier=0))
        calls = {"n": 0}

        async def always_fail(topic, value, headers=None):
            calls["n"] += 1
            raise KafkaError("nope")

        await dest._get_producer()
        fake_producer.instances[0].send_and_wait = always_fail

        with pytest.raises(DestinationError):
            await dest.send_with_retry([{"i": 1}])
        assert calls["n"] == 2


# ---------------------------------------------------------------- close


class TestClose:
    async def test_close_stops_the_producer_once_and_is_idempotent(self, fake_producer):
        dest = _dest()
        await dest.send([{"a": 1}])

        await dest.close()
        await dest.close()

        assert fake_producer.instances[0].stopped == 1
        assert dest._producer is None

    async def test_close_without_a_producer_is_a_noop(self):
        await _dest().close()

    async def test_send_after_close_starts_a_fresh_producer(self, fake_producer):
        dest = _dest()
        await dest.send([{"a": 1}])
        await dest.close()
        await dest.send([{"a": 2}])
        assert len(fake_producer.instances) == 2


# ---------------------------------------------------------------- health_check / lag


class FakeLagConsumer:
    partitions: Any = {0, 1}
    end: Dict[TopicPartition, int] = {}
    instances: List["FakeLagConsumer"] = []

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.assigned: List[TopicPartition] = []
        self.stopped = False
        self.partition_calls = 0
        FakeLagConsumer.instances.append(self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    def partitions_for_topic(self, topic: str) -> Any:
        self.partition_calls += 1
        return FakeLagConsumer.partitions

    def assign(self, tps: List[TopicPartition]) -> None:
        self.assigned = tps

    async def end_offsets(self, tps: List[TopicPartition]) -> Dict[TopicPartition, int]:
        return {tp: FakeLagConsumer.end.get(tp, 0) for tp in tps}


class FakeAdmin:
    committed: Dict[TopicPartition, Any] = {}
    error: Optional[Exception] = None
    closed = False

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs

    async def start(self) -> None:
        pass

    async def list_consumer_group_offsets(self, group_id: str) -> Dict[TopicPartition, Any]:
        if FakeAdmin.error is not None:
            raise FakeAdmin.error
        return FakeAdmin.committed

    async def close(self) -> None:
        FakeAdmin.closed = True


@pytest.fixture
def fake_lag(monkeypatch):
    FakeLagConsumer.partitions = {0, 1}
    FakeLagConsumer.end = {TopicPartition("out", 0): 100, TopicPartition("out", 1): 50}
    FakeLagConsumer.instances = []
    FakeAdmin.committed = {TopicPartition("out", 0): SimpleNamespace(offset=40)}
    FakeAdmin.error = None
    FakeAdmin.closed = False
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", FakeLagConsumer)
    monkeypatch.setattr(aiokafka.admin, "AIOKafkaAdminClient", FakeAdmin)


def _lag_dest(threshold: int = 1000, **kwargs: Any) -> KafkaDestination:
    return _dest(
        lag_health_check_enabled=True,
        consumer_group_id="downstream",
        lag_threshold=threshold,
        **kwargs,
    )


class TestHealthCheck:
    async def test_healthy_broker_without_lag_check(self, fake_producer):
        assert await _dest().health_check() is True
        assert fake_producer.instances[0].started

    async def test_unreachable_broker_is_unhealthy(self, fake_producer):
        fake_producer.fail_start = KafkaConnectionError("unreachable")
        assert await _dest().health_check() is False

    async def test_lag_check_enabled_without_group_is_healthy(self, fake_lag):
        dest = _dest(lag_health_check_enabled=True)
        assert await dest.health_check() is True
        assert FakeLagConsumer.instances == []  # nothing measured

    async def test_consumer_lag_sums_end_minus_committed(self, fake_lag):
        # p0: 100 - 40, p1: 50 - 0 (never committed)
        assert await _lag_dest()._get_consumer_lag("downstream") == 110
        consumer = FakeLagConsumer.instances[0]
        assert consumer.assigned == [TopicPartition("out", 0), TopicPartition("out", 1)]
        assert consumer.kwargs["enable_auto_commit"] is False
        assert consumer.stopped and FakeAdmin.closed

    async def test_lag_check_uses_sasl_connection_kwargs(self, fake_lag):
        await _lag_dest(sasl_username="u", sasl_password="p")._get_consumer_lag("g")
        assert FakeLagConsumer.instances[0].kwargs["sasl_plain_username"] == "u"

    async def test_lag_below_threshold_is_healthy(self, fake_lag):
        assert await _lag_dest(threshold=110).health_check() is True

    async def test_lag_over_threshold_is_unhealthy_and_logged(self, fake_lag):
        with capture_logs(kafka_mod.logger.name) as logs:
            assert await _lag_dest(threshold=109).health_check() is False
        assert "Kafka lag 110 exceeds threshold 109 for group 'downstream'" in logs

    async def test_group_without_commits_counts_the_whole_topic(self, fake_lag):
        FakeAdmin.error = RuntimeError("GroupIdNotFound")
        assert await _lag_dest()._get_consumer_lag("downstream") == 150
        assert FakeAdmin.closed

    async def test_committed_ahead_of_end_never_goes_negative(self, fake_lag):
        FakeAdmin.committed = {
            TopicPartition("out", 0): SimpleNamespace(offset=500),
            TopicPartition("out", 1): SimpleNamespace(offset=50),
        }
        assert await _lag_dest()._get_consumer_lag("downstream") == 0

    async def test_unknown_topic_waits_once_then_reports_no_lag(self, fake_lag, monkeypatch):
        FakeLagConsumer.partitions = None
        slept: List[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        monkeypatch.setattr(kafka_mod.asyncio, "sleep", fake_sleep)

        assert await _lag_dest()._get_consumer_lag("downstream") == 0
        assert slept == [0.5]
        assert FakeLagConsumer.instances[0].partition_calls == 2
        assert FakeLagConsumer.instances[0].stopped

    async def test_broken_lag_check_fails_open(self, fake_lag, monkeypatch):
        dest = _lag_dest(threshold=0)

        async def boom(group: str) -> int:
            raise RuntimeError("metadata timeout")

        monkeypatch.setattr(dest, "_get_consumer_lag", boom)
        with capture_logs(kafka_mod.logger.name) as logs:
            assert await dest.health_check() is True
        assert logs == ["Kafka lag check failed (fail open): metadata timeout"]

    async def test_slow_lag_check_times_out_and_fails_open(self, fake_lag, monkeypatch):
        import asyncio

        dest = _lag_dest(threshold=0, lag_check_timeout=0.01)

        async def hang(group: str) -> int:
            await asyncio.sleep(10)
            return 10**9

        monkeypatch.setattr(dest, "_get_consumer_lag", hang)
        assert await dest.health_check() is True
