"""KafkaDispatcher: lazy producer, ordered batch dispatch, stop conditions and close.

The "rest stay pending" guarantee: dispatch_jobs_batch reports exactly the jobs it
sent (on_sent + the return value), in order, and stops at the first transient send
failure, so the caller leaves the remainder `pending` for the next call.
"""

import asyncio
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

import pytest
from aiokafka.errors import KafkaConnectionError, KafkaError, MessageSizeTooLargeError

import reflowfy.reflow_manager.dispatcher as dispatcher_mod
from reflowfy.reflow_manager.dispatcher import (
    TOKEN_WAIT_SECONDS,
    BaseDispatcher,
    JobDispatcher,
    KafkaDispatcher,
)


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


class FakeProducer:
    instances: List["FakeProducer"] = []

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.sent: List[Dict[str, Any]] = []
        self.stopped = 0
        # job_id -> exception raised when that job is sent
        self.errors: Dict[str, BaseException] = {}
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped += 1

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        job = json.loads(value)
        error = self.errors.get(job.get("job_id"))
        if error is not None:
            raise error
        self.sent.append({"topic": topic, "job": job})


class FakeRateLimiter:
    def __init__(self, tokens: int = 10**9):
        self.tokens = tokens
        self.acquire_calls: List[Dict[str, Any]] = []
        self.consume_calls: List[Dict[str, Any]] = []

    def acquire_token(
        self, pipeline_name: str, rate_limit: Optional[float] = None, max_wait: float = 300.0
    ) -> bool:
        self.acquire_calls.append(
            {"pipeline": pipeline_name, "rate_limit": rate_limit, "max_wait": max_wait}
        )
        if self.tokens <= 0:
            return False
        self.tokens -= 1
        return True

    def consume_tokens(
        self, pipeline_name: str, count: int = 1, rate_limit: Optional[float] = None
    ) -> bool:
        self.consume_calls.append({"pipeline": pipeline_name, "rate_limit": rate_limit})
        if self.tokens < count:
            return False
        self.tokens -= count
        return True


@pytest.fixture(autouse=True)
def fake_producer(monkeypatch):
    FakeProducer.instances = []
    monkeypatch.setattr(dispatcher_mod, "AIOKafkaProducer", FakeProducer)
    return FakeProducer


def _dispatcher(tokens: int = 10**9, **kwargs: Any) -> KafkaDispatcher:
    kwargs.setdefault("kafka_bootstrap_servers", "k:9092")
    kwargs.setdefault("kafka_topic", "reflow.jobs")
    return KafkaDispatcher(rate_limiter=FakeRateLimiter(tokens), **kwargs)  # type: ignore[arg-type]


def _jobs(n: int) -> List[Dict[str, Any]]:
    return [{"schema_version": 2, "job_id": f"j{i}", "execution_id": "e"} for i in range(n)]


def _sent_ids(producer: FakeProducer) -> List[str]:
    return [m["job"]["job_id"] for m in producer.sent]


# ---------------------------------------------------------------- construction


class TestConstruction:
    def test_comma_separated_servers_are_split(self):
        d = _dispatcher(kafka_bootstrap_servers="a:1, b:2 ,")
        assert d.kafka_bootstrap_servers == ["a:1", "b:2"]

    def test_single_server_kept(self):
        assert _dispatcher().kafka_bootstrap_servers == "k:9092"

    def test_backward_compat_alias(self):
        assert JobDispatcher is BaseDispatcher
        assert isinstance(_dispatcher(), JobDispatcher)


# ---------------------------------------------------------------- producer


class TestProducer:
    async def test_producer_created_lazily_and_once_per_loop(self, fake_producer):
        d = _dispatcher()
        assert fake_producer.instances == []  # nothing until the first send

        await d.dispatch_jobs_batch(_jobs(2), "p")
        await d.dispatch_jobs_batch(_jobs(2), "p")
        await d.dispatch_job({"job_id": "single"}, "p")

        assert len(fake_producer.instances) == 1
        assert fake_producer.instances[0].kwargs == {
            "bootstrap_servers": "k:9092",
            "compression_type": "gzip",
        }

    async def test_sasl_settings_and_client_id(self, fake_producer):
        d = _dispatcher(sasl_username="svc", sasl_password="pw")
        await d.dispatch_jobs_batch(_jobs(1), "p")
        assert fake_producer.instances[0].kwargs == {
            "bootstrap_servers": "k:9092",
            "compression_type": "gzip",
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "svc",
            "sasl_plain_password": "pw",
            "client_id": "svc",
        }

    async def test_explicit_sasl_protocol_and_mechanism(self, fake_producer):
        d = _dispatcher(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="u",
            sasl_password="p",
        )
        await d.dispatch_jobs_batch(_jobs(1), "p")
        kwargs = fake_producer.instances[0].kwargs
        assert kwargs["security_protocol"] == "SASL_SSL"
        assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"

    def test_new_event_loop_gets_a_new_producer(self, fake_producer):
        """Each execution's drive loop runs in its own event loop; a producer bound
        to a closed loop must be discarded, not reused."""
        d = _dispatcher()
        asyncio.run(d.dispatch_jobs_batch(_jobs(1), "p"))
        with capture_logs(dispatcher_mod.logger.name) as logs:
            asyncio.run(d.dispatch_jobs_batch(_jobs(1), "p"))

        assert len(fake_producer.instances) == 2
        assert "Detected event loop change, resetting Kafka producer" in logs
        assert _sent_ids(fake_producer.instances[1]) == ["j0"]


# ---------------------------------------------------------------- dispatch_jobs_batch


class TestDispatchBatch:
    async def test_sends_all_in_order_and_reports_each(self, fake_producer):
        d = _dispatcher()
        reported: List[str] = []

        n = await d.dispatch_jobs_batch(
            _jobs(4), "p", rate_limit=60.0, on_sent=lambda job: reported.append(job["job_id"])
        )

        assert n == 4
        assert reported == ["j0", "j1", "j2", "j3"]
        producer = fake_producer.instances[0]
        assert _sent_ids(producer) == reported
        assert {m["topic"] for m in producer.sent} == {"reflow.jobs"}
        # one token per job, with the override rate and the short wait
        assert [c["rate_limit"] for c in d.rate_limiter.acquire_calls] == [60.0] * 4
        assert {c["max_wait"] for c in d.rate_limiter.acquire_calls} == {TOKEN_WAIT_SECONDS}

    async def test_payload_is_the_job_as_json(self, fake_producer):
        job = {"schema_version": 2, "job_id": "j", "source": {"type": "S", "config": {"a": 1}}}
        await _dispatcher().dispatch_jobs_batch([job], "p")
        assert fake_producer.instances[0].sent[0]["job"] == job

    async def test_transient_error_stops_and_reports_only_what_was_sent(self, fake_producer):
        d = _dispatcher()
        await d._get_producer()
        fake_producer.instances[0].errors["j2"] = KafkaConnectionError("broker down")
        reported: List[str] = []

        with capture_logs(dispatcher_mod.logger.name) as logs:
            n = await d.dispatch_jobs_batch(
                _jobs(5), "p", on_sent=lambda job: reported.append(job["job_id"])
            )

        assert n == 2
        assert reported == ["j0", "j1"]  # j2..j4 are not reported -> stay pending
        assert _sent_ids(fake_producer.instances[0]) == ["j0", "j1"]
        assert "Kafka error: KafkaConnectionError: broker down" in logs

    async def test_too_large_message_is_reported_unsendable_and_skipped(self, fake_producer):
        d = _dispatcher()
        await d._get_producer()
        fake_producer.instances[0].errors["j1"] = MessageSizeTooLargeError("2MB")
        reported: List[str] = []
        unsendable: List[tuple] = []

        n = await d.dispatch_jobs_batch(
            _jobs(3),
            "p",
            on_sent=lambda job: reported.append(job["job_id"]),
            on_unsendable=lambda job, reason: unsendable.append((job["job_id"], reason)),
        )

        assert n == 2
        assert reported == ["j0", "j2"]
        assert [job_id for job_id, _ in unsendable] == ["j1"]
        assert "too large for Kafka" in unsendable[0][1]

    async def test_too_large_message_without_callback_stops_the_batch(self, fake_producer):
        d = _dispatcher()
        await d._get_producer()
        fake_producer.instances[0].errors["j1"] = MessageSizeTooLargeError("2MB")

        n = await d.dispatch_jobs_batch(_jobs(3), "p")

        assert n == 1
        assert _sent_ids(fake_producer.instances[0]) == ["j0"]

    async def test_no_token_returns_what_was_sent(self, fake_producer):
        d = _dispatcher(tokens=2)
        n = await d.dispatch_jobs_batch(_jobs(5), "p")
        assert n == 2
        assert _sent_ids(fake_producer.instances[0]) == ["j0", "j1"]
        assert len(d.rate_limiter.acquire_calls) == 3  # third attempt got no token

    async def test_should_stop_before_a_job_ends_the_batch(self, fake_producer):
        d = _dispatcher()
        calls: List[Dict[str, Any]] = []

        def should_stop(**kwargs: Any) -> bool:
            calls.append(kwargs)
            sent = len(fake_producer.instances[0].sent)
            return sent >= 2 and not kwargs  # chunk-time check before the 3rd job

        n = await d.dispatch_jobs_batch(_jobs(5), "p", should_stop=should_stop)

        assert n == 2
        assert len(d.rate_limiter.acquire_calls) == 2  # no token burnt for the 3rd
        assert {"chunk": False} in calls  # re-checked after each token wait

    async def test_pause_during_the_token_wait_does_not_send(self, fake_producer):
        """The post-token re-check (chunk=False) catches a pause that landed while
        waiting for the token."""
        d = _dispatcher()
        n = await d.dispatch_jobs_batch(
            _jobs(3), "p", should_stop=lambda chunk=True: chunk is False
        )
        assert n == 0
        assert fake_producer.instances[0].sent == []
        assert len(d.rate_limiter.acquire_calls) == 1

    async def test_empty_batch(self, fake_producer):
        assert await _dispatcher().dispatch_jobs_batch([], "p") == 0


# ---------------------------------------------------------------- dispatch_job


class TestDispatchJob:
    async def test_sends_one_job(self, fake_producer):
        d = _dispatcher()
        assert await d.dispatch_job({"job_id": "x"}, "p", rate_limit=5.0) is True
        assert _sent_ids(fake_producer.instances[0]) == ["x"]
        assert d.rate_limiter.consume_calls == [{"pipeline": "p", "rate_limit": 5.0}]

    async def test_rate_limited_job_is_not_sent(self, fake_producer):
        d = _dispatcher(tokens=0)
        assert await d.dispatch_job({"job_id": "x"}, "p") is False
        assert fake_producer.instances == []  # not even connected

    async def test_kafka_error_returns_false(self, fake_producer):
        d = _dispatcher()
        await d._get_producer()
        fake_producer.instances[0].errors["x"] = KafkaError("boom")
        assert await d.dispatch_job({"job_id": "x"}, "p") is False


# ---------------------------------------------------------------- close


class TestClose:
    async def test_close_stops_producer_and_is_idempotent(self, fake_producer):
        d = _dispatcher()
        await d.dispatch_jobs_batch(_jobs(1), "p")

        await d.close()
        await d.close()

        assert fake_producer.instances[0].stopped == 1

    async def test_close_before_any_send_is_a_noop(self, fake_producer):
        await _dispatcher().close()
        assert fake_producer.instances == []

    async def test_dispatch_after_close_reconnects(self, fake_producer):
        d = _dispatcher()
        await d.dispatch_jobs_batch(_jobs(1), "p")
        await d.close()
        await d.dispatch_jobs_batch(_jobs(1), "p")
        assert len(fake_producer.instances) == 2
