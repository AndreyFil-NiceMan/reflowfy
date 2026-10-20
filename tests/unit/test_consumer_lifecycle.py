"""KafkaJobConsumer end to end against a fake AIOKafkaConsumer (no broker).

Covers start (subscription settings, connect retries), the consume loop, the
commit policy per message kind, draining on request_stop, and teardown.
"""

import json
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, List, Optional
from unittest.mock import AsyncMock

import pytest
from aiokafka.errors import CommitFailedError, KafkaConnectionError

import reflowfy.worker.consumer as consumer_mod
from reflowfy.observability import metrics
from reflowfy.worker.consumer import KafkaJobConsumer


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


class FakeKafkaConsumer:
    """Serves queued messages one poll at a time; records commits and lifecycle."""

    instances: List["FakeKafkaConsumer"] = []
    start_failures: List[BaseException] = []

    def __init__(self, *topics: str, **kwargs: Any):
        self.topics = topics
        self.kwargs = kwargs
        self.queue: List[Any] = []
        self.events: List[str] = []  # ordered log of start/poll/commit/stop
        self.on_idle: Optional[Callable[[], None]] = None
        self.commit_error: Optional[BaseException] = None
        FakeKafkaConsumer.instances.append(self)

    async def start(self) -> None:
        if FakeKafkaConsumer.start_failures:
            raise FakeKafkaConsumer.start_failures.pop(0)
        self.events.append("start")

    async def getmany(self, timeout_ms: int = 0, max_records: Optional[int] = None) -> Dict:
        assert max_records == 1  # one job between polls
        self.events.append("poll")
        if not self.queue:
            if self.on_idle:
                self.on_idle()
            return {}
        return {"tp0": [self.queue.pop(0)]}

    async def commit(self) -> None:
        if self.commit_error is not None:
            error, self.commit_error = self.commit_error, None
            raise error
        self.events.append("commit")

    async def stop(self) -> None:
        self.events.append("stop")


def _msg(value: Any) -> SimpleNamespace:
    if isinstance(value, dict):
        value = json.dumps(value).encode("utf-8")
    return SimpleNamespace(value=value)


def _job(job_id: str, **extra: Any) -> Dict[str, Any]:
    return {
        "schema_version": 2,
        "job_id": job_id,
        "execution_id": "e",
        "pipeline_name": "p",
        **extra,
    }


@pytest.fixture(autouse=True)
def fake_kafka(monkeypatch):
    FakeKafkaConsumer.instances = []
    FakeKafkaConsumer.start_failures = []
    monkeypatch.setattr(consumer_mod, "AIOKafkaConsumer", FakeKafkaConsumer)
    monkeypatch.delenv("KAFKA_MAX_POLL_INTERVAL_MS", raising=False)
    return FakeKafkaConsumer


def _worker(**kwargs: Any) -> KafkaJobConsumer:
    kwargs.setdefault("bootstrap_servers", "k:9092")
    kwargs.setdefault("topic", "reflow.jobs")
    kwargs.setdefault("database_url", "postgresql://x/y")
    worker = KafkaJobConsumer(**kwargs)
    worker.executor.execute_job = AsyncMock(return_value=True)  # type: ignore[method-assign]
    worker.executor.close = AsyncMock()  # type: ignore[method-assign]
    return worker


async def _run(worker: KafkaJobConsumer, messages: List[Any]) -> FakeKafkaConsumer:
    """Start the worker; it processes ``messages`` then stops on the first idle poll."""
    original_init = FakeKafkaConsumer.__init__

    def init(self: FakeKafkaConsumer, *topics: str, **kwargs: Any) -> None:
        original_init(self, *topics, **kwargs)
        self.queue = list(messages)
        self.on_idle = worker.request_stop

    FakeKafkaConsumer.__init__ = init  # type: ignore[method-assign]
    try:
        await worker.start()
    finally:
        FakeKafkaConsumer.__init__ = original_init  # type: ignore[method-assign]
    return FakeKafkaConsumer.instances[-1]


# ---------------------------------------------------------------- construction / start


class TestStart:
    def test_comma_separated_servers_are_split(self):
        assert _worker(bootstrap_servers="a:1, b:2,").bootstrap_servers == ["a:1", "b:2"]

    async def test_subscribes_with_manual_commit_and_one_record_per_poll(self, fake_kafka):
        consumer = await _run(_worker(group_id="g1"), [])

        assert consumer.topics == ("reflow.jobs",)
        assert consumer.kwargs == {
            "bootstrap_servers": "k:9092",
            "group_id": "g1",
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "retry_backoff_ms": 500,
            "metadata_max_age_ms": 30000,
            "max_poll_interval_ms": 3600000,
            "max_poll_records": 1,
        }

    async def test_max_poll_interval_from_env(self, fake_kafka, monkeypatch):
        monkeypatch.setenv("KAFKA_MAX_POLL_INTERVAL_MS", "120000")
        consumer = await _run(_worker(), [])
        assert consumer.kwargs["max_poll_interval_ms"] == 120000

    async def test_sasl_settings(self, fake_kafka):
        consumer = await _run(_worker(sasl_username="svc", sasl_password="pw"), [])
        assert consumer.kwargs["security_protocol"] == "SASL_PLAINTEXT"
        assert consumer.kwargs["sasl_mechanism"] == "SCRAM-SHA-256"
        assert consumer.kwargs["sasl_plain_username"] == "svc"
        assert consumer.kwargs["sasl_plain_password"] == "pw"
        assert consumer.kwargs["client_id"] == "svc"

    async def test_connect_is_retried_with_exponential_backoff(self, fake_kafka, monkeypatch):
        fake_kafka.start_failures = [KafkaConnectionError("coordinator"), KafkaConnectionError("x")]
        slept: List[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        monkeypatch.setattr(consumer_mod.asyncio, "sleep", fake_sleep)
        worker = _worker()

        with capture_logs(consumer_mod.logger.name) as logs:
            consumer = await _run(worker, [_msg(_job("j1"))])

        assert slept == [1, 2]
        assert "Kafka consumer connected" in logs
        assert worker.executor.execute_job.await_count == 1  # type: ignore[attr-defined]
        assert consumer.events[0] == "start"

    async def test_gives_up_after_ten_attempts(self, fake_kafka, monkeypatch):
        fake_kafka.start_failures = [KafkaConnectionError(f"n{i}") for i in range(10)]
        slept: List[float] = []

        async def fake_sleep(seconds: float) -> None:
            slept.append(seconds)

        monkeypatch.setattr(consumer_mod.asyncio, "sleep", fake_sleep)
        worker = _worker()

        with capture_logs(consumer_mod.logger.name) as logs:
            with pytest.raises(KafkaConnectionError):
                await worker.start()

        assert slept == [2**i for i in range(9)]
        assert "Failed to start consumer after 10 attempts" in logs
        worker.executor.execute_job.assert_not_awaited()  # type: ignore[attr-defined]


# ---------------------------------------------------------------- consume loop


class TestConsumeLoop:
    async def test_processes_each_message_then_commits_then_leaves_the_group(self, fake_kafka):
        worker = _worker()
        order: List[str] = []

        async def execute(payload: Dict[str, Any]) -> bool:
            order.append(f"run {payload['job_id']}")
            return True

        worker.executor.execute_job = execute  # type: ignore[method-assign]

        consumer = await _run(worker, [_msg(_job("j1")), _msg(_job("j2"))])

        assert order == ["run j1", "run j2"]
        assert consumer.events == ["start", "poll", "commit", "poll", "commit", "poll", "stop"]
        worker.executor.close.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_payload_is_passed_through_unchanged(self, fake_kafka):
        worker = _worker()
        job = _job("j1", source={"type": "StaticSource", "config": {"records": [1]}})
        await _run(worker, [_msg(job)])
        worker.executor.execute_job.assert_awaited_once_with(job)  # type: ignore[attr-defined]

    async def test_failed_job_is_committed_and_logged(self, fake_kafka):
        worker = _worker()
        worker.executor.execute_job = AsyncMock(return_value=False)  # type: ignore[method-assign]

        with capture_logs(consumer_mod.logger.name) as logs:
            consumer = await _run(worker, [_msg(_job("bad"))])

        assert consumer.events.count("commit") == 1
        assert "Job bad failed; recorded to DB" in logs

    @pytest.mark.parametrize("version", [None, 1, 3, "2"])
    async def test_wrong_schema_version_is_rejected_and_committed(self, fake_kafka, version):
        worker = _worker()
        job = _job("old")
        if version is None:
            del job["schema_version"]
        else:
            job["schema_version"] = version

        with capture_logs(consumer_mod.logger.name) as logs:
            consumer = await _run(worker, [_msg(job), _msg(_job("ok"))])

        executed = [c.args[0]["job_id"] for c in worker.executor.execute_job.await_args_list]  # type: ignore[attr-defined]
        assert executed == ["ok"]
        assert consumer.events.count("commit") == 2  # the rejected one is not redelivered
        assert f"Unsupported job schema_version={version!r}; skipping" in logs

    async def test_malformed_json_is_committed_and_not_retried(self, fake_kafka):
        worker = _worker()

        with capture_logs(consumer_mod.logger.name) as logs:
            consumer = await _run(worker, [_msg(b"{not json"), _msg(_job("ok"))])

        assert consumer.events == ["start", "poll", "commit", "poll", "commit", "poll", "stop"]
        assert worker.executor.execute_job.await_count == 1  # type: ignore[attr-defined]
        assert any(m.startswith("Invalid job payload:") for m in logs)

    async def test_tombstone_is_committed(self, fake_kafka):
        worker = _worker()
        consumer = await _run(worker, [_msg(None)])
        assert consumer.events.count("commit") == 1
        worker.executor.execute_job.assert_not_awaited()  # type: ignore[attr-defined]

    async def test_non_utf8_payload_is_committed_like_malformed_json(self, fake_kafka):
        consumer = await _run(_worker(), [_msg(b"\xff\xfe\x00garbage")])
        assert consumer.events.count("commit") == 1

    async def test_non_object_json_is_committed_like_malformed_json(self, fake_kafka):
        consumer = await _run(_worker(), [_msg(b"[1, 2, 3]")])
        assert consumer.events.count("commit") == 1

    async def test_a_dropped_invalid_payload_is_counted(self, fake_kafka):
        counter = metrics.jobs_processed_total.labels(pipeline="unknown", status="invalid")
        before = counter._value.get()
        await _run(_worker(), [_msg(b"\xff\xfe"), _msg(b"[1]"), _msg(b"{not json")])
        assert counter._value.get() == before + 3

    async def test_executor_crash_is_not_committed_and_the_loop_continues(self, fake_kafka):
        worker = _worker()
        worker.executor.execute_job = AsyncMock(  # type: ignore[method-assign]
            side_effect=[RuntimeError("db exploded"), True]
        )

        with capture_logs(consumer_mod.logger.name) as logs:
            consumer = await _run(worker, [_msg(_job("j1")), _msg(_job("j2"))])

        assert consumer.events == ["start", "poll", "poll", "commit", "poll", "stop"]
        assert "Job processing error" in logs

    async def test_commit_failed_after_rebalance_is_logged_not_raised(self, fake_kafka):
        worker = _worker()
        original_init = FakeKafkaConsumer.__init__

        def init(self: FakeKafkaConsumer, *topics: str, **kwargs: Any) -> None:
            original_init(self, *topics, **kwargs)
            self.commit_error = CommitFailedError("rebalanced")

        FakeKafkaConsumer.__init__ = init  # type: ignore[method-assign]
        try:
            with capture_logs(consumer_mod.logger.name) as logs:
                consumer = await _run(worker, [_msg(_job("j1")), _msg(_job("j2"))])
        finally:
            FakeKafkaConsumer.__init__ = original_init  # type: ignore[method-assign]

        assert worker.executor.execute_job.await_count == 2  # type: ignore[attr-defined]
        assert consumer.events.count("commit") == 1  # only j2's commit succeeded
        assert "Offset commit failed: group rebalanced; message will be redelivered" in logs

    async def test_poll_error_still_leaves_the_group_and_closes_the_executor(self, fake_kafka):
        worker = _worker()
        original_init = FakeKafkaConsumer.__init__

        def init(self: FakeKafkaConsumer, *topics: str, **kwargs: Any) -> None:
            original_init(self, *topics, **kwargs)

            async def broken(**kw: Any) -> Dict:
                raise KafkaConnectionError("lost")

            self.getmany = broken  # type: ignore[method-assign]

        FakeKafkaConsumer.__init__ = init  # type: ignore[method-assign]
        try:
            with pytest.raises(KafkaConnectionError):
                await worker.start()
        finally:
            FakeKafkaConsumer.__init__ = original_init  # type: ignore[method-assign]

        assert FakeKafkaConsumer.instances[-1].events == ["start", "stop"]
        worker.executor.close.assert_awaited_once()  # type: ignore[attr-defined]


# ---------------------------------------------------------------- draining


class TestDrain:
    async def test_stop_mid_job_finishes_and_commits_before_leaving(self, fake_kafka):
        """SIGTERM while a job runs: the job completes, its offset is committed,
        the next queued message is not taken, then the consumer leaves the group."""
        worker = _worker()
        ran: List[str] = []

        async def execute(payload: Dict[str, Any]) -> bool:
            ran.append(payload["job_id"])
            worker.request_stop()  # the signal lands mid-job
            return True

        worker.executor.execute_job = execute  # type: ignore[method-assign]

        consumer = await _run(worker, [_msg(_job("j1")), _msg(_job("j2"))])

        assert ran == ["j1"]
        assert consumer.events == ["start", "poll", "commit", "stop"]
        assert len(consumer.queue) == 1  # j2 stays in Kafka for another worker
        worker.executor.close.assert_awaited_once()  # type: ignore[attr-defined]

    async def test_stop_coroutine_requests_a_drain(self):
        worker = _worker()
        worker._running = True
        await worker.stop()
        assert worker._running is False
