"""Worker entrypoint (reflowfy.worker.main.main): env parsing, discovery, signal
handling and the active-worker gauge. Every side effect is faked; nothing listens."""

import logging
import signal
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional

import pytest

import reflowfy.worker.main as main_mod
from reflowfy.observability import metrics

# Built into URLs at runtime: a credentialed URL literal reads as a leaked secret
# to secret scanners (SonarCloud).
_DB_PW = "pw"
_SECRET_DB_PW = "not-logged-pw"

_ENV = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "KAFKA_GROUP_ID",
    "DATABASE_URL",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "METRICS_PORT",
    "REFLOWFY_BUILD_VERSION",
    "GIT_SHA",
)


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@contextmanager
def capture_logs(name: str) -> Iterator[List[str]]:
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


class FakeConsumer:
    instances: List["FakeConsumer"] = []
    start_error: Optional[BaseException] = None

    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self._running = True
        self.stop_requests = 0
        self.gauge_while_running: Optional[float] = None
        FakeConsumer.instances.append(self)

    def request_stop(self) -> None:
        self.stop_requests += 1
        self._running = False

    async def start(self) -> None:
        self.gauge_while_running = metrics.active_workers._value.get()
        if FakeConsumer.start_error is not None:
            raise FakeConsumer.start_error


@pytest.fixture
def harness(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)
    FakeConsumer.instances = []
    FakeConsumer.start_error = None
    calls: List[str] = []
    handlers: Dict[int, Callable[..., Any]] = {}
    ports: List[int] = []

    monkeypatch.setattr(main_mod, "setup_logging", lambda **kw: calls.append("logging"))
    monkeypatch.setattr(main_mod, "init_tracing", lambda **kw: calls.append("tracing"))
    monkeypatch.setattr(main_mod, "start_http_server", lambda port: ports.append(port))
    monkeypatch.setattr(main_mod, "discover_and_load_pipelines", lambda: calls.append("discover"))

    class RecordingConsumer(FakeConsumer):
        def __init__(self, **kwargs: Any):
            calls.append("consumer")
            super().__init__(**kwargs)

    monkeypatch.setattr(main_mod, "KafkaJobConsumer", RecordingConsumer)
    # setdefault: keep main()'s handler. asyncio.run() also installs (then restores) a
    # SIGINT handler when it sees the default one, which this fake cannot hide from it.
    monkeypatch.setattr(main_mod.signal, "signal", lambda sig, fn: handlers.setdefault(sig, fn))

    class H:
        pass

    h = H()
    h.calls, h.handlers, h.ports = calls, handlers, ports  # type: ignore[attr-defined]
    return h


def _consumer() -> FakeConsumer:
    assert len(FakeConsumer.instances) == 1
    return FakeConsumer.instances[0]


def test_defaults(harness):
    main_mod.main()

    assert _consumer().kwargs == {
        "bootstrap_servers": "localhost:9092",
        "topic": "reflow.jobs",
        "group_id": "reflowfy-workers",
        "database_url": "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy",
        "security_protocol": None,
        "sasl_mechanism": None,
        "sasl_username": None,
        "sasl_password": None,
    }
    assert harness.ports == [9100]


def test_env_overrides(harness, monkeypatch):
    env = {
        "KAFKA_BOOTSTRAP_SERVERS": "b1:9092,b2:9092",
        "KAFKA_TOPIC": "jobs.v2",
        "KAFKA_GROUP_ID": "grp",
        "DATABASE_URL": f"postgresql://u:{_DB_PW}@db:5432/x",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "KAFKA_SASL_USERNAME": "svc",
        "KAFKA_SASL_PASSWORD": "secret",
        "METRICS_PORT": "9200",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    main_mod.main()

    assert _consumer().kwargs == {
        "bootstrap_servers": "b1:9092,b2:9092",
        "topic": "jobs.v2",
        "group_id": "grp",
        "database_url": f"postgresql://u:{_DB_PW}@db:5432/x",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_username": "svc",
        "sasl_password": "secret",
    }
    assert harness.ports == [9200]


def test_observability_and_discovery_run_before_the_consumer(harness):
    main_mod.main()
    assert harness.calls == ["logging", "tracing", "discover", "consumer"]


def test_startup_log_names_the_db_host_but_never_credentials(harness, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", f"postgresql://user:{_SECRET_DB_PW}@db.internal:5432/r")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "kafka-secret")

    with capture_logs(main_mod.logger.name) as logs:
        main_mod.main()

    text = "\n".join(logs)
    assert "database=db.internal:5432/r" in text
    assert _SECRET_DB_PW not in text and "kafka-secret" not in text
    assert "SASL enabled: SASL_PLAINTEXT / SCRAM-SHA-256" in logs
    assert "Worker ready, waiting for jobs" in logs


def test_no_sasl_log_without_username(harness):
    with capture_logs(main_mod.logger.name) as logs:
        main_mod.main()
    assert not any(m.startswith("SASL enabled") for m in logs)


def test_sigterm_and_sigint_are_wired_to_a_drain(harness):
    main_mod.main()
    consumer = _consumer()
    consumer._running = True  # as while consuming

    assert set(harness.handlers) == {signal.SIGTERM, signal.SIGINT}
    assert harness.handlers[signal.SIGTERM] is harness.handlers[signal.SIGINT]

    with capture_logs(main_mod.logger.name) as logs:
        harness.handlers[signal.SIGTERM](signal.SIGTERM, None)

    assert consumer.stop_requests == 1
    assert "Shutdown signal received; finishing the current job, then stopping" in logs


def test_second_signal_exits_immediately(harness):
    main_mod.main()
    consumer = _consumer()
    consumer._running = True
    handler = harness.handlers[signal.SIGTERM]
    handler(signal.SIGTERM, None)  # first: drain

    with capture_logs(main_mod.logger.name) as logs:
        with pytest.raises(SystemExit) as excinfo:
            handler(signal.SIGINT, None)

    assert excinfo.value.code == 1
    assert consumer.stop_requests == 1  # not asked again
    assert "Second shutdown signal, exiting now" in logs


def test_active_worker_gauge_is_one_while_consuming_and_zero_after(harness):
    main_mod.main()
    assert _consumer().gauge_while_running == 1
    assert metrics.active_workers._value.get() == 0


def test_consumer_crash_exits_nonzero_and_resets_the_gauge(harness):
    FakeConsumer.start_error = RuntimeError("cannot reach kafka")

    with capture_logs(main_mod.logger.name) as logs:
        with pytest.raises(SystemExit) as excinfo:
            main_mod.main()

    assert excinfo.value.code == 1
    assert "Worker crashed" in logs
    assert metrics.active_workers._value.get() == 0


def test_keyboard_interrupt_is_a_clean_stop(harness):
    FakeConsumer.start_error = KeyboardInterrupt()

    with capture_logs(main_mod.logger.name) as logs:
        main_mod.main()  # no SystemExit

    assert "Worker stopped by user" in logs
    assert metrics.active_workers._value.get() == 0


def test_build_version_is_logged(harness, monkeypatch):
    monkeypatch.setenv("GIT_SHA", "abc123")
    with capture_logs(main_mod.logger.name) as logs:
        main_mod.main()
    assert any("build abc123" in m for m in logs)
