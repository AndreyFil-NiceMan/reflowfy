"""ElasticLogHandler and setup_logging paths the main observability suite misses.

The rule under test throughout: shipping logs must never raise into the
application or stall it — every failure is counted in logs_dropped_total and
reported on stderr at most once a minute.
"""

import logging
import queue
import time
from typing import Any, Iterator, List

import pytest

from reflowfy.observability import metrics
from reflowfy.observability import elastic_handler as eh
from reflowfy.observability.elastic_handler import ElasticLogHandler
from reflowfy.observability.logging import ECSJSONFormatter, setup_logging


def _dropped() -> float:
    return metrics.logs_dropped_total._value.get()


def _record(msg: str = "m") -> logging.LogRecord:
    return logging.LogRecord("reflowfy.t", logging.INFO, __file__, 1, msg, None, None)


@pytest.fixture
def handlers() -> Iterator[List[ElasticLogHandler]]:
    """Handlers made in a test, closed (their shipper thread stopped) afterwards."""
    made: List[ElasticLogHandler] = []
    yield made
    for h in made:
        h.close()


@pytest.fixture
def no_es_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("ELASTIC_LOG_URL", "ELASTIC_LOG_CA_CERTS", "ELASTIC_LOG_VERIFY_CERTS"):
        monkeypatch.delenv(var, raising=False)


def _handler(made: List[ElasticLogHandler], **kw: Any) -> ElasticLogHandler:
    kw.setdefault("flush_seconds", 0.05)  # short, so close() joins the thread quickly
    kw.setdefault("flush_docs", 10_000)
    h = ElasticLogHandler(service_name="t", **kw)
    made.append(h)
    return h


class _BrokenFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        raise ValueError("unserialisable extra")


def test_a_record_that_cannot_be_formatted_is_skipped(handlers: List[ElasticLogHandler]) -> None:
    h = _handler(handlers, client=object())
    h.setFormatter(_BrokenFormatter())

    h.emit(_record())  # must not raise into the caller

    assert h._q.qsize() == 0


class _RacedQueue:
    """Full on put, yet already drained by the shipper when we try to make room."""

    def put_nowait(self, item: str) -> None:
        raise queue.Full

    def get_nowait(self) -> str:
        raise queue.Empty


def test_a_full_queue_drained_concurrently_still_counts_the_loss(
    handlers: List[ElasticLogHandler],
) -> None:
    h = _handler(handlers, client=object())
    h.setFormatter(ECSJSONFormatter("t"))
    real_q = h._q
    h._q = _RacedQueue()  # type: ignore[assignment]
    before = _dropped()

    h.emit(_record())

    assert _dropped() - before == 1
    h._q = real_q


def test_without_a_url_there_is_no_client_and_logs_are_dropped_loudly(
    no_es_env: None, handlers: List[ElasticLogHandler], capsys: pytest.CaptureFixture[str]
) -> None:
    h = _handler(handlers)
    assert h._client is None
    before = _dropped()

    h._flush(['{"message": "a"}', '{"message": "b"}'])

    assert _dropped() - before == 2
    assert "ELASTIC_LOG_URL unset" in capsys.readouterr().err


def test_a_ca_bundle_is_passed_to_the_client(
    no_es_env: None, monkeypatch: pytest.MonkeyPatch, handlers: List[ElasticLogHandler]
) -> None:
    calls: dict = {}
    monkeypatch.setattr(eh, "Elasticsearch", lambda url, **kw: calls.update(kw) or object())
    monkeypatch.setenv("ELASTIC_LOG_URL", "https://es:9200")
    monkeypatch.setenv("ELASTIC_LOG_CA_CERTS", "/etc/ssl/ca.pem")

    _handler(handlers)

    assert calls["ca_certs"] == "/etc/ssl/ca.pem"


def test_a_transport_failure_drops_the_batch_without_killing_the_shipper(
    monkeypatch: pytest.MonkeyPatch,
    handlers: List[ElasticLogHandler],
    capsys: pytest.CaptureFixture[str],
) -> None:
    def down(*args: Any, **kwargs: Any) -> Any:
        list(args[1])  # consume the actions like the real helper
        raise ConnectionError("es unreachable")

    monkeypatch.setattr(eh.helpers, "bulk", down)
    h = _handler(handlers, client=object())
    before = _dropped()

    h._flush(['{"message": "a"}', '{"message": "b"}', '{"message": "c"}'])

    assert _dropped() - before == 3
    assert "log shipping failed: es unreachable" in capsys.readouterr().err
    assert h._thread.is_alive()


def test_shipping_problems_are_reported_at_most_once_a_minute(
    handlers: List[ElasticLogHandler], capsys: pytest.CaptureFixture[str]
) -> None:
    h = _handler(handlers, client=object())

    h._warn_once("first")
    h._warn_once("second")

    err = capsys.readouterr().err
    assert "first" in err and "second" not in err


def test_the_first_warning_is_printed_on_a_host_up_for_under_a_minute(
    handlers: List[ElasticLogHandler],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # time.monotonic() counts from boot: a fresh node or CI runner reads ~5s here.
    h = _handler(handlers, client=object())
    monkeypatch.setattr(eh.time, "monotonic", lambda: 5.0)

    h._warn_once("shipping failed")

    assert "shipping failed" in capsys.readouterr().err


def test_a_paused_shipper_does_not_drain_the_queue(
    monkeypatch: pytest.MonkeyPatch, handlers: List[ElasticLogHandler]
) -> None:
    shipped: List[Any] = []

    def bulk(client: Any, actions: Any, **kwargs: Any) -> Any:
        shipped.extend(actions)
        return len(shipped), []

    monkeypatch.setattr(eh.helpers, "bulk", bulk)
    h = _handler(handlers, client=object(), flush_seconds=0.02, flush_docs=1)
    h.setFormatter(ECSJSONFormatter("t"))
    h._paused = True
    time.sleep(0.1)  # let the loop see the pause

    h.emit(_record("held"))
    time.sleep(0.2)
    assert shipped == [] and h._q.qsize() == 1

    h._paused = False
    deadline = time.monotonic() + 3
    while not shipped and time.monotonic() < deadline:
        time.sleep(0.02)
    assert len(shipped) == 1


# ----- setup_logging ------------------------------------------------------------------


@pytest.fixture
def reflowfy_logger() -> Iterator[logging.Logger]:
    """setup_logging reconfigures the shared 'reflowfy' logger; put it back after."""
    logger = logging.getLogger("reflowfy")
    saved = (list(logger.handlers), logger.level, logger.propagate)
    yield logger
    for h in list(logger.handlers):
        if h not in saved[0]:
            logger.removeHandler(h)
            h.close()
    logger.handlers[:] = saved[0]
    logger.setLevel(saved[1])
    logger.propagate = saved[2]


def test_setup_logging_leaves_foreign_handlers_alone(
    monkeypatch: pytest.MonkeyPatch, reflowfy_logger: logging.Logger
) -> None:
    monkeypatch.setenv("LOG_DESTINATION", "stdout")
    foreign = logging.NullHandler()
    reflowfy_logger.addHandler(foreign)

    setup_logging(service_name="t")
    setup_logging(service_name="t")

    assert foreign in reflowfy_logger.handlers
    managed = [h for h in reflowfy_logger.handlers if getattr(h, "_reflowfy_managed", False)]
    assert len(managed) == 1


def test_elastic_destination_ships_only_to_elastic(
    monkeypatch: pytest.MonkeyPatch, reflowfy_logger: logging.Logger
) -> None:
    monkeypatch.setattr(eh, "Elasticsearch", lambda url, **kw: object())
    monkeypatch.setenv("LOG_DESTINATION", "elastic")
    monkeypatch.setenv("ELASTIC_LOG_URL", "http://es:9200")

    logger = setup_logging(service_name="worker")

    managed = [h for h in logger.handlers if getattr(h, "_reflowfy_managed", False)]
    assert [type(h) for h in managed] == [ElasticLogHandler]
    assert isinstance(managed[0].formatter, ECSJSONFormatter)
    assert logger.propagate is False
