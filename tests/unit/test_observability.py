"""Unit tests for the observability layer: logging, elastic handler, context, tracing."""

import json
import logging
import time

import pytest

from reflowfy.observability import metrics as m
from reflowfy.observability.context import log_context
from reflowfy.observability.elastic_handler import ElasticLogHandler
from reflowfy.observability.logging import ECSJSONFormatter, setup_logging


def _record(msg="hi"):
    return logging.LogRecord("reflowfy", logging.INFO, __file__, 1, msg, (), None)


# --- ECS formatter + setup_logging ---

def test_ecs_formatter_emits_expected_fields():
    rec = _record("hello")
    rec.execution_id = "exec-1"
    rec.pipeline_name = "p1"
    out = json.loads(ECSJSONFormatter(service_name="worker", environment="production").format(rec))
    assert out["message"] == "hello"
    assert out["log.level"] == "info"
    assert out["service.name"] == "worker"
    assert out["service.environment"] == "production"
    assert out["execution_id"] == "exec-1"
    assert out["pipeline_name"] == "p1"
    assert "@timestamp" in out


def test_setup_logging_is_idempotent(monkeypatch):
    monkeypatch.setenv("LOG_DESTINATION", "stdout")
    a = setup_logging(service_name="worker")
    n = len(a.handlers)
    b = setup_logging(service_name="worker")
    assert len(b.handlers) == n  # no duplicate handlers


def test_log_destination_stdout_has_no_elastic(monkeypatch):
    monkeypatch.setenv("LOG_DESTINATION", "stdout")
    logger = setup_logging(service_name="api")
    assert not any(isinstance(h, ElasticLogHandler) for h in logger.handlers)


def test_elastic_without_url_falls_back_to_stdout(monkeypatch):
    """LOG_DESTINATION=elastic but no URL must NOT black-hole logs."""
    monkeypatch.setenv("LOG_DESTINATION", "elastic")
    monkeypatch.delenv("ELASTIC_LOG_URL", raising=False)
    logger = setup_logging(service_name="api")
    assert not any(isinstance(h, ElasticLogHandler) for h in logger.handlers)
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


def test_unknown_destination_falls_back_to_stdout(monkeypatch):
    """A typo'd LOG_DESTINATION must still attach a handler, not drop all logs."""
    monkeypatch.setenv("LOG_DESTINATION", "elstic")  # typo
    logger = setup_logging(service_name="api")
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


# --- ElasticLogHandler ---

def test_bulk_flush_ships_batched_docs(monkeypatch):
    shipped = []

    def fake_bulk(client, actions, **kw):
        acts = list(actions)
        shipped.extend(acts)
        return (len(acts), [])

    monkeypatch.setattr(
        "reflowfy.observability.elastic_handler.helpers.bulk", fake_bulk
    )
    h = ElasticLogHandler(
        service_name="worker", client=object(),
        flush_docs=3, flush_seconds=0.2, queue_max=100,
    )
    h.setFormatter(ECSJSONFormatter("worker"))
    for i in range(3):
        h.emit(_record(f"m{i}"))
    time.sleep(0.6)
    h.close()
    assert len(shipped) == 3


def test_doc_level_rejections_are_counted(monkeypatch):
    """ES accepting the request but rejecting docs must NOT be silently lost."""
    def fake_bulk(client, actions, **kw):
        acts = list(actions)
        # simulate ES rejecting every doc (e.g. data-stream template conflict)
        return (0, [{"index": {"status": 400, "error": "boom"}} for _ in acts])

    monkeypatch.setattr(
        "reflowfy.observability.elastic_handler.helpers.bulk", fake_bulk
    )
    before = m.logs_dropped_total._value.get()
    h = ElasticLogHandler(
        service_name="worker", client=object(),
        flush_docs=2, flush_seconds=0.2, queue_max=100,
    )
    h.setFormatter(ECSJSONFormatter("worker"))
    for i in range(2):
        h.emit(_record(f"m{i}"))
    time.sleep(0.6)
    h.close()
    assert m.logs_dropped_total._value.get() - before >= 2


def test_build_client_selects_auth(monkeypatch):
    """username/password -> basic_auth; nothing set -> no auth."""
    calls = {}

    def fake_es(url, **kw):
        calls.clear()
        calls.update(kw)
        return object()

    monkeypatch.setattr("reflowfy.observability.elastic_handler.Elasticsearch", fake_es)
    monkeypatch.setenv("ELASTIC_LOG_URL", "http://es:9200")

    # username + password -> basic_auth
    monkeypatch.setenv("ELASTIC_LOG_USERNAME", "elastic")
    monkeypatch.setenv("ELASTIC_LOG_PASSWORD", "secret")
    h = ElasticLogHandler(service_name="t")
    assert calls.get("basic_auth") == ("elastic", "secret")
    h.close()

    # no credentials -> no auth kwargs
    monkeypatch.delenv("ELASTIC_LOG_USERNAME", raising=False)
    monkeypatch.delenv("ELASTIC_LOG_PASSWORD", raising=False)
    h = ElasticLogHandler(service_name="t")
    assert "basic_auth" not in calls
    h.close()


def test_tls_verification_off_by_default(monkeypatch):
    """TLS verification is disabled unless ELASTIC_LOG_VERIFY_CERTS=true."""
    calls = {}

    def fake_es(url, **kw):
        calls.clear()
        calls.update(kw)
        return object()

    monkeypatch.setattr("reflowfy.observability.elastic_handler.Elasticsearch", fake_es)
    monkeypatch.setenv("ELASTIC_LOG_URL", "https://es:9200")

    # default: verification off
    monkeypatch.delenv("ELASTIC_LOG_VERIFY_CERTS", raising=False)
    ElasticLogHandler(service_name="t").close()
    assert calls.get("verify_certs") is False

    # opt back in
    monkeypatch.setenv("ELASTIC_LOG_VERIFY_CERTS", "true")
    ElasticLogHandler(service_name="t").close()
    assert "verify_certs" not in calls


def test_queue_full_drops_oldest_and_counts(monkeypatch):
    monkeypatch.setattr(
        "reflowfy.observability.elastic_handler.helpers.bulk",
        lambda *a, **k: (0, []),
    )
    before = m.logs_dropped_total._value.get()
    h = ElasticLogHandler(
        service_name="worker", client=object(),
        flush_docs=10_000, flush_seconds=10, queue_max=2,
    )
    h._paused = True  # don't drain, force the queue to fill
    h.setFormatter(ECSJSONFormatter("worker"))
    for i in range(5):
        h.emit(_record(f"m{i}"))
    dropped = m.logs_dropped_total._value.get() - before
    h.close()
    assert dropped >= 3


# --- context binding ---

def test_log_context_injects_and_clears():
    records = []

    class Cap(logging.Handler):
        def emit(self, r):
            records.append(r)

    log = logging.getLogger("reflowfy.ctxtest")
    log.addHandler(Cap())
    log.setLevel(logging.INFO)
    with log_context(execution_id="e9", job_id="j9"):
        log.info("inside")
    log.info("outside")
    assert getattr(records[0], "execution_id") == "e9"
    assert getattr(records[0], "job_id") == "j9"
    assert getattr(records[1], "execution_id", None) is None


# --- trace propagation ---

def test_traceparent_roundtrips_through_metadata():
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    from reflowfy.observability.tracing import extract_and_attach, inject_trace_context

    trace.set_tracer_provider(TracerProvider())
    meta: dict = {}
    with trace.get_tracer("test").start_as_current_span("dispatch"):
        inject_trace_context(meta)
    assert "traceparent" in meta
    assert extract_and_attach(meta) is not None


def test_build_job_payload_carries_traceparent(monkeypatch):
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    from reflowfy.reflow_manager import pipeline_runner as pr

    trace.set_tracer_provider(TracerProvider())
    monkeypatch.setattr(pr.SourceFactory, "serialize", lambda s: {"type": "x", "config": {}})
    with trace.get_tracer("t").start_as_current_span("run"):
        payload = pr.build_job_payload("e", "j", "p", object(), {"foo": 1})
    assert "traceparent" in payload["metadata"]
    assert payload["metadata"]["foo"] == 1


# --- metrics wiring ---

def test_metrics_endpoint_exposes_counter():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from prometheus_client import make_asgi_app

    app = FastAPI()
    app.mount("/metrics", make_asgi_app())
    m.jobs_processed_total.labels(pipeline="p1", status="completed").inc()
    body = TestClient(app).get("/metrics").text
    assert "reflowfy_jobs_processed_total" in body


def test_record_job_metrics_increments():
    from reflowfy.worker.executor import WorkerExecutor

    ex = WorkerExecutor.__new__(WorkerExecutor)  # skip DB engine
    before = m.jobs_processed_total.labels(pipeline="p1", status="completed")._value.get()
    ex.record_job_metrics("p1", success=True, deduplicated=False, error_type=None,
                          duration=0.5, records=3)
    after = m.jobs_processed_total.labels(pipeline="p1", status="completed")._value.get()
    assert after == before + 1
    assert m.records_processed_total.labels(pipeline="p1")._value.get() >= 3


def test_record_job_metrics_skips_records_on_failure():
    """A failed job must not inflate records_processed_total."""
    from reflowfy.worker.executor import WorkerExecutor

    ex = WorkerExecutor.__new__(WorkerExecutor)
    before = m.records_processed_total.labels(pipeline="pf")._value.get()
    ex.record_job_metrics("pf", success=False, deduplicated=False,
                          error_type="ValueError", duration=0.1, records=7)
    after = m.records_processed_total.labels(pipeline="pf")._value.get()
    assert after == before  # no records counted for a failed job
