"""Unit tests for the observability layer: logging, elastic handler, context, tracing."""

import json
import logging
import time


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


def test_close_ships_lines_still_queued(monkeypatch):
    """A short-lived process (`reflowfy test`) closes the handler before the
    shipper thread has picked its lines up; none may be lost."""
    shipped = []

    def fake_bulk(client, actions, **kw):
        acts = list(actions)
        shipped.append(len(acts))
        return (len(acts), [])

    monkeypatch.setattr(
        "reflowfy.observability.elastic_handler.helpers.bulk", fake_bulk
    )
    h = ElasticLogHandler(
        service_name="cli", client=object(),
        flush_docs=2, flush_seconds=10, queue_max=100,
    )
    h.setFormatter(ECSJSONFormatter("cli"))
    for i in range(5):
        h.emit(_record(f"m{i}"))
    h.close()
    assert sum(shipped) == 5
    assert max(shipped) <= 2  # still in flush_docs-sized bulks


def test_close_is_registered_to_run_at_exit(monkeypatch):
    registered = []
    monkeypatch.setattr(
        "reflowfy.observability.elastic_handler.atexit.register", registered.append
    )
    h = ElasticLogHandler(service_name="cli", client=object())
    try:
        assert registered == [h.close]
    finally:
        h.close()


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


def test_http_url_omits_tls_kwargs(monkeypatch):
    """TLS options on an http:// URL would crash the ES client, so skip them."""
    calls = {}

    def fake_es(url, **kw):
        calls.clear()
        calls.update(kw)
        return object()

    monkeypatch.setattr("reflowfy.observability.elastic_handler.Elasticsearch", fake_es)
    monkeypatch.setenv("ELASTIC_LOG_URL", "http://es:9200")
    monkeypatch.delenv("ELASTIC_LOG_VERIFY_CERTS", raising=False)

    ElasticLogHandler(service_name="t").close()
    assert "verify_certs" not in calls
    assert "ssl_show_warn" not in calls


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


# --- hermetic fields, event names, context binding ---

def _fmt(rec, **kw):
    return json.loads(ECSJSONFormatter(**kw).format(rec))


def test_formatter_emits_every_extra_field_not_just_an_allowlist():
    rec = _record()
    rec.schedule_name = "nightly"  # was silently dropped before
    rec.jobs_total = 7
    out = _fmt(rec)
    assert out["schedule_name"] == "nightly"
    assert out["jobs_total"] == 7


def test_formatter_maps_event_and_uses_record_time():
    rec = _record()
    rec.event = "job_completed"
    rec.created = 0
    out = _fmt(rec)
    assert out["event.action"] == "job_completed"
    assert out["@timestamp"].startswith("1970-01-01")
    for key in ("service.version", "host.name", "process.pid", "log.origin.function"):
        assert key in out


def test_formatter_survives_unserializable_extra():
    rec = _record()
    rec.blob = object()
    assert "blob" in _fmt(rec)


def test_formatter_adds_error_fields():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        rec = logging.LogRecord("r", logging.ERROR, __file__, 1, "x", (), sys.exc_info())
    out = _fmt(rec)
    assert out["error.type"] == "ValueError"
    assert out["error.message"] == "boom"
    assert "error.stack_trace" in out


def test_text_formatter_shows_context():
    from reflowfy.observability.logging import TextFormatter

    rec = _record("hello")
    rec.execution_id = "e1"
    rec.event = "x"
    assert TextFormatter().format(rec).endswith("[execution_id=e1 event=x]")
    assert "[" not in TextFormatter().format(_record("plain"))


def test_bind_log_context_sync_async_and_kwargs():
    import asyncio

    from reflowfy.observability.context import bind_log_context, get_log_context

    @bind_log_context("execution_id", "pipeline_name", "missing")
    def sync(execution_id, pipeline_name=None):
        return get_log_context()

    @bind_log_context("execution_id")
    async def asy(execution_id):
        return get_log_context()

    assert sync("e1", pipeline_name="p") == {"execution_id": "e1", "pipeline_name": "p"}
    assert asyncio.run(asy(execution_id="e2")) == {"execution_id": "e2"}
    assert get_log_context() == {}


def test_entry_point_modules_log_under_the_reflowfy_logger():
    """`python -m reflowfy.x.app` makes __name__ "__main__"; a logger named after it
    is outside the 'reflowfy' logger, so its lines never got the JSON formatter or
    reached Elasticsearch."""
    import ast
    from pathlib import Path

    root = Path(__file__).parents[2] / "reflowfy"
    for rel in ("reflow_manager/app.py", "api/app.py", "worker/main.py"):
        for node in ast.parse((root / rel).read_text()).body:
            if (
                isinstance(node, ast.Assign)
                and getattr(node.targets[0], "id", None) == "logger"
                and isinstance(node.value, ast.Call)
            ):
                arg = node.value.args[0]
                assert isinstance(arg, ast.Constant) and arg.value.startswith("reflowfy."), rel
                break
        else:
            raise AssertionError(f"no module-level logger in {rel}")


# --- pod identity ---

_K8S_VARS = ("KUBERNETES_SERVICE_HOST", "POD_NAME", "POD_NAMESPACE", "NODE_NAME", "HOSTNAME")


def _clean_k8s(monkeypatch):
    for var in _K8S_VARS:
        monkeypatch.delenv(var, raising=False)


def test_pod_fields_come_from_the_downward_api(monkeypatch):
    _clean_k8s(monkeypatch)
    monkeypatch.setenv("POD_NAME", "worker-7d9f-abcde")
    monkeypatch.setenv("POD_NAMESPACE", "reflowfy-prod")
    monkeypatch.setenv("NODE_NAME", "node-3")
    out = _fmt(_record())
    assert out["kubernetes.pod.name"] == "worker-7d9f-abcde"
    assert out["kubernetes.namespace"] == "reflowfy-prod"
    assert out["kubernetes.node.name"] == "node-3"


def test_pod_name_falls_back_to_hostname_inside_kubernetes(monkeypatch):
    _clean_k8s(monkeypatch)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("HOSTNAME", "manager-5c8d-xyz12")
    out = _fmt(_record())
    assert out["kubernetes.pod.name"] == "manager-5c8d-xyz12"
    assert "kubernetes.namespace" not in out


def test_no_pod_fields_outside_kubernetes(monkeypatch):
    """A docker-compose container id must not pose as a pod name."""
    _clean_k8s(monkeypatch)
    monkeypatch.setenv("HOSTNAME", "3f2a9c1d4e5b")
    out = _fmt(_record())
    assert not [k for k in out if k.startswith("kubernetes.")]
    assert out["host.name"] == "3f2a9c1d4e5b"


def test_chart_passes_the_pod_identity_to_every_service():
    """One shared helper feeds the api, the manager and the workers."""
    from pathlib import Path

    templates = Path(__file__).parents[2] / "reflowfy/helm/reflowfy/templates"
    helper = (templates / "_helpers.tpl").read_text()
    start = helper.index('define "reflowfy.observability.env"')
    end = helper.find('{{- define "', start + 1)
    block = helper[start : end if end != -1 else len(helper)]
    for var, path in (
        ("POD_NAME", "metadata.name"),
        ("POD_NAMESPACE", "metadata.namespace"),
        ("NODE_NAME", "spec.nodeName"),
    ):
        assert f"name: {var}" in block and f"fieldPath: {path}" in block
    for service in ("api.yaml", "reflow-manager.yaml", "worker.yaml"):
        assert 'include "reflowfy.observability.env"' in (templates / service).read_text()
