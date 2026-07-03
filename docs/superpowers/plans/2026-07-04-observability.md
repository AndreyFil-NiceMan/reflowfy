# Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire logs (→ user's Elastic), metrics (→ Prometheus/Grafana), and traces (→ Elastic APM) into Reflowfy's three services, holding at ~500k jobs/hr, with ready-made dashboards.

**Architecture:** Fix the existing dead scaffolding (`observability/logging.py`, `observability/metrics.py`) rather than add parallel systems. Logs stream to the user's Elastic via a bounded, bulk-batching `logging.Handler` configured by env vars. Prometheus counters (already defined) get incremented at real call sites and exposed on `/metrics`. OpenTelemetry emits spans to Elastic APM, propagating a W3C `traceparent` through the Kafka job `metadata`. Discipline rules — no per-record logs, no high-cardinality metric labels — keep it stable at volume.

**Tech Stack:** Python 3.11, `structlog`, `elasticsearch` (8.x), `prometheus-client`, `opentelemetry-{api,sdk,exporter-otlp,instrumentation-fastapi,instrumentation-logging}`, FastAPI, Kafka, Prometheus, Grafana, Kibana/Elastic APM. Tooling: `uv run pytest`, `uv run ruff/black/mypy/pyright`.

**Spec:** `docs/superpowers/specs/2026-07-04-observability-design.md`

---

## File Structure

**Create:**
- `reflowfy/observability/elastic_handler.py` — `ElasticLogHandler` (bounded queue + background bulk shipper) and `logs_dropped_total` counter.
- `reflowfy/observability/context.py` — `bind_log_context()` / `clear_log_context()` helpers over `structlog` contextvars.
- `reflowfy/observability/tracing.py` — `init_tracing()`, `inject_trace_context()`, `extract_and_attach()`.
- `reflowfy/observability/prometheus.py` — `mount_metrics(app)` (FastAPI) and `start_metrics_server(port)` (worker).
- `tests/unit/test_elastic_handler.py`, `tests/unit/test_metrics_wiring.py`, `tests/unit/test_trace_propagation.py`.
- `deploy/observability/prometheus.yml`, `deploy/observability/grafana/` (provisioning + dashboards), `deploy/observability/kibana-saved-objects.ndjson`.

**Modify:**
- `reflowfy/observability/logging.py` — ECS structlog rendering + attach `ElasticLogHandler`.
- `reflowfy/observability/metrics.py` — add `dlq_depth`, `rate_limiter_tokens` gauges.
- `reflowfy/api/app.py`, `reflowfy/reflow_manager/app.py` — call `setup_logging`, `init_tracing`, `mount_metrics` in lifespan.
- `reflowfy/worker/main.py` — call `setup_logging`, `init_tracing`, `start_metrics_server`.
- `reflowfy/worker/executor.py` — bind log context, increment job/record metrics, extract trace, span around `execute_job`.
- `reflowfy/reflow_manager/pipeline_runner.py` — increment execution metric, inject `traceparent` into `metadata`, span around run.
- `reflowfy/templates/.env.template`, `docker-compose.yml`, `pyproject.toml`.

---

## Phase 0 — Config plumbing + revive `setup_logging`

### Task 0.1: Add observability env vars to `.env.template`

**Files:**
- Modify: `reflowfy/templates/.env.template`

- [ ] **Step 1: Append an observability block**

Add at the end of `reflowfy/templates/.env.template`:

```bash
# =============================================================================
# Observability
# =============================================================================
LOG_LEVEL=INFO                         # DEBUG|INFO|WARNING|ERROR
LOG_JSON=true                          # Emit ECS JSON to stdout (false = human text)

# Logs → your Elasticsearch (leave LOG_TO_ELASTIC=false to disable)
LOG_TO_ELASTIC=false
ELASTIC_LOG_URL=http://elasticsearch:9200
ELASTIC_LOG_INDEX=reflowfy-logs        # Daily data stream / index prefix
ELASTIC_LOG_API_KEY=                   # Base64 API key; blank = no auth
ELASTIC_LOG_FLUSH_DOCS=2000            # Bulk flush trigger (docs)
ELASTIC_LOG_FLUSH_SECONDS=1            # Bulk flush trigger (seconds)
ELASTIC_LOG_QUEUE_MAX=50000            # Bounded queue; oldest dropped when full

# Metrics (Prometheus scrape)
METRICS_PORT=9100                      # Worker /metrics port (api/manager mount on their app)

# Traces → Elastic APM (OTLP). Blank endpoint disables tracing.
OTEL_EXPORTER_OTLP_ENDPOINT=           # e.g. http://apm-server:8200
OTEL_SERVICE_NAME=reflowfy
OTEL_TRACES_SAMPLER=traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1            # 10% head sampling (raise for low volume)
```

- [ ] **Step 2: Commit**

```bash
git add reflowfy/templates/.env.template
git commit -m "feat(obs): add observability env vars to template"
```

### Task 0.2: ECS structlog rendering + wire `setup_logging`

**Files:**
- Modify: `reflowfy/observability/logging.py`
- Test: `tests/unit/test_elastic_handler.py` (shared file; first test here)

- [ ] **Step 1: Write the failing test** — `tests/unit/test_elastic_handler.py`

```python
import json
import logging
from reflowfy.observability.logging import setup_logging, ECSJSONFormatter


def test_ecs_formatter_emits_expected_fields():
    rec = logging.LogRecord(
        name="reflowfy.worker", level=logging.INFO, pathname=__file__,
        lineno=1, msg="hello", args=(), exc_info=None,
    )
    rec.execution_id = "exec-1"
    rec.pipeline_name = "p1"
    out = json.loads(ECSJSONFormatter(service_name="worker").format(rec))
    assert out["message"] == "hello"
    assert out["log.level"] == "info"
    assert out["service.name"] == "worker"
    assert out["execution_id"] == "exec-1"
    assert out["pipeline_name"] == "p1"
    assert "@timestamp" in out


def test_setup_logging_is_idempotent():
    a = setup_logging(service_name="worker")
    b = setup_logging(service_name="worker")
    # No duplicate handlers on repeated calls.
    assert len(a.handlers) == len(b.handlers)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_elastic_handler.py -v`
Expected: FAIL — `ImportError: cannot import name 'ECSJSONFormatter'`.

- [ ] **Step 3: Rewrite `reflowfy/observability/logging.py`**

```python
"""Structured (ECS) logging setup for reflowfy services."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Extra LogRecord attributes we promote to top-level ECS-ish fields.
_CONTEXT_FIELDS = ("execution_id", "job_id", "pipeline_name", "batch_id", "trace.id", "span.id")


class ECSJSONFormatter(logging.Formatter):
    """Render a LogRecord as a single ECS-shaped JSON line."""

    def __init__(self, service_name: str = "reflowfy") -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        data: Dict[str, Any] = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "log.level": record.levelname.lower(),
            "logger": record.name,
            "service.name": self.service_name,
            "message": record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            val = getattr(record, field, None)
            if val is not None:
                data[field] = val
        if record.exc_info:
            data["error.stack_trace"] = self.formatException(record.exc_info)
        return json.dumps(data)


def setup_logging(service_name: str = "reflowfy") -> logging.Logger:
    """Configure the root 'reflowfy' logger. Idempotent per service.

    Attaches a stdout handler (ECS JSON or plain text per LOG_JSON) and,
    when LOG_TO_ELASTIC=true, an ElasticLogHandler shipping to the user's ES.
    """
    logger = logging.getLogger("reflowfy")
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logger.setLevel(level)

    # Idempotent: drop our previously-installed handlers before re-adding.
    for h in list(logger.handlers):
        if getattr(h, "_reflowfy_managed", False):
            logger.removeHandler(h)

    json_logs = os.getenv("LOG_JSON", "true").lower() == "true"
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(
        ECSJSONFormatter(service_name) if json_logs
        else logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    stdout._reflowfy_managed = True  # type: ignore[attr-defined]
    logger.addHandler(stdout)

    if os.getenv("LOG_TO_ELASTIC", "false").lower() == "true":
        from reflowfy.observability.elastic_handler import ElasticLogHandler

        es_handler = ElasticLogHandler(service_name=service_name)
        es_handler.setFormatter(ECSJSONFormatter(service_name))
        es_handler._reflowfy_managed = True  # type: ignore[attr-defined]
        logger.addHandler(es_handler)

    logger.propagate = False
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a child of the configured 'reflowfy' logger."""
    return logging.getLogger(f"reflowfy.{name}" if name else "reflowfy")
```

- [ ] **Step 4: Run test — Elastic import is lazy so this passes without the handler yet**

Run: `uv run pytest tests/unit/test_elastic_handler.py::test_ecs_formatter_emits_expected_fields tests/unit/test_elastic_handler.py::test_setup_logging_is_idempotent -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add reflowfy/observability/logging.py tests/unit/test_elastic_handler.py
git commit -m "feat(obs): ECS JSON logging + idempotent setup_logging"
```

---

## Phase 1 — Elastic log handler + context binding

### Task 1.1: `ElasticLogHandler` (bounded queue + bulk shipper + drop counter)

**Files:**
- Create: `reflowfy/observability/elastic_handler.py`
- Test: `tests/unit/test_elastic_handler.py` (append)

- [ ] **Step 1: Write the failing test** (append to `tests/unit/test_elastic_handler.py`)

```python
import time
import logging as _logging
from reflowfy.observability.elastic_handler import ElasticLogHandler
from reflowfy.observability import metrics as m


def _record(msg):
    return _logging.LogRecord("reflowfy", _logging.INFO, __file__, 1, msg, (), None)


def test_bulk_flush_ships_batched_docs(monkeypatch):
    shipped = []

    def fake_bulk(client, actions, **kw):
        acts = list(actions)
        shipped.extend(acts)
        return (len(acts), [])

    monkeypatch.setattr("reflowfy.observability.elastic_handler.helpers.bulk", fake_bulk)
    h = ElasticLogHandler(service_name="worker", client=object(),
                          flush_docs=3, flush_seconds=0.2, queue_max=100)
    h.setFormatter(_logging.Formatter("%(message)s"))
    for i in range(3):
        h.emit(_record(f"m{i}"))
    time.sleep(0.5)
    h.close()
    assert len(shipped) == 3


def test_queue_full_drops_oldest_and_counts(monkeypatch):
    monkeypatch.setattr("reflowfy.observability.elastic_handler.helpers.bulk",
                        lambda *a, **k: (0, []))
    before = m.logs_dropped_total._value.get()
    h = ElasticLogHandler(service_name="worker", client=object(),
                          flush_docs=10_000, flush_seconds=10, queue_max=2)
    h._paused = True  # don't drain, force the queue to fill
    for i in range(5):
        h.emit(_record(f"m{i}"))
    dropped = m.logs_dropped_total._value.get() - before
    h.close()
    assert dropped >= 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_elastic_handler.py -k "bulk or drops" -v`
Expected: FAIL — `ModuleNotFoundError: reflowfy.observability.elastic_handler`.

- [ ] **Step 3: Add the drop counter to `metrics.py`**

Append to `reflowfy/observability/metrics.py`:

```python
# Observability self-metrics
logs_dropped_total = Counter(
    "reflowfy_logs_dropped_total",
    "Log records dropped because the Elastic ship queue was full",
)

dlq_depth = Gauge(
    "reflowfy_dlq_depth",
    "Number of jobs currently in the dead-letter queue",
)

rate_limiter_tokens = Gauge(
    "reflowfy_rate_limiter_tokens",
    "Available rate-limiter tokens",
    ["pipeline"],
)
```

- [ ] **Step 4: Create `reflowfy/observability/elastic_handler.py`**

```python
"""A logging.Handler that bulk-ships records to Elasticsearch off the hot path.

Design constraints (500k jobs/hr): never block the pipeline. Records go onto a
bounded in-memory queue; a daemon thread bulk-indexes them. When the queue is
full we drop the OLDEST record and increment a counter — a slow/hiccuping ES
must never stall job processing.
# ponytail: in-memory drop-oldest; upgrade path = local disk spool if drops hurt.
"""

import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any, List, Optional

from elasticsearch import Elasticsearch, helpers

from reflowfy.observability import metrics


class ElasticLogHandler:  # not subclassing logging.Handler to keep emit() lock-free
    """Bounded, batching, non-blocking Elasticsearch log shipper."""

    def __init__(
        self,
        service_name: str = "reflowfy",
        client: Optional[Any] = None,
        index: Optional[str] = None,
        flush_docs: Optional[int] = None,
        flush_seconds: Optional[float] = None,
        queue_max: Optional[int] = None,
    ) -> None:
        self.service_name = service_name
        self.index = index or os.getenv("ELASTIC_LOG_INDEX", "reflowfy-logs")
        self.flush_docs = flush_docs or int(os.getenv("ELASTIC_LOG_FLUSH_DOCS", "2000"))
        self.flush_seconds = flush_seconds or float(os.getenv("ELASTIC_LOG_FLUSH_SECONDS", "1"))
        qmax = queue_max or int(os.getenv("ELASTIC_LOG_QUEUE_MAX", "50000"))
        self.level = 0
        self.formatter: Any = None
        self._q: "queue.Queue[str]" = queue.Queue(maxsize=qmax)
        self._client = client if client is not None else self._build_client()
        self._stop = threading.Event()
        self._paused = False  # test hook
        self._reflowfy_managed = False
        self._thread = threading.Thread(target=self._run, name="es-log-shipper", daemon=True)
        self._thread.start()

    # --- logging.Handler protocol (duck-typed) ---
    def setFormatter(self, fmt: Any) -> None:
        self.formatter = fmt

    def setLevel(self, level: int) -> None:
        self.level = level

    def handle(self, record: Any) -> None:
        self.emit(record)

    def emit(self, record: Any) -> None:
        try:
            line = self.formatter.format(record) if self.formatter else record.getMessage()
        except Exception:
            return
        try:
            self._q.put_nowait(line)
        except queue.Full:
            # Drop oldest to make room; count the loss.
            try:
                self._q.get_nowait()
                metrics.logs_dropped_total.inc()
                self._q.put_nowait(line)
            except queue.Empty:
                metrics.logs_dropped_total.inc()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    # --- internals ---
    def _build_client(self) -> Optional[Elasticsearch]:
        url = os.getenv("ELASTIC_LOG_URL")
        if not url:
            return None
        api_key = os.getenv("ELASTIC_LOG_API_KEY") or None
        return Elasticsearch(url, api_key=api_key) if api_key else Elasticsearch(url)

    def _run(self) -> None:
        batch: List[str] = []
        last = time.monotonic()
        while not self._stop.is_set():
            if self._paused:
                time.sleep(0.05)
                continue
            timeout = max(0.0, self.flush_seconds - (time.monotonic() - last))
            try:
                batch.append(self._q.get(timeout=timeout or 0.01))
            except queue.Empty:
                pass
            due = len(batch) >= self.flush_docs or (time.monotonic() - last) >= self.flush_seconds
            if batch and due:
                self._flush(batch)
                batch = []
                last = time.monotonic()
        if batch:
            self._flush(batch)

    def _flush(self, lines: List[str]) -> None:
        if self._client is None:
            return
        index = f"{self.index}-{datetime.now(timezone.utc):%Y.%m.%d}"
        actions = ({"_index": index, "_source": line} for line in lines)
        try:
            helpers.bulk(self._client, actions, raise_on_error=False)
        except Exception:
            # Never let ES failures crash the shipper thread.
            metrics.logs_dropped_total.inc(len(lines))
```

Note: `_source` is a JSON string; set `helpers.bulk(..., )` to send pre-formatted docs. If ES rejects string bodies in your cluster, wrap as `{"_index": index, "_op_type": "index", "_source": json.loads(line)}` — the formatter already emits JSON.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/unit/test_elastic_handler.py -v`
Expected: PASS (all 4).

- [ ] **Step 6: Commit**

```bash
git add reflowfy/observability/elastic_handler.py reflowfy/observability/metrics.py tests/unit/test_elastic_handler.py
git commit -m "feat(obs): bounded bulk Elastic log handler with drop counter"
```

### Task 1.2: Log context binding helpers

**Files:**
- Create: `reflowfy/observability/context.py`
- Test: `tests/unit/test_elastic_handler.py` (append)

- [ ] **Step 1: Write the failing test** (append)

```python
from reflowfy.observability.context import log_context


def test_log_context_injects_and_clears():
    import logging as lg
    records = []

    class Cap(lg.Handler):
        def emit(self, r):
            records.append(r)

    log = lg.getLogger("reflowfy.ctxtest")
    log.addHandler(Cap())
    log.setLevel(lg.INFO)
    with log_context(execution_id="e9", job_id="j9"):
        log.info("inside")
    log.info("outside")
    assert getattr(records[0], "execution_id") == "e9"
    assert getattr(records[0], "job_id") == "j9"
    assert getattr(records[1], "execution_id", None) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_elastic_handler.py::test_log_context_injects_and_clears -v`
Expected: FAIL — `ModuleNotFoundError: reflowfy.observability.context`.

- [ ] **Step 3: Create `reflowfy/observability/context.py`**

```python
"""Bind per-job context onto every log record via a logging.Filter + contextvars."""

import contextvars
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator

_ctx: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar("reflowfy_log_ctx", default={})


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, val in _ctx.get().items():
            setattr(record, key, val)
        return True


def install_context_filter() -> None:
    """Attach the context filter to the 'reflowfy' logger. Idempotent."""
    logger = logging.getLogger("reflowfy")
    if not any(isinstance(f, _ContextFilter) for f in logger.filters):
        logger.addFilter(_ContextFilter())


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind fields (execution_id, job_id, pipeline_name, ...) for the enclosed scope."""
    install_context_filter()
    token = _ctx.set({**_ctx.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _ctx.reset(token)
```

- [ ] **Step 4: Run test**

Run: `uv run pytest tests/unit/test_elastic_handler.py::test_log_context_injects_and_clears -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/observability/context.py tests/unit/test_elastic_handler.py
git commit -m "feat(obs): per-job log context binding"
```

### Task 1.3: Wire `setup_logging` + context into worker `execute_job`

**Files:**
- Modify: `reflowfy/worker/main.py:39-42` (after discovery)
- Modify: `reflowfy/worker/executor.py:105-124`

- [ ] **Step 1: Call `setup_logging` in worker startup**

In `reflowfy/worker/main.py`, add after the imports at top:

```python
from reflowfy.observability.logging import setup_logging
```

and inside `main()`, right after `discover_and_load_pipelines(pipeline_module)` (line ~41):

```python
    setup_logging(service_name="worker")
```

- [ ] **Step 2: Bind context around `execute_job`**

In `reflowfy/worker/executor.py`, add import near the top:

```python
from reflowfy.observability.context import log_context
from reflowfy.observability.logging import get_logger

logger = get_logger("worker.executor")
```

Wrap the body of `execute_job` (from line 124 `try:` onward) so the whole job runs inside the context. Change the method so that after reading `execution_id`/`job_id`/`_pipeline_name` (lines 120-122) you enter:

```python
        with log_context(execution_id=execution_id, job_id=job_id, pipeline_name=_pipeline_name):
            return await self._execute_job_inner(job_payload, stats, execution_id, job_id, _pipeline_name)
```

Move the existing `try/except` body into a new `async def _execute_job_inner(self, job_payload, stats, execution_id, job_id, _pipeline_name) -> bool:`. (Mechanical extraction — no logic change.)

- [ ] **Step 3: Verify unit tests still pass + app imports**

Run: `uv run pytest tests/unit/ -q && uv run python -c "import reflowfy.worker.executor, reflowfy.worker.main"`
Expected: PASS, no import errors.

- [ ] **Step 4: Commit**

```bash
git add reflowfy/worker/main.py reflowfy/worker/executor.py
git commit -m "feat(obs): wire logging + job context into worker"
```

### Task 1.4: Wire `setup_logging` into api + reflow_manager lifespans

**Files:**
- Modify: `reflowfy/reflow_manager/app.py:47-52`
- Modify: `reflowfy/api/app.py` (lifespan, ~line 27)

- [ ] **Step 1: reflow_manager** — add import and call in `_startup`

Add near other imports in `reflowfy/reflow_manager/app.py`:

```python
from reflowfy.observability.logging import setup_logging  # noqa: E402
```

Find `_startup()` (called from `lifespan`) and add as its first line:

```python
    setup_logging(service_name="reflow-manager")
```

- [ ] **Step 2: api** — same in `reflowfy/api/app.py` lifespan

Add `from reflowfy.observability.logging import setup_logging` and call `setup_logging(service_name="api")` as the first line inside the lifespan's startup section.

- [ ] **Step 3: Verify imports**

Run: `uv run python -c "import reflowfy.reflow_manager.app, reflowfy.api.app"`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add reflowfy/reflow_manager/app.py reflowfy/api/app.py
git commit -m "feat(obs): wire setup_logging into api + reflow_manager"
```

---

## Phase 2 — Metrics wiring + `/metrics` + Prometheus/Grafana

### Task 2.1: Prometheus exposure helpers

**Files:**
- Create: `reflowfy/observability/prometheus.py`
- Test: `tests/unit/test_metrics_wiring.py`

- [ ] **Step 1: Write the failing test**

```python
from fastapi import FastAPI
from fastapi.testclient import TestClient
from reflowfy.observability.prometheus import mount_metrics
from reflowfy.observability import metrics


def test_metrics_endpoint_exposes_counter():
    app = FastAPI()
    mount_metrics(app)
    metrics.jobs_processed_total.labels(pipeline="p1", status="completed").inc()
    body = TestClient(app).get("/metrics").text
    assert "reflowfy_jobs_processed_total" in body
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_metrics_wiring.py -v`
Expected: FAIL — `ModuleNotFoundError: reflowfy.observability.prometheus`.

- [ ] **Step 3: Create `reflowfy/observability/prometheus.py`**

```python
"""Prometheus exposition helpers for FastAPI services and the worker."""

import os
from typing import Any

from prometheus_client import make_asgi_app, start_http_server


def mount_metrics(app: Any, path: str = "/metrics") -> None:
    """Mount the Prometheus ASGI app on a FastAPI/Starlette app."""
    app.mount(path, make_asgi_app())


def start_metrics_server(port: int | None = None) -> None:
    """Start a standalone /metrics HTTP server (for the worker loop)."""
    start_http_server(port or int(os.getenv("METRICS_PORT", "9100")))
```

- [ ] **Step 4: Run test**

Run: `uv run pytest tests/unit/test_metrics_wiring.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/observability/prometheus.py tests/unit/test_metrics_wiring.py
git commit -m "feat(obs): prometheus /metrics exposition helpers"
```

### Task 2.2: Increment metrics at worker call sites

**Files:**
- Modify: `reflowfy/worker/executor.py` (success path ~200-210, failure path ~212-219, records ~145/160)
- Test: `tests/unit/test_metrics_wiring.py` (append)

- [ ] **Step 1: Write the failing test** (append)

```python
import asyncio
from reflowfy.observability import metrics as m


def test_execute_job_increments_processed(monkeypatch):
    from reflowfy.worker.executor import WorkerExecutor

    ex = WorkerExecutor.__new__(WorkerExecutor)  # skip DB engine

    async def fake_inner(*a, **k):
        return True

    monkeypatch.setattr(ex, "_execute_job_inner", fake_inner, raising=False)
    monkeypatch.setattr(WorkerExecutor, "record_job_metrics", WorkerExecutor.record_job_metrics, raising=False)
    before = m.jobs_processed_total.labels(pipeline="p1", status="completed")._value.get()
    ex.record_job_metrics("p1", success=True, deduplicated=False, error_type=None,
                          duration=0.5, records=3)
    after = m.jobs_processed_total.labels(pipeline="p1", status="completed")._value.get()
    assert after == before + 1
    assert m.records_processed_total.labels(pipeline="p1")._value.get() >= 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_metrics_wiring.py::test_execute_job_increments_processed -v`
Expected: FAIL — `AttributeError: ... has no attribute 'record_job_metrics'`.

- [ ] **Step 3: Add a `record_job_metrics` helper + call it**

In `reflowfy/worker/executor.py` add imports:

```python
from reflowfy.observability import metrics
```

Add method to `WorkerExecutor`:

```python
    def record_job_metrics(
        self,
        pipeline: str,
        success: bool,
        deduplicated: bool,
        error_type: Optional[str],
        duration: float,
        records: int,
    ) -> None:
        status = "deduplicated" if deduplicated else ("completed" if success else "failed")
        metrics.jobs_processed_total.labels(pipeline=pipeline, status=status).inc()
        metrics.job_processing_duration_seconds.labels(pipeline=pipeline).observe(duration)
        if records:
            metrics.records_processed_total.labels(pipeline=pipeline).inc(records)
        if not success and error_type:
            metrics.jobs_failed_total.labels(pipeline=pipeline, error_type=error_type).inc()
```

Call it at each terminal return in `_execute_job_inner`:
- success (line ~208, before `return True`): `self.record_job_metrics(_pipeline_name, True, stats.deduplicated, None, stats.end_time - stats.start_time, stats.records_output)`
- dedup early-return (~178) and empty-records early-return (~152): same with `deduplicated`/`records=0` as appropriate.
- health-check fail (~190) and exception path (after setting `stats.error`): `self.record_job_metrics(_pipeline_name, False, False, type(e).__name__ if 'e' in dir() else "error", time.time() - stats.start_time, 0)`. Use the exception class name for `error_type` (low cardinality — class names, never messages).

**Rule (comment in code):** `# ponytail: error_type = exception class name only — never the message (cardinality).`

- [ ] **Step 4: Run test**

Run: `uv run pytest tests/unit/test_metrics_wiring.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/worker/executor.py tests/unit/test_metrics_wiring.py
git commit -m "feat(obs): increment job/record metrics in worker"
```

### Task 2.3: Increment execution metric + expose worker/api/manager `/metrics`

**Files:**
- Modify: `reflowfy/reflow_manager/pipeline_runner.py` (execution start)
- Modify: `reflowfy/worker/main.py`, `reflowfy/reflow_manager/app.py`, `reflowfy/api/app.py`

- [ ] **Step 1: Increment `pipeline_executions_total` in `pipeline_runner.py`**

Add `from reflowfy.observability import metrics` and, at the start of the run method (where an execution begins), determine mode from `os.getenv("EXECUTION_MODE", "local")` and:

```python
        metrics.pipeline_executions_total.labels(pipeline=pipeline_name, mode=mode).inc()
```

- [ ] **Step 2: Start worker metrics server**

In `reflowfy/worker/main.py`, add `from reflowfy.observability.prometheus import start_metrics_server` and call `start_metrics_server()` right after `setup_logging(...)`.

- [ ] **Step 3: Mount `/metrics` on both FastAPI apps**

In `reflowfy/reflow_manager/app.py` and `reflowfy/api/app.py`, after the `app = FastAPI(...)` construction add:

```python
from reflowfy.observability.prometheus import mount_metrics  # noqa: E402
mount_metrics(app)
```

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/unit/ -q && uv run python -c "import reflowfy.reflow_manager.app, reflowfy.api.app, reflowfy.worker.main"`
Expected: PASS, no import errors.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/reflow_manager/pipeline_runner.py reflowfy/worker/main.py reflowfy/reflow_manager/app.py reflowfy/api/app.py
git commit -m "feat(obs): expose /metrics on all services + execution metric"
```

### Task 2.4: Prometheus + Grafana in docker-compose

**Files:**
- Create: `deploy/observability/prometheus.yml`, `deploy/observability/grafana/provisioning/datasources/prometheus.yml`, `deploy/observability/grafana/provisioning/dashboards/dashboards.yml`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Prometheus scrape config** — `deploy/observability/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: reflowfy-api
    static_configs: [{ targets: ["reflofy-api:8000"] }]
  - job_name: reflowfy-manager
    static_configs: [{ targets: ["reflofy-reflow-manager:8001"] }]
  - job_name: reflowfy-worker
    static_configs: [{ targets: ["reflofy-worker:9100"] }]
```

(Adjust service hostnames/ports to match the container names in `docker-compose.yml`.)

- [ ] **Step 2: Grafana datasource provisioning** — `deploy/observability/grafana/provisioning/datasources/prometheus.yml`

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 3: Grafana dashboard provider** — `deploy/observability/grafana/provisioning/dashboards/dashboards.yml`

```yaml
apiVersion: 1
providers:
  - name: reflowfy
    folder: Reflowfy
    type: file
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 4: Add services to `docker-compose.yml`**

```yaml
  prometheus:
    image: prom/prometheus:v2.54.1
    volumes:
      - ./deploy/observability/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9090:9090"]
    networks: [reflofy-elastic-network]

  grafana:
    image: grafana/grafana:11.2.0
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./deploy/observability/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/observability/grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["3000:3000"]
    depends_on: [prometheus]
    networks: [reflofy-elastic-network]
```

- [ ] **Step 5: Smoke test the stack**

Run: `docker compose up -d prometheus grafana && sleep 5 && curl -s localhost:9090/-/ready`
Expected: `Prometheus Server is Ready.` Then `docker compose down`.

- [ ] **Step 6: Commit**

```bash
git add deploy/observability docker-compose.yml
git commit -m "feat(obs): prometheus + grafana in docker-compose"
```

---

## Phase 3 — Traces (OpenTelemetry → Elastic APM)

### Task 3.1: Add OTel deps + tracing module

**Files:**
- Modify: `pyproject.toml`
- Create: `reflowfy/observability/tracing.py`
- Test: `tests/unit/test_trace_propagation.py`

- [ ] **Step 1: Add deps to `pyproject.toml`**

Add to `dependencies`:

```
    "opentelemetry-api>=1.27.0",
    "opentelemetry-sdk>=1.27.0",
    "opentelemetry-exporter-otlp>=1.27.0",
    "opentelemetry-instrumentation-fastapi>=0.48b0",
    "opentelemetry-instrumentation-logging>=0.48b0",
```

Run: `uv sync`

- [ ] **Step 2: Write the failing test** — `tests/unit/test_trace_propagation.py`

```python
from reflowfy.observability.tracing import inject_trace_context, extract_and_attach


def test_traceparent_roundtrips_through_metadata():
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider

    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer("test")
    meta = {}
    with tracer.start_as_current_span("dispatch"):
        inject_trace_context(meta)
    assert "traceparent" in meta
    ctx = extract_and_attach(meta)
    assert ctx is not None  # a context was extracted
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_trace_propagation.py -v`
Expected: FAIL — `ModuleNotFoundError: reflowfy.observability.tracing`.

- [ ] **Step 4: Create `reflowfy/observability/tracing.py`**

```python
"""OpenTelemetry tracing: init, and W3C context propagation across Kafka."""

import os
from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.propagate import extract, inject

_initialized = False


def init_tracing(service_name: str = "reflowfy") -> None:
    """Configure the global tracer to export OTLP to Elastic APM.

    No-op when OTEL_EXPORTER_OTLP_ENDPOINT is blank (tracing disabled).
    """
    global _initialized
    if _initialized or not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    provider = TracerProvider(
        resource=Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", service_name)})
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    try:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor

        LoggingInstrumentor().instrument(set_logging_format=False)
    except Exception:
        pass
    _initialized = True


def instrument_fastapi(app: Any) -> None:
    """Auto-instrument a FastAPI app (no-op if tracing disabled or lib missing)."""
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except Exception:
        pass


def inject_trace_context(carrier: Dict[str, Any]) -> None:
    """Inject the current span's W3C traceparent into a dict carrier (job metadata)."""
    inject(carrier)


def extract_and_attach(carrier: Dict[str, Any]) -> Optional[Any]:
    """Extract a remote context from a carrier. Returns the extracted context."""
    return extract(carrier or {})


def get_tracer(name: str = "reflowfy") -> Any:
    return trace.get_tracer(name)
```

- [ ] **Step 5: Run test**

Run: `uv run pytest tests/unit/test_trace_propagation.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock reflowfy/observability/tracing.py tests/unit/test_trace_propagation.py
git commit -m "feat(obs): OpenTelemetry tracing module + deps"
```

### Task 3.2: Inject traceparent at dispatch, extract in worker

**Files:**
- Modify: `reflowfy/reflow_manager/pipeline_runner.py:66-83` (`build_job_payload`)
- Modify: `reflowfy/worker/executor.py:105-124` (`execute_job`)
- Test: `tests/unit/test_trace_propagation.py` (append)

- [ ] **Step 1: Write the failing test** (append)

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_trace_propagation.py::test_build_job_payload_carries_traceparent -v`
Expected: FAIL — no `traceparent` in metadata.

- [ ] **Step 3: Inject in `build_job_payload`**

In `reflowfy/reflow_manager/pipeline_runner.py`, add `from reflowfy.observability.tracing import inject_trace_context` and modify `build_job_payload` to copy metadata and inject:

```python
    enriched_metadata = dict(metadata)
    inject_trace_context(enriched_metadata)
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "execution_id": execution_id,
        "job_id": job_id,
        "pipeline_name": pipeline_name,
        "source": SourceFactory.serialize(sub_source),
        "dedup_check": dedup_check,
        "metadata": enriched_metadata,
    }
```

- [ ] **Step 4: Extract + span in worker**

In `reflowfy/worker/executor.py` `execute_job`, wrap the context-bound body in a span using the extracted parent. Replace the `with log_context(...)` block from Task 1.3 with:

```python
        from reflowfy.observability.tracing import extract_and_attach, get_tracer

        parent = extract_and_attach(job_payload.get("metadata", {}))
        with log_context(execution_id=execution_id, job_id=job_id, pipeline_name=_pipeline_name):
            with get_tracer("worker").start_as_current_span("process_job", context=parent):
                return await self._execute_job_inner(
                    job_payload, stats, execution_id, job_id, _pipeline_name
                )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/unit/test_trace_propagation.py tests/unit/test_metrics_wiring.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add reflowfy/reflow_manager/pipeline_runner.py reflowfy/worker/executor.py tests/unit/test_trace_propagation.py
git commit -m "feat(obs): propagate trace context across the Kafka hop"
```

### Task 3.3: Init tracing + instrument FastAPI at startup; APM Server in compose

**Files:**
- Modify: `reflowfy/worker/main.py`, `reflowfy/reflow_manager/app.py`, `reflowfy/api/app.py`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Worker** — after `setup_logging(...)` in `main.py`:

```python
    from reflowfy.observability.tracing import init_tracing
    init_tracing(service_name="worker")
```

- [ ] **Step 2: FastAPI apps** — in each of `reflow_manager/app.py` and `api/app.py`, after `mount_metrics(app)`:

```python
from reflowfy.observability.tracing import init_tracing, instrument_fastapi  # noqa: E402
init_tracing(service_name="reflow-manager")   # or "api"
instrument_fastapi(app)
```

- [ ] **Step 3: Add APM Server to `docker-compose.yml`**

```yaml
  apm-server:
    image: docker.elastic.co/apm/apm-server:8.11.0
    depends_on: [reflofy-elasticsearch]
    command: >
      apm-server -e
      -E apm-server.host=0.0.0.0:8200
      -E apm-server.kibana.enabled=true
      -E output.elasticsearch.hosts=["reflofy-elasticsearch:9200"]
    ports: ["8200:8200"]
    networks: [reflofy-elastic-network]
```

Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://apm-server:8200` in the api/manager/worker service env blocks of the compose file to enable tracing locally.

- [ ] **Step 4: Verify imports + unit suite**

Run: `uv run pytest tests/unit/ -q && uv run python -c "import reflowfy.reflow_manager.app, reflowfy.api.app, reflowfy.worker.main"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/worker/main.py reflowfy/reflow_manager/app.py reflowfy/api/app.py docker-compose.yml
git commit -m "feat(obs): init tracing + FastAPI instrumentation + APM server"
```

---

## Phase 4 — Dashboards, DLQ gauge, docs, E2E self-check

### Task 4.1: DLQ depth + rate-limiter gauges

**Files:**
- Modify: DLQ scheduler (`reflowfy/reflow_manager/dlq_scheduler.py`) and rate limiter (`reflowfy/reflow_manager/rate_limiter.py`)

- [ ] **Step 1: Set `dlq_depth` on each DLQ poll**

In `dlq_scheduler.py`, where the scheduler already queries DLQ rows, add `from reflowfy.observability import metrics` and after counting: `metrics.dlq_depth.set(count)`.

- [ ] **Step 2: Set `rate_limiter_tokens` when tokens are computed**

In `rate_limiter.py`, where remaining tokens are known per pipeline: `metrics.rate_limiter_tokens.labels(pipeline=pipeline).set(remaining)`.

- [ ] **Step 3: Verify**

Run: `uv run pytest tests/unit/ -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add reflowfy/reflow_manager/dlq_scheduler.py reflowfy/reflow_manager/rate_limiter.py
git commit -m "feat(obs): dlq depth + rate-limiter gauges"
```

### Task 4.2: Grafana dashboard JSON

**Files:**
- Create: `deploy/observability/grafana/dashboards/reflowfy-overview.json`

- [ ] **Step 1: Build the dashboard**

Create `deploy/observability/grafana/dashboards/reflowfy-overview.json` with panels driven by these PromQL queries (one panel each; use Grafana's "New dashboard → add panel", then export JSON — paste the exported JSON here):

- Jobs/sec by status: `sum by (status) (rate(reflowfy_jobs_processed_total[1m]))`
- Records/sec by pipeline: `sum by (pipeline) (rate(reflowfy_records_processed_total[1m]))`
- Failure rate: `sum(rate(reflowfy_jobs_failed_total[5m])) / sum(rate(reflowfy_jobs_processed_total[5m]))`
- Failures by error_type: `sum by (error_type) (rate(reflowfy_jobs_failed_total[5m]))`
- Duration p50/p95/p99: `histogram_quantile(0.95, sum by (le,pipeline) (rate(reflowfy_job_processing_duration_seconds_bucket[5m])))` (repeat for 0.5, 0.99)
- Active workers: `reflowfy_active_workers`
- DLQ depth: `reflowfy_dlq_depth`
- Executions by mode: `sum by (mode) (rate(reflowfy_pipeline_executions_total[5m]))`
- Logs dropped/sec: `rate(reflowfy_logs_dropped_total[1m])` (alert if > 0 — logs are being lost)

The dashboard's `title` must be `Reflowfy Overview` and `uid` `reflowfy-overview`; provisioning auto-loads any JSON in the dashboards dir.

- [ ] **Step 2: Verify it loads**

Run: `docker compose up -d prometheus grafana && sleep 8 && curl -s localhost:3000/api/dashboards/uid/reflowfy-overview | head -c 200`
Expected: JSON containing `"title":"Reflowfy Overview"`. Then `docker compose down`.

- [ ] **Step 3: Commit**

```bash
git add deploy/observability/grafana/dashboards/reflowfy-overview.json
git commit -m "feat(obs): grafana overview dashboard"
```

### Task 4.3: Kibana saved objects (logs + APM)

**Files:**
- Create: `deploy/observability/kibana-saved-objects.ndjson`
- Create: `deploy/observability/README.md`

- [ ] **Step 1: Build in Kibana, then export**

With logs flowing to `reflowfy-logs-*`, in Kibana: create a data view `reflowfy-logs-*`, a log-volume-by-`log.level` histogram, an error stream (filter `log.level: error`), and a per-`execution_id` search. Save all, then **Stack Management → Saved Objects → Export** into `deploy/observability/kibana-saved-objects.ndjson`. APM views come built-in once APM Server ingests — no export needed.

- [ ] **Step 2: Write import instructions** — `deploy/observability/README.md`

Document: `curl -s -u user:pass -X POST "localhost:5601/api/saved_objects/_import?overwrite=true" -H "kbn-xsrf: true" --form file=@deploy/observability/kibana-saved-objects.ndjson`, plus Grafana at `:3000`, Prometheus `:9090`, APM in Kibana.

- [ ] **Step 3: Commit**

```bash
git add deploy/observability/kibana-saved-objects.ndjson deploy/observability/README.md
git commit -m "docs(obs): kibana saved objects + observability README"
```

### Task 4.4: Docs — high-volume/sizing note + CLAUDE.md pointer

**Files:**
- Create: `docs/observability.md`
- Modify: `CLAUDE.md` (add a short Observability subsection)

- [ ] **Step 1: Write `docs/observability.md`**

Cover: the three signals + where they land; the "easy Elastic logs" quickstart (set `LOG_TO_ELASTIC`, `ELASTIC_LOG_URL`, `ELASTIC_LOG_API_KEY`); and a **High volume (500k jobs/hr)** section stating the two hard rules — (1) never log per-record, log per-job/batch; (2) never put ids in metric labels — plus the tuning knobs (`ELASTIC_LOG_FLUSH_DOCS/SECONDS/QUEUE_MAX`, `OTEL_TRACES_SAMPLER_ARG`) and the note that the real ceiling is the user's ES ingest capacity. Watch `reflowfy_logs_dropped_total` for loss.

- [ ] **Step 2: Add pointer to `CLAUDE.md`** under Architecture:

```markdown
### Observability
`reflowfy/observability/` wires logs (→ user's Elastic via `ElasticLogHandler`), metrics (Prometheus `/metrics` on all services), traces (OTel → Elastic APM, `traceparent` propagated in job `metadata`). Config is env-var driven (`.env.template`). See `docs/observability.md`. Hard rules: no per-record logs, no ids in metric labels.
```

- [ ] **Step 3: Commit**

```bash
git add docs/observability.md CLAUDE.md
git commit -m "docs(obs): observability guide + sizing rules"
```

### Task 4.5: E2E self-check

**Files:**
- Create: `tests/e2e/test_observability.py`

- [ ] **Step 1: Write the E2E test**

```python
"""E2E: after a pipeline run, all three signals are observable."""
import os
import httpx
import pytest


MANAGER_URL = os.getenv("E2E_MANAGER_URL", "http://localhost:8003")


async def test_metrics_endpoint_reports_after_run():
    # Assumes the e2e harness has already run a pipeline (see conftest fixtures).
    async with httpx.AsyncClient() as c:
        body = (await c.get(f"{MANAGER_URL}/metrics")).text
    assert "reflowfy_pipeline_executions_total" in body
    assert "reflowfy_jobs_processed_total" in body


async def test_logs_land_in_elastic(e2e_elasticsearch):
    # e2e_elasticsearch: existing fixture giving an ES client to the e2e cluster.
    e2e_elasticsearch.indices.refresh(index="reflowfy-logs-*", ignore_unavailable=True)
    res = e2e_elasticsearch.search(index="reflowfy-logs-*", size=1, ignore_unavailable=True)
    assert res["hits"]["total"]["value"] > 0
```

Note: the E2E compose must set `LOG_TO_ELASTIC=true`, `ELASTIC_LOG_URL` to the e2e ES, and expose the manager `/metrics`. Reuse the existing `e2e_elasticsearch` fixture if present; otherwise add one mirroring the elastic source tests.

- [ ] **Step 2: Run the observability E2E suite**

Run: `./scripts/run_e2e_tests.sh --test-file tests/e2e/test_observability.py`
Expected: PASS (both tests). If `reflowfy-logs-*` is empty, confirm `LOG_TO_ELASTIC=true` in the generated compose and that a run happened.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_observability.py
git commit -m "test(obs): e2e self-check for metrics + elastic logs"
```

---

## Self-Review

- **Spec coverage:** Logs→Elastic (T0.2, T1.1–1.4), easy user config (T0.1), metrics→Prom/Grafana (T2.1–2.4, T4.2), traces→APM + Kafka propagation (T3.1–3.3), dashboards (T4.2 Grafana, T4.3 Kibana), 500k-scale (bulk+bounded-drop T1.1, sampling T3.1, discipline rules T2.2/T4.4), testing (unit T1.1/2.2/3.2, E2E T4.5). All spec sections mapped.
- **Type/name consistency:** `record_job_metrics`, `log_context`, `inject_trace_context`/`extract_and_attach`, `mount_metrics`/`start_metrics_server`, `setup_logging(service_name=...)`, `init_tracing`/`instrument_fastapi` used consistently across tasks. Metric names match `observability/metrics.py` plus added `logs_dropped_total`/`dlq_depth`/`rate_limiter_tokens`.
- **Known ceilings (from spec):** in-memory drop-oldest (not disk spool), spans at ~4 boundaries only, INFO sampling deferred — all carried as `# ponytail:` comments.

## Notes for the implementer

- The worker uses `print()` heavily; this plan intentionally does **not** convert every print — it routes lifecycle context via `log_context` and adds structured logs only where they earn their keep. Converting prints to `logger` calls is optional cleanup, not required.
- Container hostnames/ports in `prometheus.yml` and the APM `OTEL_EXPORTER_OTLP_ENDPOINT` must match whatever `docker-compose.yml` actually names the services — verify against the compose file before the smoke tests.
- `helpers.bulk` doc bodies: if your ES rejects string `_source`, switch to `json.loads(line)` as noted in Task 1.1 Step 4.
