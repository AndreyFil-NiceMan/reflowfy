# Observability for Reflowfy — Design

Date: 2026-07-04
Status: Approved, ready for implementation plan

## Goal

Add **logs, metrics, and traces** to Reflowfy's three services (api, reflow_manager,
worker) so operators can see pipeline health and debug a run end-to-end across the
Kafka hop. Users must be able to send logs to *their own* Elastic with minimal config.
Deliverable includes ready-made dashboards.

Target scale: **~500k jobs/hour (~139 jobs/sec sustained, bursty higher).** The design
must hold at that volume without the observability layer becoming the bottleneck.

## Backend decisions (chosen by user)

| Signal  | Backend                     | UI       |
|---------|-----------------------------|----------|
| Logs    | User's Elasticsearch (direct handler, config-driven) | Kibana   |
| Metrics | Prometheus                  | Grafana  |
| Traces  | Elastic APM via OpenTelemetry (OTLP) | Kibana APM |

Rationale: reuse infra/deps already present (`structlog`, `prometheus-client`,
`elasticsearch` in deps; Elastic already runs in compose). Prometheus/Grafana is the
pragmatic metrics default. Traces go to Elastic APM so logs↔traces correlate in one UI
with the least new infra (only APM Server, no Jaeger).

## Current state (what exists, what's dead)

- `observability/logging.py` — a JSON formatter + `setup_logging()` that is **never
  called**. All modules use `logging.getLogger(__name__)` with Python defaults.
- `observability/metrics.py` — Prometheus counters/histograms **defined but never
  incremented**; no `/metrics` endpoint.
- Tracing: **none.**
- Config is **env-var / 12-factor** (scattered `os.getenv`, `.env.template`); there is
  no central settings class. New knobs are env vars documented in `.env.template`.
- `api` and `reflow_manager` are FastAPI apps (have lifespans). The **worker is a plain
  consumer loop** (`worker/main.py`), no HTTP server.
- Job message is built in `pipeline_runner.py` (`schema_version` = 2) with a `metadata`
  dict the worker reads via `.get()` (`executor.py`) — the trace-propagation hook.

Step one of every phase is *wiring the dead scaffolding*, not adding parallel systems.

## Guiding principles

- Env-var driven config; new knobs land in `.env.template`.
- Reuse existing deps; add only OpenTelemetry packages.
- Fix the unused `setup_logging()` / `metrics.py` rather than write new modules.
- Two discipline rules that make or break the scale target:
  - **No per-record logging.** Log per-job/per-batch only. Per-record INFO at 139
    jobs/sec is millions of docs/sec = death.
  - **No high-cardinality metric labels.** Never put `job_id`/`execution_id` in a
    Prometheus label — unbounded cardinality OOMs Prometheus. IDs go in logs/traces.

## Signal 1 — Logging → user's Elastic (direct, config-driven)

- Rewrite `observability/logging.py` to emit **ECS-shaped JSON** (`@timestamp`,
  `log.level`, `message`, `service.name`, plus `execution_id`, `job_id`,
  `pipeline_name`, `trace.id`) via `structlog`. Always emit JSON to stdout (free, works
  when Elastic is off/unreachable).
- New `ElasticLogHandler` (`logging.Handler`):
  - Buffers records on a **bounded queue**; a background thread ships them.
  - **Bulk writes** via `elasticsearch.helpers.bulk`, flushing at **N docs OR T seconds,
    whichever first** (default ~2000 docs / 1s). Single-doc indexing is forbidden — at
    ~1–2k docs/sec it falls over; a 2k bulk handles it in ~1 req/sec.
  - **Bounded + drop-oldest** when the queue is full, exporting a `logs_dropped_total`
    counter. A slow/hiccuping Elastic must never block the pipeline. This drop policy is
    load-bearing at scale, not a nicety. Mark with a `# ponytail:` comment naming the
    upgrade path (durable disk buffer) if drops ever matter.
  - Optional INFO sampling knob for high load (keep 100% WARN/ERROR): add only if volume
    proves it necessary (`# ponytail:`), not upfront.
- Config in `.env.template`: `LOG_TO_ELASTIC`, `ELASTIC_LOG_URL`, `ELASTIC_LOG_INDEX`
  (default `reflowfy-logs`, daily data stream), `ELASTIC_LOG_API_KEY`, `LOG_LEVEL`.
  This is the "easy for the user" surface: set URL + key, done.
- **Wire `setup_logging()` into all three startups** (`api/app.py`,
  `reflow_manager/app.py` lifespans; `worker/main.py`). Bind
  `execution_id`/`job_id`/`pipeline_name` via `structlog` contextvars so every line in a
  job carries them.

## Signal 2 — Metrics → Prometheus + Grafana

- Increment the **already-defined** counters/histograms at real call sites:
  - `executor.py`: `jobs_processed_total`, `jobs_failed_total`,
    `job_processing_duration_seconds`, `records_processed_total`.
  - `pipeline_runner.py`: `pipeline_executions_total`.
  - worker: `active_workers` gauge.
  - Add: `dlq_depth` gauge and rate-limiter token gauge(s).
  - Keep labels low-cardinality (`pipeline`, `status`, `error_type`, `mode` only).
- Expose `/metrics`:
  - `api` + `reflow_manager`: mount `prometheus_client` ASGI app on the FastAPI app.
  - worker: `prometheus_client.start_http_server(port)` in `worker/main.py`.
- Add **Prometheus + Grafana** to `docker-compose.yml` (dev) with a scrape config
  targeting the three services. Document the Helm equivalent (scrape annotations /
  ServiceMonitor for the KEDA-autoscaled workers). No app scaling concern — Prometheus
  aggregates in memory; scrape payload is independent of job volume.

## Signal 3 — Traces → Elastic APM (OpenTelemetry)

- Add deps: `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp`,
  `opentelemetry-instrumentation-fastapi`, `opentelemetry-instrumentation-logging`.
- `init_tracing()` at startup: OTLP exporter → `OTEL_EXPORTER_OTLP_ENDPOINT` (Elastic
  APM Server). Auto-instrument FastAPI on `api` + `reflow_manager`.
- **Head sampling** via `OTEL_TRACES_SAMPLER=traceidratio`,
  `OTEL_TRACES_SAMPLER_ARG` default `0.1` (10%). 100% at 139 jobs/sec × ~4 spans ≈ 550
  spans/sec is expensive; 10% keeps representative latency/error data. Documented knob.
- Manual spans at the ~4 hop boundaries only (not deep per-function): pipeline-run,
  source-split, job-dispatch, worker job-processing.
- **Cross-Kafka propagation:** inject W3C `traceparent` into the job message `metadata`
  at `pipeline_runner.py`; extract in the worker to continue the trace across the Kafka
  hop. Additive field, **no `schema_version` bump** (worker reads `metadata` via `.get()`).
- Stamp `trace.id`/`span.id` onto log records (via
  `opentelemetry-instrumentation-logging`) so Kibana correlates logs↔traces.
- Add APM Server container to compose.

## Dashboards (the deliverable)

- **Grafana (metrics)** — checked-in provisioned JSON, auto-loaded:
  throughput (jobs & records/sec per pipeline), failure rate + `error_type` breakdown,
  duration p50/p95/p99, active workers, DLQ depth, executions by mode, rate-limiter.
- **Kibana (logs + APM)** — exported saved-objects NDJSON, importable:
  log volume by level/service, error stream, per-execution drilldown, APM service map +
  trace waterfall + latency.

## Scale summary (500k jobs/hr ≈ 139 jobs/sec)

- **Logs (the risk):** ~700–1,400 docs/sec at 5–10 lines/job. Holds *iff* bulk batching
  + bounded-drop + no per-record logging. Real ceiling is the user's Elastic cluster
  ingest capacity, not app code.
- **Metrics:** free. Prometheus aggregates; volume-independent. Only rule: low label
  cardinality.
- **Traces:** ~550 spans/sec at 100%. Holds with head sampling (~10% default).

## Phasing (each phase ships independently)

0. Config plumbing + wire `setup_logging` into all three startups (revives dead code).
1. Elastic log handler (bulk + bounded-drop + `logs_dropped_total`) + ECS structlog +
   context binding. Discipline rule: no per-record logs.
2. Metrics wiring + `/metrics` endpoints + Prometheus/Grafana compose + Grafana
   dashboards. Discipline rule: low-cardinality labels.
3. OTel traces + APM + Kafka `traceparent` propagation + log/trace correlation + sampler
   knob.
4. Dashboards polish (Kibana NDJSON, Grafana JSON) + docs (`.env.template`, a
   high-volume/sizing note) + one E2E self-check.

## Testing

- Unit: handler buffer/flush/drop behavior + `logs_dropped_total`; metric increments at
  call sites; `traceparent` inject→extract roundtrip across the message boundary.
- E2E self-check (phase 4): after a run — `/metrics` shows non-zero counts, logs land in
  the e2e Elastic index, trace context survives the Kafka hop.

## Deliberate simplifications (known ceilings)

- ES log handler uses in-memory bounded queue + drop-oldest, not a durable disk buffer.
  Upgrade path: local disk spool if drops become unacceptable.
- Traces get manual spans at ~4 boundaries only, not deep instrumentation.
- INFO sampling under load is a deferred knob, added only if volume proves it needed.
