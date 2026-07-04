# Observability

Reflowfy emits three signals from all services (api, reflow-manager, worker):

| Signal  | Backend                        | UI          |
|---------|--------------------------------|-------------|
| Logs    | your Elasticsearch (optional)  | Kibana      |
| Metrics | Prometheus (`/metrics`)        | Grafana     |
| Traces  | Elastic APM (OpenTelemetry)    | Kibana APM  |

Everything is driven by env vars (see `.env.template`). By default logs go to
stdout only; metrics are always exposed; tracing is off until you set an OTLP
endpoint.

## Logs → your Elasticsearch (the easy part)

Point logs anywhere with one knob:

```bash
LOG_DESTINATION=elastic          # stdout | elastic | both
ELASTIC_LOG_URL=https://your-es:9200
ELASTIC_LOG_USERNAME=elastic           # username + password auth (blank = no auth)
ELASTIC_LOG_PASSWORD=your-password
ELASTIC_LOG_INDEX=reflowfy-logs        # daily index reflowfy-logs-YYYY.MM.DD
```

Logs are ECS-shaped JSON (`@timestamp`, `log.level`, `service.name`,
`service.environment`, `message`, plus `execution_id`/`job_id`/`pipeline_name`/
`trace.id` when in a job). The
handler batches with `helpers.bulk` on a background thread — it never blocks job
processing. If Elasticsearch is slow or down, records are dropped oldest-first
and counted in `reflowfy_logs_dropped_total` (watch this — non-zero means log
loss, not job loss).

## Filtering by environment (production vs local)

Every log line carries `service.environment`, set from the `ENVIRONMENT` env var:
- **local** (`docker compose` / `reflowfy run`) defaults to `local`.
- **production** (Helm deploy) defaults to `production` (`observability.environment` in the chart's `values.yaml`).

In Kibana, filter `service.environment: production` (or `local`). Override per
deploy by setting `ENVIRONMENT` in `.env`, or `--set observability.environment=staging`
with Helm.

## Metrics → Prometheus / Grafana

`GET /metrics` on api (`:8000`) and reflow-manager (`:8001`); the worker runs a
standalone exporter on `METRICS_PORT` (`:9100`). `docker compose up` starts
Prometheus (`:9090`) and Grafana (`:3000`, anonymous admin) with the
**Reflowfy Overview** dashboard auto-provisioned.

## Traces → Elastic APM

Set an endpoint to turn tracing on:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://apm-server:8200
OTEL_TRACES_SAMPLER_ARG=0.1      # 10% head sampling
```

FastAPI is auto-instrumented; the manager opens a span per run and injects a W3C
`traceparent` into the Kafka job `metadata`, which the worker extracts to
continue the trace across the hop. `docker compose` includes an `apm-server`
service wired to the existing Elasticsearch.

## High volume (≈500k jobs/hr ≈ 139 jobs/sec)

Two hard rules — breaking either is what actually falls over at scale:

1. **Never log per-record.** Log per-job or per-batch. Per-record INFO at this
   rate is millions of docs/sec.
2. **Never put ids in metric labels.** `pipeline`/`status`/`error_type`/`mode`
   only. `job_id`/`execution_id` in a Prometheus label is unbounded cardinality
   and will OOM Prometheus. Ids belong in logs and traces.

Tuning knobs: `ELASTIC_LOG_FLUSH_DOCS` / `ELASTIC_LOG_FLUSH_SECONDS` /
`ELASTIC_LOG_QUEUE_MAX` (log shipper), `OTEL_TRACES_SAMPLER_ARG` (trace volume).
The real ceiling at this rate is your Elasticsearch cluster's ingest capacity,
not Reflowfy — 1–2k log docs/sec is modest for a cluster, painful for a single
dev node.
