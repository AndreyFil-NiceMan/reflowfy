# Reflowfy

**A horizontally scalable data movement and transformation framework**

Reflowfy enables you to build pipelines that fetch data from sources, apply custom transformations, and send results to destinations—all with millions+ record scalability.

## 🎯 Key Features

- **Modern DX**: Two decorators — `@transformation` (whole batch, or `per_record=True`) and `@destination(per_record=True)` — and plain functions for reusable sources and destinations.
- **Auto-Discovery**: Pipelines and components are automatically discovered and registered — no manual `__init__.py` tracking required.
- **Horizontally Scalable**: Process millions of records in parallel using Apache Kafka.
- **Kubernetes-Native**: KEDA autoscaling from 0 to N workers based on queue lag.
- **Order-Independent**: Maximum parallelism without coordination overhead.
- **Two Execution Modes**: Local testing and distributed production execution.
- **Reliable by default**: job leases and redelivery checks, opt-in automatic retries, per-record deduplication, and executions you can pause, resume, cancel and retry.
- **Testable**: `reflowfy test` and `reflowfy.testing` run a pipeline exactly as a worker would, with no Docker and no network.

## 🏗 Architecture

```
User Request
    ↓ HTTP
API (FastAPI) ────→ ReflowManager Service (port 8001)
    │                    ↓
    │                PostgreSQL (state + checkpoints)
    │                    ↑                   ↓
    │                    │               Kafka Producer (rate limited) → Kafka Topic (reflow.jobs)
    │                    │                   ↓
    └─→ Execution Tracking               Worker Pool (KEDA scaled)
                                             ↓
                                        Destinations
```

**Components:**

- **ReflowManager**: Orchestrates jobs, enforces rate limits, and tracks state.
- **PostgreSQL**: Central source of truth for execution state and checkpoints.
- **Kafka**: Reliable job queue for load balancing.
- **Workers**: Consumers that process jobs and report status directly to PostgreSQL.
- **KEDA**: Autoscales workers based on Kafka lag.

## 🚀 Quick Start

Get up and running in minutes using the CLI scaffolding tools.

### 1. Install

```bash
pip install reflowfy
```

### 2. Initialize Project

Create a new project directory with sample pipelines, components, and Docker configurations:

```bash
reflowfy init my_project
cd my_project
```

This generates a standard project structure:

```text
my_project/
├── src/
│   ├── pipelines/      # Define pipelines here
│   ├── sources/        # Reusable source factories (plain functions)
│   ├── destinations/   # Reusable destination factories, per-record routers
│   ├── transformations/ # Reusable @transformation logic
│   └── queries/        # Query templates
├── pyproject.toml      # Project dependencies (uv)
├── uv.lock             # Locked versions — commit it
├── Dockerfile.api / Dockerfile.reflow-manager / Dockerfile.worker
├── .dockerignore
├── .env
└── docker-compose.yml
```

Dependencies are managed with [uv](https://docs.astral.sh/uv/): `uv add <package>` updates
`pyproject.toml` and `uv.lock`. The images install exactly what `uv.lock` pins
(`uv sync --locked`, so a stale lock fails the build), on top of the reflowfy base
image, and run as the non-root user `10001`. `reflowfy init` runs `uv lock` for you
when uv is installed. Building needs BuildKit (the default in Docker 23+ / compose v2).

### 3. Generate Components

Quickly scaffold new components:

```bash
reflowfy new pipeline user_sync
reflowfy new source production_elastic
reflowfy new destination data_lake_s3
reflowfy new transformation flatten_json
```

### 4. Test a Pipeline (no Docker)

The fastest loop while writing a pipeline. Fetches a capped sample through the
same execution core the worker uses, applies your transformations, and reports
what each step did to the batch:

```bash
reflowfy test my_pipeline --dry-run
```

```
✓ Fetched 5 records
  ✓ filter_active_users  5 → 2 records  (-3)
  ✓ uppercase_names      2 → 2 records  (±0)
```

When a transformation raises, it tells you which one, what it received, and the
record that caused it — instead of a framework traceback:

```
❌ Transformation #2 failed: uppercase_last
   KeyError: 'last_name'

   Failing record (index 7 of 12):
     {"id": 7, "first_name": "user7", "active": true}

   Where:
     pipelines/my_pipeline.py:36 in apply
```

| Flag | What it does |
| ---- | ------------ |
| `--dry-run` | Print records instead of writing to the destination |
| `-l, --limit N` | Cap records fetched (default 100) |
| `-v`, `-vv` | Add step timings, `runtime_params` and full tracebacks; `-vv` shows every record |
| `-p, --param k=v` | Set a parameter without being prompted (repeatable) |
| `--no-input` | Never prompt; fail if a required parameter is missing |
| `--json` | Machine-readable report, for CI |
| `--fail-fast` | For ID pipelines, stop at the first failed batch and exit non-zero |

It exits non-zero when the run fails, so `reflowfy test <name> --no-input --json`
works as a CI gate. An ID pipeline previews many independent batches, so one bad
ID is reported but does not by itself fail the run — pass `--fail-fast` for the
strict reading. A file path works in place of the name.

#### Write tests for it

`reflowfy.testing` runs a pipeline inside pytest, through the same code the worker
runs, with no Docker, Kafka or network. `reflowfy init` and `reflowfy new pipeline`
write a starter test for you.

```bash
pip install "reflowfy[test]"
pytest
```

```python
from reflowfy.testing import run_pipeline

def test_orders_are_taxed(mock_http):
    mock_http.get("https://crm.internal/customers/7", json={"tier": "gold"})

    result = run_pipeline("orders", params={"region": "eu"},
                          records=[{"id": 1, "amount": 10.0, "customer_id": 7}])

    assert result.ok
    assert result.sent[0]["tax"] == 2.0 and result.sent[0]["tier"] == "gold"
```

Or record one real run and let pytest replay it offline from then on:

```bash
reflowfy test orders --limit 50 --save-fixture tests/fixtures/orders.json --redact email
pytest                      # tests/fixtures/orders.json::test_fixture[orders]
```

Full guide: [docs/TESTING_PIPELINES.md](docs/TESTING_PIPELINES.md).

### 5. Run Locally

Start the full stack (API, Manager, Worker, Kafka, Postgres) locally using Docker Compose:

```bash
# Verify everything builds
reflowfy run --build

# Run in background
reflowfy run -d
```

### 6. Deploy

Deploy to OpenShift/Kubernetes with a single command:

```bash
reflowfy deploy
```

---

## 🧠 Core Concepts: The Modern DX

Reflowfy keeps its authoring surface small:

| You write | How |
|---|---|
| A reusable source or destination configuration | a plain function returning `elastic_source(...)`, `kafka_destination(...)`, … |
| A step over the whole batch | `@transformation` |
| A step over each record (filter, map, explode, enrich) | `@transformation(per_record=True)` |
| A destination chosen per record | `@destination(per_record=True)` |
| A step or destination with its own state (a client, a cache) | subclass `BaseTransformation` / `RecordTransformation` / `RecordDestination` |

### 1. Define Reusable Sources and Destinations

A plain function that returns a configured connector is all it takes. Put it in your
`sources/` or `destinations/` directory and import it from any pipeline.

```python
# sources/prod_elastic.py
import os
from reflowfy import elastic_source

def prod_elastic(**overrides):
    return elastic_source(
        url=os.getenv("ELASTIC_URL"),
        index="production-logs",
        **overrides
    )

# destinations/prod_kafka.py
from reflowfy import kafka_destination

def prod_kafka(**overrides):
    return kafka_destination(
        bootstrap_servers="kafka:9092",
        topic="processed-events",
        **overrides
    )
```

No decorator is needed for this: a plain function is all it takes.

#### Choosing a destination

| You need | Use |
|---|---|
| One topic, one message per record | `kafka_destination(...)` |
| One request per job, a body you build | `api_destination(url=..., body=...)` |
| One endpoint, one request per record | `api_destination(url=".../{id}", per_record=True)` |
| What a message holds varies per record | `{field}` templates or functions (below) |
| *Where* it goes varies per record — topic, cluster, region, credentials | `@destination(per_record=True)` returning one of the above |

The rule: **`@destination(per_record=True)` decides where a record goes; templates decide what each
message looks like.**

#### Per-record values: one template syntax everywhere

`{field}` is replaced by `record["field"]`; `{data.region}` walks nested dicts (records from a
source are `{"data": {...}}`). A record missing a field fails the job **before anything is
sent**, naming the record's position, never its values. Wherever a template is accepted, a
function of the record works too.

```python
kafka_destination(
    bootstrap_servers="kafka:9092",
    topic="events",
    key="{customer_id}",                      # message key: one customer, one partition, in order
    value_field="data",                       # send record["data"] as the message value
    headers={"tenant": "{tenant_id}"},        # or a function: lambda r: {"type": r["type"]}
)

api_destination(
    url="https://api.example.com/users/{data.user_id}",   # URL-encoded per record
    per_record=True,
    params={"version": "{version}"},                       # query params per record
    headers={"X-Tenant": "{tenant_id}"},                   # or a function of the record
    body_field="payload",                                  # send only record["payload"]
)
```

- `kafka_destination` sends each job's messages as **one batch**: all handed to the producer,
  acks awaited together. Order on a partition is kept.
- A per-record `api_destination` keeps **16 requests in flight** per job
  (`TRANSFORM_CONCURRENCY`), so they may land out of order; make the endpoint idempotent.
- Neither destination takes `concurrency=` any more (removed in 1.1.4): to set a limit, use
  `@destination(per_record=True, concurrency=N)`.

#### A destination per record: `@destination(per_record=True)`

When the records of one job go to different places — another topic, another cluster, other
credentials — choose the destination per record. The function is called once for every record
with `(record, runtime_params)` and returns that record's destination, built exactly as you
build one today:

```python
# destinations/by_region.py
import os
from reflowfy import destination, kafka_destination, secret

CLUSTERS = {"eu": "kafka-eu:9092", "us": "kafka-us:9092"}

@destination(per_record=True, on_error="skip")
def by_region(record, runtime_params):
    region = record["region"]
    return kafka_destination(
        bootstrap_servers=CLUSTERS[region],
        topic=f"orders-{region}-{runtime_params.get('env', 'dev')}",
        sasl_username=os.environ["KAFKA_USER"],
        sasl_password=secret(f"KAFKA_PASSWORD_{region.upper()}"),
    )

# in the pipeline
def define_destination(self, records, runtime_params):
    return by_region
```

- **Connections are shared per job.** Records whose destinations have the same class and
  config share one; beyond that, one Kafka producer serves every topic of a cluster (per
  credentials), one HTTP client every request to a host, and each cluster or host is
  health-checked once. Building a destination is cheap: none connects before its first send.
- **One limit:** `concurrency` (default `TRANSFORM_CONCURRENCY`, 16) is the job's whole budget —
  per-record API requests in flight across *all* its destinations together, destinations sent
  to at once, and records routed at once by an `async def` function. `concurrency=1` sends
  one request at a time, in order. A Kafka destination is one batch and needs no limit.
- Every record is routed before anything is sent. A failing record fails the job with nothing
  written (`record_destination 'by_region' failed on record 37 of 200: KeyError`) — unless
  `on_error="skip"`, which skips it with the exception's type as the reason.
  `raise SkipRecord("unknown region")` skips a record either way; `SkipJob` drops the job.
  `timeout=` bounds each record of an `async def` function and follows `on_error`.
- Skips are reported like a per-record transformation's: the job's `records_skipped` /
  `record_skip_reasons`, `reflowfy_records_skipped_total{transformation="by_region"}`, and a
  log line. The job's `stats` also show how many `destinations` it wrote to.
- Each destination is sent with its own retries, so a failing one does not re-send those
  already written. Order is kept within a destination, not across them.
- Put only *where* in the function's f-strings (host, region, topic, credentials) and
  per-record values in templates — `api_destination(url=f"https://{record['region']}.api.com/users/{{id}}", per_record=True)`
  groups by region and URL-encodes the id. A job that builds a destination per record logs a
  one-time warning saying so.
- The destination's name (in logs, metrics and errors) is the function's;
  `@destination("other_name", per_record=True)` sets another.
- Keyword arguments given in `define_destination` — `by_region(env="prod")` — are passed
  on to every call; a function without options can be returned bare (`return by_region`).
  Filter records in a transformation, not here: returning anything but a
  destination fails the job.

### 2. Create Reusable Transformations

One decorator, `@transformation`, in your `transformations/` directory. By default the
function gets the whole batch; with `per_record=True` it gets one record at a time.

```python
# transformations/clean_names.py
from typing import List, Optional

from reflowfy import Record, Records, RuntimeParams, SkipRecord, transformation

@transformation
def clean_names(records: Records, runtime_params: RuntimeParams) -> Records:
    """Normalize user names."""
    for record in records:
        if "name" in record:
            record["name"] = record["name"].strip().title()
    return records

@transformation(per_record=True)
def active_only(record: Record, runtime_params: RuntimeParams) -> Optional[Record]:
    return record if record.get("active") else None          # None drops the record

@transformation(per_record=True)
def one_per_item(record: Record, runtime_params: RuntimeParams) -> List[Record]:
    return [{**record, "item": i} for i in record["items"]]  # a list replaces it

@transformation(per_record=True, concurrency=32, timeout=10, on_error="skip")
async def enrich(record: Record, runtime_params: RuntimeParams, *, region: str) -> Record:
    if record.get("test"):
        raise SkipRecord("test account")                     # dropped, reason counted
    return {**record, "profile": await lookup(region, record["user_id"])}  # your own async call
```

- The step's name (logs, metrics, `reflowfy test`) is the function's;
  `@transformation("other_name")` sets another. Names are unique.
- `def` or `async def`, either way. A per-record `def` runs its records in order in a thread;
  a per-record `async def` runs `concurrency` of them at once (default
  `TRANSFORM_CONCURRENCY`, 16), each bounded by `timeout`. `concurrency`, `timeout` and
  `on_error` exist only per record, and `concurrency`/`timeout` only for `async def` —
  anything else fails when the module is imported, saying why.
- Parameters after `(records|record, runtime_params)` are options: `enrich(region="eu")`.
  A step without options is listed bare (`clean_names`) or called (`clean_names()`).

_(Subclass `BaseTransformation`, `RecordTransformation` or `SyncRecordTransformation` for
a step with its own state — say, an HTTP client opened once per job in `setup()`)._

### 3. Build a Pipeline

Pipelines connect your sources, transformations, and destinations. Subclass `AbstractPipeline` and map your components.

**Pipelines are auto-registered** upon interpretation — no manual registry calls needed!

```python
# pipelines/user_sync_pipeline.py
from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    BaseSource,
    Records,
    RuntimeParams,
    Transformations,
)
from sources.prod_elastic import prod_elastic
from destinations.prod_kafka import prod_kafka
from transformations.clean_names import clean_names

class UserSyncPipeline(AbstractPipeline[RuntimeParams]):
    # The auto-registration system uses this exact name:
    name = "user_sync_pipeline"
    rate_limit = 3000  # jobs per minute

    def define_source(self, runtime_params: RuntimeParams) -> BaseSource:
        # Override the base query for this specific pipeline
        return prod_elastic(
            base_query={"query": {"match": {"type": "user_signup"}}}
        )

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        # Steps without options are listed bare; others are called with them
        return [clean_names, active_only]

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return prod_kafka()

# ✅ That's it! The pipeline is automatically discovered.
```

### 4. Logging and Errors

**Logging.** Get a logger at module level with `get_logger(__name__)`. There is no `self` to reach for — it works the same in a transformation, a source factory, or a plain helper:

```python
# transformations/clean_names.py
from reflowfy import transformation, get_logger

logger = get_logger(__name__)

@transformation
def clean_names(records, context):
    logger.info("Normalizing %d names", len(records))
    return records
```

Every line is automatically stamped with `execution_id`, `job_id` and `pipeline_name` — you never pass them yourself:

```json
{"@timestamp": "2026-08-05T09:12:44Z", "log.level": "info",
 "logger": "reflowfy.transformations.clean_names",
 "message": "Normalizing 250 names",
 "execution_id": "exec-7f3a", "job_id": "job-0012", "pipeline_name": "user_sync_pipeline"}
```

Control it with env vars: `LOG_LEVEL` (default `INFO`), `LOG_JSON` (default `true`; `reflowfy test` defaults to `false` for readable terminal output), and `LOG_DESTINATION` (`stdout` | `elastic` | `both`).

**Errors.** Raise from any `define_*` hook or transformation to fail the job. Use `PipelineError` to mark a deliberate failure:

```python
from reflowfy import PipelineError, get_logger

logger = get_logger(__name__)

class UserSyncPipeline(AbstractPipeline[RuntimeParams]):
    name = "user_sync_pipeline"

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        if not records:
            raise PipelineError("nothing survived transformation")
        return prod_kafka()
```

The job is marked `failed` and the message, exception type and full traceback are stored on the job record. Unexpected errors are wrapped so the message names the step that failed:

```
PipelineError: define_destination of pipeline 'user_sync_pipeline' raised KeyError: 'KAFKA_URL'
```

The original exception stays reachable as `err.original_error`. Also available: `SourceError`, `DestinationError`, `TransformationError`.

> **A pipeline that fails to construct is not registered.** If your `__init__` raises, the pipeline can't be built, so it won't appear — but the traceback is logged at ERROR on startup, and the later lookup tells you why:
> `Pipeline 'user_sync' not found in registry (it failed to register: KeyError: 'ELASTIC_URL')`

### 5. Run and Control Executions

Every pipeline gets a run route on the API (port 8000): `POST /pipelines/<name>/run`, or
`POST /pipelines/<folder>/<name>/run` for a pipeline in a subfolder of `pipelines/`:

```bash
curl -X POST "http://localhost:8000/pipelines/user_sync_pipeline/run?mode=distributed&rate_limit=600&env=prod" \
  -H "Content-Type: application/json" -d '{}'
```

Scalar pipeline parameters (`env=prod`) are query parameters; list and dict ones (an ID-based
pipeline's `input_ids`) go in the JSON body. Swagger at `/docs` shows each pipeline's route.

The ReflowManager (port 8001) takes the same run directly, and controls it afterwards:

```bash
curl -X POST http://localhost:8001/run -H "Content-Type: application/json" \
  -d '{"pipeline_name": "user_sync_pipeline", "runtime_params": {}, "mode": "distributed"}'
# -> {"execution_id": "...", ...}

GET  /executions/{id}/status        # state and job counts
GET  /executions/{id}/stats         # records, skips, retries, hooks, timings
GET  /executions/{id}/jobs          # every job with its stats JSON
GET  /executions/{id}/errors        # failed jobs and their errors
POST /executions/{id}/pause         # stop dispatching; jobs in flight finish
POST /executions/{id}/resume
POST /executions/{id}/cancel        # jobs not yet started are cancelled
POST /executions/{id}/retry-failed  # requeue failed jobs (optionally ?error_contains=...)
```

`mode` is `local` (in-process, no workers) or `distributed` (Kafka and the worker pool);
`rate_limit` is jobs per minute. Set `REFLOWFY_API_TOKEN` on the API and the manager and every
route but `/health` and `/metrics` requires `Authorization: Bearer <token>`.

### 6. Pipeline Behaviour

All of these are opt-in and set on the pipeline class.

**Dropping records and jobs.** Inside a per-record transformation, return `None` to drop a record
silently or `raise SkipRecord("why")` to drop it with a reason; `raise SkipJob("why")` anywhere in
a job drops the whole job without failing it. Both are counted on the job's stats and in metrics.
Both take `extra=`, the same fields a logger call takes, for what the reason must not hold:

```python
raise SkipRecord("user inactive", extra={"user_id": record["user_id"]})  # a record_skipped line
raise SkipJob("tenant disabled", extra={"tenant": params["tenant"]})      # on the job_dropped line
```

A `SkipRecord` with `extra` gets an info line of its own (one per record); without it, skips are
only counted. The fields go to logs only, never to stats or metric labels.

**Retries.** A worker that disappears has its job re-run automatically (leases). To retry jobs
that *fail*, set `job_retry`; bad data and bugs (`KeyError`, validation errors, HTTP 4xx) are
never retried:

```python
from reflowfy import JobRetryConfig

class UserSyncPipeline(AbstractPipeline[RuntimeParams]):
    name = "user_sync_pipeline"
    job_retry = JobRetryConfig(max_retries=2, backoff_seconds=30)
```

**Rate limit.** `rate_limit = 600` (jobs per minute), or compute it in
`define_rate_limit(params)`; `?rate_limit=` on the run overrides both. Nothing set means unlimited.

**Deduplication.** `enable_duplicate_jobs = False` makes workers skip records already sent to the
same destination (across executions, for 24 h). `dedup_key = "order_id"` decides what identifies a
record.

**Validating records.** `record_type = TypedRecord[Order]` checks every fetched record against a
TypedDict (`on_invalid_record = "skip"` drops bad ones instead of failing the job), and
`validate_steps = True` checks each step's output against its return annotation.

**Schedules.** `schedules = [ScheduledRun(name="nightly", cron="0 2 * * *", params={...})]`.

**Lifecycle hooks and chaining.** `on_execution_complete(summary, params)`,
`on_execution_failed(...)` and `on_execution_cancelled(...)` run once when an execution ends;
`run_pipeline("next_pipeline", params)` inside a hook starts a follow-up run linked to this one.

---

## 🔌 Built-in Connectors

### Sources

- **Elasticsearch** (`elastic_source`): `docs_per_job=N` plans one job per N documents over a point
  in time; `window_mode="range"` gives each job its own range of the first sort key, for long
  runs that must survive an Elasticsearch restart.
- **SQL** (`sql_source`): every `{{ value }}` in the query is a bind parameter, written unquoted
  (`WHERE day >= {{ day }}`); table and column names go through `{{ name | ident }}`. Split by ID
  range.
- **HTTP API** (`id_based_api_source`): fetches IDs in batches, with offset/cursor pagination and
  several auth strategies.
- **S3** (`s3_source`, needs `boto3`): splits a bucket by prefix.

Keep credentials out of job messages with `secret("ENV_VAR")`: the worker reads the variable
from its own environment.

A source of your own subclasses `BaseSource` (`fetch` may return a list or `yield`
its records) or `AsyncSource` (`async def fetch_async`, awaited on the worker):

```python
class Rows(BaseSource):
    def fetch(self, runtime_params, limit=None):
        for row in cursor_for(self.config):
            yield {"id": row.id}

class Users(AsyncSource):
    async def fetch_async(self, runtime_params, limit=None):
        async with httpx.AsyncClient() as client:
            return (await client.get(self.config["url"])).json()
```

### Destinations

- **Kafka** (`kafka_destination`): one message per record, sent as one batch per job; per-record
  `key=` and `headers=` (templates or functions), `value_field=`.
- **HTTP** (`api_destination`): one request per job with a body you build, or `per_record=True`
  for one request per record with `{field}` URL, query and header templates. 4xx is not retried;
  5xx, 429 and connection errors are, honouring `Retry-After`.
- **Console** (`console_destination`): structured output for local debugging.

Return a list (or `fan_out(a, b, optional=[b])`) from `define_destination` to send every record
to several destinations, or a `@destination(per_record=True)` function to choose one per record.

## ⚙️ Configuration

Services are configured with environment variables. The ones you are most likely to set:

| Variable | Used by | Default | What |
|---|---|---|---|
| `DATABASE_URL` | manager, worker | — | PostgreSQL connection |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC` | manager, worker | `reflow.jobs` topic | Kafka (plus `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`) |
| `KAFKA_GROUP_ID` | worker | `reflowfy-workers` | consumer group |
| `EXECUTION_MODE` | manager | `distributed` | `local` runs jobs in-process, without Kafka |
| `PIPELINE_MODULE` | all | `pipelines` | where pipelines are discovered |
| `REFLOWFY_API_TOKEN` | API, manager | unset | bearer token required on every route but `/health`, `/metrics` |
| `TRANSFORM_CONCURRENCY` | worker | `16` | default records in flight for per-record steps and destinations |
| `MAX_IN_FLIGHT_JOBS` | manager | `1000` | jobs dispatched but not finished, per execution |
| `JOB_HEARTBEAT_SECONDS`, `JOB_RUNNING_LEASE_SECONDS`, `JOB_MAX_ATTEMPTS` | worker, manager | `30`, `300`, `3` | when a job whose worker disappeared is re-run |
| `LOG_LEVEL`, `LOG_JSON`, `LOG_DESTINATION` | all | `INFO`, `true`, `stdout` | logging (`elastic` / `both` ship to Elasticsearch) |
| `API_HOST`, `API_PORT` | API | `0.0.0.0`, `8000` | where the API listens |

## 🐳 Kubernetes Deployment

Reflowfy natively supports OpenShift/Kubernetes via `reflowfy deploy`.

1. **Configure environment**: Define connection strings in your `.env`:
   ```bash
   REGISTRY=ghcr.io/myname
   dataset=my-project
   KAFKA_BOOTSTRAP_SERVERS=prod-kafka:9092
   ```
2. **Deploy**:
   ```bash
   reflowfy deploy
   ```
   Creates the API, Manager, auto-scaled KEDA Workers, and (optionally) PostgreSQL, dynamically injecting your pipeline code into the containers.

## 🧪 Developing Reflowfy

```bash
uv sync --all-extras
uv run pytest tests/unit --cov            # unit tests (CI requires 99% coverage)
uv run ruff check reflowfy/ && uv run pyright && uv run mypy reflowfy/
./scripts/run_e2e_tests.sh                # end to end on a Docker stack (local mode)
```

`tests/e2e_distributed/` runs the same kind of tests over Kafka and real workers, against a
deployment or the local Docker stack; see its README. The `E2E (nightly)` workflow
(`.github/workflows/e2e-nightly.yml`) runs both suites, the Elasticsearch and chaos tests
included, every night and on demand.

## 📝 License

MIT
