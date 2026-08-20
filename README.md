# Reflowfy

**A horizontally scalable data movement and transformation framework**

Reflowfy enables you to build pipelines that fetch data from sources, apply custom transformations, and send results to destinations—all with millions+ record scalability.

## 🎯 Key Features

- **Modern DX**: Define reusable components with `@source`, `@destination`, and `@transformation` decorators.
- **Auto-Discovery**: Pipelines and components are automatically discovered and registered — no manual `__init__.py` tracking required.
- **Horizontally Scalable**: Process millions of records in parallel using Apache Kafka.
- **Kubernetes-Native**: KEDA autoscaling from 0 to N workers based on queue lag.
- **Order-Independent**: Maximum parallelism without coordination overhead.
- **Two Execution Modes**: Local testing and distributed production execution.

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
├── pipelines/          # Define pipelines here
├── sources/            # Reusable @source configs
├── destinations/       # Reusable @destination configs
├── transformations/    # Reusable @transformation logic
├── .env
└── docker-compose.yml
```

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

Reflowfy uses a modular, decorator-driven architecture for defining reusable components.

### 1. Define Reusable Sources and Destinations

Use the `@source` and `@destination` decorators to pre-configure connectors. These can be placed in your `sources/` and `destinations/` directories and reused across multiple pipelines.

```python
# sources/prod_elastic.py
import os
from reflowfy import source, elastic_source

@source("prod_elastic")
def prod_elastic(**overrides):
    return elastic_source(
        url=os.getenv("ELASTIC_URL"),
        index="production-logs",
        **overrides
    )

# destinations/prod_kafka.py
from reflowfy import destination, kafka_destination

@destination("prod_kafka")
def prod_kafka(**overrides):
    return kafka_destination(
        bootstrap_servers="kafka:9092",
        topic="processed-events",
        **overrides
    )
```

### 2. Create Reusable Transformations

Transformations process batches of records. Use the `@transformation` decorator in your `transformations/` directory:

```python
# transformations/clean_names.py
from reflowfy import Records, RuntimeParams, transformation

@transformation("clean_names")
def clean_names(records: Records, runtime_params: RuntimeParams) -> Records:
    """Normalize user names."""
    for record in records:
        if "name" in record:
            record["name"] = record["name"].strip().title()
    return records
```

_(You can also subclass `BaseTransformation` for more complex stateful transformations)._

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
        # Instantiate and return transformations
        return [clean_names()]

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return prod_kafka()

# ✅ That's it! The pipeline is automatically discovered.
```

### 4. Logging and Errors

**Logging.** Get a logger at module level with `get_logger(__name__)`. There is no `self` to reach for — it works the same in a transformation, a `@source` function, or a plain helper:

```python
# transformations/clean_names.py
from reflowfy import transformation, get_logger

logger = get_logger(__name__)

@transformation("clean_names")
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

### 5. Execute Pipelines

Trigger pipelines locally or in production via HTTP:

```bash
# Production Execution (Distributed via Kafka)
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "user_sync_pipeline",
    "runtime_params": {}
  }'

# Dry Run (Preview without side effects)
curl -X POST http://localhost:8001/run ... -d '{..., "dry_run": true}'
```

---

## 🔌 Built-in Connectors

### Sources

- **Elasticsearch**: Scroll-based pagination with parameter injection
- **SQL**: ID range and offset-based pagination (PostgreSQL, MySQL, etc.)
- **HTTP API**: Offset/cursor pagination with various auth strategies
- **S3**: Efficient distributed bucket processing (prefix splitting)

### Destinations

- **Kafka**: High-throughput batching and compression
- **HTTP**: Flexible webhooks with retry capabilities
- **Console**: Structured output for local debugging

## ⚙️ Configuration

Control behavior via Environment Variables:

**API Service:**

```bash
API_HOST=0.0.0.0
API_PORT=8000
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=reflow.jobs
```

**Worker Service:**

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=reflow.jobs
KAFKA_GROUP_ID=reflowfy-workers
```

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

## 📝 License

MIT
