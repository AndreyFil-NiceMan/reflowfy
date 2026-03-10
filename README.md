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

### 4. Run Locally

Start the full stack (API, Manager, Worker, Kafka, Postgres) locally using Docker Compose:

```bash
# Verify everything builds
reflowfy run --build

# Run in background
reflowfy run -d
```

### 5. Deploy

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
from reflowfy import transformation

@transformation("clean_names")
def clean_names(records, context):
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
from reflowfy import AbstractPipeline
from sources.prod_elastic import prod_elastic
from destinations.prod_kafka import prod_kafka
from transformations.clean_names import clean_names

class UserSyncPipeline(AbstractPipeline):
    # The auto-registration system uses this exact name:
    name = "user_sync_pipeline"
    rate_limit = {"jobs_per_second": 50}

    def define_parameters(self):
        # Define allowed runtime overrides
        return []

    def define_source(self, params):
        # Override the base query for this specific pipeline
        return prod_elastic(
            base_query={"query": {"match": {"type": "user_signup"}}}
        )

    def define_transformations(self, params):
        # Instantiate and return transformations
        return [clean_names()]

    def define_destination(self, params):
        return prod_kafka()

# ✅ That's it! The pipeline is automatically discovered.
```

### 4. Execute Pipelines

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
