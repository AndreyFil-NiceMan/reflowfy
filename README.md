# Reflowfy

**A horizontally scalable data movement and transformation framework**

Reflowfy enables you to build pipelines that fetch data from sources, apply custom transformations, and send results to destinations—all with millions+ record scalability.

## 🎯 Key Features

- **Horizontally Scalable**: Process millions of records in parallel
- **Kafka-Based**: Reliable message queue for job distribution
- **Kubernetes-Native**: KEDA autoscaling from 0 to N workers
- **Order-Independent**: Maximum parallelism without coordination overhead
- **User-Extensible**: Plugin architecture for sources, destinations, and transformations
- **Two Execution Modes**: Local testing and distributed production execution

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
- **Workers**: Consumers that process jobs and **report status directly to PostgreSQL**.
- **KEDA**: Autoscales workers based on Kafka lag.

## 🚀 Quick Start

Get up and running in minutes using the CLI.

### 1. Install
```bash
pip install reflowfy
```

### 2. Initialize Project
Create a new project directory with a sample pipeline and Docker configuration:
```bash
reflowfy init my_project
cd my_project
```

### 3. Run Locally
Start the full stack (API, Manager, Worker, Kafka, Postgres) locally using Docker Compose:
```bash
# Verify everything builds
reflowfy run --build

# Run in background
reflowfy run -d
```

### 4. Deploy
Deploy to OpenShift/Kubernetes with a single command:
```bash
reflowfy deploy
```

---

## 🧠 Core Concepts

Reflowfy uses a class-based architecture for pipelines, allowing for dynamic configuration and modular design.

### 1. Create Custom Transformations

Transformations are reusable units of logic that process batches of records. To create one, subclass `BaseTransformation`:

```python
from reflowfy import BaseTransformation

class XmlToJson(BaseTransformation):
    name = "xml_to_json"  # Unique identifier
    
    def apply(self, records, context):
        """
        Process a batch of records.
        Records are passed as a list of dictionaries (or source-specific format).
        Return the modified list.
        """
        return [self._parse_xml(r) for r in records]

    def _parse_xml(self, record):
        # ... logic ...
        return record
```

### 2. Build a Pipeline

Pipelines connect sources, transformations, and destinations. Subclass `AbstractPipeline` to define your logic:

```python
from reflowfy import AbstractPipeline, pipeline_registry
from reflowfy import elastic_source, kafka_destination
from .transformations import XmlToJson

class ElasticXmlPipeline(AbstractPipeline):
    name = "elastic_xml_pipeline"
    rate_limit = {"jobs_per_second": 50}

    def define_source(self, params):
        """
        Define source based on runtime parameters.
        Parameters allow you to change behavior at runtime (e.g., time ranges).
        """
        return elastic_source(
            url="http://elasticsearch:9200",
            index="logs-*",
            base_query={
                "query": {
                    "range": {
                        "@timestamp": {
                            "gte": "{{ start_time }}",  # Jinja template support
                            "lte": "{{ end_time }}"
                        }
                    }
                }
            }
        )

    def define_transformations(self, params):
        """List of transformations to apply in order."""
        return [XmlToJson()]

    def define_destination(self, params):
        """Define where data should go."""
        return kafka_destination(
            bootstrap_servers="kafka:9092",
            topic="processed-logs"
        )

# Register the pipeline so the worker and API can find it
pipeline_registry.register(ElasticXmlPipeline())
```

### 3. Run Pipeline

You can run your pipeline locally or in production via the API:

```bash
# Production Execution (Async via Kafka)
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "elastic_xml_pipeline",
    "runtime_params": {
      "start_time": "2024-01-01",
      "end_time": "2024-01-02"
    }
  }'

# Dry Run (Preview without side effects)
curl -X POST http://localhost:8001/run ... -d '{..., "dry_run": true}'
```

## 📦 Installation

```bash
# Using pip
pip install -e .

# Using Docker
docker build -f Dockerfile.api -t reflowfy-api .
docker build -f Dockerfile.worker -t reflowfy-worker .
```

## 🔌 Built-in Connectors

### Sources
- **Elasticsearch**: Scroll-based pagination with runtime parameters
- **SQL**: ID range and offset-based splitting (Postgres, MySQL, etc.)
- **HTTP API**: Offset/cursor pagination with authentication

### Destinations
- **Kafka**: Batching, compression, health checks
- **HTTP**: Webhooks with retry logic

## ⚙️ Configuration

### Environment Variables

**API:**
```bash
API_HOST=0.0.0.0
API_PORT=8000
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=reflow.jobs
```

**Worker:**
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=reflow.jobs
KAFKA_GROUP_ID=reflowfy-workers
```

| Mode | Endpoint | Use Case | Kafka | Workers |
|------|----------|----------|-------|---------|
| **Distributed** | `POST /run` | Production execution | ✅ | ✅ |
| **Dry Run** | `POST /run` (dry_run=true) | Preview/Testing | ❌ | ❌ |

## 🐳 Kubernetes Deployment

Reflowfy streamlines deployment to Kubernetes/OpenShift using the CLI and Helm.

### Deployment Concept

The deployment process uses your local configuration to configure the cluster.

1.  **Configuration**: The `.env` file in your project root is the source of truth. It defines connection strings (Kafka, Registry, DB). 
2.  **CLI**: The `reflowfy deploy` command reads this config and triggers a Helm upgrade.

### Deployed Objects

When you run `reflowfy deploy`, the following objects are created in your namespace:

*   **ReflowAPI (Deployment + Service)**: The entry point for triggering pipeline runs.
*   **ReflowManager (Deployment + Service)**: Orchestrates job distribution and manages state.
*   **ReflowWorker (Deployment + KEDA ScaledObject)**: The worker pool that processes jobs. KEDA automatically scales this deployment based on Kafka lag (0 to N replicas).
*   **PostgreSQL (Optional)**: If `DEPLOY_POSTGRES=True`, a dedicated Postgres instance is deployed. Otherwise, the system connects to your external DB.

### How to Deploy

1.  **Configure environment**:
    Ensure your `.env` file has the correct registry and Kafka settings.
    ```bash
    REGISTRY=my.registry.com
    dataset=my-project
    KAFKA_BOOTSTRAP_SERVERS=my-kafka:9092
    ```

2.  **Run Deploy**:
    ```bash
    reflowfy deploy
    ```
    *This will build/push images (if requested), generate the Helm values from your .env, and apply the chart.*

## 📝 License

MIT

## 🤝 Contributing

Contributions welcome! This is a production-grade framework designed for real-world data processing at scale.
