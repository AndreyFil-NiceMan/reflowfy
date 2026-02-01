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

Reflowfy is designed to be simple yet powerful. Here is how you define pipelines manually.

### 1. Define a Custom Transformation

```python
from reflowfy import BaseTransformation

class XmlToJson(BaseTransformation):
    name = "xml_to_json"
    
    def apply(self, records, context):
        # Your transformation logic
        return [parse_xml(r) for r in records]
```

### 2. Build a Pipeline

```python
from reflowfy import build_pipeline, pipeline_registry
from reflowfy import elastic_source, kafka_destination

# Configure source with runtime parameters
source = elastic_source(
    url="http://elasticsearch:9200",
    index="logs-*",
    base_query={
        "query": {
            "range": {
                "@timestamp": {
                    "gte": "{{ start_time }}",  # Runtime parameter
                    "lte": "{{ end_time }}"
                }
            }
        }
    }
)

# Configure destination
destination = kafka_destination(
    bootstrap_servers="kafka:9092",
    topic="processed-logs"
)

# Build and register
pipeline = build_pipeline(
    name="elastic_xml_pipeline",
    source=source,
    transformations=[XmlToJson()],
    destination=destination,
    rate_limit={"jobs_per_second": 50}
)

pipeline_registry.register(pipeline)
```

### 3. Start the API manually (if not using CLI)

```python
# In your main.py
from reflowfy.api.app import main
import examples.xml_to_json_pipeline  # Import to trigger registration

if __name__ == "__main__":
    main()
```

### 4. Execute Pipeline

**Run Distributed** (async via Kafka):
```bash
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "elastic_xml_pipeline",
    "runtime_params": {
      "start_time": "2024-01-01",
      "end_time": "2024-01-02"
    }
  }'
```

**Dry Run** (Preview jobs without executing):
```bash
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "elastic_xml_pipeline",
    "runtime_params": {
      "start_time": "2024-01-01",
      "end_time": "2024-01-02"
    },
    "dry_run": true
  }'
```
*Returns a preview of the job execution plan, sample records, and configuration.*

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

## 📊 Monitoring

Reflowfy exposes Prometheus metrics:

- `reflowfy_jobs_processed_total` - Total jobs processed
- `reflowfy_job_processing_duration_seconds` - Job processing time
- `reflowfy_records_processed_total` - Total records processed
- `reflowfy_active_workers` - Active worker count

## 🐳 Kubernetes Deployment

```bash
# Deploy with Helm (using bundled charts)
# Note: For production, we recommend using the CLI 'reflowfy deploy'
helm install reflowfy-api ./reflowfy/helm/reflowfy-api
helm install reflowfy-worker ./reflowfy/helm/reflowfy-worker
```

KEDA will automatically scale workers based on Kafka lag.

## 📝 License

MIT

## 🤝 Contributing

Contributions welcome! This is a production-grade framework designed for real-world data processing at scale.
