# 🔍 Testing Reflofy with Elasticsearch

This guide shows you how to test the Reflofy framework with an Elasticsearch source using the complete test setup.

## What's Included

- **Elasticsearch + Kibana** via Docker Compose
- **Test data** with 1000 sample documents
- **Ready-to-use pipeline** (`elastic_test_pipeline`)
- Works in **local** and **distributed** modes

## Prerequisites

- Docker and Docker Compose installed
- Python dependencies installed (`pip install -e .`)
- Elasticsearch Python client: `pip install elasticsearch`

## Quick Start

### 1️⃣ Start Elasticsearch

```bash
# Start Elasticsearch and Kibana
docker-compose -f docker-compose.elastic.yml up -d

# Check services are running
docker-compose -f docker-compose.elastic.yml ps

# Wait for health checks (may take 30-60 seconds)
# Watch logs if needed
docker-compose -f docker-compose.elastic.yml logs -f elasticsearch
```

**What's running:**
- Elasticsearch: `http://localhost:9200`
- Kibana (UI): `http://localhost:5601`

**Verify Elasticsearch:**
```bash
curl http://localhost:9200
# Should return cluster info JSON
```

### 2️⃣ Initialize Test Data

```bash
# Run the initialization script
python examples/init_elastic_test_data.py
```

**What happens:**
- Creates index: `reflofy-test-data`
- Inserts 1000 test documents with:
  - Timestamps spanning 2024
  - User information (id, name, email, region)
  - Event types (login, purchase, search, etc.)
  - Status values (active, inactive, pending, archived)
  - Rich metadata

**Verify data:**
```bash
# Check document count
curl http://localhost:9200/reflofy-test-data/_count
# Should return: {"count":1000,...}

# View sample document
curl http://localhost:9200/reflofy-test-data/_search?size=1
```

### 3️⃣ Test the Pipeline (Local Mode)

**Option A: Using Python directly**

Create a test script:
```python
# examples/run_elastic_test.py
import os
os.environ["API_HOST"] = "0.0.0.0"
os.environ["API_PORT"] = "8000"

import examples.elastic_test_pipeline  # Import to register
from reflowfy.api.app import main

if __name__ == "__main__":
    main()
```

Run it:
```bash
python examples/run_elastic_test.py
```

**Option B: Test via API**

In another terminal:
```bash
# Test pipeline (local mode - synchronous)
curl -X POST "http://localhost:8000/pipelines/elastic_test_pipeline/test?start_time=2024-01-01T00:00:00&end_time=2024-12-31T23:59:59&filter_status=active"
```

**Option C: Use Swagger UI**

1. Open http://localhost:8000/docs
2. Find `POST /pipelines/elastic_test_pipeline/test`
3. Click "Try it out"
4. Fill in parameters:
   - `start_time`: `2024-01-01T00:00:00`
   - `end_time`: `2024-12-31T23:59:59`
   - `filter_status`: `active`
5. Click "Execute"

**Expected output:**
```
🧪 Testing pipeline: elastic_test_pipeline (local)
Runtime params: {'start_time': '2024-01-01...', ...}

Fetching data from source...
  📊 Filtered: 100 → 45 records (status=active)
  
📤 CONSOLE DESTINATION - Sending 45 records
📦 Records (showing 10 of 45):

Record 1:
{
  "@timestamp": "2024-03-15T14:30:00",
  "user_name": "Alice Anderson",
  "event_type": "purchase",
  "status": "active",
  "_reflofy_processed": {
    "execution_id": "...",
    "pipeline_name": "elastic_test_pipeline",
    "framework": "reflofy"
  },
  "_summary": "purchase by Alice Anderson at 2024-03-15T14:30:00"
}
...
```

### 4️⃣ Run Distributed Mode (Optional)

To test the full distributed architecture with Kafka:

**Terminal 1: Start Kafka**
```bash
docker-compose up -d
```

**Terminal 2: Start API**
```bash
python examples/run_elastic_test.py
```

**Terminal 3: Start Worker**
```bash
python examples/run_local_worker.py
```

**Terminal 4: Send distributed job**
```bash
curl -X POST "http://localhost:8000/pipelines/elastic_test_pipeline/run?start_time=2024-01-01T00:00:00&end_time=2024-12-31T23:59:59"
```

Watch Terminal 3 for worker processing output!

## Exploring the Data

### Using Kibana

1. Open http://localhost:5601
2. Go to **Management → Stack Management → Data Views**
3. Create data view for `reflofy-test-data`
4. Go to **Analytics → Discover** to explore data
5. Try **Dev Tools** for custom queries:

```json
GET reflofy-test-data/_search
{
  "query": {
    "bool": {
      "must": [
        {"term": {"status": "active"}},
        {"term": {"event_type": "purchase"}}
      ]
    }
  }
}
```

### Using cURL

```bash
# Search by status
curl -X GET "http://localhost:9200/reflofy-test-data/_search" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": {"term": {"status": "active"}},
    "size": 5
  }'

# Count by event type
curl -X GET "http://localhost:9200/reflofy-test-data/_search" \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 0,
    "aggs": {
      "event_types": {
        "terms": {"field": "event_type"}
      }
    }
  }'
```

## Pipeline Details

The `elastic_test_pipeline` demonstrates:

**Source Configuration:**
- Connects to Elasticsearch at `localhost:9200`
- Uses scroll API for pagination (100 docs/page)
- Filters by date range using runtime parameters
- Sorts by timestamp (descending)

**Transformations:**
1. **FilterByStatus** - Keeps only records with specified status (default: "active")
2. **EnrichWithProcessingInfo** - Adds Reflofy metadata (execution ID, timestamp, etc.)
3. **FormatEventData** - Adds human-readable summary and formatted amounts

**Destination:**
- Console output (prints to stdout)
- Shows first 10 records with pretty printing

## Customizing the Pipeline

### Change Filter Status

```python
# In elastic_test_pipeline.py
FilterByStatus(allowed_status="inactive")  # Filter for inactive instead
```

### Add More Transformations

```python
class CalculateMetrics(BaseTransformation):
    name = "calculate_metrics"
    
    def apply(self, records, context):
        total_purchases = sum(
            1 for r in records 
            if r.get("event_type") == "purchase"
        )
        context["metrics"] = {"total_purchases": total_purchases}
        return records

# Add to pipeline
transformations=[
    FilterByStatus(allowed_status="active"),
    CalculateMetrics(),  # Your new transformation
    EnrichWithProcessingInfo(),
    FormatEventData(),
]
```

### Change Destination

```python
# Use Kafka instead of console
from reflowfy import kafka_destination

destination = kafka_destination(
    bootstrap_servers="localhost:9093",
    topic="processed-elastic-data",
    compression_type="gzip",
)
```

## Troubleshooting

### Elasticsearch won't start

```bash
# Check logs
docker-compose -f docker-compose.elastic.yml logs elasticsearch

# Common fix: increase Docker memory to at least 4GB
# Docker Desktop → Settings → Resources → Memory
```

### Can't connect to Elasticsearch

```bash
# Verify it's running
curl http://localhost:9200

# Check if port is in use
lsof -i :9200

# Restart services
docker-compose -f docker-compose.elastic.yml restart
```

### No data in index

```bash
# Run initialization again
python examples/init_elastic_test_data.py

# Check if index exists
curl http://localhost:9200/_cat/indices?v
```

### Pipeline not found

Make sure you imported the pipeline:
```python
import examples.elastic_test_pipeline
```

The import triggers auto-registration with the pipeline registry.

### Health check failing

```bash
# Check Elasticsearch cluster health
curl http://localhost:9200/_cluster/health

# Should show status: green or yellow
```

## Cleaning Up

```bash
# Stop services
docker-compose -f docker-compose.elastic.yml down

# Stop and remove volumes (deletes all data)
docker-compose -f docker-compose.elastic.yml down -v
```

## Next Steps

1. **Modify the query** - Edit `base_query` in `elastic_source()` to use different filters
2. **Add more fields** - Extend test data generation with domain-specific fields
3. **Test at scale** - Generate 10K+ documents and test distributed mode
4. **Create your own pipeline** - Use this as a template for your real use case
5. **Deploy to production** - Use the Helm charts to deploy to Kubernetes

## Architecture Flow

```
┌──────────────────┐
│  Elasticsearch   │ ← Test data initialization
│  (localhost:9200)│
└────────┬─────────┘
         │ Scroll API
         ▼
┌──────────────────┐
│ elastic_source() │ ← Fetches data with runtime params
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Transformations  │ ← Filter, enrich, format
│  - FilterByStatus│
│  - EnrichMeta    │
│  - FormatEvent   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│console_destination│ ← Prints to stdout
│  (or Kafka)      │
└──────────────────┘
```

## Data Schema

Each test document contains:

| Field | Type | Description |
|-------|------|-------------|
| `@timestamp` | date | ISO 8601 timestamp |
| `user_id` | keyword | Unique user identifier |
| `user_name` | text | Full name |
| `user_email` | keyword | Email address |
| `user_region` | keyword | Geographic region |
| `event_type` | keyword | Type of event |
| `event_data` | object | Event-specific data |
| `status` | keyword | Record status |
| `metadata` | object | Additional metadata |
| `session_id` | keyword | Session identifier |
| `request_id` | keyword | Request identifier |
| `processed` | boolean | Processing flag |

Happy testing! 🚀
