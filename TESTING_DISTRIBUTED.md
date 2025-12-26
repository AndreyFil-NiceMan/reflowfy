# 🚀 Testing Reflowfy in Distributed Mode (Local)

This guide shows you how to test the full Reflowfy distributed architecture locally:
- API sends jobs to Kafka
- Worker consumes from Kafka
- All running locally!

## Prerequisites

- Docker and Docker Compose installed
- Python dependencies installed (`pip install -e .`)

## Step-by-Step Testing

### 1️⃣ Start Kafka

```bash
# Start Kafka and Zookeeper with Docker Compose
docker-compose up -d

# Check if Kafka is running
docker-compose ps

# View logs (optional)
docker-compose logs -f kafka
```

**What's running:**
- Kafka broker: `localhost:9092`
- Zookeeper: `localhost:2181`
- Kafka UI: `http://localhost:8080` (browse messages visually)

### 2️⃣ Start the API (Terminal 1)

```bash
python examples/run_api_distributed.py
```

**What happens:**
- API starts on `http://localhost:8000`
- Configured to send jobs to Kafka at `localhost:9092`
- Swagger UI at `http://localhost:8000/docs`

### 3️⃣ Start the Worker (Terminal 2)

```bash
python examples/run_local_worker.py
```

**What happens:**
- Worker connects to Kafka at `localhost:9092`
- Consumes from `reflow.jobs` topic
- Waits for jobs to process

### 4️⃣ Send a Job (Distributed Mode)

**Option A: Using Swagger UI**
1. Go to `http://localhost:8000/docs`
2. Find `POST /pipelines/simple_test_pipeline/run`
3. Click "Try it out" → "Execute"
4. Watch the worker terminal for processing!

**Option B: Using cURL**
```bash
curl -X POST http://localhost:8000/pipelines/simple_test_pipeline/run
```

**Option C: Using httpie**
```bash
http POST http://localhost:8000/pipelines/simple_test_pipeline/run
```

### 5️⃣ Watch the Flow

**In Terminal 1 (API):**
```
🚀 Running pipeline: simple_test_pipeline (distributed)
🔄 Splitting source data into jobs...
✓ Dispatched 5 jobs to Kafka topic: reflow.jobs
```

**In Terminal 2 (Worker):**
```
📦 Received job: <batch_id>
🔄 Processing job: 10 records
  🔄 Applying: filter_active_users
  ✓ filter_active_users: 5 records
  🔄 Applying: uppercase_names
  ✓ uppercase_names: 5 records
  🔄 Applying: add_processing_info
  ✓ add_processing_info: 5 records
  📤 Sending 5 records to destination...
✓ Job completed successfully
```

### 6️⃣ Monitor with Kafka UI (Optional)

Open `http://localhost:8080` to:
- See the `reflow.jobs` topic
- Browse messages
- Monitor consumer lag
- View offsets

## Stopping Everything

```bash
# Stop worker (Terminal 2)
Ctrl+C

# Stop API (Terminal 1)
Ctrl+C

# Stop Kafka
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Architecture Flow

```
┌─────────────┐
│    User     │
│  (Swagger)  │
└──────┬──────┘
       │ POST /run
       ▼
┌─────────────────┐
│  Reflowfy API   │  ← Terminal 1
│  (localhost:8000)│
└────────┬────────┘
         │ Produces jobs
         ▼
┌─────────────────┐
│     Kafka       │  ← Docker
│ (localhost:9092)│
│  Topic: reflow  │
│      .jobs      │
└────────┬────────┘
         │ Consumes jobs
         ▼
┌─────────────────┐
│ Reflowfy Worker │  ← Terminal 2
│  (Local Process)│
└────────┬────────┘
         │ Outputs
         ▼
┌─────────────────┐
│    Console      │
│  (prints data)  │
└─────────────────┘
```

## Troubleshooting

**Kafka not starting?**
```bash
docker-compose logs kafka
# Check for port conflicts on 9092
```

**Worker can't connect?**
```bash
# Make sure Kafka is healthy
docker-compose ps
# Wait for health check to pass
```

**No jobs being consumed?**
```bash
# Check Kafka UI at http://localhost:8080
# Look for messages in reflow.jobs topic
```

**Want to reset everything?**
```bash
docker-compose down -v
docker-compose up -d
```

## Next Steps

- Try with multiple workers (start `run_local_worker.py` multiple times)
- Increase data volume in `simple_test_pipeline.py`
- Create your own pipelines!
- Deploy to Kubernetes with the Helm charts
