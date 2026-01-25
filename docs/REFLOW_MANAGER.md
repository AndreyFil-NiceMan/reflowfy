# ReflowManager Service

## Overview

ReflowManager is a standalone service that provides rate limiting, pipeline state management, and checkpointing for the Reflowfy framework.

## Architecture

```
API (FastAPI)
  ↓ HTTP
ReflowManager Service (FastAPI on port 8001)
  ↓
PostgreSQL (state + checkpoints)
  ↓
Kafka Producer (rate limited) → Kafka (reflow.jobs)
  ↓
Workers
```

## Features

### 1. Rate Limiting
- **Token Bucket Algorithm**: Smooth and efficient rate limiting
- **Global Rate Limiting**: Works across all API instances
- **Per-Pipeline Limits**: Override global rate limit per pipeline
- **Database-Backed**: Tokens stored in PostgreSQL for consistency

### 2. Pipeline State Management
- **Execution Tracking**: Track all pipeline executions with state (pending, running, paused, completed, failed)
- **Job Counting**: Track total jobs dispatched, completed, and failed
- **Error Tracking**: Store error messages for failed executions
- **Runtime Parameters**: Store runtime parameters with each execution

### 3. Checkpointing (Pause/Resume)
- **Batch-Level Checkpoints**: Each job batch has a checkpoint
- **Offset Storage**: Store source-specific offset/cursor data
- **Pause/Resume**: Pause pipelines and resume from last checkpoint
- **State Recovery**: Resume pipelines even after service restarts

### 4. Job Dispatch
- **Rate-Limited Dispatch**: Automatically apply rate limits when sending to Kafka
- **Batch Dispatch**: Send multiple jobs in a batch for efficiency
- **Health Checks**: Verify Kafka connectivity before dispatch

## API Endpoints

### Execution Management

#### Create Execution
```bash
POST /executions
{
  "execution_id": "abc-123",
  "pipeline_name": "my_pipeline",
  "runtime_params": {"start_date": "2024-01-01"}
}
```

#### Get Execution
```bash
GET /executions/{execution_id}
```

Returns:
```json
{
  "execution_id": "abc-123",
  "pipeline_name": "my_pipeline",
  "state": "running",
  "jobs_dispatched": 150,
  "jobs_completed": 120,
  "jobs_failed": 2,
  "created_at": "2024-01-01T12:00:00Z"
}
```

#### Pause Execution
```bash
POST /executions/{execution_id}/pause
```

#### Resume Execution
```bash
POST /executions/{execution_id}/resume
```

### Checkpointing

#### Create Checkpoint
```bash
POST /checkpoints
{
  "execution_id": "abc-123",
  "batch_id": "batch-1",
  "processed_records": 1000
}
```

#### Get Checkpoints
```bash
GET /executions/{execution_id}/checkpoints
GET /executions/{execution_id}/checkpoints?state=completed
```

#### Update Checkpoint (Called by Workers)
```bash
PATCH /checkpoints/{batch_id}
{
  "state": "completed",
  "processed_records": 1000
}
```

### Job Dispatch

#### Dispatch Jobs to Kafka
```bash
POST /dispatch
{
  "execution_id": "abc-123",
  "pipeline_name": "my_pipeline",
  "jobs": [...],
  "rate_limit": 50
}
```

Returns:
```json
{
  "execution_id": "abc-123",
  "total_jobs": 200,
  "dispatched": 150,
  "rate_limited": 50
}
```

### Statistics

#### Global Statistics
```bash
GET /statistics
```

Returns:
```json
{
  "active_executions": 5,
  "total_jobs_dispatched": 10000,
  "total_jobs_completed": 9500,
  "total_jobs_failed": 50
}
```

#### Execution Statistics
```bash
GET /executions/{execution_id}/stats
```

### Health Check
```bash
GET /health
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://reflowfy:reflowfy@localhost:5432/reflowfy` | PostgreSQL connection string |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `reflow.jobs` | Kafka topic for job dispatch |
| `MAX_JOBS_PER_SECOND` | `100` | Global rate limit (jobs/second) |
| `HOST` | `0.0.0.0` | Server host |
| `PORT` | `8001` | Server port |
| `LOG_LEVEL` | `INFO` | Logging level |

### Docker Deployment

```bash
# Start with docker-compose
docker-compose up -d

# Check logs
docker-compose logs -f reflow-manager

# Check health
curl http://localhost:8001/health
```

### Standalone Deployment

```bash
# Set environment variables
export DATABASE_URL=postgresql://user:pass@host:5432/db
export KAFKA_BOOTSTRAP_SERVERS=kafka:9092
export MAX_JOBS_PER_SECOND=100

# Run the service
python -m reflowfy.reflow_manager.app
```

## Database Schema

### Executions Table
```sql
CREATE TABLE executions (
    execution_id VARCHAR(255) PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    state VARCHAR(50) NOT NULL,
    total_jobs INTEGER DEFAULT 0,
    jobs_dispatched INTEGER DEFAULT 0,
    jobs_completed INTEGER DEFAULT 0,
    jobs_failed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    runtime_params JSONB
);
```

### Checkpoints Table
```sql
CREATE TABLE checkpoints (
    id SERIAL PRIMARY KEY,
    execution_id VARCHAR(255) REFERENCES executions(execution_id),
    batch_id VARCHAR(255) NOT NULL,
    offset_data JSONB,
    processed_records INTEGER DEFAULT 0,
    state VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);
```

### Rate Limit State Table
```sql
CREATE TABLE rate_limit_state (
    pipeline_name VARCHAR(255) PRIMARY KEY,
    tokens FLOAT NOT NULL,
    max_tokens FLOAT NOT NULL,
    refill_rate FLOAT NOT NULL,
    last_update TIMESTAMP NOT NULL
);
```

## Pause/Resume Example

### 1. Start a Pipeline
```bash
# Start execution via API
curl -X POST http://localhost:8000/pipelines/my_pipeline/run?start_date=2024-01-01
# Get execution_id from response
```

### 2. Monitor Progress
```bash
# Check execution state
curl http://localhost:8001/executions/abc-123

# Check checkpoints
curl http://localhost:8001/executions/abc-123/checkpoints
```

### 3. Pause Pipeline
```bash
# Pause the execution
curl -X POST http://localhost:8001/executions/abc-123/pause
```

### 4. Resume Pipeline
```bash
# Resume from last checkpoint
curl -X POST http://localhost:8001/executions/abc-123/resume
```

The pipeline will resume from where it left off, without reprocessing completed batches.

## Monitoring

### Health Check
```bash
curl http://localhost:8001/health
```

### Statistics Dashboard
```bash
# Global stats
curl http://localhost:8001/statistics

# Specific execution
curl http://localhost:8001/executions/abc-123/stats
```

### Database Queries
```bash
# Connect to database
docker-compose exec postgres psql -U reflowfy -d reflowfy

# Check active executions
SELECT execution_id, pipeline_name, state, jobs_dispatched, jobs_completed 
FROM executions 
WHERE state IN ('running', 'paused');

# Check rate limit state
SELECT * FROM rate_limit_state;
```

## Troubleshooting

### Service Won't Start

1. Check database connection:
   ```bash
   docker-compose ps postgres
   docker-compose logs postgres
   ```

2. Check database is initialized:
   ```bash
   docker-compose exec postgres psql -U reflowfy -d reflowfy -c "\dt"
   ```

3. Check ReflowManager logs:
   ```bash
   docker-compose logs reflow-manager
   ```

### Jobs Not Dispatching

1. Check rate limit state:
   ```bash
   curl http://localhost:8001/statistics
   ```

2. Check Kafka connection:
   ```bash
   docker-compose logs kafka
   ```

3. Increase rate limit:
   ```bash
   # Update docker-compose.yml
   MAX_JOBS_PER_SECOND: 200
   docker-compose restart reflow-manager
   ```

### Pause/Resume Not Working

1. Check checkpoints exist:
   ```bash
   curl http://localhost:8001/executions/{execution_id}/checkpoints
   ```

2. Check execution state:
   ```bash
   curl http://localhost:8001/executions/{execution_id}
   ```

## Performance

- **Rate Limiting Overhead**: < 1ms per job dispatch (token bucket check)
- **Database Operations**: Batched for efficiency
- **Kafka Dispatch**: Async with batching support
- **Recommended Rate Limit**: 50-500 jobs/second depending on job size

## Security

- Use strong database passwords in production
- Consider adding authentication to ReflowManager API
- Use TLS for database connections
- Network isolation for internal services

## API Documentation

Interactive API documentation available at:
- Swagger UI: `http://localhost:8001/docs`
- ReDoc: `http://localhost:8001/redoc`
