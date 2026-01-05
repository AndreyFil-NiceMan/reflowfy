# E2E Testing Guide for Reflofy

This guide covers how to run end-to-end tests for the Reflofy framework, including source and destination connector tests.

## Quick Start

```bash
# Run all E2E tests
./scripts/run_e2e_tests.sh

# Run only source tests
./scripts/run_e2e_tests.sh sources

# Run only destination tests
./scripts/run_e2e_tests.sh destinations
```

## Test Structure

```
tests/e2e/
├── conftest.py                 # Shared fixtures and configuration
├── sources/                    # Source connector tests
│   ├── init_elastic_test_data.py   # Initialize Elasticsearch data
│   ├── init_sql_test_data.py       # Initialize PostgreSQL data
│   ├── mock_api_server.py          # Mock API for ApiSource tests
│   ├── test_elastic_source.py      # Elasticsearch source tests
│   ├── test_sql_source.py          # SQL source tests
│   └── test_api_source.py          # API source tests
├── destinations/               # Destination connector tests
│   ├── mock_http_server.py         # Mock webhook server
│   ├── test_http_destination.py    # HTTP destination tests
│   └── test_kafka_destination.py   # Kafka destination tests
└── test_pipelines/             # Test pipeline definitions
    ├── __init__.py
    ├── elastic_source_test_pipeline.py
    ├── sql_source_test_pipeline.py
    ├── api_source_test_pipeline.py
    ├── http_dest_test_pipeline.py
    └── kafka_dest_test_pipeline.py
```

## Prerequisites

### Option 1: Use the Test Runner (Recommended)

The test runner script handles everything:

```bash
./scripts/run_e2e_tests.sh
```

### Option 2: Manual Setup

1. **Start Docker Compose services:**
   ```bash
   docker-compose -f docker-compose.e2e.yml up -d
   ```

2. **Wait for services to be healthy:**
   ```bash
   # Check PostgreSQL
   docker-compose -f docker-compose.e2e.yml exec e2e-postgres pg_isready
   
   # Check Elasticsearch
   curl http://localhost:9201/_cluster/health
   
   # Check ReflowManager
   curl http://localhost:8002/health
   ```

3. **Initialize test data:**
   ```bash
   python -m tests.e2e.sources.init_sql_test_data
   python -m tests.e2e.sources.init_elastic_test_data
   ```

4. **Start mock servers (in separate terminals):**
   ```bash
   # Mock API server (for API source tests)
   python -m tests.e2e.sources.mock_api_server
   
   # Mock HTTP server (for HTTP destination tests)
   python -m tests.e2e.destinations.mock_http_server
   ```

5. **Run tests:**
   ```bash
   pytest tests/e2e/ -v
   ```

## Service Ports

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL (E2E) | 5433 | Test database |
| Elasticsearch (E2E) | 9201 | Elasticsearch cluster |
| Kafka (E2E) | 9094 | Kafka broker |
| ReflowManager (E2E) | 8002 | Pipeline manager |
| Kafka UI (debug) | 8081 | Kafka monitoring |
| Mock API Server | 8090 | For API source tests |
| Mock HTTP Server | 8091 | For HTTP destination tests |

## Running Individual Tests

```bash
# Elasticsearch source tests
pytest tests/e2e/sources/test_elastic_source.py -v

# SQL source tests
pytest tests/e2e/sources/test_sql_source.py -v

# API source tests
pytest tests/e2e/sources/test_api_source.py -v

# HTTP destination tests
pytest tests/e2e/destinations/test_http_destination.py -v

# Kafka destination tests
pytest tests/e2e/destinations/test_kafka_destination.py -v
```

## Adding New Tests

### Adding a New Source Test

1. **Create the test pipeline** in `tests/e2e/test_pipelines/`:
   ```python
   # my_source_test_pipeline.py
   from reflowfy import build_pipeline, pipeline_registry
   from reflowfy.destinations.console import console_destination
   
   source = my_source(...)  # Your source configuration
   destination = console_destination()
   
   pipeline = build_pipeline(
       name="e2e_my_source_test",
       source=source,
       transformations=[...],
       destination=destination,
   )
   
   pipeline_registry.register(pipeline)
   ```

2. **Add import to `__init__.py`:**
   ```python
   from tests.e2e.test_pipelines.my_source_test_pipeline import pipeline as my_source_pipeline
   ```

3. **Create the test file** in `tests/e2e/sources/`:
   ```python
   # test_my_source.py
   def test_pipeline_completes(self, client):
       response = client.post("/run", json={
           "pipeline_name": "e2e_my_source_test",
       })
       # ... verify completion
   ```

### Adding a New Destination Test

1. **Create the test pipeline** with `MockSource`:
   ```python
   from reflowfy.sources.mock import mock_source, generate_sample_data
   
   source = mock_source(data=generate_sample_data(100), batch_size=10)
   destination = my_destination(...)  # Your destination
   ```

2. **Create verification mechanism** (mock server, consumer, etc.)

3. **Write tests** that verify data arrives at destination

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `E2E_REFLOW_MANAGER_URL` | `http://localhost:8002` | ReflowManager URL |
| `ELASTICSEARCH_URL` | `http://localhost:9201` | Elasticsearch URL |
| `SQL_CONNECTION_URL` | `postgresql://reflowfy:reflowfy@localhost:5433/reflowfy_e2e` | PostgreSQL URL |
| `E2E_KAFKA_SERVERS` | `localhost:9094` | Kafka bootstrap servers |
| `MOCK_API_URL` | `http://localhost:8090` | Mock API server URL |
| `MOCK_HTTP_URL` | `http://localhost:8091` | Mock HTTP server URL |

## Troubleshooting

### Services not starting
```bash
# Check Docker Compose logs
docker-compose -f docker-compose.e2e.yml logs -f

# Check specific service
docker-compose -f docker-compose.e2e.yml logs e2e-reflow-manager
```

### Tests skipped
Tests are automatically skipped if required services are unavailable. Check the skip messages for details.

### Cleanup
```bash
# Stop all services
docker-compose -f docker-compose.e2e.yml down

# Remove volumes (clean slate)
docker-compose -f docker-compose.e2e.yml down -v
```
