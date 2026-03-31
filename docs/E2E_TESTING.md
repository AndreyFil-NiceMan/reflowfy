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

This script will:

1. Build the Reflowfy package.
2. Install the package in the current environment (or venv).
3. Initialize a temporary test workspace.
4. Start all services using `reflowfy run --build`.
5. Run the tests.
6. Cleanup the workspace and build artifacts.

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

The E2E tests run in an isolated workspace using the Reflowfy CLI. To run them manually without the script (for debugging):

```bash
# 1. Build and install package
python3 -m build
pip install dist/*.whl

# 2. Create workspace
mkdir -p e2e_workspace && cd e2e_workspace

# 3. Initialize project
reflowfy init . --name e2e_pipeline

# 4. Copy wheel and patch Dockerfiles
cp ../dist/*.whl .
WHEEL=$(basename ../dist/*.whl)
for f in Dockerfile.api Dockerfile.reflow-manager Dockerfile.worker; do
  sed -i "s|RUN pip install --no-cache-dir reflowfy|COPY $WHEEL /tmp/$WHEEL\nRUN pip install --no-cache-dir /tmp/$WHEEL|" $f
done

# 5. Copy E2E config and pipelines
cp ../docker-compose.e2e.yml docker-compose.yml
cp -r ../tests .
cp -r ../tests/e2e/test_pipelines/* pipelines/

# 6. Run services
reflowfy run --build

# 7. Run tests (from project root)
cd .. && pytest tests/e2e
```

## Service Ports

| Service             | Port | Description                |
| ------------------- | ---- | -------------------------- |
| PostgreSQL (E2E)    | 5433 | Test database              |
| Elasticsearch (E2E) | 9201 | Elasticsearch cluster      |
| Kafka (E2E)         | 9094 | Kafka broker               |
| ReflowManager (E2E) | 8002 | Pipeline manager           |
| Kafka UI (debug)    | 8081 | Kafka monitoring           |
| Mock API Server     | 8092 | For API source tests       |
| Mock HTTP Server    | 8091 | For HTTP destination tests |

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

## Troubleshooting

### Services not starting

Services run in `e2e_workspace`. Only use `docker-compose` inside that directory.

```bash
cd e2e_workspace
docker-compose logs -f
```

### Clean State

Run the runner script again, as it cleans the workspace automatically.
