"""
E2E Test Configuration.

Provides fixtures and configuration for E2E tests.
"""

import os
import pytest

# ============================================================================
# Configuration
# ============================================================================

# Service URLs for E2E tests
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
API_URL = os.getenv("E2E_API_URL", "http://localhost:8003")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201")
SQL_CONNECTION_URL = os.getenv("SQL_CONNECTION_URL", "postgresql://reflowfy:reflowfy@localhost:5433/reflowfy_e2e")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "localhost:9094")
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")


# ============================================================================
# Pytest Configuration
# ============================================================================


@pytest.fixture(autouse=True)
def plain_cli_output(monkeypatch):
    """Render CLI output without ANSI codes, so substring assertions hold.

    The CLI tests assert on `result.stdout` from Typer's CliRunner. Rich forces
    colour whenever FORCE_COLOR is set in the environment — some terminals and
    CI runners set it — and its highlighter colours numbers and quoted strings,
    so an assertion for "3 batches" meets "3\x1b[0m\x1b[1;32m batches" and an
    assertion for "minReplicaCount=2" meets "minReplicaCount=\x1b[1;36m2\x1b[0m".
    Whether these tests pass then depends on who is running them.

    reflowfy.cli.utils builds its Console at import time, and Rich resolves the
    colour system once in the constructor — so clearing the environment only
    affects Consoles built later (commands that make their own). The already-built
    one needs its cached colour system cleared: with no colour system, Rich's
    Style.render returns the text untouched, which is exactly plain output.
    """
    from reflowfy.cli.utils import console

    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setattr(console, "_force_terminal", False, raising=False)
    monkeypatch.setattr(console, "_color_system", None, raising=False)


def pytest_configure(config):
    """Add custom markers."""
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end (requires running services)"
    )
    config.addinivalue_line(
        "markers", "source: mark test as source connector test"
    )
    config.addinivalue_line(
        "markers", "destination: mark test as destination connector test"
    )
    config.addinivalue_line(
        "markers", "elasticsearch: mark test as requiring Elasticsearch"
    )
    config.addinivalue_line(
        "markers", "postgres: mark test as requiring PostgreSQL"
    )
    config.addinivalue_line(
        "markers", "kafka: mark test as requiring Kafka"
    )
    config.addinivalue_line(
        "markers", "dx: mark test as Developer Experience (DX) related"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow-running"
    )
    config.addinivalue_line(
        "markers", "schedule: mark test as pipeline schedule related"
    )

def pytest_collection_modifyitems(config, items):
    """Add e2e marker to all tests in this directory."""
    for item in items:
        if "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)
        
        # Add specific markers based on test file
        fspath_str = str(item.fspath)
        if "sources" in fspath_str:
            item.add_marker(pytest.mark.source)
        if "destinations" in fspath_str:
            item.add_marker(pytest.mark.destination)
        if "elastic" in fspath_str:
            item.add_marker(pytest.mark.elasticsearch)
        if "sql" in fspath_str:
            item.add_marker(pytest.mark.postgres)
        if "kafka" in fspath_str:
            item.add_marker(pytest.mark.kafka)
        if "test_auto_registration" in fspath_str or "test_decorator" in fspath_str or "test_cli" in fspath_str:
            item.add_marker(pytest.mark.dx)
        if "test_schedule" in fspath_str:
            item.add_marker(pytest.mark.schedule)


# ============================================================================
# Session-Scoped Fixtures
# ============================================================================

@pytest.fixture(scope="session")
def check_reflow_manager():
    """Check that ReflowManager is running."""
    import httpx
    
    try:
        response = httpx.get(f"{REFLOW_MANAGER_URL}/health", timeout=5.0)
        if response.status_code != 200:
            pytest.skip(f"ReflowManager unhealthy: {response.status_code}")
        
        print(f"✅ ReflowManager is running at {REFLOW_MANAGER_URL}")
        
    except httpx.RequestError as e:
        pytest.skip(f"ReflowManager not available at {REFLOW_MANAGER_URL}: {e}")


@pytest.fixture(scope="session")
def check_elasticsearch():
    """Check that Elasticsearch is available."""
    from elasticsearch import Elasticsearch
    
    try:
        es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
        health = es.cluster.health()
        
        if health["status"] not in ["green", "yellow"]:
            pytest.skip(f"Elasticsearch cluster unhealthy: {health['status']}")
        
        print(f"✅ Elasticsearch is running at {ELASTICSEARCH_URL}")
        es.close()
        
    except Exception as e:
        pytest.skip(f"Elasticsearch not available at {ELASTICSEARCH_URL}: {e}")


@pytest.fixture(scope="session")
def check_postgres():
    """Check that PostgreSQL is available."""
    from sqlalchemy import create_engine, text
    
    try:
        engine = create_engine(SQL_CONNECTION_URL)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print("✅ PostgreSQL is running")
        engine.dispose()
        
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


@pytest.fixture(scope="session")
def check_kafka():
    """Check that Kafka is available."""
    from confluent_kafka.admin import AdminClient
    
    try:
        admin_client = AdminClient({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        })
        
        admin_client.list_topics(timeout=5.0)
        print(f"✅ Kafka is running at {KAFKA_BOOTSTRAP_SERVERS}")
        
    except Exception as e:
        pytest.skip(f"Kafka not available at {KAFKA_BOOTSTRAP_SERVERS}: {e}")


@pytest.fixture(scope="session")
def check_mock_api():
    """Check that mock API server is running."""
    import httpx
    
    try:
        response = httpx.get(f"{MOCK_API_URL}/health", timeout=5.0)
        if response.status_code != 200:
            pytest.skip("Mock API server unhealthy")
        
        print(f"✅ Mock API server is running at {MOCK_API_URL}")
        
    except httpx.RequestError as e:
        pytest.skip(f"Mock API server not available at {MOCK_API_URL}: {e}")


@pytest.fixture(scope="session")
def check_mock_http():
    """Check that mock HTTP server is running."""
    import httpx
    
    try:
        response = httpx.get(f"{MOCK_HTTP_URL}/health", timeout=5.0)
        if response.status_code != 200:
            pytest.skip("Mock HTTP server unhealthy")
        
        print(f"✅ Mock HTTP server is running at {MOCK_HTTP_URL}")
        
    except httpx.RequestError as e:
        pytest.skip(f"Mock HTTP server not available at {MOCK_HTTP_URL}: {e}")


# ============================================================================
# Utility Fixtures
# ============================================================================

@pytest.fixture
def reflow_client(check_reflow_manager):
    """HTTP client for ReflowManager API."""
    import httpx
    
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
        yield client


@pytest.fixture
def wait_for_pipeline_completion():
    """Factory fixture to wait for pipeline completion."""
    import time
    import httpx
    
    def _wait(execution_id: str, max_wait: int = 120, poll_interval: int = 2):
        """Wait for pipeline to complete and return final stats."""
        with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
            start = time.time()
            
            while time.time() - start < max_wait:
                stats = client.get(f"/executions/{execution_id}/stats").json()
                state = stats.get("state")
                
                if state in ["completed", "failed"]:
                    return stats
                
                time.sleep(poll_interval)
            
            raise TimeoutError(f"Pipeline {execution_id} did not complete within {max_wait}s")
    
    return _wait
