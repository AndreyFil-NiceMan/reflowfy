"""
E2E Test Configuration.

Provides fixtures and configuration for E2E tests.
"""

import os
import pytest

# Check if services are available before running tests
def pytest_configure(config):
    """Add custom markers."""
    config.addinivalue_line(
        "markers", "e2e: mark test as end-to-end (requires running services)"
    )


def pytest_collection_modifyitems(config, items):
    """Add e2e marker to all tests in this directory."""
    for item in items:
        if "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)


@pytest.fixture(scope="session")
def check_services():
    """Check that required services are running."""
    import httpx
    
    services = {
        "ReflowManager": os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001"),
        "API": os.getenv("API_URL", "http://localhost:8000"),
    }
    
    missing = []
    for name, url in services.items():
        try:
            response = httpx.get(f"{url}/health", timeout=5.0)
            if response.status_code != 200:
                missing.append(f"{name} ({url}) returned status {response.status_code}")
        except httpx.RequestError as e:
            missing.append(f"{name} ({url}) - {e}")
    
    if missing:
        pytest.skip(f"Required services not available: {', '.join(missing)}")


@pytest.fixture(autouse=True)
def require_services(check_services):
    """Automatically check services for all tests."""
    pass
