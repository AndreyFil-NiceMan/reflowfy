"""
E2E Tests for API Sources.

Tests IDBasedAPISource by running pipelines against a mock API server.

Prerequisites:
    - Mock API server running on localhost:8092 (run mock_api_server.py)
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/sources/test_api_source.py -v
"""

import os
import time
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="module")
def check_mock_api():
    """Verify mock API server is running."""
    try:
        response = httpx.get(f"{MOCK_API_URL}/health", timeout=5.0)
        if response.status_code != 200:
            pytest.skip(f"Mock API server unhealthy: {response.status_code}")

        print("✅ Mock API server is running")

    except httpx.RequestError as e:
        pytest.skip(f"Mock API server not available at {MOCK_API_URL}: {e}")


class TestMockAPIServer:
    """Test mock API server endpoints directly."""

    def test_health_check(self, check_mock_api):
        """Verify mock API server health."""
        response = httpx.get(f"{MOCK_API_URL}/health", timeout=10.0)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_users_offset_pagination(self, check_mock_api):
        """Test users endpoint with offset pagination."""
        # First page
        response = httpx.get(
            f"{MOCK_API_URL}/users",
            params={"offset": 0, "limit": 10},
            timeout=10.0,
        )
        assert response.status_code == 200
        data = response.json()

        assert len(data["data"]) == 10
        assert data["total"] == 100
        assert data["offset"] == 0
        assert data["has_more"] is True

        # Second page
        response = httpx.get(
            f"{MOCK_API_URL}/users",
            params={"offset": 10, "limit": 10},
            timeout=10.0,
        )
        data = response.json()
        assert data["offset"] == 10
        assert data["data"][0]["id"] == 11

    def test_users_by_id(self, check_mock_api):
        """Test getting user by ID."""
        response = httpx.get(f"{MOCK_API_URL}/users/1", timeout=10.0)
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "User 1"

    def test_users_not_found(self, check_mock_api):
        """Test 404 for non-existent user."""
        response = httpx.get(f"{MOCK_API_URL}/users/999", timeout=10.0)
        assert response.status_code == 404

    def test_products_cursor_pagination(self, check_mock_api):
        """Test products endpoint with cursor pagination."""
        # First page
        response = httpx.get(
            f"{MOCK_API_URL}/products",
            params={"limit": 10},
            timeout=10.0,
        )
        assert response.status_code == 200
        data = response.json()

        assert len(data["data"]) == 10
        assert data["total"] == 50
        assert data["next_cursor"] is not None

        # Second page
        response = httpx.get(
            f"{MOCK_API_URL}/products",
            params={"cursor": data["next_cursor"], "limit": 10},
            timeout=10.0,
        )
        data = response.json()
        assert data["data"][0]["id"] == "prod_11"


class TestIDBasedAPISourceE2E:
    """E2E tests for IDBasedAPISource with pipeline execution."""

    def test_id_based_source_pipeline_starts(self, client, check_mock_api):
        """Test that ID-based API source pipeline can start."""
        response = client.post(
            "/run",
            json={
                "pipeline_name": "e2e_api_id_source_test",
                "runtime_params": {
                    "ids": [1, 2, 3, 4, 5],
                },
            },
        )

        # Pipeline may or may not be registered
        if response.status_code == 404:
            pytest.skip("e2e_api_id_source_test pipeline not registered")

        assert response.status_code == 202
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_api_id_source_test"

    def test_id_based_source_pipeline_creates_jobs(self, client, check_mock_api):
        """Test that ID-based API source creates jobs from ID list."""
        response = client.post(
            "/run",
            json={
                "pipeline_name": "e2e_api_id_source_test",
                "runtime_params": {
                    "ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],  # 10 IDs
                    "batch_size": 2,  # 10 IDs / 2 per batch = 5 jobs
                },
            },
        )

        if response.status_code == 404:
            pytest.skip("e2e_api_id_source_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        # Wait for jobs to be created
        max_wait = 30
        start = time.time()
        total_jobs = 0

        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            total_jobs = stats.get("total_jobs", 0)

            if total_jobs > 0:
                break

            time.sleep(POLL_INTERVAL)

        assert total_jobs > 0, f"Expected jobs to be created, got {total_jobs}"
        print(f"✅ ID-based API source pipeline created {total_jobs} jobs")

    def test_id_based_source_pipeline_completes(self, client, check_mock_api):
        """Test that ID-based API source pipeline runs to completion."""
        response = client.post(
            "/run",
            json={
                "pipeline_name": "e2e_api_id_source_test",
                "runtime_params": {
                    "ids": [1, 2, 3, 4, 5],
                    "batch_size": 5,  # All 5 IDs in 1 job
                },
            },
        )

        if response.status_code == 404:
            pytest.skip("e2e_api_id_source_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        # Wait for completion
        max_wait = 120
        start = time.time()
        final_state = None
        stats = {}

        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            final_state = stats.get("state")

            if final_state in ["completed", "failed"]:
                break

            time.sleep(POLL_INTERVAL)

        # Verify completion
        assert final_state == "completed", f"Expected completed, got {final_state}. Stats: {stats}"

        # Verify job counts
        assert stats["total_jobs"] > 0
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0

        print(
            f"✅ ID-based API source pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
