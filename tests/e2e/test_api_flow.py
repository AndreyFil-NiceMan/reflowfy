"""
E2E Tests for the complete API to ReflowManager flow.

These tests verify the integration between:
- API service (localhost:8000)
- ReflowManager service (localhost:8001)

Run with: pytest tests/e2e/test_api_flow.py -v
"""

import os
import time
import uuid
import pytest
import httpx

# Configuration
API_URL = os.getenv("API_URL", "http://localhost:8000")
REFLOW_MANAGER_URL = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")
TIMEOUT = 30.0


@pytest.fixture
def api_client():
    """HTTP client for API service."""
    with httpx.Client(base_url=API_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture
def reflow_manager_client():
    """HTTP client for ReflowManager service."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


class TestAPIHealth:
    """Test API health check."""
    
    def test_api_health_check(self, api_client):
        """Test API health check endpoint."""
        response = api_client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"


class TestPipelineDiscovery:
    """Test pipeline discovery in API."""
    
    def test_list_pipelines(self, api_client):
        """Test listing available pipelines."""
        response = api_client.get("/pipelines")
        
        assert response.status_code == 200
        data = response.json()
        assert "pipelines" in data
        
        # Should have at least simple_test_pipeline
        pipeline_names = [p["name"] for p in data["pipelines"]]
        assert "simple_test_pipeline" in pipeline_names


class TestDistributedExecution:
    """Test distributed pipeline execution via API."""
    
    def test_run_pipeline_via_api(self, api_client):
        """Test running a pipeline via the API."""
        response = api_client.post("/pipelines/simple_test_pipeline/run")
        
        assert response.status_code == 200
        data = response.json()
        assert "execution_id" in data
        assert data["mode"] == "distributed"
        assert data["pipeline_name"] == "simple_test_pipeline"
    
    def test_run_pipeline_with_rate_limit(self, api_client):
        """Test running pipeline with rate limit override."""
        response = api_client.post(
            "/pipelines/simple_test_pipeline/run",
            params={"rate_limit": 5},
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "execution_id" in data
    
    def test_run_nonexistent_pipeline_via_api(self, api_client):
        """Test that running nonexistent pipeline returns 404."""
        response = api_client.post("/pipelines/nonexistent/run")
        
        assert response.status_code == 404


class TestAPIToReflowManagerFlow:
    """Test the complete flow from API to ReflowManager."""
    
    def test_api_creates_execution_in_reflow_manager(
        self, api_client, reflow_manager_client
    ):
        """Test that API run creates execution in ReflowManager."""
        # Run via API
        api_response = api_client.post("/pipelines/simple_test_pipeline/run")
        
        assert api_response.status_code == 200
        execution_id = api_response.json()["execution_id"]
        
        # Wait a moment for async dispatch
        time.sleep(2)
        
        # Verify execution exists in ReflowManager
        rm_response = reflow_manager_client.get(f"/executions/{execution_id}")
        
        assert rm_response.status_code == 200
        data = rm_response.json()
        assert data["execution_id"] == execution_id
        assert data["pipeline_name"] == "simple_test_pipeline"
    
    def test_jobs_dispatched_to_kafka(self, api_client, reflow_manager_client):
        """Test that jobs are dispatched to Kafka via ReflowManager."""
        # Run via API
        api_response = api_client.post("/pipelines/simple_test_pipeline/run")
        
        assert api_response.status_code == 200
        execution_id = api_response.json()["execution_id"]
        
        # Wait for jobs to be dispatched
        max_wait = 30
        start = time.time()
        jobs_dispatched = 0
        
        while time.time() - start < max_wait:
            rm_response = reflow_manager_client.get(f"/executions/{execution_id}")
            if rm_response.status_code == 200:
                jobs_dispatched = rm_response.json().get("jobs_dispatched", 0)
                if jobs_dispatched > 0:
                    break
            time.sleep(1)
        
        assert jobs_dispatched > 0, "No jobs were dispatched"
    
    def test_rate_limit_passed_from_api_to_reflow_manager(
        self, api_client, reflow_manager_client
    ):
        """Test that rate limit is properly passed from API to ReflowManager."""
        # Run with slow rate limit
        api_response = api_client.post(
            "/pipelines/simple_test_pipeline/run",
            params={"rate_limit": 3},  # 3 jobs/sec
        )
        
        assert api_response.status_code == 200
        execution_id = api_response.json()["execution_id"]
        
        # Wait 5 seconds
        time.sleep(5)
        
        # Check jobs dispatched
        rm_response = reflow_manager_client.get(f"/executions/{execution_id}")
        jobs_dispatched = rm_response.json().get("jobs_dispatched", 0)
        
        # With rate_limit=3, after 5 seconds should have ~15 jobs (±10 for variance)
        # But definitely not 500 (full pipeline)
        assert jobs_dispatched < 50, f"Rate limiting not applied: {jobs_dispatched} jobs in 5s"


class TestLocalExecution:
    """Test local (test) mode execution."""
    
    def test_test_pipeline_local_mode(self, api_client):
        """Test running pipeline in local/test mode."""
        response = api_client.post("/pipelines/simple_test_pipeline/test")
        
        assert response.status_code == 200
        data = response.json()
        assert data["mode"] == "local"
        assert data["pipeline_name"] == "simple_test_pipeline"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
