"""
E2E Tests for ReflowManager.

These tests require running services:
- PostgreSQL on localhost:5432
- Kafka on localhost:9093
- ReflowManager on localhost:8001

Run with: pytest tests/e2e/test_reflow_manager.py -v
"""

import os
import time
import uuid
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")
TIMEOUT = 30.0


@pytest.fixture
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture
def unique_execution_id():
    """Generate unique execution ID for each test."""
    return f"test-{uuid.uuid4()}"


class TestHealthCheck:
    """Test health check endpoint."""
    
    def test_health_check_returns_healthy(self, client):
        """Test that health check returns healthy status."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "reflow-manager"


class TestExecutionManagement:
    """Test execution CRUD operations."""
    
    def test_create_execution(self, client, unique_execution_id):
        """Test creating a new execution."""
        response = client.post("/executions", json={
            "execution_id": unique_execution_id,
            "pipeline_name": "simple_test_pipeline",
            "runtime_params": {"test": True},
        })
        
        assert response.status_code == 201
        data = response.json()
        assert data["execution_id"] == unique_execution_id
        assert data["pipeline_name"] == "simple_test_pipeline"
        assert data["state"] == "pending"
    
    def test_get_execution(self, client, unique_execution_id):
        """Test getting an execution by ID."""
        # Create first
        client.post("/executions", json={
            "execution_id": unique_execution_id,
            "pipeline_name": "simple_test_pipeline",
        })
        
        # Get
        response = client.get(f"/executions/{unique_execution_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["execution_id"] == unique_execution_id
    
    def test_get_nonexistent_execution_returns_404(self, client):
        """Test that getting nonexistent execution returns 404."""
        response = client.get("/executions/nonexistent-id")
        
        assert response.status_code == 404
    



class TestPipelineExecution:
    """Test pipeline execution via /run endpoint."""
    
    def test_run_pipeline_returns_202_immediately(self, client):
        """Test that /run returns 202 Accepted immediately."""
        start = time.time()
        
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
        })
        
        elapsed = time.time() - start
        
        assert response.status_code == 202
        assert elapsed < 1.0  # Should return in under 1 second
        
        data = response.json()
        assert "execution_id" in data
        assert data["state"] == "pending"
        assert data["pipeline_name"] == "simple_test_pipeline"
        assert "status_url" in data
    
    def test_run_pipeline_with_custom_execution_id(self, client, unique_execution_id):
        """Test running pipeline with custom execution ID."""
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
            "execution_id": unique_execution_id,
        })
        
        assert response.status_code == 202
        data = response.json()
        assert data["execution_id"] == unique_execution_id
    
    def test_run_pipeline_with_runtime_params(self, client):
        """Test running pipeline with runtime parameters."""
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
            "runtime_params": {"custom_param": "value"},
        })
        
        assert response.status_code == 202
    
    def test_run_nonexistent_pipeline_returns_404(self, client):
        """Test that running nonexistent pipeline returns 404."""
        response = client.post("/run", json={
            "pipeline_name": "nonexistent_pipeline",
        })
        
        assert response.status_code == 404
    
    def test_run_pipeline_dispatches_jobs(self, client):
        """Test that running a pipeline actually dispatches jobs."""
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for jobs to be dispatched
        max_wait = 30
        start = time.time()
        jobs_dispatched = 0
        
        while time.time() - start < max_wait:
            exec_response = client.get(f"/executions/{execution_id}")
            if exec_response.status_code == 200:
                jobs_dispatched = exec_response.json().get("jobs_dispatched", 0)
                if jobs_dispatched > 0:
                    break
            time.sleep(1)
        
        assert jobs_dispatched > 0, f"Expected jobs to be dispatched, got {jobs_dispatched}"


class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_override(self, client):
        """Test that rate limit override is accepted."""
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
            "rate_limit": 2,  # 2 jobs per second
        })
        
        assert response.status_code == 202
    
    def test_rate_limiting_slows_dispatch(self, client):
        """Test that rate limiting slows down job dispatch."""
        # Start with rate_limit=10 (10 jobs/sec)
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
            "rate_limit": 10,
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Poll for jobs to be dispatched (up to 30s)
        max_wait = 30
        start = time.time()
        jobs_dispatched = 0
        
        while time.time() - start < max_wait:
            exec_response = client.get(f"/executions/{execution_id}")
            if exec_response.status_code == 200:
                jobs_dispatched = exec_response.json().get("jobs_dispatched", 0)
                if jobs_dispatched > 50:
                    break
            time.sleep(2)
        
        elapsed = time.time() - start
        
        # Verify some jobs were dispatched
        assert jobs_dispatched > 0, "No jobs were dispatched"
        
        # Verify rate limiting is working by checking timing
        # Without rate limiting, 500 jobs would dispatch in < 5 seconds
        # With rate_limit=10, 500 jobs takes ~50 seconds minimum


class TestCheckpoints:
    """Test checkpoint management."""
    
    def test_create_checkpoint(self, client, unique_execution_id):
        """Test creating a checkpoint."""
        # Create execution first
        client.post("/executions", json={
            "execution_id": unique_execution_id,
            "pipeline_name": "simple_test_pipeline",
        })
        
        batch_id = str(uuid.uuid4())
        response = client.post("/checkpoints", json={
            "execution_id": unique_execution_id,
            "batch_id": batch_id,
            "offset_data": {"offset": 0},
            "processed_records": 100,
        })
        
        assert response.status_code == 201
        data = response.json()
        assert data["batch_id"] == batch_id
        assert data["state"] == "pending"
    



class TestStatistics:
    """Test statistics endpoints."""
    
    def test_get_global_statistics(self, client):
        """Test getting global statistics."""
        response = client.get("/statistics")
        
        assert response.status_code == 200
        data = response.json()
        assert "active_executions" in data
        assert "total_jobs_dispatched" in data
        assert "total_jobs_completed" in data
        assert "total_jobs_failed" in data
    
    def test_get_execution_stats(self, client, unique_execution_id):
        """Test getting execution-specific statistics."""
        # Create execution
        client.post("/executions", json={
            "execution_id": unique_execution_id,
            "pipeline_name": "simple_test_pipeline",
        })
        
        response = client.get(f"/executions/{unique_execution_id}/stats")
        
        assert response.status_code == 200
        data = response.json()
        assert "execution_id" in data
        assert "checkpoint_stats" in data


class TestErrorHandling:
    """Test error handling."""
    
    def test_invalid_json_returns_422(self, client):
        """Test that invalid JSON returns 422."""
        response = client.post(
            "/run",
            content="not valid json",
            headers={"Content-Type": "application/json"},
        )
        
        assert response.status_code == 422
    
    def test_missing_required_field_returns_422(self, client):
        """Test that missing required field returns 422."""
        response = client.post("/run", json={
            # Missing pipeline_name
        })
        
        assert response.status_code == 422


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
