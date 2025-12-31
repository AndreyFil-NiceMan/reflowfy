"""
E2E Tests for ReflowManager.

These tests require running services:
- PostgreSQL on localhost:5432
- Kafka on localhost:9093
- ReflowManager on localhost:8001
- Worker(s)

Run with: pytest tests/e2e/test_reflow_manager.py -v
"""

import os
import time
import uuid
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")
TIMEOUT = 60.0
POLL_INTERVAL = 2


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


class TestSimplePipeline:
    """Test simple_test_pipeline execution."""
    
    def test_run_simple_pipeline_returns_202(self, client):
        """Test that /run returns 202 Accepted immediately."""
        start = time.time()
        
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
        })
        
        elapsed = time.time() - start
        
        assert response.status_code == 202
        assert elapsed < 2.0  # Should return quickly
        
        data = response.json()
        assert "execution_id" in data
        assert data["state"] == "pending"
        assert data["pipeline_name"] == "simple_test_pipeline"
        assert "status_url" in data
    
    def test_simple_pipeline_completes(self, client):
        """Test that simple pipeline runs to completion."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        max_wait = 60
        start = time.time()
        final_state = None
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            final_state = stats.get("state")
            
            if final_state in ["completed", "failed"]:
                break
            
            time.sleep(POLL_INTERVAL)
        
        # Verify completion
        assert final_state == "completed", f"Expected completed, got {final_state}"
        
        # Verify counts
        assert stats["total_jobs"] > 0
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0
        assert stats["jobs_pending"] == 0
    
    def test_simple_pipeline_stats_format(self, client):
        """Test that stats endpoint returns correct format."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
        })
        
        execution_id = response.json()["execution_id"]
        
        # Wait a bit for jobs to be created
        time.sleep(3)
        
        # Get stats
        stats = client.get(f"/executions/{execution_id}/stats").json()
        
        # Verify new flat format
        assert "execution_id" in stats
        assert "pipeline_name" in stats
        assert "state" in stats
        assert "total_jobs" in stats
        assert "jobs_dispatched" in stats
        assert "jobs_pending" in stats
        assert "jobs_completed" in stats
        assert "jobs_failed" in stats
        assert "current_checkpoint" in stats
        assert "created_at" in stats
        assert "updated_at" in stats
        assert "checkpoints" in stats


class TestElasticPipeline:
    """Test elastic_test_pipeline execution."""
    
    def test_run_elastic_pipeline_returns_202(self, client):
        """Test that elastic pipeline starts."""
        response = client.post("/run", json={
            "pipeline_name": "elastic_test_pipeline",
            "runtime_params": {
                "start_time": "2025-01-01T00:00:00",
                "end_time": "2025-12-01T00:00:00",
            },
        })
        
        assert response.status_code == 202
        data = response.json()
        assert data["pipeline_name"] == "elastic_test_pipeline"
    
    def test_elastic_pipeline_dispatches_jobs(self, client):
        """Test that elastic pipeline dispatches jobs."""
        response = client.post("/run", json={
            "pipeline_name": "elastic_test_pipeline",
            "runtime_params": {
                "start_time": "2025-01-01T00:00:00",
                "end_time": "2025-12-01T00:00:00",
            },
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for jobs to be dispatched
        max_wait = 60
        start = time.time()
        jobs_dispatched = 0
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            jobs_dispatched = stats.get("jobs_dispatched", 0)
            
            if jobs_dispatched > 0:
                break
            
            time.sleep(POLL_INTERVAL)
        
        assert jobs_dispatched > 0, f"Expected jobs dispatched, got {jobs_dispatched}"
    
    def test_elastic_pipeline_checkpoints(self, client):
        """Test that elastic pipeline creates checkpoints."""
        response = client.post("/run", json={
            "pipeline_name": "elastic_test_pipeline",
            "runtime_params": {
                "start_time": "2025-01-01T00:00:00",
                "end_time": "2025-12-01T00:00:00",
            },
        })
        
        execution_id = response.json()["execution_id"]
        
        # Wait for checkpoints to be created
        max_wait = 30
        start = time.time()
        checkpoints = []
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            checkpoints = stats.get("checkpoints", [])
            
            if len(checkpoints) > 0:
                break
            
            time.sleep(POLL_INTERVAL)
        
        assert len(checkpoints) > 0, "Expected checkpoints to be created"
        
        # Verify checkpoint format
        first_checkpoint = checkpoints[0]
        assert "batch_number" in first_checkpoint
        assert "total_jobs" in first_checkpoint
        assert "state" in first_checkpoint


class TestRateLimiting:
    """Test rate limiting functionality."""
    
    def test_rate_limit_override(self, client):
        """Test that rate limit override is accepted."""
        response = client.post("/run", json={
            "pipeline_name": "simple_test_pipeline",
            "rate_limit": 5,  # 5 jobs per second
        })
        
        assert response.status_code == 202


class TestErrorHandling:
    """Test error handling."""
    
    def test_run_nonexistent_pipeline_returns_404(self, client):
        """Test that running nonexistent pipeline returns 404."""
        response = client.post("/run", json={
            "pipeline_name": "nonexistent_pipeline",
        })
        
        assert response.status_code == 404
    
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
    
    def test_get_nonexistent_execution_stats_returns_404(self, client):
        """Test that getting stats for nonexistent execution returns 404."""
        response = client.get("/executions/nonexistent-id/stats")
        
        assert response.status_code == 404


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
