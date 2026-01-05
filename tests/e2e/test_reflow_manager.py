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
REFLOW_MANAGER_URL = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8002")
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
            "pipeline_name": "e2e_http_dest_test",
        })
        
        elapsed = time.time() - start
        
        assert response.status_code == 202
        assert elapsed < 2.0  # Should return quickly
        
        data = response.json()
        assert "execution_id" in data
        assert data["state"] == "pending"
        assert data["pipeline_name"] == "e2e_http_dest_test"
        assert "status_url" in data
    
    def test_simple_pipeline_completes(self, client):
        """Test that simple pipeline runs to completion."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
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
            "pipeline_name": "e2e_http_dest_test",
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
    """Test e2e_elastic_source_test execution."""
    
    def test_run_elastic_pipeline_returns_202(self, client):
        """Test that elastic pipeline starts."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_source_test",
            "runtime_params": {
                "start_time": "2025-01-01T00:00:00",
                "end_time": "2025-12-01T00:00:00",
            },
        })
        
        assert response.status_code == 202
        data = response.json()
        assert data["pipeline_name"] == "e2e_elastic_source_test"
    
    def test_elastic_pipeline_dispatches_jobs(self, client):
        """Test that elastic pipeline dispatches jobs."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_source_test",
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
            "pipeline_name": "e2e_elastic_source_test",
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
            "pipeline_name": "e2e_http_dest_test",
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



class TestCrashRecovery:
    """Test crash recovery mechanisms."""

    def test_pipeline_recovers_after_manager_restart(self, client):
        """
        Test that pipeline execution resumes after manager restart.
        
        1. Start a long-running pipeline (slow rate limit)
        2. Wait for it to start running
        3. Restart ReflowManager container
        4. Wait for ReflowManager to come back up
        5. Verify pipeline resumes and completes
        """
        import subprocess
        
        # 1. Start pipeline with slow rate limit to allow time for restart
        # 100 items / 10 batch_size = 10 batches. 
        # 1 job/sec = 10+ seconds execution time.
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
            "rate_limit": 1.0, 
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # 2. Wait for it to be running and process at least one job
        max_wait = 10
        start = time.time()
        running = False
        
        while time.time() - start < max_wait:
            time.sleep(1)
            try:
                stats = client.get(f"/executions/{execution_id}/stats").json()
                if stats["state"] == "running" and stats["jobs_completed"] > 0:
                    running = True
                    break
            except Exception:
                pass
                
        assert running, "Pipeline did not start running or process jobs in time"
        
        print(f"\n⚡ Restarting ReflowManager (simulating crash)...")
        
        # 3. Restart container
        subprocess.run(
            ["docker", "restart", "reflofy-e2e-reflow-manager"], 
            check=True, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.DEVNULL
        )
        
        print("Waiting for ReflowManager to recover...")
        
        # 4. Wait for service to be healthy again
        # We need a new client or retry loop because connection was broken
        backoff = 1
        restored = False
        for _ in range(15):
            try:
                # Use a specific timeout for connection attempts
                response = httpx.get(f"{REFLOW_MANAGER_URL}/health", timeout=2.0)
                if response.status_code == 200:
                    restored = True
                    break
            except Exception:
                time.sleep(backoff)
                
        assert restored, "ReflowManager failed to recover after restart"
        print("✅ ReflowManager recovered!")
        
        # 5. Verify pipeline completes
        # It might take a moment to resume
        max_wait = 60
        start = time.time()
        final_state = None
        
        while time.time() - start < max_wait:
            try:
                stats = client.get(f"/executions/{execution_id}/stats").json()
                final_state = stats.get("state")
                print(f"Status: {final_state}, Completed: {stats.get('jobs_completed')}/{stats.get('total_jobs')}, Dispatched: {stats.get('jobs_dispatched')}")
                
                if final_state in ["completed", "failed"]:
                    break
            except Exception as e:
                # Might have intermittent connection errors immediately after start
                print(f"Error fetching stats: {e}")
                pass
                
            time.sleep(POLL_INTERVAL)
            
        assert final_state == "completed", f"Pipeline failed to complete after recovery (State: {final_state})"
        
        # Verify stats
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
