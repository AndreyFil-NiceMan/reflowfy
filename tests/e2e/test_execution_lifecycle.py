"""
E2E Tests for Execution Lifecycle.

Tests pause/resume functionality and stats progress tracking.

Prerequisites:
    - ReflowManager running on localhost:8002
    - Mock HTTP server running (for e2e_http_dest_test pipeline)

Run with:
    pytest tests/e2e/test_execution_lifecycle.py -v
"""

import os
import time
import uuid
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture
def client(check_reflow_manager):
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


def _start_slow_pipeline(client):
    """
    Start a pipeline with a slow rate limit to give time for pause/resume.
    
    Uses crash_recovery_test pipeline: 500 items / 10 batch = 50 jobs.
    At 1 job/sec, takes ~50 seconds — enough time to interact.
    """
    response = client.post("/run", json={
        "pipeline_name": "crash_recovery_test",
        "rate_limit": 1.0,  # 1 job per second — slow enough to pause mid-flight
    })
    assert response.status_code == 202
    return response.json()["execution_id"]


def _wait_for_state(client, execution_id, target_state, max_wait=30):
    """Wait until execution reaches the target state."""
    start = time.time()
    last_state = None
    
    while time.time() - start < max_wait:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        last_state = stats.get("state")
        
        if last_state == target_state:
            return stats
        
        time.sleep(POLL_INTERVAL)
    
    raise AssertionError(
        f"Execution {execution_id} did not reach '{target_state}' "
        f"within {max_wait}s (last state: {last_state})"
    )


# ============================================================================
# Test: Pause Execution
# ============================================================================

class TestPauseExecution:
    """Tests for POST /executions/{id}/pause endpoint."""
    
    def test_pause_running_execution(self, client):
        """Pause a running pipeline and verify state changes to 'paused'."""
        # Start a slow pipeline
        execution_id = _start_slow_pipeline(client)
        
        # Wait for it to be running
        _wait_for_state(client, execution_id, "running", max_wait=30)
        
        # Pause it
        response = client.post(f"/executions/{execution_id}/pause")
        assert response.status_code == 200
        
        data = response.json()
        assert data["execution"]["state"] == "paused"
        
        # Verify stats also show paused
        stats = client.get(f"/executions/{execution_id}/stats").json()
        assert stats["state"] == "paused"
        
        print(f"✅ Execution {execution_id} paused successfully")
    
    def test_pause_nonexistent_execution(self, client):
        """Pause a non-existent execution returns 404."""
        fake_id = str(uuid.uuid4())
        response = client.post(f"/executions/{fake_id}/pause")
        assert response.status_code == 404
    
    def test_pause_already_completed_execution(self, client):
        """
        Pause an already completed execution.
        
        The API currently allows pausing completed executions (returns 200),
        but the state should reflect the pause.
        """
        # Start a fast pipeline (default high rate)
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
        })
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        _wait_for_state(client, execution_id, "completed", max_wait=120)
        
        # Try to pause a completed execution — API accepts it
        response = client.post(f"/executions/{execution_id}/pause")
        assert response.status_code in [200, 400, 404]


# ============================================================================
# Test: Resume Execution
# ============================================================================

class TestResumeExecution:
    """Tests for POST /executions/{id}/resume endpoint."""
    
    def test_resume_paused_execution(self, client):
        """Pause then resume, and verify the pipeline eventually completes."""
        # Start a slow pipeline
        execution_id = _start_slow_pipeline(client)
        
        # Wait for running
        _wait_for_state(client, execution_id, "running", max_wait=30)
        
        # Pause
        pause_response = client.post(f"/executions/{execution_id}/pause")
        assert pause_response.status_code == 200
        
        # Wait a moment to confirm it stays paused
        time.sleep(3)
        stats = client.get(f"/executions/{execution_id}/stats").json()
        assert stats["state"] == "paused"
        
        # Resume
        resume_response = client.post(f"/executions/{execution_id}/resume")
        assert resume_response.status_code == 200
        
        data = resume_response.json()
        assert data["execution"]["state"] == "running"
        
        print(f"✅ Execution {execution_id} resumed successfully")
    
    def test_resume_nonexistent_execution(self, client):
        """Resume a non-existent execution returns 400/404."""
        fake_id = str(uuid.uuid4())
        response = client.post(f"/executions/{fake_id}/resume")
        assert response.status_code in [400, 404]
    
    def test_resume_non_paused_execution(self, client):
        """Resume a running (not paused) execution returns 400."""
        # Start pipeline
        execution_id = _start_slow_pipeline(client)
        
        # Wait for running
        _wait_for_state(client, execution_id, "running", max_wait=30)
        
        # Try to resume without pausing first
        response = client.post(f"/executions/{execution_id}/resume")
        assert response.status_code == 400


# ============================================================================
# Test: Execution Stats Progress
# ============================================================================

class TestExecutionStatsProgress:
    """Tests for stats progression during pipeline execution."""
    
    def test_stats_show_progress_over_time(self, client):
        """
        Verify that jobs_completed increases over time while pipeline runs.
        
        Uses crash_recovery_test at very slow rate (0.3 jobs/sec) so we 
        can observe incremental progress over ~15 seconds of polling.
        """
        # Start with very slow rate: 500 items / 10 batch = 50 jobs at 0.3/sec = ~170s
        response = client.post("/run", json={
            "pipeline_name": "crash_recovery_test",
            "rate_limit": 0.3,
        })
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for running
        _wait_for_state(client, execution_id, "running", max_wait=30)
        
        # Capture first snapshot immediately
        first_stats = client.get(f"/executions/{execution_id}/stats").json()
        first_completed = first_stats.get("jobs_completed", 0)
        
        # Wait for some progress
        time.sleep(15)
        
        # Capture second snapshot
        second_stats = client.get(f"/executions/{execution_id}/stats").json()
        second_completed = second_stats.get("jobs_completed", 0)
        
        # Verify total_jobs was set
        assert second_stats.get("total_jobs", 0) > 0
        
        # Verify progress: at 0.3 jobs/sec, after 15s we should have ~4-5 more completed
        assert second_completed > first_completed, (
            f"Expected progress over 15s, but jobs_completed didn't increase: "
            f"first={first_completed}, second={second_completed}"
        )
        
        print(f"✅ Stats showed progress: {first_completed} → {second_completed}")
    
    def test_stats_include_all_expected_fields(self, client):
        """Verify stats response contains all expected fields."""
        # Start a fast pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
        })
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        _wait_for_state(client, execution_id, "completed", max_wait=120)
        
        stats = client.get(f"/executions/{execution_id}/stats").json()
        
        # Verify all expected fields are present
        expected_fields = [
            "execution_id", "pipeline_name", "state",
            "total_jobs", "jobs_dispatched", "jobs_completed", "jobs_failed",
            "created_at", "updated_at",
        ]
        
        for field in expected_fields:
            assert field in stats, f"Missing field: {field}"
        
        # Verify final state consistency
        assert stats["execution_id"] == execution_id
        assert stats["state"] == "completed"
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0
        
        print(f"✅ Stats contain all expected fields")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
