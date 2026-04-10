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

import httpx
import pytest

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


class TestRateLimiting:
    """Test rate limiting functionality."""

    def test_rate_limit_override(self, client):
        """Test that rate limit override is accepted."""
        response = client.post(
            "/run",
            json={
                "pipeline_name": "e2e_http_dest_test",
                "rate_limit": 5,  # 5 jobs per second
            },
        )

        assert response.status_code == 202


class TestErrorHandling:
    """Test error handling."""

    def test_run_nonexistent_pipeline_returns_404(self, client):
        """Test that running nonexistent pipeline returns 404."""
        response = client.post(
            "/run",
            json={
                "pipeline_name": "nonexistent_pipeline",
            },
        )

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
        response = client.post(
            "/run",
            json={
                # Missing pipeline_name
            },
        )

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
        # crash_recovery_test pipeline has 500 items / 10 batch_size = 50 jobs.
        # Using 0.5 jobs/sec = ~100 seconds total execution time
        response = client.post(
            "/run",
            json={
                "pipeline_name": "crash_recovery_test",
                "rate_limit": 0.5,  # Slow - 1 job every 2 seconds
            },
        )

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        # 2. Wait for it to be running with jobs dispatched
        # We want to restart mid-execution to test recovery
        max_wait = 20
        start = time.time()
        can_proceed = False
        pipeline_state = None

        while time.time() - start < max_wait:
            time.sleep(1)
            try:
                stats = client.get(f"/executions/{execution_id}/stats").json()
                pipeline_state = stats.get("state")
                jobs_dispatched = stats.get("jobs_dispatched", 0)
                jobs_completed = stats.get("jobs_completed", 0)
                print(
                    f"  [DEBUG] Stats: state={pipeline_state}, completed={jobs_completed}, dispatched={jobs_dispatched}, total={stats.get('total_jobs')}"
                )

                # Proceed when running with at least some jobs dispatched
                if pipeline_state == "running" and jobs_dispatched > 0:
                    can_proceed = True
                    break
            except Exception as e:
                print(f"  [DEBUG] Stats error: {e}")
                pass

        assert can_proceed, (
            f"Pipeline did not reach running state with dispatched jobs (last state: {pipeline_state})"
        )

        print("\n⚡ Restarting ReflowManager (simulating crash)...")

        # 3. Restart container
        subprocess.run(
            ["docker", "restart", "reflofy-e2e-reflow-manager"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
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
        # With 50 jobs at 0.5/sec, completion takes up to 100+ seconds after restart
        max_wait = 180
        start = time.time()
        stats = None
        final_state = None

        while time.time() - start < max_wait:
            try:
                stats = client.get(f"/executions/{execution_id}/stats").json()
                final_state = stats.get("state")
                print(
                    f"Status: {final_state}, Completed: {stats.get('jobs_completed')}/{stats.get('total_jobs')}, Dispatched: {stats.get('jobs_dispatched')}"
                )

                if final_state in ["completed", "failed"]:
                    break
            except Exception as e:
                # Might have intermittent connection errors immediately after start
                print(f"Error fetching stats: {e}")
                pass

            time.sleep(POLL_INTERVAL)

        assert final_state == "completed", (
            f"Pipeline failed to complete after recovery (State: {final_state})"
        )

        # Verify stats
        if stats is None:
            assert False, "Stats are None after recovery"
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
