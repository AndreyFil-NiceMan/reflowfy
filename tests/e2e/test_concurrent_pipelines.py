"""
E2E Tests for Concurrent Pipeline Executions.

Tests that the system correctly handles multiple pipelines running
simultaneously and the same pipeline running with different params.

Prerequisites:
    - ReflowManager running on localhost:8002
    - All test infrastructure (Kafka, Postgres, Elasticsearch, mock servers)

Run with:
    pytest tests/e2e/test_concurrent_pipelines.py -v
"""

import os
import time
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
TIMEOUT = 60.0
POLL_INTERVAL = 3


@pytest.fixture
def client(check_reflow_manager):
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


def _wait_for_completion(client, execution_id, max_wait=180):
    """Wait for a pipeline execution to complete."""
    start = time.time()
    
    while time.time() - start < max_wait:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        state = stats.get("state")
        
        if state in ["completed", "failed"]:
            return stats
        
        time.sleep(POLL_INTERVAL)
    
    raise TimeoutError(
        f"Execution {execution_id} did not complete within {max_wait}s"
    )


class TestConcurrentDifferentPipelines:
    """Test running two different pipelines simultaneously."""
    
    def test_two_different_pipelines_complete_independently(self, client):
        """
        Start HTTP-dest and Kafka-dest pipelines concurrently.
        Both should complete independently without interfering.
        """
        # Reset mock HTTP server
        try:
            httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
        except Exception:
            pass
        
        # Start both pipelines
        response_http = client.post("/run", json={
            "pipeline_name": "e2e_api_dest_test",
            "runtime_params": {"tenant_id": "concurrent-test", "env": "staging"},
        })
        assert response_http.status_code == 202
        http_exec_id = response_http.json()["execution_id"]
        
        response_kafka = client.post("/run", json={
            "pipeline_name": "e2e_kafka_dest_test",
        })
        assert response_kafka.status_code == 202
        kafka_exec_id = response_kafka.json()["execution_id"]
        
        # Verify they got different execution IDs
        assert http_exec_id != kafka_exec_id
        
        print(f"Started HTTP pipeline: {http_exec_id}")
        print(f"Started Kafka pipeline: {kafka_exec_id}")
        
        # Wait for both to complete
        http_stats = _wait_for_completion(client, http_exec_id)
        kafka_stats = _wait_for_completion(client, kafka_exec_id)
        
        # Verify both completed successfully
        assert http_stats["state"] == "completed", (
            f"HTTP pipeline failed: {http_stats}"
        )
        assert kafka_stats["state"] == "completed", (
            f"Kafka pipeline failed: {kafka_stats}"
        )
        
        # Verify both processed their jobs
        assert http_stats["jobs_completed"] == http_stats["total_jobs"]
        assert http_stats["jobs_failed"] == 0
        
        assert kafka_stats["jobs_completed"] == kafka_stats["total_jobs"]
        assert kafka_stats["jobs_failed"] == 0
        
        print(
            f"✅ Both pipelines completed: "
            f"HTTP={http_stats['jobs_completed']} jobs, "
            f"Kafka={kafka_stats['jobs_completed']} jobs"
        )


class TestConcurrentSamePipeline:
    """Test running the same pipeline twice with different executions."""
    
    def test_same_pipeline_twice_gets_unique_executions(self, client):
        """
        Start the same pipeline twice — both should get unique execution IDs
        and complete independently.
        """
        # Start the same pipeline twice
        response_1 = client.post("/run", json={
            "pipeline_name": "e2e_api_dest_test",
            "runtime_params": {"tenant_id": "concurrent-run-1", "env": "staging"},
        })
        assert response_1.status_code == 202
        exec_id_1 = response_1.json()["execution_id"]

        response_2 = client.post("/run", json={
            "pipeline_name": "e2e_api_dest_test",
            "runtime_params": {"tenant_id": "concurrent-run-2", "env": "staging"},
        })
        assert response_2.status_code == 202
        exec_id_2 = response_2.json()["execution_id"]
        
        # Must be different execution IDs
        assert exec_id_1 != exec_id_2
        
        # Both should have the same pipeline name
        assert response_1.json()["pipeline_name"] == "e2e_api_dest_test"
        assert response_2.json()["pipeline_name"] == "e2e_api_dest_test"
        
        # Wait for both to complete
        stats_1 = _wait_for_completion(client, exec_id_1)
        stats_2 = _wait_for_completion(client, exec_id_2)
        
        # Both should complete
        assert stats_1["state"] == "completed", f"Exec 1 failed: {stats_1}"
        assert stats_2["state"] == "completed", f"Exec 2 failed: {stats_2}"
        
        assert stats_1["jobs_completed"] == stats_1["total_jobs"]
        assert stats_2["jobs_completed"] == stats_2["total_jobs"]
        
        print(
            f"✅ Same pipeline ran twice independently: "
            f"exec1={stats_1['jobs_completed']} jobs, "
            f"exec2={stats_2['jobs_completed']} jobs"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
