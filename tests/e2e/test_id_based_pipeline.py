"""
E2E Tests for IdBasedPipeline Feature.

Tests the IdBasedPipeline by running a pipeline that processes
multiple IDs dynamically, each with its own source resolution.

Prerequisites:
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/test_id_based_pipeline.py -v
"""

import os
import time
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


def _wait_for_completion(client, execution_id, max_wait=120):
    """Wait for pipeline execution to complete and return final stats."""
    start = time.time()
    final_state = None
    stats = {}
    
    while time.time() - start < max_wait:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        final_state = stats.get("state")
        
        if final_state in ["completed", "failed"]:
            return stats
        
        time.sleep(POLL_INTERVAL)
    
    raise TimeoutError(
        f"Pipeline {execution_id} did not complete within {max_wait}s. "
        f"Last state: {final_state}, Stats: {stats}"
    )


class TestIdBasedPipelineE2E:
    """E2E tests for the IdBasedPipeline feature."""
    
    def test_id_based_pipeline_starts(self, client):
        """Test that IdBasedPipeline can start with a list of IDs."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_pipeline_test",
            "runtime_params": {
                "ids": ["alpha", "beta", "gamma"],
            },
        })
        
        if response.status_code == 404:
            pytest.skip("e2e_id_based_pipeline_test pipeline not registered")
        
        assert response.status_code == 202, f"Expected 202, got {response.status_code}: {response.text}"
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_id_based_pipeline_test"
        assert data["state"] == "pending"
        
        print(f"✅ IdBasedPipeline started: {data['execution_id']}")
    
    def test_id_based_pipeline_creates_jobs_per_id(self, client):
        """
        Test that IdBasedPipeline creates jobs for each ID.
        
        With 3 IDs × 10 records per ID / 5 batch size = 6 jobs total
        (2 jobs per ID).
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_pipeline_test",
            "runtime_params": {
                "ids": ["id_1", "id_2", "id_3"],
                "records_per_id": 10,  # 10 records per ID
            },
        })
        
        if response.status_code == 404:
            pytest.skip("e2e_id_based_pipeline_test pipeline not registered")
        
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
        
        # 3 IDs × 10 records / 5 batch_size = 6 jobs
        expected_jobs = 6
        assert total_jobs == expected_jobs, (
            f"Expected {expected_jobs} jobs (3 IDs × 2 batches), got {total_jobs}"
        )
        
        print(f"✅ IdBasedPipeline created {total_jobs} jobs for 3 IDs")
    
    def test_id_based_pipeline_completes_successfully(self, client):
        """Test that IdBasedPipeline runs to completion with all jobs passing."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_pipeline_test",
            "runtime_params": {
                "ids": ["user_001", "user_002"],
                "records_per_id": 5,
            },
        })
        
        if response.status_code == 404:
            pytest.skip("e2e_id_based_pipeline_test pipeline not registered")
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        stats = _wait_for_completion(client, execution_id, max_wait=120)
        
        # Verify completion
        assert stats["state"] == "completed", (
            f"Expected completed, got {stats['state']}. Stats: {stats}"
        )
        
        # Verify job counts: 2 IDs × 5 records / 5 batch_size = 2 jobs
        assert stats["total_jobs"] == 2
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0
        
        print(f"✅ IdBasedPipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs")
    
    def test_id_based_pipeline_single_id(self, client):
        """Test IdBasedPipeline works with a single ID."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_pipeline_test",
            "runtime_params": {
                "ids": ["single_entity"],
                "records_per_id": 5,
            },
        })
        
        if response.status_code == 404:
            pytest.skip("e2e_id_based_pipeline_test pipeline not registered")
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        stats = _wait_for_completion(client, execution_id, max_wait=120)
        
        assert stats["state"] == "completed"
        # 1 ID × 5 records / 5 batch_size = 1 job
        assert stats["total_jobs"] == 1
        assert stats["jobs_completed"] == 1
        assert stats["jobs_failed"] == 0
        
        print(f"✅ Single-ID pipeline completed: {stats['jobs_completed']} job")
    
    def test_id_based_pipeline_many_ids(self, client):
        """Test IdBasedPipeline with many IDs to verify scaling."""
        ids = [f"entity_{i}" for i in range(10)]
        
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_pipeline_test",
            "runtime_params": {
                "ids": ids,
                "records_per_id": 5,  # 10 IDs × 5 records / 5 batch = 10 jobs
            },
        })
        
        if response.status_code == 404:
            pytest.skip("e2e_id_based_pipeline_test pipeline not registered")
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        stats = _wait_for_completion(client, execution_id, max_wait=120)
        
        assert stats["state"] == "completed"
        # 10 IDs × 5 records / 5 batch_size = 10 jobs
        assert stats["total_jobs"] == 10
        assert stats["jobs_completed"] == 10
        assert stats["jobs_failed"] == 0
        
        print(f"✅ Many-IDs pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs from {len(ids)} IDs")
    
    def test_id_based_pipeline_missing_ids_fails(self, client):
        """Test that omitting 'ids' parameter returns validation error."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_pipeline_test",
            "runtime_params": {},  # No 'ids' provided
        })
        
        if response.status_code == 404:
            pytest.skip("e2e_id_based_pipeline_test pipeline not registered")
        
        # The pipeline should either:
        # 1. Return 202 and then fail during execution (ids validation)
        # 2. Return 4xx directly
        if response.status_code == 202:
            execution_id = response.json()["execution_id"]
            stats = _wait_for_completion(client, execution_id, max_wait=60)
            assert stats["state"] == "failed", (
                f"Expected failed (missing ids), got {stats['state']}"
            )
            print("✅ Missing IDs correctly results in failed execution")
        else:
            # Direct validation error
            assert response.status_code in [400, 422]
            print("✅ Missing IDs correctly rejected at API level")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
