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


class TestIdBasedBatchPipelineE2E:
    """E2E tests for IdBasedPipeline with ids_batch_size > 1."""

    def test_ids_batch_size_groups_ids(self, client):
        """
        Test that ids_batch_size=2 groups IDs so define_source receives
        lists of 2 IDs, not individual IDs.

        6 IDs / batch_size=2 → 3 source resolutions.
        Each source call: 2 IDs × 5 records = 10 records / batch_size=5 → 2 jobs.
        Total: 3 × 2 = 6 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_batch_pipeline_test",
            "runtime_params": {
                "ids": ["a", "b", "c", "d", "e", "f"],
                "records_per_id": 5,
            },
        })

        if response.status_code == 404:
            pytest.skip("e2e_id_based_batch_pipeline_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        stats = _wait_for_completion(client, execution_id, max_wait=120)

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        # 6 IDs / 2 per batch = 3 batches; each batch: 2×5=10 records / 5 batch_size = 2 jobs
        assert stats["total_jobs"] == 6
        assert stats["jobs_completed"] == 6
        assert stats["jobs_failed"] == 0

        print(f"✅ Batch pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs (batch_size=2)")

    def test_ids_batch_size_odd_remainder(self, client):
        """
        Test that an uneven batch (5 IDs with batch_size=2) correctly
        handles the final batch of 1 ID.

        5 IDs / batch_size=2 → 3 source resolutions (batches of 2, 2, 1).
        Batch 1: 2 IDs × 5 = 10 records → 2 jobs
        Batch 2: 2 IDs × 5 = 10 records → 2 jobs
        Batch 3: 1 ID  × 5 =  5 records → 1 job
        Total: 5 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_batch_pipeline_test",
            "runtime_params": {
                "ids": ["x1", "x2", "x3", "x4", "x5"],
                "records_per_id": 5,
            },
        })

        if response.status_code == 404:
            pytest.skip("e2e_id_based_batch_pipeline_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        stats = _wait_for_completion(client, execution_id, max_wait=120)

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert stats["total_jobs"] == 5
        assert stats["jobs_completed"] == 5
        assert stats["jobs_failed"] == 0

        print(f"✅ Odd-remainder batch pipeline completed: {stats['jobs_completed']} jobs")


class TestRawListSearchPipelineE2E:
    """
    E2E tests for E2ERawListSearchPipeline.

    Verifies that IDBasedAPISource with ``batch_id_key=None`` sends a raw JSON
    array as the POST body (not wrapped in an object) and correctly processes
    the response.

    Job math:  N IDs / ids_batch_size=5 → N/5 POST calls → 1 job per call
    (each call returns ≤5 users, batch_size=5 → 1 SourceJob).
    """

    PIPELINE = "e2e_raw_list_search_pipeline"

    def test_raw_list_pipeline_starts(self, client):
        """Pipeline starts with a list of user IDs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 6))},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        data = response.json()
        assert data["state"] == "pending"
        print(f"✅ Raw-list pipeline started: {data['execution_id']}")

    def test_raw_list_pipeline_single_batch(self, client):
        """5 IDs → 1 POST → 1 job."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 6)), "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 1
        assert stats["jobs_completed"] == 1
        assert stats["jobs_failed"] == 0
        print("✅ Raw-list single batch: 1 job")

    def test_raw_list_pipeline_multiple_batches(self, client):
        """20 IDs / ids_batch_size=5 → 4 POST calls → 4 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 21)), "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 4
        assert stats["jobs_completed"] == 4
        assert stats["jobs_failed"] == 0
        print(f"✅ Raw-list 4 batches: {stats['jobs_completed']}/4 jobs")

    def test_raw_list_pipeline_partial_last_batch(self, client):
        """13 IDs / ids_batch_size=5 → batches [5,5,3] → 3 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 14)), "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 3
        assert stats["jobs_completed"] == 3
        assert stats["jobs_failed"] == 0
        print("✅ Raw-list partial last batch: 3 jobs")

    def test_raw_list_pipeline_small_job_batches(self, client):
        """10 IDs, batch_size=2 → 2 POST calls (5 IDs each) → each returns 5 users / 2 = 3 jobs per call → 6 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 11)), "batch_size": 2},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        # 2 POST calls × ceil(5/2)=3 jobs each = 6 jobs
        assert stats["total_jobs"] == 6
        assert stats["jobs_completed"] == 6
        assert stats["jobs_failed"] == 0
        print(f"✅ Raw-list small job batches: {stats['jobs_completed']}/6 jobs")

    def test_raw_list_missing_ids_returns_empty(self, client):
        """IDs that don't exist (101-105) → empty results → 0 jobs → completed."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(101, 106))},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["jobs_failed"] == 0
        print(f"✅ Raw-list missing IDs: completed with {stats['total_jobs']} jobs (expected 0 or few)")


class TestPatchBulkPipelineE2E:
    """
    E2E tests for E2EPatchBulkPipeline.

    Verifies that IDBasedAPISource with ``method="PATCH"`` and ``request_body``
    correctly merges the extra field into the request body alongside the IDs.

    Job math (active_only=False):
    N IDs / ids_batch_size=8 → N/8 PATCH calls → each returns 8 users / batch_size=4 → 2 jobs per call.

    Job math (active_only=True):
    Users where ``id % 3 == 0`` are inactive. Roughly 2/3 of users are active.
    In a batch of 8: ~5-6 active → ceil(~6/4)=2 jobs per call.
    """

    PIPELINE = "e2e_patch_bulk_pipeline"

    def test_patch_bulk_starts(self, client):
        """Pipeline starts with user IDs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 9))},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        print(f"✅ PATCH bulk pipeline started: {response.json()['execution_id']}")

    def test_patch_bulk_all_users(self, client):
        """
        active_only=False — all users returned.

        16 IDs / 8 = 2 batches × (8/4=2 jobs each) = 4 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": list(range(1, 17)),
                "active_only": False,
                "batch_size": 4,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 4
        assert stats["jobs_completed"] == 4
        assert stats["jobs_failed"] == 0
        print(f"✅ PATCH all-users: {stats['jobs_completed']}/4 jobs")

    def test_patch_bulk_active_only(self, client):
        """
        active_only=True — only active users returned (fewer records).

        IDs [1..8]: inactive IDs are 3, 6 (id % 3 == 0) → 6 active users.
        6 active / batch_size=4 → 2 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": list(range(1, 9)),   # IDs 1-8
                "active_only": True,
                "batch_size": 4,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        # 1 batch (8 IDs) → 6 active users → ceil(6/4) = 2 jobs
        assert stats["total_jobs"] == 2
        assert stats["jobs_completed"] == 2
        assert stats["jobs_failed"] == 0
        print(f"✅ PATCH active-only: {stats['jobs_completed']}/2 jobs")

    def test_patch_bulk_large_batch(self, client):
        """
        32 IDs / ids_batch_size=8 = 4 PATCH calls.
        active_only=False, batch_size=4 → 8/4=2 jobs per call → 8 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": list(range(1, 33)),
                "active_only": False,
                "batch_size": 4,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 8
        assert stats["jobs_completed"] == 8
        assert stats["jobs_failed"] == 0
        print(f"✅ PATCH large batch: {stats['jobs_completed']}/8 jobs")

    def test_patch_bulk_partial_batch(self, client):
        """
        20 IDs / ids_batch_size=8 → batches [8, 8, 4] → 3 PATCH calls.
        active_only=False, batch_size=4:
        - Batch 1 (8 users / 4): 2 jobs
        - Batch 2 (8 users / 4): 2 jobs
        - Batch 3 (4 users / 4): 1 job
        Total: 5 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": list(range(1, 21)),
                "active_only": False,
                "batch_size": 4,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 5
        assert stats["jobs_completed"] == 5
        assert stats["jobs_failed"] == 0
        print(f"✅ PATCH partial batch: {stats['jobs_completed']}/5 jobs")


class TestPerIdPostPipelineE2E:
    """
    E2E tests for E2EPerIdPostPipeline.

    Verifies per-ID POST mode: one ``POST /users/{id}/enrich`` per user,
    with ``{id}`` substituted in the request body.

    Job math:
    N IDs / ids_batch_size=5 → N/5 define_source calls.
    Each call: 5 individual POST requests grouped by batch_size=5 → 1 SourceJob.
    Total jobs = N / ids_batch_size.
    """

    PIPELINE = "e2e_per_id_post_pipeline"

    def test_per_id_post_starts(self, client):
        """Pipeline starts with user IDs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 6))},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        print(f"✅ Per-ID POST pipeline started: {response.json()['execution_id']}")

    def test_per_id_post_single_batch(self, client):
        """5 IDs / ids_batch_size=5 → 1 define_source call → 1 SourceJob."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 6)), "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 1
        assert stats["jobs_completed"] == 1
        assert stats["jobs_failed"] == 0
        print("✅ Per-ID POST single batch: 1 job")

    def test_per_id_post_multiple_batches(self, client):
        """15 IDs / ids_batch_size=5 → 3 define_source calls → 3 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 16)), "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 3
        assert stats["jobs_completed"] == 3
        assert stats["jobs_failed"] == 0
        print(f"✅ Per-ID POST 3 batches: {stats['jobs_completed']}/3 jobs")

    def test_per_id_post_partial_last_batch(self, client):
        """12 IDs / ids_batch_size=5 → batches [5,5,2] → 3 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(1, 13)), "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 3
        assert stats["jobs_completed"] == 3
        assert stats["jobs_failed"] == 0
        print("✅ Per-ID POST partial last batch: 3 jobs")

    def test_per_id_post_nonexistent_ids_skipped(self, client):
        """
        IDs [101..105] don't exist → all 5 POST calls return 404 →
        ``_fetch_by_id`` skips them → 0 records → 0 SourceJobs.
        Pipeline should still complete (not fail).
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": list(range(101, 106))},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["jobs_failed"] == 0
        print(f"✅ Per-ID POST nonexistent IDs: completed, {stats['total_jobs']} jobs (expected 0)")

    def test_per_id_post_mixed_valid_invalid(self, client):
        """
        Mix of valid [1..3] and invalid [101..102] IDs.

        ids_batch_size=5 → 1 batch with [1,2,3,101,102].
        3 valid POST responses + 2 404s → 3 records.
        batch_size=5 → 3 records fit in 1 SourceJob → 1 job.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": [1, 2, 3, 101, 102],
                "batch_size": 5,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 1
        assert stats["jobs_completed"] == 1
        assert stats["jobs_failed"] == 0
        print(f"✅ Per-ID POST mixed IDs: {stats['jobs_completed']} job (3 valid records)")


class TestProductsBatchPipelineE2E:
    """
    E2E tests for E2EProductsBatchPipeline.

    Verifies that IDBasedAPISource with a non-default ``batch_id_key``
    (``"product_ids"``) correctly sends product IDs to POST /products/lookup
    and processes the response.

    Job math:
    N product IDs / ids_batch_size=10 → N/10 POST calls.
    Each call returns up to 10 products / batch_size=5 → 2 jobs per call.
    """

    PIPELINE = "e2e_products_batch_pipeline"

    def test_products_pipeline_starts(self, client):
        """Pipeline starts with product IDs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": [f"prod_{i}" for i in range(1, 6)]},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        print(f"✅ Products pipeline started: {response.json()['execution_id']}")

    def test_products_pipeline_single_batch(self, client):
        """10 product IDs → 1 POST → 10 products / batch_size=5 → 2 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": [f"prod_{i}" for i in range(1, 11)],
                "batch_size": 5,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 2
        assert stats["jobs_completed"] == 2
        assert stats["jobs_failed"] == 0
        print(f"✅ Products single batch: {stats['jobs_completed']}/2 jobs")

    def test_products_pipeline_multiple_batches(self, client):
        """20 product IDs / ids_batch_size=10 → 2 POST calls → 4 jobs."""
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": [f"prod_{i}" for i in range(1, 21)],
                "batch_size": 5,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 4
        assert stats["jobs_completed"] == 4
        assert stats["jobs_failed"] == 0
        print(f"✅ Products multiple batches: {stats['jobs_completed']}/4 jobs")

    def test_products_pipeline_partial_batch(self, client):
        """
        25 product IDs / ids_batch_size=10 → batches [10,10,5] → 3 POST calls.
        batch_size=5:
        - Batch 1: 10 products → 2 jobs
        - Batch 2: 10 products → 2 jobs
        - Batch 3:  5 products → 1 job
        Total: 5 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": [f"prod_{i}" for i in range(1, 26)],
                "batch_size": 5,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 5
        assert stats["jobs_completed"] == 5
        assert stats["jobs_failed"] == 0
        print(f"✅ Products partial batch: {stats['jobs_completed']}/5 jobs")

    def test_products_pipeline_nonexistent_ids(self, client):
        """
        Non-existent product IDs (prod_99, prod_100) → /products/lookup returns
        empty items → 0 SourceJobs → pipeline completes cleanly.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": ["prod_99", "prod_100", "prod_999"],
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["jobs_failed"] == 0
        print(f"✅ Products nonexistent IDs: completed, {stats['total_jobs']} jobs (expected 0)")

    def test_products_pipeline_mixed_valid_invalid(self, client):
        """
        Mix of valid (prod_1..5) and invalid (prod_99..100) product IDs.

        10 IDs in one batch: 5 valid + 5 invalid → 5 products returned.
        5 products / batch_size=5 → 1 job.
        """
        ids = [f"prod_{i}" for i in range(1, 6)] + ["prod_99", "prod_100", "prod_101", "prod_102", "prod_103"]
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {"ids": ids, "batch_size": 5},
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"])
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 1
        assert stats["jobs_completed"] == 1
        assert stats["jobs_failed"] == 0
        print(f"✅ Products mixed IDs: {stats['jobs_completed']} job (5 valid products)")

    def test_products_pipeline_all_categories(self, client):
        """
        All 50 products cover categories A, B, C.
        50 IDs / ids_batch_size=10 → 5 POST calls → 10/5=2 jobs each → 10 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": self.PIPELINE,
            "runtime_params": {
                "ids": [f"prod_{i}" for i in range(1, 51)],
                "batch_size": 5,
            },
        })
        if response.status_code == 404:
            pytest.skip(f"{self.PIPELINE} not registered")
        assert response.status_code == 202
        stats = _wait_for_completion(client, response.json()["execution_id"], max_wait=180)
        assert stats["state"] == "completed", f"Failed: {stats}"
        assert stats["total_jobs"] == 10
        assert stats["jobs_completed"] == 10
        assert stats["jobs_failed"] == 0
        print(f"✅ Products all categories: {stats['jobs_completed']}/10 jobs")


class TestIdBasedAPIBatchPipelineE2E:
    """E2E tests for IdBasedPipeline sourcing from POST /users/batch."""

    def test_api_batch_pipeline_starts(self, client):
        """Test that the API batch pipeline starts with a list of user IDs."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_api_batch_pipeline_test",
            "runtime_params": {
                "ids": list(range(1, 11)),  # 10 user IDs
            },
        })

        if response.status_code == 404:
            pytest.skip("e2e_id_based_api_batch_pipeline_test pipeline not registered")

        assert response.status_code == 202, f"Expected 202, got {response.status_code}: {response.text}"
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_id_based_api_batch_pipeline_test"
        assert data["state"] == "pending"

        print(f"✅ API batch pipeline started: {data['execution_id']}")

    def test_api_batch_pipeline_completes(self, client):
        """
        Test that the pipeline completes with the expected job count.

        20 IDs / ids_batch_size=10 → 2 POST calls.
        Each POST returns 10 users; batch_size=5 → 2 jobs per POST.
        Total: 4 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_api_batch_pipeline_test",
            "runtime_params": {
                "ids": list(range(1, 21)),  # 20 user IDs (all active + inactive mix)
                "batch_size": 5,
            },
        })

        if response.status_code == 404:
            pytest.skip("e2e_id_based_api_batch_pipeline_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        stats = _wait_for_completion(client, execution_id, max_wait=120)

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        # 20 IDs / 10 ids_batch_size = 2 batches
        # Each batch: 10 users / batch_size=5 = 2 jobs
        # Total: 4 jobs
        assert stats["total_jobs"] == 4
        assert stats["jobs_completed"] == 4
        assert stats["jobs_failed"] == 0

        print(f"✅ API batch pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs")

    def test_api_batch_pipeline_single_batch(self, client):
        """
        Test with exactly ids_batch_size IDs (single POST call).

        10 IDs / ids_batch_size=10 → 1 POST call.
        10 users / batch_size=5 → 2 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_api_batch_pipeline_test",
            "runtime_params": {
                "ids": list(range(1, 11)),  # exactly 10 IDs
                "batch_size": 5,
            },
        })

        if response.status_code == 404:
            pytest.skip("e2e_id_based_api_batch_pipeline_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        stats = _wait_for_completion(client, execution_id, max_wait=120)

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert stats["total_jobs"] == 2
        assert stats["jobs_completed"] == 2
        assert stats["jobs_failed"] == 0

        print(f"✅ API batch pipeline (single batch) completed: {stats['jobs_completed']} jobs")

    def test_api_batch_pipeline_partial_last_batch(self, client):
        """
        Test with an uneven number of IDs so the last batch is smaller.

        25 IDs / ids_batch_size=10 → 3 batches (10, 10, 5).
        Each full batch: 10 users / batch_size=5 = 2 jobs.
        Last batch: 5 users / batch_size=5 = 1 job.
        Total: 5 jobs.
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_id_based_api_batch_pipeline_test",
            "runtime_params": {
                "ids": list(range(1, 26)),  # 25 IDs
                "batch_size": 5,
            },
        })

        if response.status_code == 404:
            pytest.skip("e2e_id_based_api_batch_pipeline_test pipeline not registered")

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        stats = _wait_for_completion(client, execution_id, max_wait=120)

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert stats["total_jobs"] == 5
        assert stats["jobs_completed"] == 5
        assert stats["jobs_failed"] == 0

        print(f"✅ API batch pipeline (partial last batch) completed: {stats['jobs_completed']} jobs")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
