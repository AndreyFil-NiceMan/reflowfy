"""
E2E Tests: enable_duplicate_jobs flag.

Verifies that:
- enable_duplicate_jobs=False  → consecutive runs skip jobs that already exist in the DB
- enable_duplicate_jobs=True   → jobs always run with fresh UUIDs (baseline)
- API override works: pipeline default can be flipped per-request

Tests are designed to be idempotent — they pass whether the DB is fresh or has
data from previous runs of the same test suite.

Requires a running ReflowManager (local execution mode).
Pipeline definitions: tests/e2e/test_pipelines/dedup_test_pipeline.py
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
POLL_INTERVAL = 2       # seconds between stat polls
MAX_WAIT = 60           # seconds to wait for pipeline completion


# ============================================================================
# Helpers
# ============================================================================

def _run_pipeline(client: httpx.Client, pipeline_name: str, **extra) -> str:
    """POST /run and return execution_id. Asserts 202."""
    body = {"pipeline_name": pipeline_name, **extra}
    response = client.post("/run", json=body)
    assert response.status_code == 202, (
        f"POST /run returned {response.status_code}: {response.text}"
    )
    execution_id = response.json()["execution_id"]
    assert execution_id, "Response missing execution_id"
    return execution_id


def _wait_for_completion(client: httpx.Client, execution_id: str) -> dict:
    """Poll /executions/{id}/stats until terminal state, then return stats."""
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(
        f"Execution {execution_id} did not finish within {MAX_WAIT}s"
    )


def _pipeline_info(client: httpx.Client, pipeline_name: str) -> dict:
    """GET /pipelines/{name} and return the JSON body."""
    response = client.get(f"/pipelines/{pipeline_name}")
    return response.json() if response.status_code == 200 else {}


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def client(check_reflow_manager):
    """HTTP client scoped to this test module."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


# ============================================================================
# Test: enable_duplicate_jobs = False  (deduplication ON)
# ============================================================================

class TestDedupOff:
    """Tests for enable_duplicate_jobs=False (pipeline-level default).

    The e2e DB may persist between test suite runs, so any prior run will have
    already inserted the hash-based job IDs. Tests are written to be idempotent:
    they do not assume the DB is empty at start.
    """

    def test_dedup_pipeline_completes(self, client):
        """Pipeline with dedup ON always finishes in 'completed' state with no failures,
        regardless of whether jobs are new (first run) or skipped (subsequent run)."""
        execution_id = _run_pipeline(client, "e2e_dedup_off")
        stats = _wait_for_completion(client, execution_id)

        assert stats["state"] == "completed", f"Expected completed, got {stats['state']}"
        assert stats["jobs_failed"] == 0
        # total_jobs may be 0 (all skipped) or >0 (first ever run) — both are valid

    def test_uuid_mode_always_creates_jobs(self, client):
        """With enable_duplicate_jobs=True (API override), fresh UUIDs are used so jobs
        always run regardless of DB state. This is the reliable way to verify execution."""
        execution_id = _run_pipeline(client, "e2e_dedup_off", enable_duplicate_jobs=True)
        stats = _wait_for_completion(client, execution_id)

        assert stats["state"] == "completed", f"Expected completed, got {stats['state']}"
        assert stats["total_jobs"] > 0, (
            "UUID mode must always create new jobs — total_jobs should be > 0"
        )
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0

    def test_consecutive_dedup_runs_skip_all_jobs(self, client):
        """Two consecutive dedup runs: after the first, the second must skip every job."""
        # First run — may create jobs or skip all (depending on DB state)
        first_id = _run_pipeline(client, "e2e_dedup_off")
        first_stats = _wait_for_completion(client, first_id)
        assert first_stats["state"] == "completed"
        assert first_stats["jobs_failed"] == 0

        # Second run immediately after — hash IDs definitely in DB now → all skipped
        second_id = _run_pipeline(client, "e2e_dedup_off")
        assert second_id != first_id, "Each execution must get a unique execution_id"
        second_stats = _wait_for_completion(client, second_id)

        assert second_stats["state"] == "completed", (
            f"Second run should complete cleanly, got {second_stats['state']}"
        )
        assert second_stats["jobs_failed"] == 0
        assert second_stats["total_jobs"] == 0, (
            f"All jobs should be skipped on second run, got total_jobs={second_stats['total_jobs']}"
        )

    def test_api_override_enables_duplicates(self, client):
        """enable_duplicate_jobs=True API override bypasses dedup even on a
        pipeline whose class sets enable_duplicate_jobs=False."""
        # Ensure at least one dedup run has happened (populates hash IDs)
        _wait_for_completion(client, _run_pipeline(client, "e2e_dedup_off"))

        # Override to allow duplicates — must create fresh jobs via UUID
        override_id = _run_pipeline(client, "e2e_dedup_off", enable_duplicate_jobs=True)
        override_stats = _wait_for_completion(client, override_id)

        assert override_stats["state"] == "completed"
        assert override_stats["total_jobs"] > 0, (
            "enable_duplicate_jobs=True override must create new jobs"
        )
        assert override_stats["jobs_completed"] == override_stats["total_jobs"]


# ============================================================================
# Test: enable_duplicate_jobs = True  (deduplication OFF — baseline)
# ============================================================================

class TestDedupOn:
    """Tests for enable_duplicate_jobs=True (allow duplicates, baseline)."""

    def test_pipeline_always_creates_jobs(self, client):
        """UUID-based pipeline always creates jobs — no skipping ever occurs."""
        execution_id = _run_pipeline(client, "e2e_dedup_on")
        stats = _wait_for_completion(client, execution_id)

        assert stats["state"] == "completed"
        assert stats["total_jobs"] > 0, "UUID mode must always create jobs"
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0

    def test_second_run_also_executes_all_jobs(self, client):
        """Both runs create and execute the same number of jobs (UUIDs differ each time)."""
        first_id = _run_pipeline(client, "e2e_dedup_on")
        first_stats = _wait_for_completion(client, first_id)
        assert first_stats["state"] == "completed"
        jobs_first = first_stats["total_jobs"]

        second_id = _run_pipeline(client, "e2e_dedup_on")
        second_stats = _wait_for_completion(client, second_id)

        assert second_stats["state"] == "completed"
        assert second_stats["total_jobs"] == jobs_first, (
            "Both runs should dispatch the same number of jobs"
        )
        assert second_stats["jobs_completed"] == second_stats["total_jobs"]

    def test_api_override_disables_duplicates(self, client):
        """enable_duplicate_jobs=False API override enforces dedup on a pipeline
        that would normally allow duplicates."""
        # First override run — uses hash IDs; may or may not find existing jobs
        first_id = _run_pipeline(client, "e2e_dedup_on", enable_duplicate_jobs=False)
        first_stats = _wait_for_completion(client, first_id)
        assert first_stats["state"] == "completed"
        assert first_stats["jobs_failed"] == 0

        # Second override run — hash IDs now guaranteed in DB → all skipped
        second_id = _run_pipeline(client, "e2e_dedup_on", enable_duplicate_jobs=False)
        second_stats = _wait_for_completion(client, second_id)

        assert second_stats["state"] == "completed"
        assert second_stats["jobs_failed"] == 0
        assert second_stats["total_jobs"] == 0, (
            "Second dedup run must skip all jobs"
        )


# ============================================================================
# Test: Pipeline API visibility
# ============================================================================

class TestPipelineApiVisibility:
    """Verify that enable_duplicate_jobs is exposed in the pipeline API response."""

    def test_dedup_off_pipeline_exposes_flag(self, client):
        """Pipeline with enable_duplicate_jobs=False reports the flag via to_dict()."""
        info = _pipeline_info(client, "e2e_dedup_off")
        if not info:
            pytest.skip("GET /pipelines/{name} endpoint not available")

        assert "enable_duplicate_jobs" in info, (
            "Pipeline info response must include 'enable_duplicate_jobs'"
        )
        assert info["enable_duplicate_jobs"] is False

    def test_dedup_on_pipeline_exposes_flag(self, client):
        """Pipeline with enable_duplicate_jobs=True reports the flag via to_dict()."""
        info = _pipeline_info(client, "e2e_dedup_on")
        if not info:
            pytest.skip("GET /pipelines/{name} endpoint not available")

        assert "enable_duplicate_jobs" in info
        assert info["enable_duplicate_jobs"] is True

    def test_run_request_accepts_enable_duplicate_jobs_field(self, client):
        """POST /run accepts enable_duplicate_jobs in the request body (no 422)."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_dedup_on",
            "enable_duplicate_jobs": False,
        })
        assert response.status_code == 202, (
            f"POST /run rejected enable_duplicate_jobs field: {response.text}"
        )
        # Clean up — wait for completion so it doesn't interfere with other tests
        execution_id = response.json()["execution_id"]
        _wait_for_completion(client, execution_id)
