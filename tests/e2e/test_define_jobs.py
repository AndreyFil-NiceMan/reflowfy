"""
E2E Tests for custom job splitting via define_jobs.

The pipeline calls the mock API (``GET /objects`` → 3 objects) from
``define_jobs`` and chunks the response itself, so the job count is decided
entirely by pipeline code rather than by a source's ``split()``.

Prerequisites:
    - ReflowManager running on localhost:8002
    - Mock API server running on localhost:8092

Run with:
    pytest tests/e2e/test_define_jobs.py -v
"""

import os
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
TIMEOUT = 60.0

PIPELINE_NAME = "e2e_define_jobs"
OBJECT_COUNT = 3  # GET /objects returns {"obj1": 1, "obj2": 2, "obj3": 3}


@pytest.fixture(scope="module")
def client(check_reflow_manager, check_mock_api):
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


def _run(client, job_size):
    """Trigger the pipeline with a job size and return the execution id."""
    response = client.post(
        "/run",
        json={"pipeline_name": PIPELINE_NAME, "runtime_params": {"job_size": job_size}},
    )
    if response.status_code == 404:
        pytest.skip(f"{PIPELINE_NAME} pipeline not registered")
    assert response.status_code == 202, f"Run failed: {response.text}"
    return response.json()["execution_id"]


class TestDefineJobsSplitting:
    """define_jobs owns the job count; chunk(size=N) sets it."""

    def test_size_one_dispatches_one_job_per_object(
        self, client, wait_for_pipeline_completion
    ):
        execution_id = _run(client, job_size=1)

        stats = wait_for_pipeline_completion(execution_id)

        assert stats["state"] == "completed", f"Expected completed, got {stats}"
        assert stats["total_jobs"] == OBJECT_COUNT, (
            f"Expected {OBJECT_COUNT} jobs (3 objects, size=1), got {stats['total_jobs']}"
        )
        assert stats["jobs_completed"] == OBJECT_COUNT
        assert stats["jobs_failed"] == 0

    def test_size_two_dispatches_two_jobs(self, client, wait_for_pipeline_completion):
        execution_id = _run(client, job_size=2)

        stats = wait_for_pipeline_completion(execution_id)

        assert stats["state"] == "completed", f"Expected completed, got {stats}"
        assert stats["total_jobs"] == 2, (
            f"Expected 2 jobs (chunks of 2 and 1), got {stats['total_jobs']}"
        )
        assert stats["jobs_completed"] == 2
        assert stats["jobs_failed"] == 0
