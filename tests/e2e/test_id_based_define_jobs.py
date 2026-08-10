"""
E2E tests for an IdBasedPipeline that owns its splitting via define_jobs.

The pipeline fans each input ID out into 3 jobs. Verifies:
  1. The job count comes from define_jobs, not from ids_batch_size.
  2. Every planned child reaches a worker and lands at the destination.
  3. The job payload is narrowed — an overridden define_jobs sets no job_params,
     so the manager must still strip the full `ids` list instead of shipping
     every ID to every job.

Prerequisites:
    - ReflowManager running on localhost:8002
    - Mock HTTP server running on localhost:8091

Run with:
    pytest tests/e2e/test_id_based_define_jobs.py -v
"""

import os
import time

import httpx
import pytest

BASE_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("MOCK_HTTP_BASE_URL", "http://localhost:8091")

PIPELINE_NAME = "e2e_id_based_define_jobs"
PARENT_IDS = [7, 8]
CHILDREN_PER_ID = 3
EXPECTED_JOBS = len(PARENT_IDS) * CHILDREN_PER_ID


def _run(ids: list) -> str:
    resp = httpx.post(
        f"{BASE_URL}/run",
        json={"pipeline_name": PIPELINE_NAME, "runtime_params": {"ids": ids}},
        timeout=30,
    )
    if resp.status_code == 404:
        pytest.skip(f"{PIPELINE_NAME} pipeline not registered")
    assert resp.status_code == 202, f"Run failed: {resp.text}"
    return resp.json()["execution_id"]


def _wait(execution_id: str, timeout: int = 120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = httpx.get(f"{BASE_URL}/executions/{execution_id}/stats", timeout=10).json()
        if data.get("state") in ("completed", "failed"):
            return data
        time.sleep(2)
    raise TimeoutError(f"Execution {execution_id} did not complete in {timeout}s")


def _received(execution_id: str) -> list:
    """This execution's records only.

    The mock server is shared by the whole suite and scheduled pipelines fire on
    their own cron while these tests run, so its total is not ours to assert on.
    id_fanout_stamp puts the execution id on every record it touches.
    """
    resp = httpx.get(f"{MOCK_URL}/records?limit=1000", timeout=10)
    resp.raise_for_status()
    records = resp.json().get("records", [])
    return [r for r in records if r.get("_execution_id") == execution_id]


class TestIdBasedDefineJobs:
    """define_jobs owns the splitting for an IdBasedPipeline."""

    def setup_method(self):
        httpx.delete(f"{MOCK_URL}/reset", timeout=10)

    def test_one_id_fans_out_into_many_jobs(self, check_reflow_manager, check_mock_http):
        execution_id = _run(PARENT_IDS)

        stats = _wait(execution_id)

        assert stats["state"] == "completed", f"Expected completed, got {stats}"
        assert stats["total_jobs"] == EXPECTED_JOBS, (
            f"Expected {EXPECTED_JOBS} jobs ({len(PARENT_IDS)} ids x {CHILDREN_PER_ID}), "
            f"got {stats['total_jobs']}"
        )
        assert stats["jobs_completed"] == EXPECTED_JOBS
        assert stats["jobs_failed"] == 0

    def test_every_child_reaches_the_destination(self, check_reflow_manager, check_mock_http):
        execution_id = _run(PARENT_IDS)
        assert _wait(execution_id)["state"] == "completed"

        children = sorted(r["child_id"] for r in _received(execution_id))
        expected = sorted(
            f"{parent}-{child}" for parent in PARENT_IDS for child in range(CHILDREN_PER_ID)
        )
        assert children == expected, f"Expected {expected}, got {children}"

    def test_job_payload_does_not_carry_the_full_ids_list(
        self, check_reflow_manager, check_mock_http
    ):
        """A job handling one child ID has no use for every other ID in the run."""
        execution_id = _run(PARENT_IDS)
        assert _wait(execution_id)["state"] == "completed"

        records = _received(execution_id)
        assert len(records) == EXPECTED_JOBS
        for record in records:
            assert record["saw_ids_param"] is False, (
                f"Worker received the full 'ids' list in runtime_params: {record}"
            )

    def test_each_job_runs_with_the_params_its_plan_gave_it(
        self, check_reflow_manager, check_mock_http
    ):
        """job(...) params must reach the worker's transformations, per job."""
        execution_id = _run(PARENT_IDS)
        assert _wait(execution_id)["state"] == "completed"

        records = _received(execution_id)
        assert len(records) == EXPECTED_JOBS
        for record in records:
            # Every child of parent 7 must see current_id 7, not 8 and not None.
            assert record["job_current_id"] == record["parent_id"], (
                f"Job ran with the wrong per-job params: {record}"
            )
