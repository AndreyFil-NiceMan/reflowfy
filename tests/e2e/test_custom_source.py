"""
E2E: a source defined in the user's project runs on the worker side.

``e2e_custom_source`` plans with its own ``E2ERangeSource.split()``; every job is
rebuilt from the serialized ``{type, config}`` by SourceFactory, which only knows the
class because it registered itself when discovery imported the pipeline module.

Prerequisites:
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/test_custom_source.py -v
"""

import os

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
PIPELINE_NAME = "e2e_custom_source"


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
        yield client


def test_custom_source_slices_are_rebuilt_and_fetched(client, wait_for_pipeline_completion):
    response = client.post(
        "/run",
        json={"pipeline_name": PIPELINE_NAME, "runtime_params": {"count": 25, "per_job": 10}},
    )
    if response.status_code == 404:
        pytest.skip(f"{PIPELINE_NAME} pipeline not registered")
    assert response.status_code == 202, response.text
    execution_id = response.json()["execution_id"]

    stats = wait_for_pipeline_completion(execution_id)
    assert stats["state"] == "completed", stats
    assert stats["total_jobs"] == 3
    assert stats["jobs_completed"] == 3 and stats["jobs_failed"] == 0

    jobs = client.get(f"/executions/{execution_id}/jobs").json()
    assert sum(j["processed_records"] or 0 for j in jobs) == 25
    for job in jobs:
        source = job["job_payload"]["source"]
        assert source["type"] == "E2ERangeSource"
        # the literal key is in the job row, but never in an API response
        assert source["config"]["api_key"] == "***"
        assert "e2e-literal-key" not in str(job)
