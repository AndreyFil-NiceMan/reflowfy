"""
E2E tests for runtime_params destination pipeline.

Verifies that runtime_params mutated by transformations are included
in the destination payload sent to the mock HTTP server.
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 120
PIPELINE_NAME = "e2e_runtime_params_destination"


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=MAX_WAIT) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_mock_http(check_mock_http):
    try:
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
    except Exception:
        pass
    yield


def _run_pipeline(client, runtime_params=None):
    payload = {"pipeline_name": PIPELINE_NAME}
    if runtime_params is not None:
        payload["runtime_params"] = runtime_params

    response = client.post("/run", json=payload)
    if response.status_code == 404:
        pytest.skip(f"Pipeline '{PIPELINE_NAME}' not registered")
    assert response.status_code == 202, f"Expected 202, got {response.status_code}: {response.text}"
    execution_id = response.json()["execution_id"]

    deadline = time.monotonic() + MAX_WAIT
    while time.monotonic() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return execution_id, stats
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Pipeline '{PIPELINE_NAME}' did not complete in {MAX_WAIT}s")


def _get_batches():
    resp = httpx.get(f"{MOCK_HTTP_URL}/stats", timeout=10.0)
    resp.raise_for_status()
    return resp.json().get("batches", [])


class TestRuntimeParamsDestination:
    def test_runtime_params_in_destination_payload(self, client, check_mock_http):
        execution_id, stats = _run_pipeline(client, runtime_params={"tenant": "acme"})
        if stats["state"] != "completed":
            errors = client.get(f"/executions/{execution_id}/errors").json()
            print(f"Execution errors: {errors}")
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"

        batches = _get_batches()
        assert batches, "No batches received by mock HTTP server"

        matched = 0
        for batch in batches:
            runtime_params = batch.get("extra_body_fields", {}).get("runtime_params", {})
            if runtime_params.get("execution_id") != execution_id:
                continue
            matched += 1
            assert runtime_params.get("tenant") == "acme"
            assert runtime_params.get("source_marker") == "mock_source"
            assert runtime_params.get("step1_count") == 10
            assert runtime_params.get("step1_ran") is True
            assert runtime_params.get("pipeline_name") == PIPELINE_NAME
        assert matched > 0, "No batches matched this execution_id"

    def test_records_stamped_by_transforms(self, client, check_mock_http):
        execution_id, stats = _run_pipeline(client, runtime_params={"tenant": "acme"})
        if stats["state"] != "completed":
            errors = client.get(f"/executions/{execution_id}/errors").json()
            print(f"Execution errors: {errors}")
        assert stats["state"] == "completed"

        resp = httpx.get(f"{MOCK_HTTP_URL}/records", params={"limit": 100}, timeout=10.0).json()
        records = [r for r in resp["records"] if r.get("_execution_id") == execution_id]
        assert records, "No records for this execution received by mock HTTP server"

        for record in records:
            assert record.get("_step1") is True
            assert record.get("_step2") is True
            assert record.get("_execution_id") == execution_id
