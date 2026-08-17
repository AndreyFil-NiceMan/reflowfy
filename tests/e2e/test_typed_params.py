"""
E2E tests for typed runtime_params.

The pipeline under test (`tests/e2e/test_pipelines/typed_params_pipeline.py`)
declares its parameters as a TypedDict instead of a hand-written
`define_parameters()` list. These tests assert the derived declaration actually
drives the running system: defaults applied, choices enforced, required
parameters required, and declared enrichment keys reaching the destination.
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
API_URL = os.getenv("E2E_API_URL", "http://localhost:8003")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 120
PIPELINE_NAME = "e2e_typed_params"


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


def _post_run(client, runtime_params):
    response = client.post(
        "/run", json={"pipeline_name": PIPELINE_NAME, "runtime_params": runtime_params}
    )
    if response.status_code == 404:
        pytest.skip(f"Pipeline '{PIPELINE_NAME}' not registered")
    return response


def _run_pipeline(client, runtime_params):
    response = _post_run(client, runtime_params)
    assert response.status_code == 202, f"Expected 202, got {response.status_code}: {response.text}"
    execution_id = response.json()["execution_id"]

    deadline = time.monotonic() + MAX_WAIT
    while time.monotonic() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return execution_id, stats
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Pipeline '{PIPELINE_NAME}' did not complete in {MAX_WAIT}s")


def _params_for(execution_id):
    """The runtime_params each batch of this execution carried to the destination."""
    resp = httpx.get(f"{MOCK_HTTP_URL}/stats", timeout=10.0)
    resp.raise_for_status()
    seen = []
    for batch in resp.json().get("batches", []):
        params = batch.get("extra_body_fields", {}).get("runtime_params", {})
        if params.get("execution_id") == execution_id:
            seen.append(params)
    return seen


class TestTypedParams:
    def test_derived_default_is_applied(self, client, check_mock_http):
        """multiplier is declared with Param(default=3) and not passed here."""
        execution_id, stats = _run_pipeline(client, {"env": "dev"})
        if stats["state"] != "completed":
            print(f"Execution errors: {client.get(f'/executions/{execution_id}/errors').json()}")
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"

        batches = _params_for(execution_id)
        assert batches, "No batches matched this execution_id"
        for params in batches:
            assert params.get("multiplier") == 3, "derived default was not applied"
            assert params.get("env") == "dev"

        records = httpx.get(f"{MOCK_HTTP_URL}/records", params={"limit": 100}, timeout=10.0).json()[
            "records"
        ]
        mine = [r for r in records if r.get("_execution_id") == execution_id]
        assert mine, "No records for this execution"
        # e2e_mock values are 1-based; each was multiplied by the defaulted 3.
        for record in mine:
            assert record["value"] % 3 == 0, f"value not multiplied: {record}"
            assert record["_env"] == "dev"

    def test_caller_value_overrides_derived_default(self, client, check_mock_http):
        execution_id, stats = _run_pipeline(client, {"env": "prod", "multiplier": 5})
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"

        for params in _params_for(execution_id):
            assert params.get("multiplier") == 5
            assert params.get("env") == "prod"

    def test_declared_enrichment_keys_reach_destination(self, client, check_mock_http):
        """Keys written by define_source and by a transformation both arrive."""
        execution_id, stats = _run_pipeline(client, {"env": "dev", "label": "tagged"})
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"

        batches = _params_for(execution_id)
        assert batches, "No batches matched this execution_id"
        for params in batches:
            assert params.get("derived_label") == "tagged", "define_source enrichment missing"
            assert params.get("multiplied_count") == 10, "transformation enrichment missing"
            assert params.get("pipeline_name") == PIPELINE_NAME

    def test_literal_choice_is_enforced_by_generated_route(self):
        """`env: Literal["dev", "prod"]` becomes a Literal on the generated route.

        The whole chain: the TypedDict annotation → the derived
        PipelineParameter.choices → `Literal[...]` on the API service's
        auto-generated route (api/routes.py) → a 422 from FastAPI.
        """
        try:
            response = httpx.post(
                f"{API_URL}/pipelines/{PIPELINE_NAME}/run?env=staging", timeout=30
            )
        except httpx.RequestError as exc:
            pytest.skip(f"API service not available at {API_URL}: {exc}")
        if response.status_code == 404:
            pytest.skip(f"Pipeline '{PIPELINE_NAME}' not registered on the API service")

        assert (
            response.status_code == 422
        ), f"Expected 422 for env='staging', got {response.status_code}: {response.text}"
        assert (
            "dev" in response.text and "prod" in response.text
        ), f"422 did not mention the derived choices: {response.text}"

    def test_required_param_is_required(self, client, check_mock_http):
        """`env: Required[...]` is derived as required=True, so a run without it fails.

        The manager accepts the request and validates while dispatching (that is
        where `pipeline.resolve()` runs), so the rejection shows up as a failed
        execution rather than a 4xx on the POST.
        """
        execution_id, stats = _run_pipeline(client, {})
        assert (
            stats["state"] == "failed"
        ), f"Expected the execution to fail without 'env', got {stats}"
        assert stats["total_jobs"] == 0, f"No job should be dispatched: {stats}"
