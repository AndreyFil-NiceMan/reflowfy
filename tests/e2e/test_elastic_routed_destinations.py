"""
E2E test for elastic source routing into two destinations.

Verifies:
- Elastic scroll batches become jobs.
- Transformation adds per-record metadata.
- Destination is selected per job based on transformation metadata.
- Both destination routes are used for a single execution.
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
PIPELINE_NAME = "e2e_elastic_routed_destinations"
POLL_INTERVAL = 2
MAX_WAIT = 180


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


def _run_pipeline(client) -> tuple[str, dict]:
    response = client.post(
        "/run",
        json={
            "pipeline_name": PIPELINE_NAME,
            "runtime_params": {
                "start_time": "2020-01-01T00:00:00",
                "end_time": "2030-12-31T23:59:59",
            },
        },
    )

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


def _get_mock_stats() -> dict:
    resp = httpx.get(f"{MOCK_HTTP_URL}/stats", timeout=10.0)
    resp.raise_for_status()
    return resp.json()


def _get_mock_records() -> list:
    resp = httpx.get(f"{MOCK_HTTP_URL}/records", params={"limit": 2000}, timeout=10.0)
    resp.raise_for_status()
    return resp.json().get("records", [])


class TestElasticRoutedDestinations:
    def test_routes_jobs_to_two_destinations(self, client, check_elasticsearch):
        execution_id, stats = _run_pipeline(client)
        if stats["state"] != "completed":
            errors = client.get(f"/executions/{execution_id}/errors").json()
            print(f"Execution errors: {errors}")

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert stats["total_jobs"] > 1, "Expected multiple jobs from elastic scroll"

        mock_stats = _get_mock_stats()
        assert mock_stats["total_batches"] > 0, "No batches received by mock HTTP server"

        routes = set()
        destination_names = set()

        for batch in mock_stats.get("batches", []):
            runtime_params = batch.get("extra_body_fields", {}).get("runtime_params", {})
            if runtime_params.get("execution_id") != execution_id:
                continue

            route = batch.get("query_params", {}).get("route")
            if route:
                routes.add(route)

            destination_name = batch.get("extra_body_fields", {}).get("destination_name")
            if destination_name:
                destination_names.add(destination_name)

        assert "primary" in routes, f"Expected primary route in {routes}"
        assert "secondary" in routes, f"Expected secondary route in {routes}"
        assert "primary" in destination_names
        assert "secondary" in destination_names

    def test_transformation_metadata_present_on_records(self, client, check_elasticsearch):
        execution_id, stats = _run_pipeline(client)
        if stats["state"] != "completed":
            errors = client.get(f"/executions/{execution_id}/errors").json()
            print(f"Execution errors: {errors}")

        assert stats["state"] == "completed"

        records = [r for r in _get_mock_records() if r.get("_execution_id") == execution_id]
        assert records, "No records for this execution received by mock HTTP server"

        route_targets = {r.get("_route_target") for r in records}

        for record in records:
            assert record.get("_source_type") == "elasticsearch"
            assert record.get("_test_pipeline") == "elastic_routed_destinations"
            assert record.get("_event_type") is not None
            assert isinstance(record.get("_has_amount"), bool)
            assert isinstance(record.get("_page_num"), int)
            assert record.get("_route_target") in {"primary", "secondary"}

        assert "primary" in route_targets
        assert "secondary" in route_targets
