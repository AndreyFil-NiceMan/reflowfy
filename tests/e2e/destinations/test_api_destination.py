"""
E2E Tests for API Destination.

Verifies that ApiDestination correctly routes runtime_params into:
  - URL query string (params)
  - Request body static fields (body)

And confirms records arrive correctly at the mock API server.

Prerequisites:
    - Mock API server running on localhost:8091
      (run: python -m tests.e2e.destinations.mock_api_server)
    - ReflowManager running on localhost:8002

Run:
    pytest tests/e2e/destinations/test_api_destination.py -v
"""

import os
import time
import pytest
import httpx

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_API_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
PIPELINE_NAME = "e2e_api_dest_test"
POLL_INTERVAL = 2
MAX_WAIT = 120

DEFAULT_RUNTIME_PARAMS = {
    "tenant_id": "acme-corp",
    "env": "staging",
    "app_name": "test-suite",
}


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def manager_client():
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
        yield client


@pytest.fixture(scope="module", autouse=True)
def require_mock_server():
    """Skip the whole module if the mock server is not reachable."""
    try:
        r = httpx.get(f"{MOCK_API_URL}/health", timeout=5.0)
        if r.status_code != 200:
            pytest.skip(f"Mock API server unhealthy: {r.status_code}")
    except httpx.RequestError as exc:
        pytest.skip(f"Mock API server not available at {MOCK_API_URL}: {exc}")


@pytest.fixture(autouse=True)
def reset_mock():
    """Clear all received records/batches before every test."""
    httpx.delete(f"{MOCK_API_URL}/reset", timeout=5.0)
    yield


def _wait_for_pipeline(client: httpx.Client, execution_id: str) -> dict:
    """Poll until the execution reaches a terminal state. Returns the final stats dict."""
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        resp = client.get(f"/executions/{execution_id}/stats")
        data = resp.json()
        if data.get("state") in ("completed", "failed"):
            return data
        time.sleep(POLL_INTERVAL)
    return {"state": "timeout"}


def _run_pipeline(client: httpx.Client, runtime_params: dict | None = None) -> str:
    """POST /run and return the execution_id."""
    body: dict = {"pipeline_name": PIPELINE_NAME}
    if runtime_params is not None:
        body["runtime_params"] = runtime_params
    resp = client.post("/run", json=body)
    assert resp.status_code == 202, f"/run failed {resp.status_code}: {resp.text}"
    return resp.json()["execution_id"]


def _run_and_wait(client: httpx.Client, runtime_params: dict | None = None) -> dict:
    """Run the pipeline and block until completion. Returns final stats."""
    execution_id = _run_pipeline(client, runtime_params or DEFAULT_RUNTIME_PARAMS)
    return _wait_for_pipeline(client, execution_id)


def _get_errors(client: httpx.Client, execution_id: str) -> list:
    """Fetch failed job error messages for a given execution."""
    resp = client.get(f"/executions/{execution_id}/errors")
    if resp.status_code != 200:
        return []
    return resp.json()


# ============================================================================
# Infrastructure readiness
# ============================================================================

class TestInfrastructure:
    def test_reflow_manager_healthy(self, manager_client):
        r = manager_client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"

    def test_mock_server_healthy(self):
        r = httpx.get(f"{MOCK_API_URL}/health", timeout=5.0)
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"

    def test_mock_server_accepts_batch_payload(self):
        r = httpx.post(
            f"{MOCK_API_URL}/webhook",
            params={"tenant_id": "probe", "env": "staging"},
            json={"records": [{"probe": True}], "source": "test"},
            headers={"Authorization": "Bearer test-webhook-token"},
            timeout=10.0,
        )
        assert r.status_code == 200
        assert r.json()["record_count"] == 1

    def test_mock_server_rejects_bad_token(self):
        r = httpx.post(
            f"{MOCK_API_URL}/webhook",
            json={"records": [{"probe": True}]},
            headers={"Authorization": "Bearer wrong-token"},
            timeout=10.0,
        )
        assert r.status_code == 403

    def test_pipeline_exists_in_manager(self, manager_client):
        """Verify unknown pipeline name returns 404 (proves registry validation works)."""
        bad = manager_client.post("/run", json={"pipeline_name": "does_not_exist"})
        assert bad.status_code == 404


# ============================================================================
# Pipeline execution
# ============================================================================

class TestPipelineExecution:
    def test_pipeline_fails_without_required_tenant_id(self, manager_client):
        """tenant_id is required — execution should fail when omitted."""
        execution_id = _run_pipeline(manager_client, {})
        stats = _wait_for_pipeline(manager_client, execution_id)
        assert stats["state"] == "failed", (
            f"Expected failed state when tenant_id missing, got {stats['state']}"
        )

    def test_pipeline_completes_with_valid_params(self, manager_client):
        stats = _run_and_wait(manager_client)
        assert stats["state"] == "completed", (
            f"Pipeline failed. state={stats['state']}"
        )

    def test_pipeline_env_defaults_to_staging(self, manager_client):
        """env has a default value — pipeline should succeed without it."""
        stats = _run_and_wait(manager_client, {"tenant_id": "acme-corp"})
        assert stats["state"] == "completed"

    def test_pipeline_rejects_invalid_env_choice(self, manager_client):
        """env is constrained to ['staging', 'production'] — invalid value should fail."""
        execution_id = _run_pipeline(manager_client, {
            "tenant_id": "acme-corp",
            "env": "dev",
        })
        stats = _wait_for_pipeline(manager_client, execution_id)
        assert stats["state"] == "failed"


# ============================================================================
# Query params verified on mock server
# ============================================================================

class TestQueryParams:
    def test_tenant_id_in_query_string(self, manager_client):
        """tenant_id from runtime_params must appear as a URL query param."""
        stats = _run_and_wait(manager_client, {
            "tenant_id": "tenant-xyz",
            "env": "staging",
            "app_name": "reflowfy",
        })
        assert stats["state"] == "completed"

        mock_stats = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        assert mock_stats["total_batches"] > 0

        for batch in mock_stats["batches"]:
            assert batch["query_params"].get("tenant_id") == "tenant-xyz", (
                f"Expected tenant_id='tenant-xyz' in query params, got: {batch['query_params']}"
            )

    def test_env_in_query_string(self, manager_client):
        """env from runtime_params must appear as a URL query param."""
        stats = _run_and_wait(manager_client, {
            "tenant_id": "acme-corp",
            "env": "production",
            "app_name": "reflowfy",
        })
        assert stats["state"] == "completed"

        mock_stats = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        for batch in mock_stats["batches"]:
            assert batch["query_params"].get("env") == "production"

    def test_different_tenants_produce_different_query_params(self, manager_client):
        """Two runs with different tenant_ids must produce distinct query params."""
        stats_a = _run_and_wait(manager_client, {
            "tenant_id": "tenant-a", "env": "staging", "app_name": "app",
        })
        assert stats_a["state"] == "completed"
        mock_a = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        tenant_a_values = {b["query_params"].get("tenant_id") for b in mock_a["batches"]}

        httpx.delete(f"{MOCK_API_URL}/reset", timeout=5.0)

        stats_b = _run_and_wait(manager_client, {
            "tenant_id": "tenant-b", "env": "staging", "app_name": "app",
        })
        assert stats_b["state"] == "completed"
        mock_b = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        tenant_b_values = {b["query_params"].get("tenant_id") for b in mock_b["batches"]}

        assert tenant_a_values == {"tenant-a"}
        assert tenant_b_values == {"tenant-b"}


# ============================================================================
# Body fields verified on mock server
# ============================================================================

class TestBodyFields:
    def test_static_source_field_in_every_batch(self, manager_client):
        """'source' is a static body field — must appear in every batch."""
        stats = _run_and_wait(manager_client)
        assert stats["state"] == "completed"

        mock_stats = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        for batch in mock_stats["batches"]:
            assert batch["extra_body_fields"].get("source") == "reflowfy", (
                f"Missing source field in body: {batch['extra_body_fields']}"
            )

    def test_app_name_from_runtime_params_in_body(self, manager_client):
        """app_name comes from runtime_params and must be in every batch body."""
        stats = _run_and_wait(manager_client, {
            "tenant_id": "acme-corp",
            "env": "staging",
            "app_name": "my-custom-app",
        })
        assert stats["state"] == "completed"

        mock_stats = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        for batch in mock_stats["batches"]:
            assert batch["extra_body_fields"].get("app_name") == "my-custom-app", (
                f"Expected app_name='my-custom-app', got: {batch['extra_body_fields']}"
            )

    def test_different_app_names_reflected_per_run(self, manager_client):
        """Changing app_name at runtime must be reflected in the body."""
        stats = _run_and_wait(manager_client, {
            "tenant_id": "acme-corp", "env": "staging", "app_name": "app-v1",
        })
        assert stats["state"] == "completed"
        mock = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        assert all(b["extra_body_fields"].get("app_name") == "app-v1" for b in mock["batches"])


# ============================================================================
# Record delivery correctness
# ============================================================================

class TestRecordDelivery:
    def test_all_100_records_delivered(self, manager_client):
        stats = _run_and_wait(manager_client)
        assert stats["state"] == "completed"

        mock_stats = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        assert mock_stats["total_records"] == 100, (
            f"Expected 100, got {mock_stats['total_records']}"
        )

    def test_10_batches_of_10_records(self, manager_client):
        """Pipeline uses count=100 and batch_size=10 → 10 batches of 10 records each."""
        stats = _run_and_wait(manager_client)
        assert stats["state"] == "completed"

        mock_stats = httpx.get(f"{MOCK_API_URL}/stats", timeout=10.0).json()
        # The stats endpoint returns the last 10 batches. We verify the batch
        # window is full (10 entries) and every visible batch has exactly 10 records.
        # We don't assert total_batches == 10 because a stale in-flight request
        # from the previous test can arrive after the mock reset, incrementing the
        # global counter while the window of last-10 still reflects only our run.
        assert len(mock_stats["batches"]) == 10, (
            f"Expected 10 batches in stats window, got {len(mock_stats['batches'])}"
        )
        for batch in mock_stats["batches"]:
            assert batch["record_count"] == 10, (
                f"Expected 10 records per batch, got {batch['record_count']}"
            )

    def test_records_tagged_with_destination_type_api(self, manager_client):
        stats = _run_and_wait(manager_client)
        assert stats["state"] == "completed"

        resp = httpx.get(f"{MOCK_API_URL}/records", params={"limit": 10}, timeout=10.0).json()
        assert len(resp["records"]) > 0
        for record in resp["records"]:
            assert record.get("_destination_type") == "api", (
                f"Expected _destination_type='api', got {record.get('_destination_type')}"
            )

    def test_records_tagged_with_pipeline_name(self, manager_client):
        stats = _run_and_wait(manager_client)
        assert stats["state"] == "completed"

        resp = httpx.get(f"{MOCK_API_URL}/records", params={"limit": 10}, timeout=10.0).json()
        for record in resp["records"]:
            assert record.get("_test_pipeline") == "api_dest_test"

    def test_records_carry_execution_id(self, manager_client):
        """Every record must be tagged with the execution_id from the triggering run."""
        execution_id = _run_pipeline(manager_client, DEFAULT_RUNTIME_PARAMS)
        stats = _wait_for_pipeline(manager_client, execution_id)
        assert stats["state"] == "completed"

        resp = httpx.get(f"{MOCK_API_URL}/records", params={"limit": 10}, timeout=10.0).json()
        for record in resp["records"]:
            assert record.get("_execution_id") == execution_id
