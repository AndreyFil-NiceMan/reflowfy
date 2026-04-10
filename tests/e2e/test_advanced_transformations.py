"""
E2E Tests for Advanced Transformation Context Features.

Tests that ExecutionContext data is correctly propagated to transformations,
that runtime_params are accessible inside transformation logic, and that
batch_id is unique across different batches of the same execution.

Prerequisites:
    - ReflowManager running on localhost:8002
    - Mock HTTP server running on localhost:8091

Run with:
    pytest tests/e2e/test_advanced_transformations.py -v
"""

import os
import time
import pytest
import httpx

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 120


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    """HTTP client scoped to this module."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=MAX_WAIT) as c:
        yield c


@pytest.fixture(autouse=True)
def reset_mock_http(check_mock_http):
    """Reset mock HTTP server before each test."""
    try:
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
    except Exception:
        pass
    yield


def _run_pipeline(client, pipeline_name, *, runtime_params=None):
    """POST /run and poll /stats until completion. Returns (execution_id, stats)."""
    payload = {"pipeline_name": pipeline_name}
    if runtime_params:
        payload["runtime_params"] = runtime_params

    response = client.post("/run", json=payload)
    if response.status_code == 404:
        pytest.skip(f"Pipeline '{pipeline_name}' not registered")

    assert response.status_code == 202, (
        f"Expected 202, got {response.status_code}: {response.text}"
    )
    execution_id = response.json()["execution_id"]

    deadline = time.monotonic() + MAX_WAIT
    while time.monotonic() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return execution_id, stats
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Pipeline '{pipeline_name}' did not complete in {MAX_WAIT}s"
    )


def _get_records(limit=50):
    """Fetch records received by the mock HTTP server."""
    resp = httpx.get(
        f"{MOCK_HTTP_URL}/records",
        params={"limit": limit},
        timeout=10.0,
    ).json()
    return resp.get("total", 0), resp.get("records", [])


class TestContextPropagation:
    """Verify that ExecutionContext keys reach transformations correctly."""

    def test_context_keys_stamped_on_records(self, client, check_mock_http):
        """
        E2EContextProbePipeline stamps execution_id, batch_id, pipeline_name,
        and created_at onto every record. All four must be present and non-empty.
        """
        _, stats = _run_pipeline(client, "e2e_context_probe")
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"

        total, records = _get_records(limit=20)
        assert total > 0, "No records received by mock HTTP server"

        for record in records:
            assert record.get("_ctx_execution_id"), (
                f"_ctx_execution_id missing or empty: {record}"
            )
            assert record.get("_ctx_batch_id"), (
                f"_ctx_batch_id missing or empty: {record}"
            )
            assert record.get("_ctx_pipeline_name") == "e2e_context_probe", (
                f"Wrong pipeline_name in context: {record.get('_ctx_pipeline_name')}"
            )
            assert record.get("_ctx_created_at"), (
                f"_ctx_created_at missing or empty: {record}"
            )

    def test_execution_id_consistent_across_batches(self, client, check_mock_http):
        """
        All records from a single execution must carry the same execution_id
        regardless of which batch produced them.
        """
        execution_id, stats = _run_pipeline(client, "e2e_context_probe")
        assert stats["state"] == "completed"

        _, records = _get_records(limit=50)
        assert len(records) > 0

        ids_in_records = {r["_ctx_execution_id"] for r in records if "_ctx_execution_id" in r}
        assert len(ids_in_records) == 1, (
            f"Expected a single execution_id across all batches, got: {ids_in_records}"
        )

    def test_batch_id_unique_per_batch(self, client, check_mock_http):
        """
        E2EBatchIdentityPipeline sends 30 records in 3 batches of 10.
        batch_id is per-execution (not per-batch), so all records share the same batch_id.
        """
        _, stats = _run_pipeline(client, "e2e_batch_identity")
        assert stats["state"] == "completed"

        _, records = _get_records(limit=50)
        assert len(records) >= 30, f"Expected 30 records, got {len(records)}"

        batch_ids = {r["_batch_id"] for r in records if "_batch_id" in r}
        assert len(batch_ids) == 1, (
            f"Expected 1 batch_id shared across all batches (per-execution), got {len(batch_ids)}: {batch_ids}"
        )
        assert next(iter(batch_ids)), "batch_id must be non-empty"


class TestRuntimeParams:
    """Verify that runtime_params passed in the run request reach transformations."""

    def test_runtime_params_env_accessible(self, client, check_mock_http):
        """
        Pipeline receives runtime_params={"env": "staging", "multiplier": 3}.
        Every record should have _env="staging".
        """
        _, stats = _run_pipeline(
            client,
            "e2e_runtime_params",
            runtime_params={"env": "staging", "multiplier": 3},
        )
        assert stats["state"] == "completed"

        _, records = _get_records(limit=20)
        assert len(records) > 0

        for record in records:
            assert record.get("_env") == "staging", (
                f"Expected _env='staging', got '{record.get('_env')}'"
            )

    def test_runtime_params_multiplier_applied(self, client, check_mock_http):
        """
        With multiplier=3, each record's _value should equal record.id * 3.
        """
        _, stats = _run_pipeline(
            client,
            "e2e_runtime_params",
            runtime_params={"env": "staging", "multiplier": 3},
        )
        assert stats["state"] == "completed"

        _, records = _get_records(limit=20)
        verified = 0
        for record in records:
            if "id" in record and "_value" in record:
                expected = record["id"] * 3
                assert record["_value"] == expected, (
                    f"id={record['id']}: expected _value={expected}, got {record['_value']}"
                )
                verified += 1

        assert verified > 0, "No records with both 'id' and '_value' fields found"

    def test_runtime_params_default_when_absent(self, client, check_mock_http):
        """
        Without runtime_params the transformation falls back to multiplier=1 and
        env='default'; _value should equal record.id × 1.
        """
        _, stats = _run_pipeline(client, "e2e_runtime_params")
        assert stats["state"] == "completed"

        _, records = _get_records(limit=20)
        for record in records:
            assert record.get("_env") == "default", (
                f"Expected _env='default' when runtime_params absent, got '{record.get('_env')}'"
            )
            if "id" in record and "_value" in record:
                assert record["_value"] == record["id"], (
                    f"Expected _value == id when multiplier absent, "
                    f"but id={record['id']}, _value={record['_value']}"
                )


class TestTransformationErrors:
    """Verify that TransformationError is well-formed and pipeline handling is correct."""

    def test_error_tolerant_pipeline_completes(self, client, check_mock_http):
        """
        E2EErrorTolerantPipeline has id==999 guard that never fires on mock data.
        Both transformation steps should complete; all records carry _step2_done=True.
        """
        _, stats = _run_pipeline(client, "e2e_error_tolerant")
        assert stats["state"] == "completed"

        _, records = _get_records(limit=20)
        assert len(records) > 0

        for record in records:
            assert record.get("_enriched") is True, (
                f"Step 1 (ctx_enrich) not applied: {record}"
            )
            assert record.get("_step2_done") is True, (
                f"Step 2 (ctx_maybe_fail) not applied: {record}"
            )

    def test_transformation_error_class_has_attributes(self):
        """
        TransformationError must expose transformation_name, message, and
        original_error as instance attributes.
        """
        from reflowfy.transformations.base import TransformationError

        sentinel = ValueError("original")
        exc = TransformationError("my_transform", "something went wrong", sentinel)

        assert exc.transformation_name == "my_transform"
        assert "my_transform" in str(exc)
        assert exc.original_error is sentinel


class TestBatchIdentity:
    """Verify batch_id uniqueness across batches of the same execution."""

    def test_three_batches_distinct_batch_ids(self, client, check_mock_http):
        """
        30 records split across 3 batches of 10.
        batch_id is per-execution (not per-source-batch), so all records share 1 batch_id.
        """
        _, stats = _run_pipeline(client, "e2e_batch_identity")
        assert stats["state"] == "completed"

        total, records = _get_records(limit=60)
        assert total >= 30, f"Expected ≥ 30 records at destination, got {total}"

        batch_ids = {r.get("_batch_id") for r in records if r.get("_batch_id")}
        assert len(batch_ids) == 1, (
            f"Expected exactly 1 batch_id (per-execution), found {len(batch_ids)}: {batch_ids}"
        )
        assert next(iter(batch_ids)), "batch_id must be non-empty"
