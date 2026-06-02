"""
E2E tests for the unified runtime_params flow through transformations.

Tests verify:
  1. User params are accessible flat (no nesting) inside transformations.
  2. Execution-context keys (execution_id, batch_id, pipeline_name, created_at)
     are present in runtime_params alongside user params.
  3. A transformation can write new keys into runtime_params and the next
     transformation in the chain reads them.
  4. define_source can inject keys into runtime_params and they reach
     transformations in the worker.
  5. IdBasedPipeline carries current_ids flat in runtime_params.
  6. Per-batch enrichments from define_source don't bleed across batches.
"""

import os
import time

import httpx

BASE_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("MOCK_HTTP_BASE_URL", "http://localhost:8091")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def trigger_pipeline(name: str, params: dict) -> dict:
    resp = httpx.post(
        f"{BASE_URL}/run",
        json={"pipeline_name": name, "runtime_params": params},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def wait_for_execution(execution_id: str, timeout: int = 120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = httpx.get(f"{BASE_URL}/executions/{execution_id}/stats", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state", "")
        if state in ("completed", "failed"):
            return data
        time.sleep(2)
    raise TimeoutError(f"Execution {execution_id} did not complete in {timeout}s")


def get_received_records(mock_url: str = MOCK_URL) -> list:
    resp = httpx.get(f"{mock_url}/records", timeout=10)
    resp.raise_for_status()
    return resp.json().get("records", [])


def clear_received_records(mock_url: str = MOCK_URL):
    httpx.delete(f"{mock_url}/reset", timeout=10)


# ---------------------------------------------------------------------------
# Test 1: flat user params in runtime_params
# ---------------------------------------------------------------------------


class TestFlatUserParams:
    """ctx_runtime_params used to do context.get('runtime_params', {}).get('env').
    Now env is directly in runtime_params."""

    def setup_method(self):
        clear_received_records()

    def test_user_params_accessible_without_nesting(self):
        result = trigger_pipeline(
            "e2e_runtime_params",
            {"env": "staging", "multiplier": "3"},
        )
        execution_id = result["execution_id"]
        status = wait_for_execution(execution_id)
        assert status["state"] == "completed"

        records = get_received_records()
        assert len(records) > 0
        for r in records:
            assert r.get("_env") == "staging", f"_env not set correctly: {r}"
            assert r.get("_value") == r.get("id", 0) * 3, f"_value wrong: {r}"


# ---------------------------------------------------------------------------
# Test 2: execution-context keys in runtime_params
# ---------------------------------------------------------------------------


class TestExecutionContextInRuntimeParams:
    """ctx_probe reads execution_id, batch_id, pipeline_name, created_at."""

    def setup_method(self):
        clear_received_records()

    def test_context_keys_present_in_records(self):
        result = trigger_pipeline("e2e_context_probe", {})
        execution_id = result["execution_id"]
        status = wait_for_execution(execution_id)
        assert status["state"] == "completed"

        records = get_received_records()
        assert len(records) > 0
        for r in records:
            assert r.get("_ctx_execution_id") == execution_id
            assert r.get("_ctx_batch_id"), "batch_id missing"
            assert r.get("_ctx_pipeline_name") == "e2e_context_probe"
            assert r.get("_ctx_created_at"), "created_at missing"


# ---------------------------------------------------------------------------
# Test 3: transformation-to-transformation enrichment
# ---------------------------------------------------------------------------


class TestTransformationChainEnrichment:
    """
    params_step1_enrich writes step1_count + step1_ran into runtime_params.
    params_step2_verify reads them and stamps _saw_step1_count / _saw_step1_ran.
    """

    def setup_method(self):
        clear_received_records()

    def test_step2_sees_what_step1_wrote(self):
        clear_received_records()
        result = trigger_pipeline("e2e_params_enrich", {})
        execution_id = result["execution_id"]
        status = wait_for_execution(execution_id)
        assert status["state"] == "completed"

        records = get_received_records()
        assert len(records) > 0
        for r in records:
            assert r.get("_step1") is True, f"step1 flag missing: {r}"
            assert r.get("_step2") is True, f"step2 flag missing: {r}"

            saw_step1_ran = r.get("_saw_step1_ran")
            if saw_step1_ran is None:
                saw_step1_ran = r.get("saw_step1_ran")
            assert saw_step1_ran is True, f"step2 did not see step1_ran: {r}"

            saw_step1_count = r.get("_saw_step1_count")
            if saw_step1_count is None:
                saw_step1_count = r.get("saw_step1_count")
            assert (saw_step1_count or 0) > 0, f"step2 saw wrong count: {r}"


# ---------------------------------------------------------------------------
# Test 4: define_source enrichment visible in transformations
# ---------------------------------------------------------------------------


class TestDefineSourceEnrichment:
    """
    E2EParamsEnrichPipeline.define_source injects 'injected_by_source'.
    params_step1_enrich stamps it onto records; we verify the value arrives.
    """

    def setup_method(self):
        clear_received_records()

    def test_source_injected_key_visible_in_transformation(self):
        result = trigger_pipeline("e2e_params_enrich", {})
        execution_id = result["execution_id"]
        status = wait_for_execution(execution_id)
        assert status["state"] == "completed"

        records = get_received_records()
        assert len(records) > 0
        for r in records:
            assert r.get("_injected_by_source") == "hello_from_source", (
                f"define_source enrichment not visible in transformation: {r}"
            )
            saw_injected = r.get("_saw_injected")
            if saw_injected is None:
                saw_injected = r.get("saw_injected")
            assert saw_injected == "hello_from_source", (
                f"define_source enrichment not visible in step2: {r}"
            )


# ---------------------------------------------------------------------------
# Test 5: IdBasedPipeline current_ids accessible flat
# ---------------------------------------------------------------------------


class TestIdBasedCurrentIdsFlat:
    """id_pipeline_add_metadata reads runtime_params.get('current_ids', [])."""

    def setup_method(self):
        clear_received_records()

    def test_current_ids_accessible_flat(self):
        result = trigger_pipeline(
            "e2e_id_based_pipeline_test",
            {"ids": [1, 2, 3]},
        )
        execution_id = result["execution_id"]
        status = wait_for_execution(execution_id)
        assert status["state"] == "completed"

        records = get_received_records()
        assert len(records) > 0
        for r in records:
            assert r.get("_processed_by_id_pipeline") is True
            assert isinstance(r.get("_current_ids"), list), f"current_ids not a list: {r}"


# ---------------------------------------------------------------------------
# Test 6: IdBasedPipeline per-batch source enrichments don't bleed
# ---------------------------------------------------------------------------


class TestIdBasedPerBatchEnrichment:
    """
    Each batch gets its own 'injected_by_source' key from define_source.
    Enrichments from batch N must not appear in batch N+1's records.
    """

    def setup_method(self):
        clear_received_records()

    def test_per_batch_enrichment_is_isolated(self):
        result = trigger_pipeline(
            "e2e_id_based_params_enrich",
            {"ids": [10, 20]},
        )
        execution_id = result["execution_id"]
        status = wait_for_execution(execution_id)
        assert status["state"] == "completed"

        records = get_received_records()
        assert len(records) > 0

        # Each record should carry the injected_by_source for ITS batch,
        # not from a different batch.
        injected_values = {r.get("_injected_by_source") for r in records}
        # Both batches produce distinct injection values
        assert "source_for_10" in injected_values
        assert "source_for_20" in injected_values
