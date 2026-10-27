"""E2E: record_type and validate_steps on a running stack (pipelines in typed_records_pipeline.py).

- skip: the invalid order is dropped before any transformation, the rest are
  delivered, and the skip is on the job's stats and the skipped-records counter
- fail: the job fails naming the record and the field, nothing is delivered
- step: a transformation that breaks its own return type fails the job naming it
"""

import os
import re
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 60

SKIPPED_METRIC = re.compile(
    r'reflowfy_records_skipped_total\{[^}]*pipeline="e2e_typed_records_skip"'
    r'[^}]*transformation="record_type"[^}]*\}\s+([0-9.e+]+)'
)


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


def _run(client, pipeline, marker):
    resp = client.post(
        "/run", json={"pipeline_name": pipeline, "runtime_params": {"marker": marker}}
    )
    assert resp.status_code == 202, f"/run failed: {resp.text}"
    return resp.json()["execution_id"]


def _wait(client, execution_id):
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{execution_id} did not finish in {MAX_WAIT}s")


def _delivered(marker):
    records = httpx.get(f"{MOCK_URL}/records", timeout=10).json().get("records", [])
    return [
        r
        for r in records
        if isinstance(r, dict)
        and isinstance(r.get("data"), dict)
        and r["data"].get("marker") == marker
    ]


def _skipped_metric(client):
    match = SKIPPED_METRIC.search(client.get("/metrics").text)
    return float(match.group(1)) if match else 0.0


class TestTypedRecords:
    def test_skip_drops_the_invalid_record_and_delivers_the_rest(self, client):
        marker = f"typed-skip-{time.time_ns()}"
        before = _skipped_metric(client)

        execution_id = _run(client, "e2e_typed_records_skip", marker)
        stats = _wait(client, execution_id)

        assert stats["state"] == "completed", stats
        assert stats["jobs_failed"] == 0, stats
        delivered = _delivered(marker)
        assert sorted(r["data"]["id"] for r in delivered) == [1, 3], delivered
        assert {r["data"]["id"]: r["tax"] for r in delivered} == {1: 2.0, 3: 1.0}

        (job,) = client.get(f"/executions/{execution_id}/jobs").json()
        assert job["stats"]["records_input"] == 3
        assert job["stats"]["records_skipped"] == 1
        assert job["stats"]["record_skip_reasons"] == {"invalid data.amount: missing": 1}
        assert "record_type" in job["stats"]["transformation_times"]
        assert _skipped_metric(client) == before + 1

    def test_fail_fails_the_job_naming_the_record(self, client):
        marker = f"typed-fail-{time.time_ns()}"

        execution_id = _run(client, "e2e_typed_records_fail", marker)
        stats = _wait(client, execution_id)

        assert stats["state"] == "failed", stats
        assert stats["jobs_failed"] == 1, stats
        (error,) = client.get(f"/executions/{execution_id}/errors").json()
        assert (
            "record 2 of 3 does not match TypedRecord[Order]: data.amount: missing"
            in error["error_message"]
        ), error
        assert "RecordValidationError" in error["error_traceback"]
        assert _delivered(marker) == []

    def test_validate_steps_fails_the_step_that_broke_its_return_type(self, client):
        marker = f"typed-step-{time.time_ns()}"

        execution_id = _run(client, "e2e_typed_records_step", marker)
        stats = _wait(client, execution_id)

        assert stats["state"] == "failed", stats
        (error,) = client.get(f"/executions/{execution_id}/errors").json()
        assert "Transformation 'e2e_break_tax' failed" in error["error_message"], error
        assert (
            "returned record 1 of 3 does not match TaxedOrder: tax: float_type"
            in error["error_message"]
        ), error
        assert "StepOutputError" in error["error_traceback"]
        assert _delivered(marker) == []
