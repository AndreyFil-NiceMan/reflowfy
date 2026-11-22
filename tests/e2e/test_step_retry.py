"""E2E: step retries (``RetryConfig``) on a real run.

``e2e_step_retry`` (``test_pipelines/step_retry_pipeline.py``) has a source, a per-record
and a batch transformation, and a destination that each call the mock API, which
answers 503 to the first ``fail_first`` calls per step. The tests check what the mock
received (how often each step was called, how far apart), the job's stored
``step_retries``, and ``reflowfy_step_retries_total`` on the manager's ``/metrics``
(local mode runs jobs in the manager's process).
"""

import os
import re
import time
import uuid
from typing import Any, Dict, List

import httpx
import pytest

from tests.e2e.polling import POLL_INTERVAL

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
PIPELINE = "e2e_step_retry"
MAX_WAIT = 90
TERMINAL = ("completed", "failed", "cancelled")

SOURCE_STEP = "source:E2EFlakyHttpSource"
RECORD_STEP = "e2e_step_retry_record"
BATCH_STEP = "e2e_step_retry_batch"
DEST_STEP = "destination:ApiDestination"


@pytest.fixture(scope="module")
def manager():
    client = httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=30)
    try:
        client.get("/health").raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f"ReflowManager not reachable at {REFLOW_MANAGER_URL}: {exc}")
    yield client
    client.close()


def _run(manager: httpx.Client, **params: Any) -> Dict[str, Any]:
    """Start the pipeline with ``params``, wait for the end; the execution's stats."""
    resp = manager.post("/run", json={"pipeline_name": PIPELINE, "runtime_params": params})
    assert resp.status_code == 202, resp.text
    execution_id = resp.json()["execution_id"]
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        stats = manager.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in TERMINAL:
            return {"execution_id": execution_id, **stats}
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{execution_id} did not finish in {MAX_WAIT}s")


def _job_stats(manager: httpx.Client, execution_id: str) -> Dict[str, Any]:
    jobs = manager.get(f"/executions/{execution_id}/jobs").json()
    assert len(jobs) == 1, jobs
    return jobs[0]["stats"] or {}


def _calls(run: str, step: str) -> List[Dict[str, Any]]:
    return httpx.get(f"{MOCK_URL}/gaps/{run}/{step}", timeout=10).json()["requests"]


def _statuses(run: str, step: str) -> List[int]:
    return [call["status"] for call in _calls(run, step)]


def _retries_metric(manager: httpx.Client, step: str) -> float:
    """``reflowfy_step_retries_total{pipeline="e2e_step_retry", step=...}``, 0 if absent."""
    body = manager.get("/metrics").text
    pattern = (
        r'^reflowfy_step_retries_total\{pipeline="'
        + PIPELINE
        + r'",step="'
        + re.escape(step)
        + r'"\} ([0-9.e+]+)$'
    )
    match = re.search(pattern, body, re.MULTILINE)
    return float(match.group(1)) if match else 0.0


def _key(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


class TestStepRetry:
    def test_every_step_retries_and_the_run_completes(self, manager: httpx.Client):
        run = _key("sr")
        steps = {SOURCE_STEP: 2, RECORD_STEP: 2, BATCH_STEP: 1, DEST_STEP: 1}
        before = {step: _retries_metric(manager, step) for step in steps}

        stats = _run(manager, run=run, source_fail=2, record_fail=2, batch_fail=1, dest_fail=1)

        assert stats["state"] == "completed", stats
        # The source: two 503s, then the fetch that worked, backoff between them.
        source = _calls(run, "source")
        assert [c["status"] for c in source] == [503, 503, 200]
        assert source[1]["at"] - source[0]["at"] >= 0.15  # min_wait_seconds=0.2
        # One record at a time: the first record failed twice, the others never.
        assert _statuses(run, "record") == [503, 503, 200, 200, 200]
        assert _statuses(run, "batch") == [503, 200]
        # The destination's first send failed and the whole send was retried: every
        # record arrived, carrying what both transformations added.
        dest = _calls(run, "dest")
        assert dest[0]["status"] == 503
        delivered = [c["body"] for c in dest if c["status"] == 200]
        assert sorted({r["id"] for r in delivered}) == [0, 1, 2]
        assert all(r["looked_up"] and r["batched"] for r in delivered)

        job = _job_stats(manager, stats["execution_id"])
        assert job.get("step_retries") == steps, job
        for step, retries in steps.items():
            assert _retries_metric(manager, step) == before[step] + retries, step

    def test_a_source_that_keeps_failing_fails_the_job(self, manager: httpx.Client):
        run = _key("srx")
        stats = _run(manager, run=run, source_fail=10)

        assert stats["state"] == "failed", stats
        assert _statuses(run, "source") == [503, 503, 503]  # max_attempts=3
        # Nothing past the source ran.
        assert _calls(run, "record") == [] and _calls(run, "dest") == []
        job = _job_stats(manager, stats["execution_id"])
        assert job.get("step_retries") == {SOURCE_STEP: 2}, job
        assert job.get("success") is False
        assert "503" in (job.get("error") or ""), job

    def test_a_run_without_failures_records_no_retries(self, manager: httpx.Client):
        run = _key("sr0")
        stats = _run(manager, run=run)

        assert stats["state"] == "completed", stats
        assert _statuses(run, "source") == [200]
        assert _statuses(run, "record") == [200, 200, 200]
        assert _statuses(run, "batch") == [200]
        assert _statuses(run, "dest") == [200, 200, 200]
        job = _job_stats(manager, stats["execution_id"])
        assert "step_retries" not in job, job
