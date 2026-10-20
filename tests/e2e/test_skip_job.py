"""E2E: a pipeline drops one job with SkipJob while its sibling still delivers.

The e2e_skip_job pipeline plans two jobs (one record each). The record marked
``drop`` raises SkipJob — from define_destination by default, or from a
transformation when ``drop_at="transformation"``. Observed end to end:

- the dropped job delivers nothing, and does NOT fail the execution
- its reason is stored on the job's stats row (skip_reason)
- the run is counted as reflowfy_jobs_processed_total{status="skipped"}
"""

import os
import re
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
# In distributed mode the worker drops the job, so the counter is in the worker's
# registry (scripts/e2e-distributed.override.yml publishes it here). Not up in local mode.
WORKER_METRICS_URL = os.getenv("E2E_WORKER_METRICS_URL", "http://localhost:9102/metrics")
POLL_INTERVAL = 2
MAX_WAIT = 60

SKIPPED_METRIC = re.compile(
    r'reflowfy_jobs_processed_total\{[^}]*pipeline="e2e_skip_job"[^}]*status="skipped"[^}]*\}'
    r"\s+([0-9.e+]+)"
)


def _run(client, marker, **params):
    body = {
        "pipeline_name": "e2e_skip_job",
        "runtime_params": {"marker": marker, **params},
    }
    resp = client.post("/run", json=body)
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
    """Records the mock webhook received for this run only."""
    records = httpx.get(f"{MOCK_URL}/records", timeout=10).json().get("records", [])
    return [
        r["data"]
        for r in records
        if isinstance(r, dict)
        and isinstance(r.get("data"), dict)
        and r["data"].get("marker") == marker
    ]


def _skipped_metric(client):
    """The skipped counter summed over the manager and, when one is up, the worker:
    the job is dropped in whichever process ran it."""
    texts = [client.get("/metrics").text]
    try:
        texts.append(httpx.get(WORKER_METRICS_URL, timeout=5.0).text)
    except httpx.HTTPError:
        pass  # local mode: no worker
    total = 0.0
    for text in texts:
        match = SKIPPED_METRIC.search(text)
        total += float(match.group(1)) if match else 0.0
    return total


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


class TestSkipJob:
    @pytest.mark.parametrize("drop_at", ["destination", "transformation"])
    def test_dropped_job_delivers_nothing_and_does_not_fail(self, client, drop_at):
        marker = f"skip-{drop_at}-{int(time.time())}"

        stats = _wait(client, _run(client, marker, drop_at=drop_at))

        assert stats["state"] == "completed", stats
        assert stats["jobs_failed"] == 0, "a dropped job must not fail the execution"
        assert stats["total_jobs"] == 2, stats

        delivered = _delivered(marker)
        assert [r["id"] for r in delivered] == [
            1
        ], f"only the undropped job may deliver, got {delivered}"

    def test_drop_reason_is_stored_on_the_job(self, client):
        marker = f"skip-stats-{int(time.time())}"
        execution_id = _run(client, marker)
        _wait(client, execution_id)

        jobs = client.get(f"/executions/{execution_id}/jobs").json()
        reasons = [(j["stats"] or {}).get("skip_reason") for j in jobs]
        dropped = [r for r in reasons if r]

        assert len(dropped) == 1, f"exactly one job should carry a skip_reason: {reasons}"
        assert "destination dropped record 2" in dropped[0]
        for j in jobs:
            if (j["stats"] or {}).get("skip_reason"):
                assert j["state"] == "completed", "dropped job is completed, not failed"
                assert j["processed_records"] == 0

    def test_drop_is_counted_as_skipped_in_metrics(self, client):
        before = _skipped_metric(client)

        _wait(client, _run(client, f"skip-metric-{int(time.time())}"))

        assert (
            _skipped_metric(client) == before + 1
        ), "the dropped job must be counted as status=skipped, not completed"
