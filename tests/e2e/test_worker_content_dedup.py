"""E2E: worker-side content deduplication.

enable_duplicate_jobs=False now means the WORKER deduplicates by record
content (not the manager by descriptor). Observed at the destination:
- same payload twice  -> second run delivers nothing, deduplicated_jobs=1
- changed payload     -> delivered again
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 60


def _run(client, payload, **extra):
    body = {
        "pipeline_name": "e2e_content_dedup",
        "runtime_params": {"payload": payload},
        **extra,
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


def _records():
    return httpx.get(f"{MOCK_URL}/records", timeout=10).json()


def _reset():
    httpx.delete(f"{MOCK_URL}/reset", timeout=10)


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


class TestWorkerContentDedup:
    def test_same_payload_twice_delivers_once(self, client):
        payload = f"dup-{int(time.time())}"
        _reset()

        first = _wait(client, _run(client, payload))
        assert first["state"] == "completed", first
        after_first = len(_records())
        assert after_first >= 1, "first run must deliver records"

        second = _wait(client, _run(client, payload))
        assert second["state"] == "completed", second
        assert len(_records()) == after_first, (
            "second run with identical data must deliver nothing"
        )
        assert second["deduplicated_jobs"] >= 1, (
            "worker must report the skipped job as deduplicated"
        )
        assert second["jobs_failed"] == 0

    def test_changed_payload_delivers_again(self, client):
        base = f"chg-{int(time.time())}"
        _reset()

        _wait(client, _run(client, base + "-v1"))
        n_after_v1 = len(_records())
        assert n_after_v1 >= 1

        changed = _wait(client, _run(client, base + "-v2"))
        assert changed["state"] == "completed", changed
        assert len(_records()) > n_after_v1, (
            "changed data must be re-processed and delivered"
        )
        assert changed["deduplicated_jobs"] == 0

    def test_dedup_run_still_creates_and_dispatches_jobs(self, client):
        """Unlike the old manager-side dedup, jobs are now always created
        and dispatched; dedup happens at the worker."""
        payload = f"jobs-{int(time.time())}"
        _wait(client, _run(client, payload))
        second = _wait(client, _run(client, payload))
        assert second["total_jobs"] > 0, (
            "jobs are always created now; dedup is a worker outcome"
        )
        assert second["deduplicated_jobs"] == second["total_jobs"]
