"""E2E: per-record deduplication (enable_duplicate_jobs=False + dedup_key).

Pipeline: tests/e2e/test_pipelines/record_dedup_pipeline.py — two jobs with
overlapping ids (1-4, 3-6), records stamped with sent_at, dedup_key = (run, id).
Observed at the mock HTTP destination:
- one run delivers ids 1-6 exactly once
- the same run again delivers nothing, every job deduplicated
- a new run tag is new work
"""

import os
import time
from collections import Counter

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 60


def _run(client, run_tag):
    resp = client.post(
        "/run", json={"pipeline_name": "e2e_record_dedup", "runtime_params": {"run": run_tag}}
    )
    assert resp.status_code == 202, f"/run failed: {resp.text}"
    execution_id = resp.json()["execution_id"]
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{execution_id} did not finish in {MAX_WAIT}s")


def _ids(run_tag):
    records = httpx.get(f"{MOCK_URL}/records", params={"limit": 1000}, timeout=10).json()
    return Counter(r["id"] for r in records.get("records", []) if r.get("run") == run_tag)


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


class TestRecordDedup:
    def test_overlapping_jobs_deliver_each_record_once(self, client):
        tag = f"overlap-{time.time_ns()}"
        stats = _run(client, tag)
        assert stats["state"] == "completed", stats
        assert stats["total_jobs"] == 2 and stats["jobs_failed"] == 0
        assert _ids(tag) == Counter({i: 1 for i in range(1, 7)})

    def test_repeating_the_run_delivers_nothing(self, client):
        tag = f"repeat-{time.time_ns()}"
        _run(client, tag)
        again = _run(client, tag)
        assert again["state"] == "completed", again
        assert again["deduplicated_jobs"] == again["total_jobs"] == 2
        assert _ids(tag) == Counter({i: 1 for i in range(1, 7)})

    def test_a_new_run_tag_is_new_work(self, client):
        first, second = f"a-{time.time_ns()}", f"b-{time.time_ns()}"
        _run(client, first)
        stats = _run(client, second)
        assert stats["deduplicated_jobs"] == 0
        assert sum(_ids(second).values()) == 6
