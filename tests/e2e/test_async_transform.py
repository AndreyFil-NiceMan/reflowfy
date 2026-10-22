"""E2E: async transformations — a per-record lookup runs concurrently, capped.

The e2e_async_transform pipeline runs one job: a ``RecordTransformation`` that
calls the mock API's ``/slow-users`` once per record (``concurrency`` at a time),
an ``async def`` batch step, and a sync step. Observed end to end:

- every record arrives enriched by all three steps, in input order
- the lookups really overlap, and never exceed the cap (the mock counts them)
- a 404 drops one record; a 500 fails the job naming the record's index
- SkipJob from the async step completes the job with nothing delivered
- ``reflowfy test`` runs the same pipeline outside the stack
"""

import os
import time
from typing import Any, Dict, List

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_HTTP_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
MOCK_API_URL = os.getenv("E2E_MOCK_API_URL", "http://localhost:8092")
PIPELINE = "e2e_async_transform"
STEPS = {"e2e_enrich_slow_user", "e2e_async_label", "e2e_sync_stamp"}
POLL_INTERVAL = 1
MAX_WAIT = 90


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


def _marker(name: str) -> str:
    return f"async-{name}-{time.time_ns()}"


def _run(client: httpx.Client, marker: str, **params: Any) -> str:
    body = {"pipeline_name": PIPELINE, "runtime_params": {"marker": marker, **params}}
    resp = client.post("/run", json=body)
    assert resp.status_code == 202, f"/run failed: {resp.text}"
    return resp.json()["execution_id"]


def _wait(client: httpx.Client, execution_id: str) -> Dict[str, Any]:
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{execution_id} did not finish in {MAX_WAIT}s")


def _jobs(client: httpx.Client, execution_id: str) -> List[Dict[str, Any]]:
    return client.get(f"/executions/{execution_id}/jobs").json()


def _delivered(marker: str) -> List[Dict[str, Any]]:
    """Records the mock webhook received for this run only, in arrival order."""
    total = httpx.get(f"{MOCK_HTTP_URL}/records", params={"limit": 1}, timeout=10).json()["total"]
    records = httpx.get(
        f"{MOCK_HTTP_URL}/records", params={"limit": total}, timeout=30
    ).json()["records"]
    return [
        r["data"]
        for r in records
        if isinstance(r, dict)
        and isinstance(r.get("data"), dict)
        and r["data"].get("marker") == marker
    ]


def _api_stats(marker: str) -> Dict[str, int]:
    return httpx.get(f"{MOCK_API_URL}/slow-users/{marker}/stats", timeout=10).json()


class TestAsyncTransformations:
    def test_every_record_is_enriched_by_every_step_in_order(self, client):
        marker = _marker("order")
        execution_id = _run(client, marker, count=40, concurrency=8, delay_ms=50)

        stats = _wait(client, execution_id)

        assert stats["state"] == "completed", stats
        delivered = _delivered(marker)
        assert [r["id"] for r in delivered] == list(range(1, 41))
        for r in delivered:
            assert r["user"] == f"User {r['id']}"
            assert (r["label"], r["stamp"]) == ("async", "sync")
        (job,) = _jobs(client, execution_id)
        assert set((job["stats"] or {}).get("transformation_times", {})) == STEPS

    def test_lookups_overlap_but_never_exceed_the_cap(self, client):
        marker = _marker("cap")
        execution_id = _run(client, marker, count=40, concurrency=8, delay_ms=200)

        assert _wait(client, execution_id)["state"] == "completed"

        api = _api_stats(marker)
        assert api["calls"] == 40
        assert 2 <= api["peak_concurrency"] <= 8, api
        (job,) = _jobs(client, execution_id)
        step_seconds = job["stats"]["transformation_times"]["e2e_enrich_slow_user"]
        # One by one: 40 x 200 ms = 8 s. Eight at a time: about 1 s.
        assert step_seconds < 4, f"lookups did not run concurrently ({step_seconds}s)"

    def test_concurrency_one_runs_the_lookups_one_by_one(self, client):
        marker = _marker("serial")
        execution_id = _run(client, marker, count=6, concurrency=1, delay_ms=50)

        assert _wait(client, execution_id)["state"] == "completed"
        assert _api_stats(marker) == {"calls": 6, "peak_concurrency": 1}

    def test_a_404_drops_only_that_record(self, client):
        marker = _marker("drop")
        execution_id = _run(client, marker, count=10, delay_ms=10, drop_id=5)

        assert _wait(client, execution_id)["state"] == "completed"
        assert [r["id"] for r in _delivered(marker)] == [1, 2, 3, 4, 6, 7, 8, 9, 10]

    def test_a_500_fails_the_job_naming_the_transformation_and_the_record(self, client):
        marker = _marker("fail")
        execution_id = _run(client, marker, count=10, delay_ms=10, fail_id=7)

        stats = _wait(client, execution_id)

        assert stats["state"] == "failed", stats
        assert _delivered(marker) == []
        errors = client.get(f"/executions/{execution_id}/errors").json()
        assert len(errors) == 1
        message = errors[0]["error_message"]
        assert "e2e_enrich_slow_user" in message
        assert "record 6" in message  # id 7 is at index 6

    def test_skip_job_from_an_async_step_completes_with_nothing_delivered(self, client):
        marker = _marker("skip")
        execution_id = _run(client, marker, count=5, delay_ms=10, skip=True)

        stats = _wait(client, execution_id)

        assert stats["state"] == "completed", stats
        assert stats["jobs_failed"] == 0
        assert _delivered(marker) == []
        (job,) = _jobs(client, execution_id)
        assert job["stats"]["skip_reason"] == "async step asked to skip"
        assert job["processed_records"] == 0


class TestAsyncTransformationsCli:
    def test_reflowfy_test_runs_the_async_pipeline(self, check_reflow_manager):
        """`reflowfy test` (no Docker) goes through the same async core."""
        from typer.testing import CliRunner

        from reflowfy.cli.main import app

        marker = _marker("cli")
        result = CliRunner().invoke(
            app,
            [
                "test",
                PIPELINE,
                "--dry-run",
                "--no-input",
                "--json",
                "-p",
                f"marker={marker}",
                "-p",
                "count=12",
                "-p",
                "delay_ms=20",
            ],
            env={"MOCK_API_URL": MOCK_API_URL, "PIPELINE_MODULE": "tests.e2e.test_pipelines"},
        )

        assert result.exit_code == 0, result.output
        assert _api_stats(marker)["calls"] == 12
