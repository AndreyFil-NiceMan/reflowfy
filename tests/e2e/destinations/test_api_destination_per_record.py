"""
E2E tests for ``api_destination(per_record=True, concurrency=...)``.

Runs ``e2e_api_per_record_test`` and checks, on the mock webhook server, what each
record's request looked like: one ``PUT /items/<run>/<id>`` per record, the record's id
in the URL, the record (or only its ``payload``) as the body, and how many requests of
one job were open at once.

Each test passes its own ``run`` key, so the mock files its requests separately and a
delivery still in flight from another test can't land in this one's assertions.

Prerequisites:
    - Mock webhook server on localhost:8091 (tests.e2e.destinations.mock_api_server)
    - ReflowManager on localhost:8002

Run:
    pytest tests/e2e/destinations/test_api_destination_per_record.py -v
"""

import os
import time
import uuid
from typing import Any, Dict, List

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_API_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091").removesuffix("/webhook")
PIPELINE_NAME = "e2e_api_per_record_test"
MAX_WAIT = 120


@pytest.fixture(scope="module")
def manager():
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
        yield client


@pytest.fixture(scope="module", autouse=True)
def require_mock_server():
    try:
        r = httpx.get(f"{MOCK_API_URL}/health", timeout=5.0)
        if r.status_code != 200:
            pytest.skip(f"Mock API server unhealthy: {r.status_code}")
    except httpx.RequestError as exc:
        pytest.skip(f"Mock API server not available at {MOCK_API_URL}: {exc}")


@pytest.fixture
def run_key() -> str:
    return f"per-record-{uuid.uuid4().hex[:10]}"


def _run_and_wait(manager: httpx.Client, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = manager.post("/run", json={"pipeline_name": PIPELINE_NAME, "runtime_params": params})
    assert resp.status_code == 202, f"/run failed {resp.status_code}: {resp.text}"
    execution_id = resp.json()["execution_id"]
    deadline = time.time() + MAX_WAIT
    stats: Dict[str, Any] = {}
    while time.time() < deadline:
        stats = manager.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            break
        time.sleep(1)
    stats["execution_id"] = execution_id
    return stats


def _items(run_key: str) -> Dict[str, Any]:
    return httpx.get(f"{MOCK_API_URL}/items/{run_key}", timeout=10.0).json()


def _ids(received: List[Dict[str, Any]]) -> List[int]:
    return [int(r["item_id"]) for r in received]


class TestPerRecordDelivery:
    def test_one_put_per_record_with_the_id_in_the_url(self, manager, run_key):
        stats = _run_and_wait(manager, {"run": run_key, "count": 30, "batch_size": 10})

        assert stats["state"] == "completed", stats
        assert (stats["total_jobs"], stats["jobs_completed"]) == (3, 3), stats
        received = _items(run_key)["received"]
        assert sorted(_ids(received)) == list(range(1, 31))  # each record once
        for request in received:
            assert request["method"] == "PUT"
            # The URL placeholder came from the same record that is the body.
            assert request["body"]["id"] == int(request["item_id"])
            assert set(request["body"]) == {"id", "name"}

    def test_default_concurrency_sends_one_at_a_time_in_order(self, manager, run_key):
        stats = _run_and_wait(
            manager, {"run": run_key, "count": 20, "batch_size": 10, "delay_ms": 30}
        )

        assert stats["state"] == "completed", stats
        items = _items(run_key)
        assert items["peak_by_job"] == {"1": 1, "11": 1}
        # Each job's requests arrived in record order.
        for job in ("1", "11"):
            ids = [int(r["item_id"]) for r in items["received"] if r["job"] == job]
            assert ids == sorted(ids) and len(ids) == 10

    def test_key_field_sends_only_the_payload(self, manager, run_key):
        stats = _run_and_wait(
            manager, {"run": run_key, "count": 10, "batch_size": 10, "use_key_field": True}
        )

        assert stats["state"] == "completed", stats
        received = _items(run_key)["received"]
        assert sorted(_ids(received)) == list(range(1, 11))
        for request in received:
            # {"id", "payload", "internal"} on the record; only the payload went out.
            assert set(request["body"]) == {"id", "name"}
            assert request["body"]["id"] == int(request["item_id"])


class TestPerRecordConcurrency:
    def test_requests_overlap_within_a_job_up_to_the_cap(self, manager, run_key):
        stats = _run_and_wait(
            manager,
            {
                "run": run_key,
                "count": 80,
                "batch_size": 40,
                "concurrency": 8,
                "delay_ms": 100,  # every request holds its slot long enough to overlap
            },
        )

        assert stats["state"] == "completed", stats
        items = _items(run_key)
        assert sorted(_ids(items["received"])) == list(range(1, 81))
        assert set(items["peak_by_job"]) == {"1", "41"}, items["peak_by_job"]
        assert all(2 <= peak <= 8 for peak in items["peak_by_job"].values()), items[
            "peak_by_job"
        ]

    def test_concurrency_is_faster_than_one_at_a_time(self, manager, run_key):
        params = {"count": 40, "batch_size": 40, "delay_ms": 100}

        started = time.time()
        sequential = _run_and_wait(manager, {**params, "run": f"{run_key}-seq"})
        sequential_s = time.time() - started

        started = time.time()
        concurrent = _run_and_wait(
            manager, {**params, "run": f"{run_key}-conc", "concurrency": 10}
        )
        concurrent_s = time.time() - started

        assert sequential["state"] == concurrent["state"] == "completed"
        # 40 x 100 ms one at a time is >= 4 s of requests; ten at a time about 0.4 s.
        assert sequential_s - concurrent_s > 2.0, (sequential_s, concurrent_s)


class TestPerRecordFailure:
    def test_a_failed_record_fails_its_job_and_the_error_names_its_position(
        self, manager, run_key
    ):
        stats = _run_and_wait(
            manager,
            {"run": run_key, "count": 20, "batch_size": 10, "fail_id": "7", "concurrency": 4},
        )

        assert stats["state"] == "failed", stats
        assert (stats["jobs_completed"], stats["jobs_failed"]) == (1, 1), stats
        errors = manager.get(f"/executions/{stats['execution_id']}/errors").json()
        assert len(errors) == 1
        message = errors[0]["error_message"]
        # id 7 is the 7th record of the first job (ids 1..10).
        assert "record 7 of 10: HTTP 503" in message, message
        # The other job delivered all of its records.
        delivered = set(_ids(_items(run_key)["received"]))
        assert set(range(11, 21)) <= delivered
        assert 7 not in delivered
