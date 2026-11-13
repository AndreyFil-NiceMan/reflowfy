"""
E2E: a source whose fetch yields, an AsyncSource, and async pipeline hooks.

``e2e_streaming_source`` used to send ``'<', 'g', 'e', 'n', …`` — one character of the
generator's repr per record — and report success; an async
``should_apply_transformation`` returning False used to be ignored.

Prerequisites:
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/test_streaming_source.py -v
"""

import os

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
PIPELINE_NAME = "e2e_streaming_source"


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
        yield client


@pytest.mark.parametrize(
    "kind, even_only, expected",
    [
        ("generator", False, 25),  # every number, not the characters of a repr
        ("generator", True, 13),  # the async should_apply said yes: evens of 0..24
        ("async", False, 25),
        ("async", True, 13),
    ],
)
def test_records_arrive_and_the_async_hook_decides(
    client, wait_for_pipeline_completion, kind, even_only, expected
):
    response = client.post(
        "/run",
        json={
            "pipeline_name": PIPELINE_NAME,
            "runtime_params": {"kind": kind, "even_only": even_only},
        },
    )
    if response.status_code == 404:
        pytest.skip(f"{PIPELINE_NAME} pipeline not registered")
    assert response.status_code == 202, response.text
    execution_id = response.json()["execution_id"]

    stats = wait_for_pipeline_completion(execution_id)
    assert stats["state"] == "completed", stats
    assert stats["total_jobs"] == 3
    assert stats["jobs_completed"] == 3 and stats["jobs_failed"] == 0

    jobs = client.get(f"/executions/{execution_id}/jobs").json()
    assert sum(j["processed_records"] or 0 for j in jobs) == expected
