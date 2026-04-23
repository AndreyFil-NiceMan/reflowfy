"""
E2E Tests for Rate Limiting.

Verifies the token-bucket rate limiter (reflowfy/reflow_manager/rate_limiter.py)
through real pipeline runs against the ReflowManager API.

Prerequisites:
    - ReflowManager running on localhost:8002
    - Mock HTTP server running on localhost:8091

Run with:
    pytest tests/e2e/test_rate_limiting.py -v
    pytest tests/e2e/test_rate_limiting.py -m slow -v   # includes timing tests
"""

import os
import time
import threading
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


def _run_pipeline(client, pipeline_name, *, rate_limit=None, runtime_params=None):
    """
    POST /run for the given pipeline and poll /stats until done.

    Returns (execution_id, stats, elapsed_seconds).
    elapsed_seconds is measured from POST to completion.
    """
    payload = {"pipeline_name": pipeline_name}
    if rate_limit is not None:
        payload["rate_limit"] = rate_limit
    if runtime_params:
        payload["runtime_params"] = runtime_params

    start = time.monotonic()
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
            elapsed = time.monotonic() - start
            return execution_id, stats, elapsed
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Pipeline '{pipeline_name}' (exec {execution_id}) did not complete in {MAX_WAIT}s"
    )


class TestRateLimiting:
    """Verify token-bucket rate limiting through live pipeline executions."""

    @pytest.mark.slow
    def test_slow_rate_throttles_jobs(self, client, check_mock_http):
        """
        Pipeline with rate=1 job/s and 5 single-record batches should take
        at least 3 seconds (4 waits of ~1 s each, with 50 % tolerance).
        """
        _, stats, elapsed = _run_pipeline(client, "e2e_slow_rate")

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert elapsed >= 3.0, (
            f"Expected ≥ 3 s for rate=1, 5 batches but finished in {elapsed:.2f} s"
        )

    def test_fast_rate_no_throttle(self, client, check_mock_http):
        """
        Pipeline with rate=500 job/s and 50 records across 5 batches should
        complete in under 10 s (no meaningful throttling delay).
        """
        _, stats, elapsed = _run_pipeline(client, "e2e_fast_rate")

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert elapsed < 10.0, (
            f"rate=500 pipeline took {elapsed:.2f} s — unexpectedly slow"
        )

    @pytest.mark.slow
    def test_runtime_rate_override(self, client, check_mock_http):
        """
        E2ERateLimitOverridePipeline has class default=500 jobs/s but the test
        passes rate_limit=1 in the request body, overriding it.
        With 5 batches at 1/s the run should still take ≥ 3 s.
        """
        _, stats, elapsed = _run_pipeline(
            client, "e2e_rate_override", rate_limit=1.0
        )

        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        assert elapsed >= 3.0, (
            f"Override to rate=1 should throttle but finished in {elapsed:.2f} s"
        )

    @pytest.mark.slow
    def test_concurrent_pipelines_isolated(self, client, check_mock_http):
        """
        Run slow (rate=1) and fast (rate=500) pipelines concurrently in threads.
        The fast pipeline must complete strictly before the slow one.
        """
        results = {}

        def run_slow():
            _, _, elapsed = _run_pipeline(client, "e2e_slow_rate")
            results["slow"] = elapsed

        def run_fast():
            _, _, elapsed = _run_pipeline(client, "e2e_fast_rate")
            results["fast"] = elapsed

        t_slow = threading.Thread(target=run_slow)
        t_fast = threading.Thread(target=run_fast)
        t_slow.start()
        t_fast.start()
        t_slow.join(timeout=MAX_WAIT)
        t_fast.join(timeout=MAX_WAIT)

        assert "fast" in results and "slow" in results, "One of the threads did not complete"
        assert results["fast"] < results["slow"], (
            f"Fast pipeline ({results['fast']:.2f} s) should finish before "
            f"slow pipeline ({results['slow']:.2f} s)"
        )

    def test_stats_show_all_records_processed(self, client, check_mock_http):
        """
        Fast pipeline: 50 records across 5 batches — stats should show
        jobs_completed=5 (one job per batch, 5 batches total).
        """
        _, stats, _ = _run_pipeline(client, "e2e_fast_rate")

        assert stats["state"] == "completed"
        completed = stats.get("jobs_completed", 0)
        assert completed == 5, (
            f"Expected 5 completed jobs (5 batches), got {completed}. Stats: {stats}"
        )

    def test_records_reach_destination(self, client, check_mock_http):
        """
        Fast pipeline: all records should arrive at the mock HTTP destination.
        """
        _, stats, _ = _run_pipeline(client, "e2e_fast_rate")
        assert stats["state"] == "completed"

        resp = httpx.get(
            f"{MOCK_HTTP_URL}/records",
            params={"limit": 100},
            timeout=10.0,
        ).json()

        assert resp["total"] > 0, "No records received by mock HTTP server"
        records = resp["records"]
        assert any("_rl_dispatched_at" in r for r in records), "Passthrough transformation was not applied to any record"
        assert any("_rl_batch_id" in r for r in records), "batch_id not stamped in any record"
