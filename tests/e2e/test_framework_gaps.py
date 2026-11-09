"""E2E: the framework-gaps features (PRs #105–#110) on a running stack.

Pipelines: ``tests/e2e/test_pipelines/framework_gaps_pipelines.py``. They send to the
mock API's ``/gaps/{run}/{name}``, which logs every request with its time; each test
uses its own ``run`` key. The same tests pass in local mode and over Kafka with real
workers (``scripts/e2e-distributed.override.yml``).
"""

import os
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
API_URL = os.getenv("E2E_API_URL", "http://localhost:8003")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
# Where the manager/worker containers reach the mock (callback_url is called from there).
MOCK_INTERNAL_URL = os.getenv("E2E_MOCK_HTTP_INTERNAL_URL", "http://reflofy-e2e-mock-http:8091")
MAX_WAIT = 90
TERMINAL = ("completed", "failed", "cancelled")


@pytest.fixture(scope="module")
def manager():
    client = httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=30)
    try:
        client.get("/health").raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f"ReflowManager not reachable at {REFLOW_MANAGER_URL}: {exc}")
    yield client
    client.close()


def _run_key(name: str) -> str:
    return f"{name}-{uuid.uuid4().hex[:8]}"


def _start(manager: httpx.Client, pipeline: str, params: Dict[str, Any], **extra: Any) -> str:
    resp = manager.post(
        "/run", json={"pipeline_name": pipeline, "runtime_params": params, **extra}
    )
    assert resp.status_code == 202, resp.text
    return resp.json()["execution_id"]


def _wait(manager: httpx.Client, execution_id: str, timeout: float = MAX_WAIT) -> Dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        stats = manager.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in TERMINAL:
            return stats
        time.sleep(1)
    raise TimeoutError(f"{execution_id} did not finish in {timeout}s")


def _jobs(manager: httpx.Client, execution_id: str) -> List[Dict[str, Any]]:
    return manager.get(f"/executions/{execution_id}/jobs").json()


def _requests(run: str, name: str) -> List[Dict[str, Any]]:
    return httpx.get(f"{MOCK_URL}/gaps/{run}/{name}", timeout=10).json()["requests"]


def _wait_requests(run: str, name: str, count: int, timeout: float = 30) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        got = _requests(run, name)
        if len(got) >= count:
            return got
        time.sleep(0.5)
    return _requests(run, name)


def _ids(requests: List[Dict[str, Any]], status: Optional[int] = 200) -> List[int]:
    return sorted(
        r["body"]["id"] for r in requests if status is None or r["status"] == status
    )


# ---------------------------------------------------------------------------
# Sources (#4 bind params, | ident, #6 trailing ';', #5 secret() + redaction)
# ---------------------------------------------------------------------------


class TestSqlSource:
    def test_values_are_bound_and_rows_arrive(self, manager):
        run = _run_key("sql")
        stats = _wait(manager, _start(manager, "e2e_gaps_sql", {"run": run, "status": "active"}))
        assert stats["state"] == "completed", stats
        sent = _requests(run, "sql")
        assert sent and all(r["body"]["data"]["status"] == "active" for r in sent)

    def test_an_injection_attempt_is_a_value_not_sql(self, manager):
        run = _run_key("sqlinj")
        execution_id = _start(
            manager, "e2e_gaps_sql", {"run": run, "status": "x' OR '1'='1"}
        )
        stats = _wait(manager, execution_id)
        # Bound as a value it matches no row: nothing planned, nothing sent.
        assert stats["state"] == "completed", stats
        assert _requests(run, "sql") == []

    def test_a_trailing_semicolon_is_accepted(self, manager):
        run = _run_key("sqlsemi")
        params = {"run": run, "status": "active", "semicolon": True}
        stats = _wait(manager, _start(manager, "e2e_gaps_sql", params))
        assert stats["state"] == "completed", stats
        assert _requests(run, "sql")

    def test_a_non_identifier_table_fails_the_run(self, manager):
        run = _run_key("sqlident")
        params = {"run": run, "status": "active", "table": "test_events; DROP TABLE x"}
        stats = _wait(manager, _start(manager, "e2e_gaps_sql", params))
        assert stats["state"] == "failed", stats
        assert "ident" in (stats.get("error_message") or ""), stats
        assert _requests(run, "sql") == []

    def test_job_payloads_hide_the_connection_password(self, manager):
        run = _run_key("sqlredact")
        execution_id = _start(manager, "e2e_gaps_sql", {"run": run, "status": "active"})
        _wait(manager, execution_id)
        jobs = _jobs(manager, execution_id)
        assert jobs
        url = jobs[0]["job_payload"]["source"]["config"]["connection_url"]
        assert "reflowfy:reflowfy@" not in url and "@" in url, url

    def test_secret_ref_is_resolved_where_the_job_runs(self, manager):
        run = _run_key("sqlsecret")
        params = {"run": run, "status": "active", "use_secret": True}
        execution_id = _start(manager, "e2e_gaps_sql", params)
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats
        assert _requests(run, "sql")
        config = _jobs(manager, execution_id)[0]["job_payload"]["source"]["config"]
        assert config["connection_url"] == {"$secret": "SQL_CONNECTION_URL"}


# ---------------------------------------------------------------------------
# Transformations (#10 options, #3 should_apply_transformation, #11, #14)
# ---------------------------------------------------------------------------


class TestTransformations:
    def test_options_declined_step_and_skipped_records(self, manager):
        run = _run_key("tf")
        execution_id = _start(manager, "e2e_gaps_transform", {"run": run})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats

        sent = _requests(run, "out")
        assert _ids(sent) == [2, 4, 6]  # odd ids skipped by on_error="skip"
        assert all(r["body"]["label"] == "opt" for r in sent)  # option reached the step
        assert not any("declined_step_ran" in r["body"] for r in sent)

        job_stats = _jobs(manager, execution_id)[0]["stats"]
        assert job_stats["records_skipped"] == 3
        assert job_stats["record_skip_reasons"] == {"ValueError": 3}

    def test_a_per_record_step_returning_a_list_fails_the_job(self, manager):
        run = _run_key("tfcontract")
        execution_id = _start(manager, "e2e_gaps_transform", {"run": run, "scenario": "contract"})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "failed", stats
        job = _jobs(manager, execution_id)[0]
        assert "one record or None" in (job["error_message"] or ""), job["error_message"]
        assert _requests(run, "out") == []


# ---------------------------------------------------------------------------
# Destinations (#17 fan_out / list / multicast, #8 4xx and Retry-After)
# ---------------------------------------------------------------------------


class TestDestinations:
    def test_fan_out_with_a_failing_optional_destination(self, manager):
        run = _run_key("fanout")
        execution_id = _start(manager, "e2e_gaps_dest", {"run": run, "scenario": "fanout"})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats
        assert _ids(_requests(run, "main")) == [1, 2, 3, 4]
        assert _requests(run, "optional")  # it was tried, and failed
        job_stats = _jobs(manager, execution_id)[0]["stats"]
        assert job_stats.get("destinations_failed_optional"), job_stats

    def test_fan_out_with_a_failing_required_destination_fails_the_job(self, manager):
        run = _run_key("fanoutreq")
        execution_id = _start(
            manager, "e2e_gaps_dest", {"run": run, "scenario": "fanout_required"}
        )
        stats = _wait(manager, execution_id)
        assert stats["state"] == "failed", stats

    def test_define_destination_returning_a_list(self, manager):
        run = _run_key("list")
        stats = _wait(manager, _start(manager, "e2e_gaps_dest", {"run": run, "scenario": "list"}))
        assert stats["state"] == "completed", stats
        assert _ids(_requests(run, "main")) == [1, 2, 3, 4]
        assert _ids(_requests(run, "copy")) == [1, 2, 3, 4]

    def test_record_destination_multicast(self, manager):
        run = _run_key("multi")
        execution_id = _start(manager, "e2e_gaps_dest", {"run": run, "scenario": "multicast"})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats
        assert _ids(_requests(run, "main")) == [1, 2, 3, 4]
        assert _ids(_requests(run, "even")) == [2, 4]

    def test_a_404_is_not_retried(self, manager):
        run = _run_key("perm")
        execution_id = _start(manager, "e2e_gaps_dest", {"run": run, "scenario": "permanent"})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "failed", stats
        assert len(_requests(run, "permanent")) == 1
        job = _jobs(manager, execution_id)[0]
        assert job["stats"].get("error_type") == "PermanentDestinationError", job["stats"]
        assert "404" in (job["error_message"] or "")

    def test_retry_after_is_honoured(self, manager):
        run = _run_key("ra")
        execution_id = _start(manager, "e2e_gaps_dest", {"run": run, "scenario": "retry_after"})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats
        sent = _requests(run, "retry_after")
        assert [r["status"] for r in sent] == [503, 200]
        # The destination's own backoff is 0.1 s; only Retry-After: 2 explains the gap.
        assert sent[1]["at"] - sent[0]["at"] >= 1.8, sent


# ---------------------------------------------------------------------------
# Job retries (#23)
# ---------------------------------------------------------------------------


class TestJobRetry:
    def test_a_transient_failure_is_retried_and_the_run_completes(self, manager):
        run = _run_key("jr")
        execution_id = _start(manager, "e2e_gaps_retry", {"run": run, "fail_first": 1})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats
        sent = _requests(run, "retry")
        assert [r["status"] for r in sent] == [503, 200]
        assert sent[1]["at"] - sent[0]["at"] >= 0.9  # backoff_seconds=1
        job_stats = _jobs(manager, execution_id)[0]["stats"]
        assert job_stats.get("retry_count") == 1, job_stats

    def test_retries_stop_after_max_retries(self, manager):
        run = _run_key("jrmax")
        execution_id = _start(manager, "e2e_gaps_retry", {"run": run, "fail_first": 10})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "failed", stats
        assert len(_requests(run, "retry")) == 3  # first run + max_retries=2

    def test_a_permanent_error_is_never_retried(self, manager):
        run = _run_key("jrperm")
        execution_id = _start(manager, "e2e_gaps_retry", {"run": run, "status": 404})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "failed", stats
        assert len(_requests(run, "retry")) == 1


# ---------------------------------------------------------------------------
# Lifecycle hooks, chaining, callback_url (#25), define_rate_limit (#3)
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_complete_hook_receives_the_summary(self, manager):
        run = _run_key("hook")
        execution_id = _start(manager, "e2e_gaps_hooks", {"run": run})
        assert _wait(manager, execution_id)["state"] == "completed"
        hooks = _wait_requests(run, "hook_complete", 1)
        assert len(hooks) == 1, hooks
        summary = hooks[0]["body"]
        assert summary["execution_id"] == execution_id
        assert summary["state"] == "completed"
        assert summary["records_out"] == 2
        assert _requests(run, "hook_failed") == []

    def test_failed_hook_fires_once(self, manager):
        run = _run_key("hookfail")
        execution_id = _start(manager, "e2e_gaps_hooks", {"run": run, "fail": True})
        assert _wait(manager, execution_id)["state"] == "failed"
        hooks = _wait_requests(run, "hook_failed", 1)
        time.sleep(2)  # a second firing would have arrived by now
        assert len(_requests(run, "hook_failed")) == 1, hooks
        assert hooks[0]["body"]["jobs_failed"] == 1

    def test_run_pipeline_from_a_hook_starts_a_child_run(self, manager):
        run = _run_key("chain")
        execution_id = _start(manager, "e2e_gaps_hooks", {"run": run, "chain": True})
        assert _wait(manager, execution_id)["state"] == "completed"
        child_hooks = _wait_requests(f"{run}-child", "hook_complete", 1, timeout=60)
        assert len(child_hooks) == 1, child_hooks
        child = child_hooks[0]["body"]
        assert child["parent_execution_id"] == execution_id
        stats = manager.get(f"/executions/{child['execution_id']}/stats").json()
        assert stats["parent_execution_id"] == execution_id

    def test_callback_url_on_the_manager(self, manager):
        run = _run_key("cb")
        callback = f"{MOCK_INTERNAL_URL}/gaps/{run}/callback"
        execution_id = _start(manager, "e2e_gaps_hooks", {"run": run}, callback_url=callback)
        assert _wait(manager, execution_id)["state"] == "completed"
        calls = _wait_requests(run, "callback", 1)
        assert len(calls) == 1 and calls[0]["body"]["execution_id"] == execution_id

    def test_callback_url_through_the_api(self, manager):
        try:
            httpx.get(f"{API_URL}/health", timeout=5).raise_for_status()
        except httpx.HTTPError as exc:
            pytest.skip(f"API not reachable at {API_URL}: {exc}")
        run = _run_key("cbapi")
        callback = f"{MOCK_INTERNAL_URL}/gaps/{run}/callback"
        resp = httpx.post(
            f"{API_URL}/pipelines/e2e_gaps_hooks/run",
            params={"run": run, "callback_url": callback},
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        execution_id = resp.json()["execution_id"]
        assert _wait(manager, execution_id)["state"] == "completed"
        calls = _wait_requests(run, "callback", 1)
        assert len(calls) == 1 and calls[0]["body"]["execution_id"] == execution_id

    def test_define_rate_limit_is_used_and_persisted(self, manager):
        run = _run_key("rate")
        execution_id = _start(manager, "e2e_gaps_hooks", {"run": run, "rpm": 600})
        stats = _wait(manager, execution_id)
        assert stats["state"] == "completed", stats
        assert stats["rate_limit"] == 600

    def test_the_run_override_wins_over_define_rate_limit(self, manager):
        run = _run_key("rateov")
        execution_id = _start(manager, "e2e_gaps_hooks", {"run": run, "rpm": 600}, rate_limit=1200)
        stats = _wait(manager, execution_id)
        assert stats["rate_limit"] == 1200

    def test_no_rate_limit_means_unlimited(self, manager):
        run = _run_key("rateno")
        stats = _wait(manager, _start(manager, "e2e_gaps_hooks", {"run": run}))
        assert stats["state"] == "completed", stats
        assert stats["rate_limit"] is None
