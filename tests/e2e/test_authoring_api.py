"""E2E: the changes after 1.1.6 on a running stack (#119, #120, #121, #123).

Pipeline: ``tests/e2e/test_pipelines/authoring_api_pipeline.py`` (and ``e2e_gaps_sql``).
Deliveries are read from the mock API's ``/gaps/{run}/{name}`` log; job outcomes from
the manager. The same tests run over Kafka with real workers when the stack uses
``scripts/e2e-distributed.override.yml``.
"""

import os
from typing import Any, Dict, Iterator, List

import httpx
import pytest

from tests.e2e.test_framework_gaps import _ids, _jobs, _requests, _run_key, _start, _wait
from tests.e2e.test_pipelines.authoring_api_pipeline import (
    EXPECTED_STEPS_SKIPS,
    expected_steps_ids,
)

PIPELINE = "e2e_authoring_api"
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")


@pytest.fixture(scope="module")
def manager() -> Iterator[httpx.Client]:
    client = httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=30)
    try:
        client.get("/health").raise_for_status()
    except httpx.HTTPError as exc:
        pytest.skip(f"ReflowManager not reachable at {REFLOW_MANAGER_URL}: {exc}")
    yield client
    client.close()


def _skip_totals(jobs: List[Dict[str, Any]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for job in jobs:
        for reason, count in ((job["stats"] or {}).get("record_skip_reasons") or {}).items():
            totals[reason] = totals.get(reason, 0) + count
    return totals


class TestDecoratorApi:
    def test_bare_steps_default_names_and_plain_def_per_record(self, manager):
        """#121: bare steps, default names, and every per-record return of a ``def``."""
        run = _run_key("auth-steps")
        execution_id = _start(manager, PIPELINE, {"run": run})
        stats = _wait(manager, execution_id)

        assert stats["state"] == "completed", stats
        assert stats["jobs_failed"] == 0, stats
        assert _ids(_requests(run, "out")) == expected_steps_ids()

        jobs = _jobs(manager, execution_id)
        assert len(jobs) == 4
        assert _skip_totals(jobs) == EXPECTED_STEPS_SKIPS
        for job in jobs:
            times = job["stats"]["transformation_times"]
            # The bare decorator names the step after the function; name= overrides it.
            assert {"e2e_auth_flatten", "e2e_auth_each", "e2e_auth_fragile_v2"} <= set(times)

    def test_a_bare_record_destination_routes_every_record(self, manager):
        """#121: ``@destination(per_record=True)``, default name, returned bare."""
        run = _run_key("auth-route")
        execution_id = _start(manager, PIPELINE, {"run": run, "scenario": "route"})
        stats = _wait(manager, execution_id)

        assert stats["state"] == "completed", stats
        assert _ids(_requests(run, "even")) == list(range(2, 21, 2))
        assert _ids(_requests(run, "odd")) == list(range(1, 21, 2))
        for job in _jobs(manager, execution_id):
            assert job["stats"]["destinations"] == 2, job["stats"]
            assert "e2e_auth_by_parity" in job["stats"]["transformation_times"]


class TestSkipRecordThenSkipJob:
    def test_the_dropped_job_keeps_the_records_it_skipped_first(self, manager):
        """#120: a job dropped by SkipJob still reports the SkipRecords before it."""
        run = _run_key("auth-drop")
        execution_id = _start(manager, PIPELINE, {"run": run, "scenario": "skip_then_drop"})
        stats = _wait(manager, execution_id)

        assert stats["state"] == "completed", stats
        assert stats["jobs_failed"] == 0, stats
        assert _ids(_requests(run, "out")) == list(range(6, 21, 2))  # job 1 dropped

        jobs = _jobs(manager, execution_id)
        dropped = [j for j in jobs if (j["stats"] or {}).get("skip_reason")]
        assert len(dropped) == 1, [j["stats"] for j in jobs]
        dropped_stats = dropped[0]["stats"]
        assert dropped_stats["skip_reason"] == "first job"
        assert dropped_stats["record_skip_reasons"] == {"odd": 3}
        assert dropped_stats["records_skipped"] == 3
        others = [j for j in jobs if j is not dropped[0]]
        # The odd ids outside the first job (7, 9, 11, 13, 15, 17, 19) were skipped too.
        assert _skip_totals(others) == {"odd": 7}


class TestRemovals:
    def test_a_quoted_sql_value_fails_the_run_saying_to_unquote_it(self, manager):
        """#123: ``'{{ x }}'`` is no longer bound in place of the literal."""
        run = _run_key("auth-sqlq")
        params = {"run": run, "status": "active", "quoted": True}
        stats = _wait(manager, _start(manager, "e2e_gaps_sql", params))

        assert stats["state"] == "failed", stats
        assert "not '{{ x }}'" in (stats.get("error_message") or ""), stats
        assert _requests(run, "sql") == []

    def test_unquoted_sql_values_are_bound(self, manager):
        run = _run_key("auth-sqlu")
        stats = _wait(manager, _start(manager, "e2e_gaps_sql", {"run": run, "status": "active"}))
        assert stats["state"] == "completed", stats
        sent = _requests(run, "sql")
        assert sent and all(r["body"]["data"]["status"] == "active" for r in sent)

    def test_a_callback_url_is_ignored(self, manager):
        """#119: ``callback_url`` is gone; sent anyway, the run proceeds and nothing is POSTed."""
        run = _run_key("auth-cb")
        callback = f"http://reflofy-e2e-mock-http:8091/gaps/{run}/callback"
        execution_id = _start(manager, PIPELINE, {"run": run}, callback_url=callback)
        stats = _wait(manager, execution_id)

        assert stats["state"] == "completed", stats
        assert _ids(_requests(run, "out")) == expected_steps_ids()
        assert _requests(run, "callback") == []
        assert "callback_url" not in stats
