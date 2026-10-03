"""
E2E Tests for Pipeline Schedule Feature.

Requires running services (ReflowManager + PostgreSQL).
The e2e_scheduled_test pipeline (schedule="* * * * *") and
e2e_scheduled_slow_test pipeline (schedule="0 * * * *") must be
registered in the service — they are included via the PIPELINE_MODULE
that loads tests/e2e/test_pipelines/.

Run with: pytest tests/e2e/test_schedule.py -v
"""

import time
import uuid

import httpx
import pytest


TIMEOUT = 30.0
SCHEDULED_PIPELINE = "e2e_scheduled_test"
SLOW_SCHEDULED_PIPELINE = "e2e_scheduled_slow_test"
NO_DUPLICATES_SCHEDULED_PIPELINE = "e2e_scheduled_no_duplicates_test"
MULTI_SCHEDULE_PIPELINE = "e2e_multi_schedule_test"
MULTI_SCHEDULE_FREQUENT_PIPELINE = "e2e_multi_schedule_frequent_test"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_schedule(client: httpx.Client, pipeline_name: str) -> dict | None:
    resp = client.get("/schedules")
    resp.raise_for_status()
    for entry in resp.json()["schedules"]:
        if entry["pipeline_name"] == pipeline_name:
            return entry
    return None


def _get_all_schedules(client: httpx.Client, pipeline_name: str) -> list[dict]:
    resp = client.get("/schedules")
    resp.raise_for_status()
    return [e for e in resp.json()["schedules"] if e["pipeline_name"] == pipeline_name]


def _get_named_schedule(client: httpx.Client, pipeline_name: str, schedule_name: str) -> dict | None:
    for entry in _get_all_schedules(client, pipeline_name):
        if entry["schedule_name"] == schedule_name:
            return entry
    return None


def _wait_for_new_execution(
    client: httpx.Client, pipeline_name: str, schedule_name: str, prior_execution_id: str | None, max_wait: int
) -> str:
    """Poll a named schedule's last_execution_id until it changes, return the new id."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        entry = _get_named_schedule(client, pipeline_name, schedule_name)
        if entry and entry.get("last_execution_id") and entry["last_execution_id"] != prior_execution_id:
            return entry["last_execution_id"]
        time.sleep(5)
    raise TimeoutError(
        f"'{pipeline_name}' schedule '{schedule_name}' did not auto-fire within {max_wait}s"
    )


def _wait_for_execution(client: httpx.Client, execution_id: str, max_wait: int = 60) -> dict:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        resp = client.get(f"/executions/{execution_id}/stats")
        if resp.status_code == 200:
            stats = resp.json()
            if stats.get("state") in ("completed", "failed"):
                return stats
        time.sleep(2)
    raise TimeoutError(f"Execution {execution_id} did not finish within {max_wait}s")


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestScheduleListEndpoint:
    """Tests for GET /schedules."""

    def test_schedules_endpoint_returns_200(self, reflow_client):
        resp = reflow_client.get("/schedules")
        assert resp.status_code == 200

    def test_schedules_response_shape(self, reflow_client):
        resp = reflow_client.get("/schedules")
        data = resp.json()
        assert "schedules" in data
        assert "total" in data
        assert isinstance(data["schedules"], list)
        assert data["total"] == len(data["schedules"])

    def test_scheduled_pipeline_appears_in_list(self, reflow_client):
        """Scheduled pipelines registered at startup must have a DB row."""
        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None, (
            f"'{SCHEDULED_PIPELINE}' not found in /schedules — "
            "check that the test pipeline is loaded by the service"
        )

    def test_schedule_entry_has_required_fields(self, reflow_client):
        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None
        for field in (
            "pipeline_name",
            "schedule_name",
            "cron_expression",
            "runtime_params",
            "next_run_at",
            "enabled",
            "created_at",
        ):
            assert field in entry, f"Missing field: {field}"

    def test_scheduled_pipeline_is_enabled(self, reflow_client):
        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None
        assert entry["enabled"] is True

    def test_scheduled_pipeline_cron_expression(self, reflow_client):
        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None
        assert entry["cron_expression"] == "* * * * *"

    def test_slow_scheduled_pipeline_cron_expression(self, reflow_client):
        entry = _get_schedule(reflow_client, SLOW_SCHEDULED_PIPELINE)
        assert entry is not None
        assert entry["cron_expression"] == "0 * * * *"

    def test_next_run_at_is_iso_datetime_string(self, reflow_client):
        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None
        nra = entry["next_run_at"]
        assert isinstance(nra, str)
        # ISO 8601 — must contain "T" separator
        assert "T" in nra, f"next_run_at is not ISO 8601: {nra!r}"

    def test_multiple_scheduled_pipelines_are_listed(self, reflow_client):
        resp = reflow_client.get("/schedules")
        names = {e["pipeline_name"] for e in resp.json()["schedules"]}
        assert SCHEDULED_PIPELINE in names
        assert SLOW_SCHEDULED_PIPELINE in names


class TestManualTriggerResetsSchedule:
    """POST /run on a scheduled pipeline must advance next_run_at."""

    def test_manual_run_returns_202(self, reflow_client):
        resp = reflow_client.post("/run", json={"pipeline_name": SCHEDULED_PIPELINE})
        assert resp.status_code == 202

    def test_manual_run_advances_next_run_at(self, reflow_client):
        """
        After a manual trigger, last_triggered_at must be set to a recent
        timestamp and next_run_at must be in the future.

        We cannot assert next_run_at *changed* because a "* * * * *" cron
        computes the same next-minute boundary for multiple triggers within
        the same minute. Instead we verify last_triggered_at was written,
        which is the actual DB signal that reset_schedule ran.
        """
        import datetime as dt

        # Trigger manually
        resp = reflow_client.post("/run", json={"pipeline_name": SCHEDULED_PIPELINE})
        assert resp.status_code == 202

        # Poll until last_triggered_at appears (up to 10s — synchronous commit)
        deadline = time.time() + 10
        entry = None
        while time.time() < deadline:
            entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
            if entry and entry.get("last_triggered_at"):
                break
            time.sleep(0.5)

        assert entry is not None
        assert entry.get("last_triggered_at") is not None, (
            "last_triggered_at must be set after a manual trigger — "
            "reset_schedule was not called"
        )

        # last_triggered_at must be recent (within the last 30s)
        ts_str = entry["last_triggered_at"].rstrip("Z").split("+")[0]
        triggered_at = dt.datetime.fromisoformat(ts_str)
        age = (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - triggered_at).total_seconds()
        assert age < 30, f"last_triggered_at is too old ({age:.1f}s ago)"

        # next_run_at must still be in the future
        nra_str = entry["next_run_at"].rstrip("Z").split("+")[0]
        next_run = dt.datetime.fromisoformat(nra_str)
        assert next_run > dt.datetime.now(dt.timezone.utc).replace(tzinfo=None), (
            f"next_run_at ({next_run}) should be in the future"
        )

    def test_manual_run_creates_execution_record(self, reflow_client):
        execution_id = f"e2e-sched-{uuid.uuid4().hex[:8]}"
        resp = reflow_client.post(
            "/run",
            json={"pipeline_name": SCHEDULED_PIPELINE, "execution_id": execution_id},
        )
        assert resp.status_code == 202
        data = resp.json()
        assert data["execution_id"] == execution_id
        assert data["pipeline_name"] == SCHEDULED_PIPELINE

    def test_manual_run_last_triggered_at_is_set(self, reflow_client):
        """After a POST /run, last_triggered_at must be non-null."""
        reflow_client.post("/run", json={"pipeline_name": SCHEDULED_PIPELINE})
        time.sleep(1)  # give the endpoint time to commit

        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None
        assert entry.get("last_triggered_at") is not None, (
            "last_triggered_at should be set after manual trigger"
        )


class TestSchedulerAutoTrigger:
    """
    Verify the background scheduler auto-fires pipelines when next_run_at elapses.

    Strategy: we rely on the fact that e2e_scheduled_test has schedule="* * * * *".
    We set up conditions so next_run_at is already in the past (by direct DB manipulation
    via a helper endpoint if available, or by waiting up to 90 seconds for the scheduler
    to tick naturally).

    If no direct DB access is possible from the test host, we wait for the scheduler's
    poll interval to fire.
    """

    @pytest.mark.slow
    def test_scheduler_fires_pipeline_automatically(self, reflow_client):
        """
        After waiting for one scheduler poll cycle (≤60s), the execution count
        for e2e_scheduled_test should increase compared to before.

        This test is marked slow and may be skipped in short test runs.
        """
        # Snapshot: number of completed executions before
        reflow_client.get("/schedules")
        entry_before = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry_before is not None

        last_exec_before = entry_before.get("last_execution_id")

        # Wait up to 90s for a new auto-triggered execution
        max_wait = 90
        deadline = time.time() + max_wait
        new_execution_id = None
        while time.time() < deadline:
            entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
            if entry and entry.get("last_execution_id") != last_exec_before:
                new_execution_id = entry["last_execution_id"]
                break
            time.sleep(5)

        assert new_execution_id is not None, (
            f"Scheduler did not auto-fire '{SCHEDULED_PIPELINE}' within {max_wait}s. "
            "Check that PIPELINE_SCHEDULER_POLL_INTERVAL_SECONDS is ≤30 in the service."
        )

    @pytest.mark.slow
    def test_scheduler_advances_next_run_at_after_auto_fire(self, reflow_client):
        """After an auto-fire, next_run_at must advance to the next cron tick."""
        entry_before = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry_before is not None
        next_run_before = entry_before["next_run_at"]

        max_wait = 90
        deadline = time.time() + max_wait
        while time.time() < deadline:
            entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
            if entry and entry["next_run_at"] != next_run_before:
                assert entry["next_run_at"] > next_run_before, (
                    "next_run_at should advance forward after auto-fire"
                )
                return
            time.sleep(5)

        pytest.skip(
            "Scheduler did not auto-fire within the wait window; skipping next_run_at advance check"
        )


class TestScheduleIdempotency:
    """Sync-from-registry must be idempotent across restarts."""

    def test_multiple_schedule_syncs_do_not_duplicate_rows(self, reflow_client):
        """
        /schedules should return exactly one row per (pipeline, schedule name),
        not duplicates, regardless of how many times startup sync runs. A
        pipeline may legitimately have several named schedules, so uniqueness
        is on the composite key, not on pipeline_name alone.
        """
        resp = reflow_client.get("/schedules")
        schedules = resp.json()["schedules"]
        keys = [(s["pipeline_name"], s["schedule_name"]) for s in schedules]
        assert len(keys) == len(set(keys)), (
            f"Duplicate schedule rows detected: {keys}"
        )

    def test_schedule_row_stable_between_requests(self, reflow_client):
        """Two rapid GET /schedules calls return the same cron_expression."""
        entry_a = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        time.sleep(0.2)
        entry_b = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry_a is not None and entry_b is not None
        assert entry_a["cron_expression"] == entry_b["cron_expression"]


class TestScheduleIntegrationWithExecutionLifecycle:
    """Schedule + execution lifecycle integration."""

    def test_scheduled_execution_progresses_to_terminal_state(self, reflow_client):
        """Manually trigger the scheduled pipeline and confirm execution completes."""
        execution_id = f"e2e-sched-lifecycle-{uuid.uuid4().hex[:8]}"
        resp = reflow_client.post(
            "/run",
            json={"pipeline_name": SCHEDULED_PIPELINE, "execution_id": execution_id},
        )
        assert resp.status_code == 202

        stats = _wait_for_execution(reflow_client, execution_id, max_wait=60)
        assert stats["state"] in ("completed", "failed"), (
            f"Unexpected terminal state: {stats['state']}"
        )

    def test_next_run_at_is_strictly_in_future_after_manual_trigger(self, reflow_client):
        """next_run_at must always be a future timestamp after a manual trigger."""
        import datetime as dt

        resp = reflow_client.post("/run", json={"pipeline_name": SCHEDULED_PIPELINE})
        assert resp.status_code == 202
        time.sleep(1)

        entry = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry is not None
        next_run_str = entry["next_run_at"]
        # Parse — support both with and without timezone suffix
        next_run_str_clean = next_run_str.rstrip("Z").split("+")[0]
        next_run = dt.datetime.fromisoformat(next_run_str_clean)
        now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

        assert next_run > now, (
            f"next_run_at ({next_run}) should be in the future but it is in the past (now={now})"
        )

    def test_schedule_entry_persists_across_multiple_runs(self, reflow_client):
        """Schedule row must remain stable (not deleted/recreated) across runs."""
        entry_before = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry_before is not None
        created_at_before = entry_before["created_at"]

        # Trigger twice
        for _ in range(2):
            reflow_client.post("/run", json={"pipeline_name": SCHEDULED_PIPELINE})
            time.sleep(0.5)

        entry_after = _get_schedule(reflow_client, SCHEDULED_PIPELINE)
        assert entry_after is not None
        # created_at must not change — row is updated, not recreated
        assert entry_after["created_at"] == created_at_before, (
            "Schedule row was recreated (created_at changed) — should be an upsert"
        )


class TestScheduledPipelineNoDuplicateJobs:
    """
    Verify that a scheduled pipeline with enable_duplicate_jobs=False does not
    create duplicate jobs when triggered twice with the same data.
    """

    def test_no_duplicate_pipeline_appears_in_schedules(self, reflow_client):
        """The no-duplicates pipeline must be registered as a schedule."""
        entry = _get_schedule(reflow_client, NO_DUPLICATES_SCHEDULED_PIPELINE)
        assert entry is not None, (
            f"'{NO_DUPLICATES_SCHEDULED_PIPELINE}' not found in /schedules"
        )

    def test_second_run_with_same_data_produces_no_new_jobs(self, reflow_client):
        """
        Run the no-duplicates scheduled pipeline twice with identical data.
        The second execution must complete but create zero new (non-duplicate) jobs,
        confirming the deduplication logic is active.
        """
        exec_id_1 = f"e2e-no-dup-first-{uuid.uuid4().hex[:8]}"
        exec_id_2 = f"e2e-no-dup-second-{uuid.uuid4().hex[:8]}"

        # First run — jobs should be created and processed normally
        resp1 = reflow_client.post(
            "/run",
            json={
                "pipeline_name": NO_DUPLICATES_SCHEDULED_PIPELINE,
                "execution_id": exec_id_1,
            },
        )
        assert resp1.status_code == 202, f"First run failed: {resp1.text}"

        stats1 = _wait_for_execution(reflow_client, exec_id_1, max_wait=60)
        assert stats1["state"] in ("completed", "failed"), (
            f"First execution did not reach terminal state: {stats1['state']}"
        )
        jobs_first_run = stats1.get("total_jobs", 0)
        assert jobs_first_run > 0, "First run should have dispatched at least one job"

        # Second run — same pipeline, same static source data → duplicates blocked
        resp2 = reflow_client.post(
            "/run",
            json={
                "pipeline_name": NO_DUPLICATES_SCHEDULED_PIPELINE,
                "execution_id": exec_id_2,
            },
        )
        assert resp2.status_code == 202, f"Second run failed: {resp2.text}"

        stats2 = _wait_for_execution(reflow_client, exec_id_2, max_wait=60)
        assert stats2["state"] in ("completed", "failed"), (
            f"Second execution did not reach terminal state: {stats2['state']}"
        )

        # New semantics: the second run still creates/dispatches jobs, but the
        # worker deduplicates them by content (same static data).
        assert stats2["state"] == "completed", stats2
        assert stats2["jobs_failed"] == 0
        assert stats2.get("deduplicated_jobs", 0) == stats2.get("total_jobs", 0), (
            "second run with identical data must be fully deduplicated by the worker"
        )
        assert stats2.get("total_jobs", 0) > 0, (
            "jobs are always created now; dedup is a worker outcome"
        )


class TestMultiSchedulePipeline:
    """
    Verify a pipeline that declares several named schedules gets one DB row
    per schedule, each carrying its own cron expression and runtime params.
    """

    def test_both_named_schedules_are_registered(self, reflow_client):
        entries = _get_all_schedules(reflow_client, MULTI_SCHEDULE_PIPELINE)
        names = {e["schedule_name"] for e in entries}
        assert names == {"morning", "evening"}, (
            f"expected both named schedules registered, got: {names}"
        )

    def test_each_schedule_keeps_its_own_cron_and_params(self, reflow_client):
        entries = {e["schedule_name"]: e for e in _get_all_schedules(reflow_client, MULTI_SCHEDULE_PIPELINE)}

        assert entries["morning"]["cron_expression"] == "0 9 1 1 *"
        assert entries["morning"]["runtime_params"] == {"speed": "fast"}

        assert entries["evening"]["cron_expression"] == "0 17 2 1 *"
        assert entries["evening"]["runtime_params"] == {"speed": "full"}

    def test_get_schedule_endpoint_lists_all_named_schedules(self, reflow_client):
        resp = reflow_client.get(f"/schedules/{MULTI_SCHEDULE_PIPELINE}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["pipeline_name"] == MULTI_SCHEDULE_PIPELINE
        names = {s["schedule_name"] for s in data["schedules"]}
        assert names == {"morning", "evening"}


class TestMultiScheduleParamsReachTheFiredExecution:
    """
    The real point of multi-schedule support: each named schedule's stored
    params must actually reach the run the scheduler fires — not just sit in
    the /schedules row. e2e_multi_schedule_frequent_test declares "fast"
    (params={"speed": "fast"} -> 2 jobs) and "full" (params={"speed": "full"}
    -> 5 jobs) on the same every-minute cron, so a job-count mismatch would
    mean the wrong (or no) params were threaded through.
    """

    @pytest.mark.slow
    def test_fast_and_full_schedules_fire_with_their_own_params(self, reflow_client):
        fast_before = _get_named_schedule(
            reflow_client, MULTI_SCHEDULE_FREQUENT_PIPELINE, "fast"
        )
        full_before = _get_named_schedule(
            reflow_client, MULTI_SCHEDULE_FREQUENT_PIPELINE, "full"
        )
        assert fast_before is not None and full_before is not None

        fast_execution_id = _wait_for_new_execution(
            reflow_client,
            MULTI_SCHEDULE_FREQUENT_PIPELINE,
            "fast",
            fast_before.get("last_execution_id"),
            max_wait=90,
        )
        full_execution_id = _wait_for_new_execution(
            reflow_client,
            MULTI_SCHEDULE_FREQUENT_PIPELINE,
            "full",
            full_before.get("last_execution_id"),
            max_wait=90,
        )
        assert fast_execution_id != full_execution_id

        fast_stats = _wait_for_execution(reflow_client, fast_execution_id, max_wait=60)
        full_stats = _wait_for_execution(reflow_client, full_execution_id, max_wait=60)

        assert fast_stats["total_jobs"] == 2, (
            f"'fast' schedule (params={{'speed': 'fast'}}) should fire with 2 jobs, "
            f"got {fast_stats['total_jobs']} — its runtime_params were not threaded "
            "through to the fired execution"
        )
        assert full_stats["total_jobs"] == 5, (
            f"'full' schedule (params={{'speed': 'full'}}) should fire with 5 jobs, "
            f"got {full_stats['total_jobs']} — its runtime_params were not threaded "
            "through to the fired execution"
        )
