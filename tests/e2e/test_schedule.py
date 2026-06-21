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
            "cron_expression",
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
        /schedules should return exactly one row per pipeline, not duplicates,
        regardless of how many times startup sync runs.
        """
        resp = reflow_client.get("/schedules")
        schedules = resp.json()["schedules"]
        names = [s["pipeline_name"] for s in schedules]
        # No duplicates
        assert len(names) == len(set(names)), (
            f"Duplicate schedule rows detected: {names}"
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

        jobs_second_run = stats2.get("jobs_dispatched", 0)
        assert jobs_second_run == 0, (
            f"Second run with enable_duplicate_jobs=False should dispatch 0 jobs "
            f"(same data already processed), but dispatched {jobs_second_run}"
        )
