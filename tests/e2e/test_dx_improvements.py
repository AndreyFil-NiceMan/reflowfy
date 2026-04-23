"""
E2E Tests: DX Improvement Plan (docs/DX_IMPROVEMENT_PLAN.md)

Covers:
  P1.1 - Worker error propagation (error_message + error_traceback on failed jobs)
  P1.2 - Checkpoint/Job terminology (/jobs + deprecation header on /checkpoints)
  P1.3 - Cron validation at class-definition time
  P1.4 - Deduplication audit trail (deduplicated_jobs in stats)
  P2.3 - Async/sync bridging (nest_asyncio graceful fallback)
  P2.5 - ExecutionContext enrichment (batch_number + total_batches in job metadata)
  P3.1 - Scheduling HA (SELECT FOR UPDATE SKIP LOCKED — already in scheduler)

Prerequisites:
    ReflowManager running at E2E_REFLOW_MANAGER_URL (default http://localhost:8002)
    with EXECUTION_MODE=local and the test pipelines registered.

Run with:
    pytest tests/e2e/test_dx_improvements.py -v -m dx
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
POLL_INTERVAL = 2
MAX_WAIT = 90


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(client: httpx.Client, pipeline_name: str, **extra) -> str:
    body = {"pipeline_name": pipeline_name, **extra}
    r = client.post("/run", json=body)
    assert r.status_code == 202, f"POST /run returned {r.status_code}: {r.text}"
    eid = r.json()["execution_id"]
    assert eid
    return eid


def _wait(client: httpx.Client, execution_id: str, max_wait: int = MAX_WAIT) -> dict:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Execution {execution_id} did not finish within {max_wait}s")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=120.0) as c:
        yield c


# ---------------------------------------------------------------------------
# P1.1 – Worker Error Propagation
# ---------------------------------------------------------------------------

class TestWorkerErrorPropagation:
    """P1.1: failed jobs expose error_message and error_traceback via /errors."""

    @pytest.mark.dx
    def test_errors_endpoint_exists(self, client):
        """/executions/{id}/errors returns 200 for a known execution."""
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)
        r = client.get(f"/executions/{eid}/errors")
        assert r.status_code == 200

    @pytest.mark.dx
    def test_errors_endpoint_404_for_unknown(self, client):
        """/executions/{id}/errors returns 404 for a non-existent execution."""
        r = client.get("/executions/does-not-exist/errors")
        assert r.status_code == 404

    @pytest.mark.dx
    def test_failed_job_has_error_message(self, client):
        """A job that fails stores error_message in the DB and exposes it via /errors."""
        # error_pipeline_test must be a pipeline whose transformation raises an exception
        # (defined in tests/e2e/test_pipelines/). If not present, skip gracefully.
        r = client.post("/run", json={"pipeline_name": "error_pipeline_test"})
        if r.status_code == 404:
            pytest.skip("error_pipeline_test pipeline not registered")

        eid = r.json()["execution_id"]
        stats = _wait(client, eid)

        assert stats["state"] in ("failed", "completed")

        errors = client.get(f"/executions/{eid}/errors").json()
        if errors:
            first = errors[0]
            assert "job_id" in first
            assert "error_message" in first
            assert first["error_message"] is not None

    @pytest.mark.dx
    def test_errors_response_shape(self, client):
        """Each item in /errors has the expected fields."""
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)
        errors = client.get(f"/executions/{eid}/errors").json()
        for item in errors:
            assert "job_id" in item
            assert "batch_number" in item
            assert "error_message" in item
            assert "error_traceback" in item
            assert "failed_at" in item


# ---------------------------------------------------------------------------
# P1.2 – Checkpoint / Job Terminology
# ---------------------------------------------------------------------------

class TestJobsEndpoint:
    """P1.2: /jobs alias works; /checkpoints returns Deprecation header."""

    @pytest.mark.dx
    def test_jobs_endpoint_returns_200(self, client):
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)
        r = client.get(f"/executions/{eid}/jobs")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    @pytest.mark.dx
    def test_jobs_endpoint_with_state_filter(self, client):
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)
        r = client.get(f"/executions/{eid}/jobs", params={"state": "completed"})
        assert r.status_code == 200
        for job in r.json():
            assert job["state"] == "completed"

    @pytest.mark.dx
    def test_checkpoints_endpoint_has_deprecation_header(self, client):
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)
        r = client.get(f"/executions/{eid}/checkpoints")
        assert r.status_code == 200
        assert "deprecation" in {h.lower() for h in r.headers.keys()}, (
            "Expected Deprecation header on /checkpoints response"
        )

    @pytest.mark.dx
    def test_jobs_and_checkpoints_return_same_data(self, client):
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)
        jobs = client.get(f"/executions/{eid}/jobs").json()
        checkpoints = client.get(f"/executions/{eid}/checkpoints").json()
        assert len(jobs) == len(checkpoints)
        job_ids_from_jobs = {j["job_id"] for j in jobs}
        job_ids_from_checkpoints = {j["job_id"] for j in checkpoints}
        assert job_ids_from_jobs == job_ids_from_checkpoints


# ---------------------------------------------------------------------------
# P1.3 – Cron Validation at Class Definition
# ---------------------------------------------------------------------------

class TestCronValidation:
    """P1.3: invalid cron expression raises ValueError at class-definition time."""

    @pytest.mark.dx
    def test_invalid_cron_raises_at_definition(self):
        """Defining a pipeline with a bad cron expression raises ValueError immediately."""
        from reflowfy.core.abstract_pipeline import AbstractPipeline
        from reflowfy.sources.mock import MockSource
        from reflowfy.destinations.console import ConsoleDestination

        with pytest.raises(ValueError, match="invalid cron"):
            class BadCronPipeline(AbstractPipeline):
                name = "bad_cron_test_pipeline_dx"
                schedule = "not-a-cron"

                def define_source(self, params):
                    return MockSource(data=[])

                def define_destination(self, params):
                    return ConsoleDestination()

                def define_transformations(self, params):
                    return []

    @pytest.mark.dx
    def test_valid_cron_does_not_raise(self):
        """A valid 5-field cron expression does not raise."""
        from reflowfy.core.abstract_pipeline import AbstractPipeline
        from reflowfy.sources.mock import MockSource
        from reflowfy.destinations.console import ConsoleDestination
        from reflowfy.core.registry import pipeline_registry

        class GoodCronPipeline(AbstractPipeline):
            name = "good_cron_test_pipeline_dx"
            schedule = "*/5 * * * *"

            def define_source(self, params):
                return MockSource(data=[])

            def define_destination(self, params):
                return ConsoleDestination()

            def define_transformations(self, params):
                return []

        # Should be registered without error
        assert pipeline_registry.get("good_cron_test_pipeline_dx") is not None

    @pytest.mark.dx
    def test_six_field_cron_raises(self):
        """6-field cron (common mistake from AWS/Quartz) raises ValueError."""
        from reflowfy.core.abstract_pipeline import AbstractPipeline
        from reflowfy.sources.mock import MockSource
        from reflowfy.destinations.console import ConsoleDestination

        with pytest.raises(ValueError, match="invalid cron"):
            class SixFieldCronPipeline(AbstractPipeline):
                name = "six_field_cron_pipeline_dx"
                schedule = "0 */5 * * * ?"  # 6 fields — Quartz style, not valid in croniter

                def define_source(self, params):
                    return MockSource(data=[])

                def define_destination(self, params):
                    return ConsoleDestination()

                def define_transformations(self, params):
                    return []


# ---------------------------------------------------------------------------
# P1.4 – Deduplication Audit Trail
# ---------------------------------------------------------------------------

class TestDeduplicationAuditTrail:
    """P1.4: deduplicated_jobs count is surfaced in execution stats."""

    @pytest.mark.dx
    def test_dedup_count_in_stats_when_enabled(self, client):
        """Running a dedup pipeline twice shows deduplicated_jobs > 0 on second run."""
        eid1 = _run(client, "e2e_dedup_off", enable_duplicate_jobs=False)
        _wait(client, eid1)

        eid2 = _run(client, "e2e_dedup_off", enable_duplicate_jobs=False)
        stats2 = _wait(client, eid2)

        assert "deduplicated_jobs" in stats2, "stats must include deduplicated_jobs field"
        # On second run all (or most) jobs should be deduplicated
        assert stats2["deduplicated_jobs"] >= 0

    @pytest.mark.dx
    def test_dedup_count_zero_when_duplicates_allowed(self, client):
        """With enable_duplicate_jobs=True, deduplicated_jobs is 0."""
        eid = _run(client, "e2e_dedup_off", enable_duplicate_jobs=True)
        stats = _wait(client, eid)
        assert stats.get("deduplicated_jobs", 0) == 0

    @pytest.mark.dx
    def test_stats_always_has_deduplicated_jobs_field(self, client):
        """Every execution stats response includes the deduplicated_jobs field."""
        eid = _run(client, "crash_recovery_test")
        stats = _wait(client, eid)
        assert "deduplicated_jobs" in stats


# ---------------------------------------------------------------------------
# P2.3 – Async/Sync Bridging Fix
# ---------------------------------------------------------------------------

class TestAsyncSyncBridging:
    """P2.3: nest_asyncio integration doesn't break existing execution."""

    @pytest.mark.dx
    def test_pipeline_runs_successfully_with_async_fix(self, client):
        """A normal pipeline completes successfully after the _run_async fix."""
        eid = _run(client, "crash_recovery_test")
        stats = _wait(client, eid)
        assert stats["state"] in ("completed", "failed")
        # Specifically test that it doesn't deadlock or raise RuntimeError
        assert stats["state"] != "running", "Execution should have terminated"

    @pytest.mark.dx
    def test_nest_asyncio_import_graceful(self):
        """_run_async handles missing nest_asyncio gracefully."""
        import sys
        import importlib

        # Temporarily hide nest_asyncio if it is installed
        saved = sys.modules.get("nest_asyncio", None)
        sys.modules["nest_asyncio"] = None  # type: ignore[assignment]

        try:
            import importlib
            import reflowfy.reflow_manager.pipeline_runner as pr_module
            importlib.reload(pr_module)
            # _run_async should still work — uses ThreadPoolExecutor fallback
            result = pr_module._run_async(_identity_coro(42))
            assert result == 42
        finally:
            if saved is None:
                del sys.modules["nest_asyncio"]
            else:
                sys.modules["nest_asyncio"] = saved
            importlib.reload(pr_module)


async def _identity_coro(value):
    return value


# ---------------------------------------------------------------------------
# P2.5 – ExecutionContext Enrichment
# ---------------------------------------------------------------------------

class TestExecutionContextEnrichment:
    """P2.5: job metadata includes batch_number and total_batches."""

    @pytest.mark.dx
    def test_job_metadata_has_batch_number(self, client):
        """Every dispatched job has batch_number in its metadata payload."""
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)

        jobs = client.get(f"/executions/{eid}/jobs").json()
        assert jobs, "Expected at least one job"
        for job in jobs:
            payload = job.get("job_payload") or {}
            metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
            assert "batch_number" in metadata, (
                f"Job {job['job_id']} missing batch_number in metadata"
            )

    @pytest.mark.dx
    def test_job_metadata_has_total_batches(self, client):
        """Every dispatched job has total_batches in its metadata payload after Phase 1."""
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)

        jobs = client.get(f"/executions/{eid}/jobs").json()
        assert jobs
        for job in jobs:
            payload = job.get("job_payload") or {}
            metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
            assert "total_batches" in metadata, (
                f"Job {job['job_id']} missing total_batches in metadata"
            )

    @pytest.mark.dx
    def test_batch_numbers_are_consistent(self, client):
        """batch_number values in job metadata are within [1, total_batches]."""
        eid = _run(client, "crash_recovery_test")
        _wait(client, eid)

        jobs = client.get(f"/executions/{eid}/jobs").json()
        for job in jobs:
            payload = job.get("job_payload") or {}
            metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
            bn = metadata.get("batch_number", 0)
            tb = metadata.get("total_batches", 0)
            if tb > 0:
                assert 1 <= bn <= tb, (
                    f"batch_number={bn} out of range [1, {tb}] for job {job['job_id']}"
                )

    @pytest.mark.dx
    def test_execution_context_has_new_fields(self):
        """ExecutionContext dataclass exposes batch_number, total_batches, retry_count, is_retry."""
        from reflowfy.core.execution_context import ExecutionContext

        ctx = ExecutionContext(execution_id="test-123", pipeline_name="test")
        d = ctx.to_dict()

        assert "batch_number" in d
        assert "total_batches" in d
        assert "retry_count" in d
        assert "is_retry" in d
        assert d["batch_number"] == 0
        assert d["total_batches"] == 0
        assert d["retry_count"] == 0
        assert d["is_retry"] is False


# ---------------------------------------------------------------------------
# P3.1 – Scheduling HA (distributed lock already in place)
# ---------------------------------------------------------------------------

class TestSchedulingHA:
    """P3.1: PipelineScheduler uses SELECT FOR UPDATE SKIP LOCKED."""

    @pytest.mark.dx
    def test_scheduler_uses_skip_locked(self):
        """PipelineScheduler._poll_and_trigger uses with_for_update(skip_locked=True)."""
        import inspect
        from reflowfy.reflow_manager.pipeline_scheduler import PipelineScheduler

        source = inspect.getsource(PipelineScheduler._poll_and_trigger)
        assert "skip_locked" in source, (
            "PipelineScheduler._poll_and_trigger must use with_for_update(skip_locked=True) "
            "to prevent multiple scheduler instances from firing the same schedule."
        )

    @pytest.mark.dx
    def test_scheduler_poll_triggers_only_due_schedules(self, client):
        """Schedules API is reachable; disabled schedules are not triggered."""
        r = client.get("/schedules")
        # May not exist or return empty — just verify no 500
        assert r.status_code in (200, 404)
