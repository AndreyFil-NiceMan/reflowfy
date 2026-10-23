"""WorkerExecutor: every outcome of _execute_job_inner, the claim, the heartbeat and
the job-row update.

Each terminal outcome is checked on all three observability channels (CLAUDE.md →
Observability): the log line, the JobStats persisted on the job row, and the
``reflowfy_jobs_processed_total`` status label.

Tests marked ``real_claim`` run against a real SQLite database (aiosqlite) so the
claim, heartbeat and job-row writes execute their actual SQL.
"""

import asyncio
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import AsyncMock

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.ext.asyncio import create_async_engine

import reflowfy.worker.executor as executor_mod
from reflowfy.core.exceptions import SkipJob
from reflowfy.core.registry import pipeline_registry
from reflowfy.destinations.base import DestinationError
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.transformations.base import BaseTransformation
from reflowfy.worker.executor import JobStats, WorkerExecutor

EXECUTOR_LOGGER = "reflowfy.worker.executor"


# ---------------------------------------------------------------- helpers


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@contextmanager
def capture_logs(name: str = EXECUTOR_LOGGER) -> Iterator[List[str]]:
    """Collect a logger's messages directly (the 'reflowfy' logger may not propagate)."""
    target = logging.getLogger(name)
    handler = _ListHandler()
    old_level = target.level
    target.addHandler(handler)
    target.setLevel(logging.DEBUG)
    try:
        yield handler.messages
    finally:
        target.removeHandler(handler)
        target.setLevel(old_level)


def processed(pipeline: str, status: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "reflowfy_jobs_processed_total", {"pipeline": pipeline, "status": status}
        )
        or 0.0
    )


def failed_by_type(pipeline: str, error_type: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "reflowfy_jobs_failed_total", {"pipeline": pipeline, "error_type": error_type}
        )
        or 0.0
    )


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class FakeDest:
    def __init__(
        self,
        healthy: bool = True,
        send_error: Optional[BaseException] = None,
        close_error: Optional[Exception] = None,
        send_delay: float = 0.0,
    ):
        self.healthy = healthy
        self.send_error = send_error
        self.close_error = close_error
        self.send_delay = send_delay
        self.config: Dict[str, Any] = {"topic": "out"}
        self.sent: List[Any] = []
        self.closed = False
        self.during_send: List[Any] = []

    async def health_check(self) -> bool:
        return self.healthy

    async def send_with_retry(self, records: List[Any], params: Dict[str, Any]) -> None:
        if self.send_delay:
            await asyncio.sleep(self.send_delay)
        if self.send_error is not None:
            raise self.send_error
        self.sent.extend(records)

    async def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakePipeline:
    def __init__(self, name: str, dest: FakeDest, transformations: Optional[List[Any]] = None):
        self.name = name
        self.dest = dest
        self.transformations = transformations or []
        self.destination_resolved = False

    def define_transformations(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
        return list(self.transformations)

    def define_destination(self, records: List[Any], params: Dict[str, Any]) -> FakeDest:
        self.destination_resolved = True
        return self.dest


class Explode(BaseTransformation):
    name = "phase2_explode"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        raise ValueError("bad record shape")


class DropJob(BaseTransformation):
    name = "phase2_drop_job"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        raise SkipJob("upstream not ready")


def _payload(pipeline: str, job_id: str = "j", records: Optional[List[Any]] = None, **extra):
    return {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": job_id,
        "pipeline_name": pipeline,
        "source": {
            "type": "StaticSource",
            "config": {"records": [{"id": 1}, {"id": 2}] if records is None else records},
        },
        "metadata": {"runtime_params": {}},
        **extra,
    }


@pytest.fixture
def register(monkeypatch):
    """Make pipeline_registry.get resolve the given FakePipeline objects by name."""
    pipelines: Dict[str, FakePipeline] = {}
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipelines.get(name))

    def _register(pipe: FakePipeline) -> FakePipeline:
        pipelines[pipe.name] = pipe
        return pipe

    return _register


@pytest.fixture
def stub_worker(monkeypatch):
    """A WorkerExecutor whose job-row update is recorded instead of written."""
    worker = WorkerExecutor(database_url="postgresql://x/y")
    updates: List[JobStats] = []

    async def record(execution_id: str, job_id: str, stats: JobStats) -> None:
        updates.append(stats)

    monkeypatch.setattr(worker, "_update_job_in_db", record)
    worker.updates = updates  # type: ignore[attr-defined]
    return worker


async def _make_db(tmp_path, jobs: Dict[str, str], attempts: int = 0) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path}/worker.db"
    engine = create_async_engine(url)
    old = _now() - timedelta(hours=1)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            Execution.__table__.insert().values(
                execution_id="e",
                pipeline_name="p",
                state="running",
                created_at=old,
                updated_at=old,
            )
        )
        for job_id, state in jobs.items():
            await conn.execute(
                Job.__table__.insert().values(
                    job_id=job_id,
                    execution_id="e",
                    job_payload={},
                    state=state,
                    attempts=attempts,
                    created_at=old,
                    updated_at=old,
                )
            )
    await engine.dispose()
    return url


async def _rows(url: str) -> Dict[str, Any]:
    engine = create_async_engine(url)
    async with engine.connect() as conn:
        rows = {r.job_id: r for r in await conn.execute(Job.__table__.select())}
    await engine.dispose()
    return rows


# ---------------------------------------------------------------- outcomes (stubbed DB)


class TestOutcomes:
    async def test_destination_failure_fails_the_job_with_the_error(self, register, stub_worker):
        dest = FakeDest(send_error=DestinationError("kafka", "broker gone"))
        register(FakePipeline("p2-destfail", dest))
        before = processed("p2-destfail", "failed")
        before_typed = failed_by_type("p2-destfail", "DestinationError")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-destfail"))

        assert ok is False
        (stats,) = stub_worker.updates
        assert stats.success is False
        assert stats.error == "Destination 'kafka' error: broker gone"
        assert stats.error_type == "DestinationError"
        assert "DestinationError" in stats.error_traceback
        assert stats.to_dict()["error"] == stats.error
        assert "Job failed: Destination 'kafka' error: broker gone" in logs
        assert processed("p2-destfail", "failed") == before + 1
        assert failed_by_type("p2-destfail", "DestinationError") == before_typed + 1
        assert dest.closed  # producer released even on failure

    async def test_transformation_failure_never_reaches_the_destination(
        self, register, stub_worker
    ):
        dest = FakeDest()
        pipe = register(FakePipeline("p2-tfail", dest, [Explode()]))
        before = processed("p2-tfail", "failed")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-tfail"))

        assert ok is False
        (stats,) = stub_worker.updates
        assert stats.error_type == "TransformationError"
        assert "phase2_explode" in stats.error
        assert "bad record shape" in stats.error
        assert any(m.startswith("Job failed:") for m in logs)
        assert processed("p2-tfail", "failed") == before + 1
        assert not pipe.destination_resolved
        assert dest.sent == []

    async def test_unhealthy_destination_fails_without_sending(self, register, stub_worker):
        dest = FakeDest(healthy=False)
        register(FakePipeline("p2-unhealthy", dest))
        before = processed("p2-unhealthy", "failed")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-unhealthy"))

        assert ok is False
        (stats,) = stub_worker.updates
        assert stats.error == "Destination health check failed"
        assert stats.error_type is None
        assert "Destination health check failed" in logs
        assert processed("p2-unhealthy", "failed") == before + 1
        assert dest.sent == [] and dest.closed

    async def test_success_records_counts_timings_and_metrics(self, register, stub_worker):
        dest = FakeDest()
        register(FakePipeline("p2-ok", dest))
        before = processed("p2-ok", "completed")
        before_records = (
            REGISTRY.get_sample_value("reflowfy_records_processed_total", {"pipeline": "p2-ok"})
            or 0.0
        )

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-ok"))

        assert ok is True
        (stats,) = stub_worker.updates
        assert (stats.success, stats.records_input, stats.records_output) == (True, 2, 2)
        assert stats.end_time is not None and stats.destination_write_time >= 0
        assert any(m.startswith("Job completed") and "2 records" in m for m in logs)
        assert processed("p2-ok", "completed") == before + 1
        assert (
            REGISTRY.get_sample_value("reflowfy_records_processed_total", {"pipeline": "p2-ok"})
            == before_records + 2
        )
        assert dest.sent == [{"id": 1}, {"id": 2}]

    async def test_redelivered_message_is_skipped_and_counted(
        self, register, stub_worker, monkeypatch
    ):
        dest = FakeDest()
        pipe = register(FakePipeline("p2-redeliver", dest))
        monkeypatch.setattr(stub_worker, "_claim_job", AsyncMock(return_value="completed"))
        before = processed("p2-redeliver", "redelivered")
        before_completed = processed("p2-redeliver", "completed")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-redeliver"))

        assert ok is True  # commit: the job row belongs to the delivery that ran it
        assert stub_worker.updates == []  # the row keeps that delivery's result
        assert "Job already completed — skipping redelivered message" in logs
        assert processed("p2-redeliver", "redelivered") == before + 1
        assert processed("p2-redeliver", "completed") == before_completed
        assert not pipe.destination_resolved

    async def test_cancelled_claim_is_skipped_and_counted(self, register, stub_worker, monkeypatch):
        register(FakePipeline("p2-cancel", FakeDest()))
        monkeypatch.setattr(stub_worker, "_claim_job", AsyncMock(return_value="cancelled"))
        before = processed("p2-cancel", "cancelled")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-cancel"))

        assert ok is True
        assert stub_worker.updates == []
        assert "Execution was cancelled — skipping job" in logs
        assert processed("p2-cancel", "cancelled") == before + 1

    async def test_empty_slice_completes_with_no_records(self, register, stub_worker):
        pipe = register(FakePipeline("p2-empty", FakeDest()))
        before = processed("p2-empty", "completed")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-empty", records=[]))

        assert ok is True
        (stats,) = stub_worker.updates
        assert stats.skip_reason is None and stats.records_output == 0
        assert "No records to process" in logs
        assert processed("p2-empty", "completed") == before + 1
        assert not pipe.destination_resolved

    async def test_destination_close_failure_is_logged_not_fatal(self, register, stub_worker):
        register(FakePipeline("p2-closefail", FakeDest(close_error=RuntimeError("socket"))))

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-closefail"))

        assert ok is True
        assert "Failed to close destination: socket" in logs


# ---------------------------------------------------------------- content dedup


def _claims(monkeypatch, won: bool = True, legacy_held: bool = False):
    """Stub the record claim: every hash is won (or none is). Returns the claim mock."""

    async def claim(session_factory, hashes, job_id):
        return set(hashes) if won else set()

    claim_mock = AsyncMock(side_effect=claim)
    monkeypatch.setattr(executor_mod, "claim_record_hashes", claim_mock)
    monkeypatch.setattr(executor_mod, "held_by_other_job", AsyncMock(return_value=legacy_held))
    return claim_mock


class TestContentDedup:
    async def test_lost_claims_are_deduplicated_and_not_written(
        self, register, stub_worker, monkeypatch
    ):
        dest = FakeDest()
        register(FakePipeline("p2-dedup", dest))
        claim = _claims(monkeypatch, won=False)
        before = processed("p2-dedup", "deduplicated")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(_payload("p2-dedup", dedup_check=True))

        assert ok is True
        (stats,) = stub_worker.updates
        assert stats.deduplicated is True and stats.records_output == 0
        assert stats.records_deduplicated == 2
        assert "Content already processed — deduplicated" in logs
        assert processed("p2-dedup", "deduplicated") == before + 1
        assert dest.sent == []
        # one hash per record, claimed under this job
        args = claim.await_args.args
        assert len(args[1]) == 2 and args[2] == "j"

    async def test_a_job_the_previous_release_wrote_is_deduplicated(
        self, register, stub_worker, monkeypatch
    ):
        dest = FakeDest()
        register(FakePipeline("p2-legacy", dest))
        claim = _claims(monkeypatch, legacy_held=True)

        assert await stub_worker.execute_job(_payload("p2-legacy", dedup_check=True)) is True
        assert stub_worker.updates[0].deduplicated is True
        assert dest.sent == []
        claim.assert_not_awaited()

    async def test_the_legacy_guard_can_be_switched_off(
        self, register, stub_worker, monkeypatch
    ):
        monkeypatch.setenv("DEDUP_HONOR_LEGACY_HASH", "false")
        dest = FakeDest()
        register(FakePipeline("p2-legacy-off", dest))
        _claims(monkeypatch, legacy_held=True)

        assert await stub_worker.execute_job(_payload("p2-legacy-off", dedup_check=True)) is True
        executor_mod.held_by_other_job.assert_not_awaited()  # type: ignore[attr-defined]
        assert dest.sent == [{"id": 1}, {"id": 2}]

    async def test_the_guard_checks_the_previous_releases_destination_identity(
        self, register, stub_worker, monkeypatch
    ):
        """The release before last kept an X-API-Key header in the identity; a job it
        wrote is still found by that hash."""
        dest = FakeDest()
        dest.config = {"url": "http://x", "headers": {"X-API-Key": "k"}}
        register(FakePipeline("p2-legacy-prev", dest))
        _claims(monkeypatch)

        assert await stub_worker.execute_job(_payload("p2-legacy-prev", dedup_check=True))
        job_hashes = executor_mod.held_by_other_job.await_args.args[1]  # type: ignore[attr-defined]
        assert len(job_hashes) == 3 and len(set(job_hashes)) == 3

    async def test_no_claim_without_dedup_check(self, register, stub_worker, monkeypatch):
        register(FakePipeline("p2-nodedup", FakeDest()))
        claim = _claims(monkeypatch, won=False)

        assert await stub_worker.execute_job(_payload("p2-nodedup")) is True
        claim.assert_not_awaited()

    async def test_won_claims_are_released_when_the_destination_is_unhealthy(
        self, register, stub_worker, monkeypatch
    ):
        register(FakePipeline("p2-release-h", FakeDest(healthy=False)))
        _claims(monkeypatch)
        release = AsyncMock()
        monkeypatch.setattr(executor_mod, "release_record_hashes", release)

        assert await stub_worker.execute_job(_payload("p2-release-h", dedup_check=True)) is False
        release.assert_awaited_once()
        assert len(release.await_args.args[1]) == 2
        assert release.await_args.args[2] == "j"

    async def test_won_claims_are_released_when_the_send_fails(
        self, register, stub_worker, monkeypatch
    ):
        register(FakePipeline("p2-release-f", FakeDest(send_error=DestinationError("x", "y"))))
        _claims(monkeypatch)
        release = AsyncMock(side_effect=RuntimeError("db down too"))  # swallowed
        monkeypatch.setattr(executor_mod, "release_record_hashes", release)

        assert await stub_worker.execute_job(_payload("p2-release-f", dedup_check=True)) is False
        release.assert_awaited_once()
        assert stub_worker.updates[0].error_type == "DestinationError"

    async def test_shutdown_mid_send_releases_the_claims_and_propagates(
        self, register, stub_worker, monkeypatch
    ):
        """A cancelled task (scale-down) bypasses `except Exception`; the claims must
        still be released so the redelivered message is not marked deduplicated."""
        dest = FakeDest(send_error=asyncio.CancelledError())
        register(FakePipeline("p2-release-c", dest))
        _claims(monkeypatch)
        release = AsyncMock(side_effect=asyncio.CancelledError())  # swallowed as well
        monkeypatch.setattr(executor_mod, "release_record_hashes", release)

        with pytest.raises(asyncio.CancelledError):
            await stub_worker.execute_job(_payload("p2-release-c", dedup_check=True))

        release.assert_awaited_once()
        assert stub_worker.updates == []  # the row is left for the redelivery
        assert dest.closed


# ---------------------------------------------------------------- real database


@pytest.mark.real_claim
class TestClaimJob:
    @pytest.mark.parametrize("state", ["pending", "dispatched"])
    async def test_claimable_states_become_running_and_count_an_attempt(self, tmp_path, state):
        url = await _make_db(tmp_path, {"j": state}, attempts=1)
        worker = WorkerExecutor(database_url=url)
        try:
            assert await worker._claim_job("j") is None
        finally:
            await worker.close()

        row = (await _rows(url))["j"]
        assert (row.state, row.attempts) == ("running", 2)
        assert row.updated_at > _now() - timedelta(minutes=1)

    @pytest.mark.parametrize(
        "state", ["running", "completed", "failed", "deduplicated", "cancelled"]
    )
    async def test_other_states_block_and_are_left_untouched(self, tmp_path, state):
        url = await _make_db(tmp_path, {"j": state}, attempts=1)
        worker = WorkerExecutor(database_url=url)
        try:
            assert await worker._claim_job("j") == state
        finally:
            await worker.close()

        row = (await _rows(url))["j"]
        assert (row.state, row.attempts) == (state, 1)

    async def test_untracked_job_runs(self, tmp_path):
        url = await _make_db(tmp_path, {})
        worker = WorkerExecutor(database_url=url)
        try:
            assert await worker._claim_job("nobody") is None
        finally:
            await worker.close()

    async def test_database_error_fails_open(self, tmp_path):
        url = f"sqlite+aiosqlite:///{tmp_path}/no_tables.db"  # no jobs table -> error
        worker = WorkerExecutor(database_url=url)
        try:
            with capture_logs() as logs:
                assert await worker._claim_job("j") is None
        finally:
            await worker.close()
        assert "Could not claim job (database error); running it anyway" in logs


@pytest.mark.real_claim
class TestUpdateJobInDb:
    async def test_completed_stats_json_is_persisted(self, tmp_path):
        url = await _make_db(tmp_path, {"j": "running"})
        worker = WorkerExecutor(database_url=url)
        stats = JobStats()
        stats.records_input, stats.records_output = 5, 4
        stats.transformation_times = {"t": 0.01}
        stats.success = True
        stats.end_time = stats.start_time + 2
        try:
            await worker._update_job_in_db("e", "j", stats)
        finally:
            await worker.close()

        row = (await _rows(url))["j"]
        assert row.state == "completed"
        assert row.processed_records == 4
        assert row.completed_at is not None
        assert row.error_message is None and row.error_traceback is None
        assert row.stats["records_input"] == 5
        assert row.stats["duration_seconds"] == 2
        assert row.stats["throughput_records_per_second"] == 2
        assert row.stats["transformation_times"] == {"t": 0.01}
        assert row.stats["success"] is True

    async def test_failed_job_stores_error_and_traceback(self, tmp_path):
        url = await _make_db(tmp_path, {"j": "running"})
        worker = WorkerExecutor(database_url=url)
        stats = JobStats()
        stats.error, stats.error_traceback = "boom", "Traceback ...boom"
        stats.end_time = stats.start_time
        try:
            await worker._update_job_in_db("e", "j", stats)
        finally:
            await worker.close()

        row = (await _rows(url))["j"]
        assert row.state == "failed"
        assert row.error_message == "boom"
        assert row.error_traceback == "Traceback ...boom"
        assert row.stats["error"] == "boom" and row.stats["success"] is False

    async def test_deduplicated_wins_over_success(self, tmp_path):
        url = await _make_db(tmp_path, {"j": "running"})
        worker = WorkerExecutor(database_url=url)
        stats = JobStats()
        stats.success = stats.deduplicated = True
        try:
            await worker._update_job_in_db("e", "j", stats)
        finally:
            await worker.close()
        assert (await _rows(url))["j"].state == "deduplicated"

    async def test_missing_row_is_logged_and_nothing_is_created(self, tmp_path):
        url = await _make_db(tmp_path, {})
        worker = WorkerExecutor(database_url=url)
        try:
            with capture_logs() as logs:
                await worker._update_job_in_db("e", "ghost", JobStats())
        finally:
            await worker.close()
        assert "Job not found in database" in logs
        assert await _rows(url) == {}

    async def test_database_error_is_logged_not_raised(self, tmp_path):
        worker = WorkerExecutor(database_url=f"sqlite+aiosqlite:///{tmp_path}/none.db")
        try:
            with capture_logs() as logs:
                await worker._update_job_in_db("e", "j", JobStats())
        finally:
            await worker.close()
        assert any(m.startswith("Failed to update database:") for m in logs)


@pytest.mark.real_claim
class TestEndToEndOnSqlite:
    """The whole worker path against a real job row: claim, run, persist."""

    async def test_dropped_job_persists_skip_reason_and_counts_skipped(self, tmp_path, register):
        url = await _make_db(tmp_path, {"j": "dispatched"})
        dest = FakeDest()
        register(FakePipeline("p2-skip", dest, [DropJob()]))
        before = processed("p2-skip", "skipped")
        worker = WorkerExecutor(database_url=url)
        try:
            with capture_logs() as logs:
                assert await worker.execute_job(_payload("p2-skip")) is True
        finally:
            await worker.close()

        row = (await _rows(url))["j"]
        assert row.state == "completed" and row.attempts == 1
        assert row.stats["skip_reason"] == "upstream not ready"
        assert row.processed_records == 0
        assert "Job dropped by pipeline (upstream not ready)" in logs
        assert processed("p2-skip", "skipped") == before + 1
        assert dest.sent == []

    async def test_failed_destination_persists_error_on_the_row(self, tmp_path, register):
        url = await _make_db(tmp_path, {"j": "dispatched"})
        register(FakePipeline("p2-rowfail", FakeDest(send_error=DestinationError("api", "503"))))
        worker = WorkerExecutor(database_url=url)
        try:
            assert await worker.execute_job(_payload("p2-rowfail")) is False
        finally:
            await worker.close()

        row = (await _rows(url))["j"]
        assert row.state == "failed"
        assert row.error_message == "Destination 'api' error: 503"
        assert "DestinationError" in row.error_traceback
        assert row.stats["success"] is False

    async def test_long_job_heartbeats_while_running(self, tmp_path, register, monkeypatch):
        """updated_at moves forward during a long send, so the manager's running
        lease does not mistake the job for a dead one."""
        monkeypatch.setenv("JOB_HEARTBEAT_SECONDS", "0.05")
        url = await _make_db(tmp_path, {"j": "dispatched"})
        seen: List[datetime] = []

        class SlowDest(FakeDest):
            async def send_with_retry(self, records, params):
                seen.append((await _rows(url))["j"].updated_at)  # right after the claim
                await asyncio.sleep(0.3)
                seen.append((await _rows(url))["j"].updated_at)  # after a few beats
                self.sent.extend(records)

        register(FakePipeline("p2-hb", SlowDest()))
        worker = WorkerExecutor(database_url=url)
        try:
            assert await worker.execute_job(_payload("p2-hb")) is True
        finally:
            await worker.close()

        assert len(seen) == 2 and seen[1] > seen[0]
        assert (await _rows(url))["j"].state == "completed"

    async def test_heartbeat_has_stopped_when_the_job_returns(self, register, stub_worker):
        """Cancelled but not awaited, the heartbeat could still be writing while the
        worker closed its engine."""
        register(FakePipeline("p2-hb-stop", FakeDest()))
        tasks_before = asyncio.all_tasks()

        assert await stub_worker.execute_job(_payload("p2-hb-stop")) is True

        leftover = asyncio.all_tasks() - tasks_before - {asyncio.current_task()}
        assert [t for t in leftover if not t.done()] == []

    async def test_heartbeat_failure_is_logged_and_retried(self, monkeypatch):
        monkeypatch.setenv("JOB_HEARTBEAT_SECONDS", "0.01")
        worker = WorkerExecutor(database_url="postgresql://x/y")

        def broken_session():
            raise RuntimeError("pool exhausted")

        monkeypatch.setattr(worker, "_async_session", broken_session)
        with capture_logs() as logs:
            beat = asyncio.create_task(worker._heartbeat_job("j"))
            await asyncio.sleep(0.1)
            beat.cancel()
            with pytest.raises(asyncio.CancelledError):
                await beat
        await worker.close()

        failures = [m for m in logs if m == "Heartbeat failed; will retry"]
        assert len(failures) >= 2  # kept beating after the first failure
