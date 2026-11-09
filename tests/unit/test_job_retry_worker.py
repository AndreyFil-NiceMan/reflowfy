"""job_retry on the worker and in the drive loop, against a real (SQLite) database.

The worker decides: a failed job whose error matches the payload's job_retry config, with
retries left, goes back to ``pending`` with ``next_attempt_at`` set; the drive loop sends
it once that time has passed, and the execution cannot end before.
"""

import logging
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.pipeline_runner as runner_mod
from reflowfy.core.job_retry import JobRetryConfig
from reflowfy.core.registry import pipeline_registry
from reflowfy.destinations.base import DestinationError
from reflowfy.observability import metrics
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner
from reflowfy.reflow_manager.rate_limiter import RateLimiter
from reflowfy.worker.executor import JobStats, WorkerExecutor


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch):
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)

pytestmark = pytest.mark.real_claim

PIPE = "job_retry_worker_pipeline"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def db_file(tmp_path) -> str:
    return f"{tmp_path}/retry.db"


@pytest.fixture
def db(db_file) -> Session:
    engine = create_engine(f"sqlite:///{db_file}", connect_args={"timeout": 30})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(
        Execution(
            execution_id="e", pipeline_name=PIPE, state="running", total_jobs=1, plan_complete=True
        )
    )
    session.add(
        Job(job_id="j", execution_id="e", job_payload={}, batch_number=1, state="dispatched")
    )
    session.commit()
    yield session
    session.close()
    engine.dispose()


class _Destination:
    """Fails with the queued errors, then succeeds."""

    def __init__(self, errors: List[BaseException], healthy: bool = True) -> None:
        self.errors = errors
        self.healthy = healthy
        self.sent: List[Any] = []

    async def health_check(self) -> bool:
        return self.healthy

    async def send_with_retry(self, records: List[Any], params: Any) -> None:
        if self.errors:
            raise self.errors.pop(0)
        self.sent.extend(records)

    async def close(self) -> None:
        pass


def _register(monkeypatch, destination: _Destination) -> None:
    pipe = SimpleNamespace(
        name=PIPE,
        define_transformations=lambda records, params: [],
        define_destination=lambda records, params: destination,
    )
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipe)


def _payload(retry: Optional[JobRetryConfig]) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {"runtime_params": {}}
    if retry is not None:
        metadata["job_retry"] = retry.to_dict()
    return {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": PIPE,
        "source": {"type": "StaticSource", "config": {"records": [{"id": 1}]}},
        "metadata": metadata,
    }


def _job(db: Session) -> Job:
    db.expire_all()
    return db.query(Job).filter(Job.job_id == "j").one()


def _metric(status: str) -> float:
    return metrics.jobs_processed_total.labels(pipeline=PIPE, status=status)._value.get()


def _failed_metric(error_type: str) -> float:
    return metrics.jobs_failed_total.labels(pipeline=PIPE, error_type=error_type)._value.get()


async def _run(db_file: str, payload: Dict[str, Any]) -> bool:
    executor = WorkerExecutor(database_url=f"sqlite+aiosqlite:///{db_file}")
    try:
        return await executor.execute_job(payload)
    finally:
        await executor.close()


def _make_due(db: Session) -> None:
    db.query(Job).update({"next_attempt_at": _now() - timedelta(seconds=1), "state": "dispatched"})
    db.commit()


async def test_a_transient_failure_is_retried_with_backoff_until_retries_run_out(
    db, db_file, monkeypatch, caplog
):
    caplog.set_level("INFO", logger="reflowfy")
    errors: List[BaseException] = [DestinationError("kafka", "down %d" % i) for i in range(3)]
    _register(monkeypatch, _Destination(errors))
    retry = JobRetryConfig(max_retries=2, backoff_seconds=30, multiplier=2)
    scheduled_before = _metric("retry_scheduled")
    failed_before = _failed_metric("DestinationError")

    assert await _run(db_file, _payload(retry)) is False

    job = _job(db)
    assert job.state == "pending"
    assert job.attempts == 0  # each retry gets the full lease budget again
    assert job.completed_at is None
    assert job.next_attempt_at is not None
    assert 25 <= (job.next_attempt_at - _now()).total_seconds() <= 31
    assert "down 0" in (job.error_message or "")
    assert job.stats is not None
    assert job.stats["retry_count"] == 1
    assert job.stats["retry_scheduled"] is True and job.stats["retry_in_seconds"] == 30.0
    assert job.stats["error_history"] == [
        {"type": "DestinationError", "message": "Destination 'kafka' error: down 0"}
    ]
    assert _metric("retry_scheduled") == scheduled_before + 1
    assert _failed_metric("DestinationError") == failed_before  # not a final failure
    assert "Job failed (DestinationError), retry 1/2 in 30s" in caplog.text

    # An old message delivered again before the backoff passed is not run.
    executor = WorkerExecutor(database_url=f"sqlite+aiosqlite:///{db_file}")
    try:
        assert await executor._claim_job("j") == "pending"
    finally:
        await executor.close()

    _make_due(db)
    assert await _run(db_file, _payload(retry)) is False
    job = _job(db)
    assert job.state == "pending" and job.stats["retry_count"] == 2
    assert 55 <= (job.next_attempt_at - _now()).total_seconds() <= 61  # 30 * 2
    assert len(job.stats["error_history"]) == 2

    _make_due(db)
    assert await _run(db_file, _payload(retry)) is False
    job = _job(db)
    assert job.state == "failed"
    assert job.next_attempt_at is None  # cleared by the claim
    assert job.completed_at is not None
    assert job.stats["retry_count"] == 2
    assert "retry_scheduled" not in job.stats
    assert [h["message"][-6:] for h in job.stats["error_history"]] == [
        "down 0",
        "down 1",
        "down 2",
    ]
    assert _failed_metric("DestinationError") == failed_before + 1
    assert "not retried: 2 retries used" in caplog.text


async def test_a_retried_job_that_succeeds_completes(db, db_file, monkeypatch):
    destination = _Destination([TimeoutError("slow")])
    _register(monkeypatch, destination)
    retry = JobRetryConfig(max_retries=1, backoff_seconds=0)

    assert await _run(db_file, _payload(retry)) is False
    assert _job(db).state == "pending"
    _make_due(db)
    assert await _run(db_file, _payload(retry)) is True

    job = _job(db)
    assert job.state == "completed"
    assert destination.sent == [{"id": 1}]
    # The stats of the run that succeeded: no retry fields from the failed runs.
    assert "retry_count" not in (job.stats or {})


async def test_an_error_that_is_not_retried_fails_at_once(db, db_file, monkeypatch, caplog):
    caplog.set_level("INFO", logger="reflowfy")
    _register(monkeypatch, _Destination([KeyError("missing")]))

    assert await _run(db_file, _payload(JobRetryConfig())) is False

    job = _job(db)
    assert job.state == "failed"
    assert job.stats["retry_count"] == 0
    assert job.stats["error_history"][0]["type"] == "KeyError"
    assert "not retried: error is not retried" in caplog.text


async def test_without_job_retry_a_failure_is_final_as_before(db, db_file, monkeypatch):
    _register(monkeypatch, _Destination([DestinationError("kafka", "down")]))

    assert await _run(db_file, _payload(None)) is False

    job = _job(db)
    assert job.state == "failed"
    assert "retry_count" not in job.stats and "error_history" not in job.stats


async def test_a_failed_health_check_is_retried_as_a_destination_error(db, db_file, monkeypatch):
    _register(monkeypatch, _Destination([], healthy=False))

    assert await _run(db_file, _payload(JobRetryConfig(backoff_seconds=5))) is False

    job = _job(db)
    assert job.state == "pending"
    assert job.stats["error_history"][0]["type"] == "DestinationError"


async def test_no_retry_once_the_execution_has_ended(db, db_file, monkeypatch, caplog):
    """A job of a cancelled (or force-failed) execution is not put back in the queue:
    nothing would ever send it again."""
    db.query(Execution).update({"state": "cancelled"})
    db.commit()
    _register(monkeypatch, _Destination([DestinationError("kafka", "down")]))
    scheduled_before = _metric("retry_scheduled")

    assert await _run(db_file, _payload(JobRetryConfig())) is False

    job = _job(db)
    assert job.state == "failed"
    assert job.next_attempt_at is None
    assert job.stats["retry_count"] == 0
    assert _metric("retry_scheduled") == scheduled_before


async def test_an_unreadable_config_is_ignored(db, db_file, monkeypatch, caplog):
    _register(monkeypatch, _Destination([DestinationError("kafka", "down")]))
    payload = _payload(None)
    payload["metadata"]["job_retry"] = {"max_retries": "many"}

    assert await _run(db_file, payload) is False

    assert _job(db).state == "failed"
    assert "unreadable job_retry" in caplog.text


async def test_earlier_retries_count_from_zero_when_they_cannot_be_read(monkeypatch, caplog):
    executor = WorkerExecutor(database_url="postgresql://x/y")

    def broken() -> Any:
        raise RuntimeError("database down")

    monkeypatch.setattr(executor, "_async_session", broken)
    assert await executor._previous_retries("j") == (0, [])
    assert "earlier retries" in caplog.text


async def test_previous_retries_ignore_malformed_stats(db, db_file):
    executor = WorkerExecutor(database_url=f"sqlite+aiosqlite:///{db_file}")
    try:
        assert await executor._previous_retries("j") == (0, [])  # no stats yet
        db.query(Job).update(
            {"stats": {"retry_count": True, "error_history": ["x", {"type": "A", "message": ""}]}}
        )
        db.commit()
        assert await executor._previous_retries("j") == (0, [{"type": "A", "message": ""}])
        db.query(Job).update({"stats": {"retry_count": 3, "error_history": "x"}})
        db.commit()
        assert await executor._previous_retries("j") == (3, [])
    finally:
        await executor.close()


def test_retry_fields_stay_out_of_stats_without_job_retry() -> None:
    stats = JobStats().to_dict()
    assert not {"retry_count", "error_history", "retry_scheduled"} & set(stats)


# ----- manager side ---------------------------------------------------------------------


def test_a_scheduled_retry_is_not_dispatched_before_its_time(db):
    manager = JobManager(db)
    db.query(Job).update({"state": "pending", "next_attempt_at": _now() + timedelta(minutes=5)})
    db.commit()
    assert manager.get_pending_jobs("e", limit=10) == []
    assert manager.count_scheduled_retries("e") == 1

    db.query(Job).update({"next_attempt_at": _now() - timedelta(seconds=1)})
    db.commit()
    assert [j.job_id for j in manager.get_pending_jobs("e", limit=10)] == ["j"]
    assert manager.count_scheduled_retries("e") == 0


def test_marking_a_send_dispatched_leaves_a_retry_scheduled_after_it_pending(db):
    """A fast worker ran the message and scheduled a retry before the manager marked
    the send: marked dispatched, the retry would wait out the dispatch lease."""
    manager = JobManager(db)
    sent_after = _now()
    db.query(Job).update({"state": "pending", "next_attempt_at": sent_after + timedelta(seconds=1)})
    db.commit()
    manager.mark_job_dispatched("j", sent_after=sent_after)
    job = _job(db)
    assert job.state == "pending" and job.next_attempt_at is not None

    # A due retry being sent now is marked, and its next_attempt_at cleared.
    manager.mark_job_dispatched("j", sent_after=sent_after + timedelta(seconds=2))
    job = _job(db)
    assert job.state == "dispatched" and job.next_attempt_at is None


def test_retry_failed_clears_a_scheduled_time(db):
    db.query(Job).update({"state": "failed", "next_attempt_at": _now()})
    db.commit()
    assert JobManager(db).requeue_failed("e") == 1
    assert _job(db).next_attempt_at is None


class _FlakyLocalDispatcher(LocalDispatcher):
    def _create_executor(self) -> WorkerExecutor:
        return WorkerExecutor(database_url=self.url)


def test_local_mode_retries_a_job_and_the_execution_waits_for_it(db, db_file, monkeypatch):
    """End to end in local mode: the job fails, is retried by the drive loop once due,
    succeeds, and only then does the execution complete."""
    monkeypatch.setattr(runner_mod, "CHECKPOINT_POLL_INTERVAL", 0.05)
    destination = _Destination([ConnectionResetError("reset")])
    _register(monkeypatch, destination)
    db.query(Job).update(
        {
            "state": "pending",
            "job_payload": _payload(JobRetryConfig(max_retries=1, backoff_seconds=0.3)),
        }
    )
    db.commit()
    dispatcher = _FlakyLocalDispatcher(RateLimiter(db))
    dispatcher.url = f"sqlite+aiosqlite:///{db_file}"  # type: ignore[attr-defined]
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher)

    dispatched, completed, failed = runner._drive_execution("e", PIPE, None)

    assert (completed, failed) == (1, 0)
    db.expire_all()
    assert db.query(Execution).one().state == "completed"
    job = _job(db)
    assert job.state == "completed"
    assert destination.sent == [{"id": 1}]


def test_local_mode_sends_nothing_while_the_backoff_runs(db, monkeypatch):
    """The drive loop does not complete, and does not send, while a retry waits."""
    db.query(Job).update({"state": "pending", "next_attempt_at": _now() + timedelta(hours=1)})
    db.commit()
    sent: List[Any] = []

    class _Recorder(LocalDispatcher):
        async def dispatch_jobs_batch(self, jobs, *args: Any, **kwargs: Any) -> int:
            sent.extend(jobs)
            return len(jobs)

    runner = PipelineRunner(ExecutionManager(db), JobManager(db), _Recorder(RateLimiter(db)))
    track = {"finished": -1.0, "progress_at": 0.0, "warned_at": 0.0, "lease_checked_at": 0.0}
    monkeypatch.setattr(runner_mod, "CHECKPOINT_POLL_INTERVAL", 0)
    runner.execution_manager.claim_driver("e", "me", 60)
    for _ in range(3):
        assert asyncio.run(runner._drive_step("e", PIPE, None, "k", "me", track)) is None
    assert sent == []
    assert _job(db).state == "pending"
