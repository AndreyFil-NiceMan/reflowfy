"""DLQScheduler: the "run this pipeline later with these params" queue.

Polling picks up due pending jobs only, groups them per pipeline and runs each group
through ``run_pipeline``. A failed run reschedules its jobs with exponential backoff and
archives them once ``max_retries`` is reached. The on-demand ``process_*_immediately``
paths behind ``/dlq/.../dispatch`` share that failure handling. All on SQLite.
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import reflowfy.reflow_manager.dlq_scheduler as dlq_mod
from reflowfy.observability import metrics
from reflowfy.reflow_manager.dlq_scheduler import DLQScheduler
from reflowfy.reflow_manager.models import Base, DLQJob, DLQJobArchive


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch):
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)


@pytest.fixture
def sessions(monkeypatch) -> Callable[[], Session]:
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(dlq_mod, "SessionLocal", factory)
    yield factory
    engine.dispose()


@pytest.fixture
def db(sessions) -> Session:
    session = sessions()
    yield session
    session.close()


def _dlq(
    db: Session,
    pipeline: str = "p",
    *,
    due: bool = True,
    status: str = "pending",
    retry_count: int = 0,
    max_retries: int = 5,
    **payload: Any,
) -> int:
    job = DLQJob(
        job_payload=payload,
        pipeline_name=pipeline,
        scheduled_at=_now() + (timedelta(minutes=-1) if due else timedelta(hours=1)),
        delay_minutes=60,
        status=status,
        retry_count=retry_count,
        max_retries=max_retries,
    )
    db.add(job)
    db.commit()
    return job.id


def _job(db: Session, job_id: int) -> Optional[DLQJob]:
    db.expire_all()
    return db.query(DLQJob).filter(DLQJob.id == job_id).first()


class _Runner:
    def __init__(self, calls: List[Dict[str, Any]], error: Optional[Exception] = None) -> None:
        self.calls = calls
        self.error = error

    def run_pipeline(
        self, pipeline_name: str, runtime_params: Dict[str, Any], execution_id: str
    ) -> Dict[str, Any]:
        self.calls.append(
            {"pipeline": pipeline_name, "params": runtime_params, "execution_id": execution_id}
        )
        if self.error:
            raise self.error
        return {"execution_id": execution_id}


def _scheduler(calls: List[Dict[str, Any]], error: Optional[Exception] = None) -> DLQScheduler:
    return DLQScheduler(pipeline_runner_factory=lambda: _Runner(calls, error))


# ----- _poll_and_process ----------------------------------------------------------


def test_poll_runs_only_due_pending_jobs_grouped_by_pipeline(db):
    a1 = _dlq(db, "a", ids=[1])
    a2 = _dlq(db, "a", ids=[1])
    later = _dlq(db, "a", due=False, ids=[9])
    done = _dlq(db, "a", status="completed", ids=[8])
    b1 = _dlq(db, "b", region="eu")
    calls: List[Dict[str, Any]] = []

    _scheduler(calls)._poll_and_process()

    assert metrics.dlq_depth._value.get() == 4  # pending jobs at poll time, due or not
    by_pipeline = {c["pipeline"]: c for c in calls}
    assert sorted(by_pipeline) == ["a", "b"]  # one execution per pipeline
    assert by_pipeline["a"]["params"] == {"ids": [1], "_dlq_source": True, "_dlq_job_ids": [a1, a2]}
    assert by_pipeline["b"]["params"] == {"region": "eu", "_dlq_source": True, "_dlq_job_ids": [b1]}
    for job_id, pipeline in ((a1, "a"), (a2, "a"), (b1, "b")):
        job = _job(db, job_id)
        assert job.status == "completed"
        assert job.processed_at is not None
        assert job.execution_id == by_pipeline[pipeline]["execution_id"]
        assert job.execution_id.startswith("dlq-")
    assert _job(db, later).status == "pending"
    assert _job(db, done).execution_id is None  # an already-processed job is left alone


def test_poll_with_nothing_due_runs_nothing(db):
    _dlq(db, due=False)
    calls: List[Dict[str, Any]] = []

    _scheduler(calls)._poll_and_process()

    assert calls == []
    assert metrics.dlq_depth._value.get() == 1


def test_every_due_job_runs_with_its_own_params(db):
    _dlq(db, "a", current_id=1)
    _dlq(db, "a", current_id=2)
    calls: List[Dict[str, Any]] = []

    _scheduler(calls)._poll_and_process()

    assert sorted(c["params"]["current_id"] for c in calls) == [1, 2]


def test_a_failed_run_reschedules_with_backoff(db):
    job_id = _dlq(db)
    calls: List[Dict[str, Any]] = []
    before = _now()

    _scheduler(calls, RuntimeError("pipeline exploded"))._poll_and_process()

    job = _job(db, job_id)
    assert (job.status, job.retry_count, job.error_message) == ("pending", 1, "pipeline exploded")
    assert before + timedelta(minutes=5) <= job.scheduled_at <= _now() + timedelta(minutes=5)
    assert job.execution_id is None and job.processed_at is None
    assert db.query(DLQJobArchive).count() == 0


def test_without_a_runner_factory_jobs_are_retried_not_lost(db):
    job_id = _dlq(db)

    DLQScheduler(pipeline_runner_factory=None)._poll_and_process()

    job = _job(db, job_id)
    assert (job.status, job.retry_count) == ("pending", 1)
    assert "factory not configured" in job.error_message


def test_backoff_doubles_from_five_minutes():
    scheduler = DLQScheduler()
    assert [scheduler._calculate_backoff(n) for n in range(1, 6)] == [5, 10, 20, 40, 80]


def test_a_job_is_retried_then_archived_after_max_retries(db):
    job_id = _dlq(db, max_retries=3, ids=[7])
    created_at = _job(db, job_id).created_at
    calls: List[Dict[str, Any]] = []
    scheduler = _scheduler(calls, RuntimeError("still broken"))

    for attempt in (1, 2):
        scheduler._poll_and_process()
        job = _job(db, job_id)
        assert (job.status, job.retry_count) == ("pending", attempt)
        job.scheduled_at = _now() - timedelta(seconds=1)  # let the backoff elapse
        db.commit()

    scheduler._poll_and_process()

    assert len(calls) == 3
    assert _job(db, job_id) is None  # moved out of the live queue
    archived = db.query(DLQJobArchive).one()
    assert archived.id == job_id
    assert (archived.pipeline_name, archived.job_payload) == ("p", {"ids": [7]})
    assert (archived.retry_count, archived.max_retries) == (3, 3)
    assert archived.error_message == "still broken"
    assert archived.created_at == created_at
    assert archived.archived_at is not None


def test_a_poll_error_rolls_back_and_propagates(db, monkeypatch, caplog):
    job_id = _dlq(db)
    scheduler = _scheduler([])

    def crash(session, pipeline_name, jobs):
        for job in jobs:
            job.status = "processing"
        session.flush()
        raise RuntimeError("db went away")

    monkeypatch.setattr(scheduler, "_process_pipeline_jobs", crash)

    with caplog.at_level(logging.ERROR, logger=dlq_mod.__name__):
        with pytest.raises(RuntimeError, match="db went away"):
            scheduler._poll_and_process()

    assert _job(db, job_id).status == "pending"  # not stranded in "processing"
    assert any("DLQ poll error" in r.getMessage() for r in caplog.records)


# ----- process_job_immediately ----------------------------------------------------


def test_process_job_immediately_runs_one_job(db):
    job_id = _dlq(db, due=False, ids=[3])  # on demand: not due yet
    _dlq(db, due=False)
    calls: List[Dict[str, Any]] = []

    execution_id = _scheduler(calls).process_job_immediately(db, job_id)

    assert execution_id and execution_id.startswith("dlq-")
    assert calls == [
        {
            "pipeline": "p",
            "execution_id": execution_id,
            "params": {"ids": [3], "_dlq_source": True, "_dlq_job_ids": [job_id]},
        }
    ]
    job = _job(db, job_id)
    assert (job.status, job.execution_id) == ("completed", execution_id)
    assert job.processed_at is not None


def test_process_job_immediately_for_an_unknown_job_returns_none(db):
    calls: List[Dict[str, Any]] = []
    assert _scheduler(calls).process_job_immediately(db, 404) is None
    assert calls == []


@pytest.mark.parametrize("status", ["processing", "completed"])
def test_process_job_immediately_refuses_a_job_that_is_not_pending(db, status):
    job_id = _dlq(db, status=status)
    calls: List[Dict[str, Any]] = []

    with pytest.raises(ValueError, match=f"not pending \\(status: {status}\\)"):
        _scheduler(calls).process_job_immediately(db, job_id)

    assert calls == []
    assert _job(db, job_id).status == status


def test_process_job_immediately_failure_is_retried_and_re_raised(db):
    job_id = _dlq(db, due=False)

    with pytest.raises(RuntimeError, match="nope"):
        _scheduler([], RuntimeError("nope")).process_job_immediately(db, job_id)

    job = _job(db, job_id)
    assert (job.status, job.retry_count, job.error_message) == ("pending", 1, "nope")


def test_process_job_immediately_failure_on_the_last_retry_archives(db):
    job_id = _dlq(db, retry_count=4, max_retries=5)

    with pytest.raises(RuntimeError):
        _scheduler([], RuntimeError("nope")).process_job_immediately(db, job_id)

    assert _job(db, job_id) is None
    assert db.query(DLQJobArchive).one().retry_count == 5


# ----- process_pipeline_immediately -----------------------------------------------


def test_process_pipeline_immediately_runs_that_pipelines_pending_jobs(db):
    mine = [_dlq(db, "a", due=False, k=1), _dlq(db, "a", k=1)]
    finished = _dlq(db, "a", status="completed")
    other = _dlq(db, "b")
    calls: List[Dict[str, Any]] = []

    count, execution_ids = _scheduler(calls).process_pipeline_immediately(db, "a")

    assert count == 2
    assert len(execution_ids) == 1  # same params: one execution
    execution_id = execution_ids[0]
    assert [c["pipeline"] for c in calls] == ["a"]
    assert calls[0]["params"]["_dlq_job_ids"] == mine
    for job_id in mine:
        assert (_job(db, job_id).status, _job(db, job_id).execution_id) == (
            "completed",
            execution_id,
        )
    assert _job(db, finished).execution_id is None
    assert _job(db, other).status == "pending"


def test_process_pipeline_immediately_with_nothing_pending(db):
    _dlq(db, "a", status="completed")
    calls: List[Dict[str, Any]] = []
    assert _scheduler(calls).process_pipeline_immediately(db, "a") == (0, [])
    assert calls == []


def test_process_pipeline_immediately_runs_each_distinct_params_once(db):
    ids = [_dlq(db, "a", k=1), _dlq(db, "a", k=2), _dlq(db, "a", k=1)]
    calls: List[Dict[str, Any]] = []

    count, execution_ids = _scheduler(calls).process_pipeline_immediately(db, "a")

    assert count == 3 and len(execution_ids) == 2
    assert sorted(c["params"]["k"] for c in calls) == [1, 2]
    assert _job(db, ids[0]).execution_id == _job(db, ids[2]).execution_id == execution_ids[0]
    assert _job(db, ids[1]).execution_id == execution_ids[1]


def test_process_pipeline_immediately_a_failed_group_does_not_cost_the_others(db):
    ids = [_dlq(db, "a", k=1), _dlq(db, "a", k=2), _dlq(db, "a", k=3)]
    calls: List[Dict[str, Any]] = []

    class _FailsOnTwo(_Runner):
        def run_pipeline(self, pipeline_name, runtime_params, execution_id):
            super().run_pipeline(pipeline_name, runtime_params, execution_id)
            if runtime_params["k"] == 2:
                raise RuntimeError("k=2 exploded")
            return {"execution_id": execution_id}

    scheduler = DLQScheduler(pipeline_runner_factory=lambda: _FailsOnTwo(calls))
    count, execution_ids = scheduler.process_pipeline_immediately(db, "a")

    # The group after the failing one still ran, and is not charged a retry.
    assert sorted(c["params"]["k"] for c in calls) == [1, 2, 3]
    assert count == 2 and len(execution_ids) == 2
    assert [_job(db, i).status for i in ids] == ["completed", "pending", "completed"]
    assert (_job(db, ids[1]).retry_count, _job(db, ids[2]).retry_count) == (1, 0)


def test_process_pipeline_immediately_failure_retries_every_job(db):
    ids = [_dlq(db, "a"), _dlq(db, "a", retry_count=4, max_retries=5)]

    with pytest.raises(RuntimeError, match="down"):
        _scheduler([], RuntimeError("down")).process_pipeline_immediately(db, "a")

    first = _job(db, ids[0])
    assert (first.status, first.retry_count) == ("pending", 1)
    assert _job(db, ids[1]) is None  # that one was on its last retry
    assert db.query(DLQJobArchive).one().id == ids[1]


# ----- lifecycle ------------------------------------------------------------------


def _counting_poll(monkeypatch, fail_first: bool = False) -> Dict[str, Any]:
    state: Dict[str, Any] = {"n": 0, "event": threading.Event()}

    def poll(self):
        state["n"] += 1
        state["event"].set()
        if fail_first and state["n"] == 1:
            raise RuntimeError("transient")

    monkeypatch.setattr(DLQScheduler, "_poll_and_process", poll)
    return state


def test_start_and_stop_are_idempotent_and_stop_interrupts_the_wait(monkeypatch, caplog):
    polls = _counting_poll(monkeypatch)
    scheduler = DLQScheduler(poll_interval=3600)

    scheduler.stop()  # never started
    scheduler.start()
    thread = scheduler._thread
    with caplog.at_level(logging.WARNING, logger=dlq_mod.__name__):
        scheduler.start()
    assert scheduler._thread is thread
    assert any("already running" in r.getMessage() for r in caplog.records)
    assert polls["event"].wait(5)

    started = time.monotonic()
    scheduler.stop()
    assert time.monotonic() - started < 5  # did not sit out the 1 h poll interval
    assert not thread.is_alive()
    assert scheduler._thread is None and not scheduler._running
    scheduler.stop()
    assert polls["n"] == 1


def test_a_poll_exception_does_not_kill_the_loop(monkeypatch, caplog):
    polls = _counting_poll(monkeypatch, fail_first=True)
    scheduler = DLQScheduler(poll_interval=0)

    with caplog.at_level(logging.ERROR, logger=dlq_mod.__name__):
        scheduler.start()
        try:
            deadline = time.monotonic() + 5
            while polls["n"] < 3 and time.monotonic() < deadline:
                time.sleep(0.005)
        finally:
            scheduler.stop()

    assert polls["n"] >= 3
    assert any(r.getMessage() == "DLQ Scheduler error" for r in caplog.records)


def test_the_module_singleton_starts_and_stops(monkeypatch):
    _counting_poll(monkeypatch)
    monkeypatch.setattr(dlq_mod, "_scheduler", None)
    monkeypatch.setattr(dlq_mod, "DLQ_POLL_INTERVAL_SECONDS", 3600)

    def factory() -> None:
        return None

    scheduler = dlq_mod.init_dlq_scheduler(factory)
    try:
        assert dlq_mod.get_dlq_scheduler() is scheduler
        assert scheduler._running and scheduler.pipeline_runner_factory is factory
    finally:
        dlq_mod.stop_dlq_scheduler()

    assert dlq_mod.get_dlq_scheduler() is None
    assert not scheduler._running
    dlq_mod.stop_dlq_scheduler()
