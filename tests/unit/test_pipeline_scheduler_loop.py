"""PipelineScheduler's poll loop against a real (SQLite) pipeline_schedules table.

``test_pipeline_scheduler.py`` covers the registry sync and ``test_scheduler_async_dispatch``
the background dispatch; this file covers what fires: ``_poll_and_trigger`` over due,
not-due and disabled schedules, a trigger that cannot start its run, the failure paths of
the dispatch thread, and the loop's start/stop lifecycle.
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Callable, Dict, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import reflowfy.reflow_manager.pipeline_scheduler as ps_mod
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.models import Base, Execution, PipelineSchedule
from reflowfy.reflow_manager.pipeline_scheduler import PipelineScheduler


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch):
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)


@pytest.fixture
def engine():
    # One shared connection: the poll session holds a write lock (Postgres locks rows,
    # SQLite the whole file) while the trigger creates the execution in its own session.
    eng = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def sessions(engine, monkeypatch) -> Callable[[], Session]:
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(ps_mod, "SessionLocal", factory)
    return factory


def _schedule(
    db: Session,
    pipeline: str,
    name: str = "default",
    *,
    due: bool = True,
    enabled: str = "true",
    cron: str = "*/5 * * * *",
    **params: Any,
) -> None:
    offset = timedelta(minutes=-1 if due else 10)
    db.add(
        PipelineSchedule(
            pipeline_name=pipeline,
            schedule_name=name,
            cron_expression=cron,
            runtime_params=params,
            next_run_at=_now() + offset,
            enabled=enabled,
        )
    )
    db.commit()


def _rows(db: Session) -> Dict[str, PipelineSchedule]:
    db.expire_all()
    return {f"{r.pipeline_name}/{r.schedule_name}": r for r in db.query(PipelineSchedule)}


class _Runner:
    """Real ExecutionManager on the test database; jobs recorded, not run."""

    def __init__(self, sessions: Callable[[], Session], ran: List[Dict[str, Any]]) -> None:
        self.execution_manager = ExecutionManager(sessions())
        self._ran = ran

    def run_pipeline_jobs(self, **kwargs: Any) -> None:
        self._ran.append(kwargs)


@pytest.fixture
def sync_threads(monkeypatch):
    """Run the scheduler's dispatch thread inline, so its outcome is visible at once."""

    class _InlineThread:
        def __init__(self, target, daemon=None):
            self._target = target

        def start(self):
            self._target()

    monkeypatch.setattr(ps_mod.threading, "Thread", _InlineThread)


# ----- _poll_and_trigger ----------------------------------------------------------


def test_poll_fires_only_due_enabled_schedules(sessions, sync_threads):
    db = sessions()
    _schedule(db, "due", region="eu")
    _schedule(db, "later", due=False)
    _schedule(db, "off", enabled="false")
    before = _rows(db)
    later_next, off_next = before["later/default"].next_run_at, before["off/default"].next_run_at
    ran: List[Dict[str, Any]] = []
    scheduler = PipelineScheduler(pipeline_runner_factory=lambda: _Runner(sessions, ran))

    scheduler._poll_and_trigger()

    rows = _rows(db)
    fired = rows["due/default"]
    assert fired.last_execution_id and fired.last_execution_id.startswith("sched-default-")
    assert fired.last_triggered_at is not None
    assert fired.next_run_at > _now()  # advanced past now
    assert fired.next_run_at.minute % 5 == 0
    assert (rows["later/default"].next_run_at, rows["later/default"].last_triggered_at) == (
        later_next,
        None,
    )
    assert (rows["off/default"].next_run_at, rows["off/default"].last_execution_id) == (
        off_next,
        None,
    )
    execution = db.query(Execution).one()
    assert execution.execution_id == fired.last_execution_id
    assert (execution.pipeline_name, execution.state) == ("due", "pending")
    assert execution.runtime_params == {"region": "eu"}
    assert ran == [
        {
            "execution_id": fired.last_execution_id,
            "pipeline_name": "due",
            "runtime_params": {"region": "eu"},
        }
    ]


def test_each_due_named_schedule_gets_its_own_execution(sessions, sync_threads):
    db = sessions()
    _schedule(db, "p", "morning", mode="fast")
    _schedule(db, "p", "evening", mode="full")
    ran: List[Dict[str, Any]] = []

    PipelineScheduler(pipeline_runner_factory=lambda: _Runner(sessions, ran))._poll_and_trigger()

    assert sorted(r["runtime_params"]["mode"] for r in ran) == ["fast", "full"]
    assert {e.execution_id.split("-")[1] for e in db.query(Execution)} == {"morning", "evening"}


def test_poll_with_nothing_due_triggers_nothing(sessions, monkeypatch):
    db = sessions()
    _schedule(db, "later", due=False)
    scheduler = PipelineScheduler(pipeline_runner_factory=lambda: pytest.fail("not called"))
    calls: List[Any] = []
    monkeypatch.setattr(scheduler, "_trigger_pipeline", lambda *a: calls.append(a))

    scheduler._poll_and_trigger()

    assert calls == []


def test_a_poll_error_rolls_back_and_propagates(sessions, monkeypatch, caplog):
    db = sessions()
    _schedule(db, "due")
    before = _rows(db)["due/default"].next_run_at
    scheduler = PipelineScheduler(pipeline_runner_factory=lambda: None)

    def half_then_crash(session, schedule, now):
        schedule.next_run_at = now + timedelta(days=1)
        session.flush()
        raise RuntimeError("db went away")

    monkeypatch.setattr(scheduler, "_trigger_pipeline", half_then_crash)

    with caplog.at_level(logging.ERROR, logger=ps_mod.__name__):
        with pytest.raises(RuntimeError, match="db went away"):
            scheduler._poll_and_trigger()

    assert _rows(db)["due/default"].next_run_at == before  # nothing half-applied
    assert any("poll error" in r.getMessage() for r in caplog.records)


# ----- _trigger_pipeline ----------------------------------------------------------


def test_trigger_without_a_runner_factory_still_advances_the_timer(sessions, caplog):
    db = sessions()
    _schedule(db, "due")

    with caplog.at_level(logging.ERROR, logger=ps_mod.__name__):
        PipelineScheduler(pipeline_runner_factory=None)._poll_and_trigger()

    row = _rows(db)["due/default"]
    assert row.last_execution_id is None  # nothing was started
    assert row.last_triggered_at is not None
    assert row.next_run_at > _now()  # no tight retry loop on a permanent error
    assert db.query(Execution).count() == 0
    assert any(
        "Failed to trigger scheduled pipeline 'due'" in r.getMessage() for r in caplog.records
    )


def test_trigger_whose_execution_cannot_be_created_advances_and_dispatches_nothing(sessions):
    db = sessions()
    _schedule(db, "due")
    ran: List[Dict[str, Any]] = []

    class _Broken(_Runner):
        def __init__(self) -> None:
            super().__init__(sessions, ran)

            def refuse(**_kwargs):
                raise RuntimeError("insert failed")

            self.execution_manager.create_execution = refuse  # type: ignore[method-assign]

    PipelineScheduler(pipeline_runner_factory=_Broken)._poll_and_trigger()

    row = _rows(db)["due/default"]
    assert row.last_execution_id is None
    assert row.next_run_at > _now()
    assert ran == []


def test_a_new_tick_fires_while_the_previous_run_is_still_active(sessions, sync_threads):
    """Overlap is allowed: an execution still running does not hold the next tick back."""
    db = sessions()
    _schedule(db, "p", cron="* * * * *")
    ran: List[Dict[str, Any]] = []
    scheduler = PipelineScheduler(pipeline_runner_factory=lambda: _Runner(sessions, ran))
    scheduler._poll_and_trigger()
    first = _rows(db)["p/default"].last_execution_id
    ExecutionManager(db).update_execution_state(first, "running")
    db.query(PipelineSchedule).update({"next_run_at": _now() - timedelta(seconds=1)})
    db.commit()

    scheduler._poll_and_trigger()

    second = _rows(db)["p/default"].last_execution_id
    assert second != first
    db.expire_all()
    assert {e.execution_id: e.state for e in db.query(Execution)} == {
        first: "running",
        second: "pending",
    }
    assert len(ran) == 2


def test_a_failed_dispatch_marks_the_execution_failed(sessions, sync_threads):
    db = sessions()
    _schedule(db, "due")
    closed: List[bool] = []

    class _Failing(_Runner):
        def __init__(self) -> None:
            super().__init__(sessions, [])
            real_close = self.execution_manager.db.close
            self.execution_manager.db.close = lambda: closed.append(True) or real_close()

        def run_pipeline_jobs(self, **kwargs: Any) -> None:
            raise RuntimeError("source unreachable")

    PipelineScheduler(pipeline_runner_factory=_Failing)._poll_and_trigger()

    db.expire_all()
    execution = db.query(Execution).one()
    assert execution.state == "failed"
    assert execution.error_message == "source unreachable"
    assert closed  # the dispatch thread's own session is closed


def test_dispatch_thread_swallows_errors_while_recording_the_failure(sync_threads):
    """A database that is down for the failure record and the close must not raise
    out of the daemon thread (the scheduler would not see it anyway)."""

    def broken(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("db down")

    runner = SimpleNamespace(
        run_pipeline_jobs=broken,
        execution_manager=SimpleNamespace(
            update_execution_state=broken, db=SimpleNamespace(close=broken)
        ),
    )
    scheduler = PipelineScheduler(pipeline_runner_factory=lambda: runner)

    scheduler._dispatch_async("p", "sched-x", {})  # must not raise


@pytest.mark.parametrize("method", ["_create_execution", "_dispatch_async"])
def test_helpers_refuse_without_a_runner_factory(method):
    with pytest.raises(RuntimeError, match="factory not configured"):
        getattr(PipelineScheduler(pipeline_runner_factory=None), method)("p", "e", {})


# ----- lifecycle ------------------------------------------------------------------


def _counting_poll(monkeypatch, fail_first: bool = False) -> Dict[str, Any]:
    state: Dict[str, Any] = {"n": 0, "event": threading.Event()}

    def poll(self):
        state["n"] += 1
        state["event"].set()
        if fail_first and state["n"] == 1:
            raise RuntimeError("transient")

    monkeypatch.setattr(PipelineScheduler, "_poll_and_trigger", poll)
    return state


def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert predicate()


def test_start_and_stop_are_idempotent(monkeypatch, caplog):
    polls = _counting_poll(monkeypatch)
    scheduler = PipelineScheduler(poll_interval=3600)

    scheduler.stop()  # never started: no-op
    scheduler.start()
    thread = scheduler._thread
    with caplog.at_level(logging.WARNING, logger=ps_mod.__name__):
        scheduler.start()
    assert scheduler._thread is thread  # no second thread
    assert any("already running" in r.getMessage() for r in caplog.records)
    assert polls["event"].wait(5)

    scheduler.stop()
    assert scheduler._thread is None and not scheduler._running
    assert not thread.is_alive()  # the long poll wait was interrupted, not waited out
    scheduler.stop()
    assert polls["n"] == 1


def test_a_poll_exception_does_not_kill_the_loop(monkeypatch, caplog):
    polls = _counting_poll(monkeypatch, fail_first=True)
    scheduler = PipelineScheduler(poll_interval=0)

    with caplog.at_level(logging.ERROR, logger=ps_mod.__name__):
        scheduler.start()
        try:
            _wait_for(lambda: polls["n"] >= 3)
        finally:
            scheduler.stop()

    assert any(r.getMessage() == "Pipeline Scheduler error" for r in caplog.records)


def test_the_module_singleton_starts_and_stops(monkeypatch):
    _counting_poll(monkeypatch)
    monkeypatch.setattr(ps_mod, "_scheduler", None)
    monkeypatch.setattr(ps_mod, "PIPELINE_SCHEDULER_POLL_INTERVAL", 3600)

    def factory() -> None:
        return None

    scheduler = ps_mod.init_pipeline_scheduler(factory)
    try:
        assert ps_mod.get_pipeline_scheduler() is scheduler
        assert scheduler._running and scheduler.pipeline_runner_factory is factory
    finally:
        ps_mod.stop_pipeline_scheduler()

    assert ps_mod.get_pipeline_scheduler() is None
    assert not scheduler._running
    ps_mod.stop_pipeline_scheduler()  # nothing to stop: no-op
