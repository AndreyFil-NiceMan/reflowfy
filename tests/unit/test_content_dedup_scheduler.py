"""ContentDedupScheduler's sweep loop (the purge queries are in test_content_dedup_sweep.py).

The loop is stepped by hand through a controllable stop event, so "one sweep per
interval" is checked without waiting out an interval.
"""

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import reflowfy.reflow_manager.content_dedup_scheduler as sweep_mod
from reflowfy.reflow_manager.content_dedup_scheduler import ContentDedupScheduler
from reflowfy.reflow_manager.models import Base, Execution, Job, ProcessedContent


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
    monkeypatch.setattr(sweep_mod, "SessionLocal", factory)
    yield factory
    engine.dispose()


class _SteppedStop(threading.Event):
    """The loop's stop event, but each between-sweeps wait blocks until ``step()``."""

    def __init__(self) -> None:
        super().__init__()
        self.timeouts: List[float] = []
        self._waiting = threading.Semaphore(0)
        self._go = threading.Semaphore(0)

    def wait(self, timeout=None):  # type: ignore[override]
        self.timeouts.append(timeout)
        self._waiting.release()
        self._go.acquire()
        return self.is_set()

    def set(self) -> None:
        super().set()
        self._go.release()

    def wait_for_sweep(self) -> None:
        assert self._waiting.acquire(timeout=5), "the loop did not finish a sweep"

    def step(self) -> None:
        self._go.release()


def _stepped(scheduler: ContentDedupScheduler) -> _SteppedStop:
    stop = _SteppedStop()
    scheduler._stop_event = stop
    return stop


def _count_sweeps(monkeypatch, fail_first: bool = False) -> List[int]:
    sweeps: List[int] = []

    def purge(db, retention_hours, now=None):
        sweeps.append(retention_hours)
        if fail_first and len(sweeps) == 1:
            raise RuntimeError("deadlock detected")
        return 0

    monkeypatch.setattr(sweep_mod, "purge_expired_content", purge)
    return sweeps


def test_one_sweep_per_interval(sessions, monkeypatch):
    sweeps = _count_sweeps(monkeypatch)
    scheduler = ContentDedupScheduler(retention_hours=24, sweep_interval=900)
    stop = _stepped(scheduler)

    scheduler.start()
    try:
        stop.wait_for_sweep()
        assert sweeps == [24]  # sweeps at once on start
        assert stop.timeouts == [900]

        stop.step()  # one interval elapses
        stop.wait_for_sweep()
        assert sweeps == [24, 24]
        assert stop.timeouts == [900, 900]
    finally:
        scheduler.stop()

    assert len(sweeps) == 2  # stopping does not run another sweep
    assert scheduler._thread is None and not scheduler._running


def test_a_sweep_that_raises_does_not_kill_the_loop(sessions, monkeypatch, caplog):
    sweeps = _count_sweeps(monkeypatch, fail_first=True)
    scheduler = ContentDedupScheduler(sweep_interval=900)
    stop = _stepped(scheduler)

    with caplog.at_level(logging.ERROR, logger=sweep_mod.__name__):
        scheduler.start()
        try:
            stop.wait_for_sweep()
            stop.step()
            stop.wait_for_sweep()
        finally:
            scheduler.stop()

    assert len(sweeps) == 2
    assert any(r.getMessage() == "Content Dedup Sweeper error" for r in caplog.records)


def test_a_sweep_purges_expired_hashes_and_executions_and_commits(sessions, caplog):
    db = sessions()
    old = _now() - timedelta(hours=48)
    db.add(
        ProcessedContent(
            content_hash="old", pipeline_name="p", job_id="j", execution_id="x", created_at=old
        )
    )
    db.add(ProcessedContent(content_hash="new", pipeline_name="p", job_id="j", execution_id="x"))
    db.add(Execution(execution_id="done", pipeline_name="p", state="completed", completed_at=old))
    db.add(Execution(execution_id="live", pipeline_name="p", state="running"))
    db.add(Job(job_id="jd", execution_id="done", job_payload={}, state="completed"))
    db.add(Job(job_id="jl", execution_id="live", job_payload={}, state="running"))
    db.commit()
    scheduler = ContentDedupScheduler(
        retention_hours=24, execution_retention_hours=24, sweep_interval=900
    )
    stop = _stepped(scheduler)

    with caplog.at_level(logging.INFO, logger=sweep_mod.__name__):
        scheduler.start()
        try:
            stop.wait_for_sweep()
        finally:
            scheduler.stop()

    db.expire_all()
    assert [r.content_hash for r in db.query(ProcessedContent)] == ["new"]
    assert [e.execution_id for e in db.query(Execution)] == ["live"]
    assert [j.job_id for j in db.query(Job)] == ["jl"]
    messages = [r.getMessage() for r in caplog.records]
    assert "Content Dedup Sweeper purged 1 expired hash(es)" in messages
    assert any("purged 1 execution(s) older than 24h" in m for m in messages)


def test_start_and_stop_are_idempotent(sessions, monkeypatch):
    _count_sweeps(monkeypatch)
    scheduler = ContentDedupScheduler(sweep_interval=3600)

    scheduler.stop()  # never started
    scheduler.start()
    thread = scheduler._thread
    scheduler.start()
    assert scheduler._thread is thread

    scheduler.stop()  # the real stop event interrupts the hour-long wait
    assert not thread.is_alive()
    scheduler.stop()
    assert scheduler._thread is None


def test_the_module_singleton_starts_and_stops(sessions, monkeypatch):
    _count_sweeps(monkeypatch)
    monkeypatch.setattr(sweep_mod, "_sweeper", None)

    sweeper = sweep_mod.init_content_dedup_scheduler()
    try:
        assert sweeper._running
        assert sweep_mod._sweeper is sweeper
    finally:
        sweep_mod.stop_content_dedup_scheduler()

    assert sweep_mod._sweeper is None
    assert not sweeper._running
    sweep_mod.stop_content_dedup_scheduler()
