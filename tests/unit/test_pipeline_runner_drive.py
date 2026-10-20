"""PipelineRunner internals: resume, the dispatch lease, one drive step, finalization.

Covers the parts of ``reflow_manager/pipeline_runner.py`` that the end-to-end drive-loop
tests (``test_dispatch_recovery_fixes.py``) reach only on their happy path:

- ``resume_execution`` from paused, cancelled mid-loop and replaced by another loop;
- ``_wait_for_driver_lease`` against a fresh lease, a stale one and the same host;
- ``_drive_step``'s in-flight top-up, lease sweep and no-progress warning (frozen clock);
- ``_drive_async``'s database backoff;
- ``_finalize_execution_state`` for every way an execution can end;
- ``run_pipeline`` / ``run_pipeline_jobs`` error handling and ``_run_async`` in a running loop.

Everything runs on SQLite with fake dispatchers; no clock sleeps longer than a few ms.
"""

import asyncio
import logging
import socket
import sys
import time
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.pipeline_runner as runner_mod
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.execution.job_runner import job
from reflowfy.observability import metrics
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner, _PlanLease

# ----- helpers --------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch):
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)


def _db_error() -> OperationalError:
    return OperationalError("SELECT 1", {}, Exception("connection refused"))


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path}/runner.db", connect_args={"timeout": 30})
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _execution(db: Session, state: str = "running", **cols: Any) -> None:
    cols.setdefault("plan_complete", True)
    db.add(Execution(execution_id="e", pipeline_name="p", state=state, total_jobs=0, **cols))
    db.commit()


def _add_jobs(db: Session, n: int, state: str = "pending", **cols: Any) -> List[str]:
    start = db.query(Job).count()
    ids = [f"j{start + i:03d}" for i in range(n)]
    for job_id in ids:
        db.add(
            Job(
                job_id=job_id,
                execution_id="e",
                job_payload={"job_id": job_id, "pipeline_name": "p"},
                batch_number=1,
                state=state,
                **cols,
            )
        )
    db.commit()
    return ids


def _states(db: Session) -> Dict[str, str]:
    db.expire_all()
    return {j.job_id: j.state for j in db.query(Job).order_by(Job.job_id)}


def _exec(db: Session) -> Execution:
    db.expire_all()
    return db.query(Execution).one()


class _Dispatcher:
    """Sends ``per_call`` jobs per call; ``complete`` plays an instant worker.

    ``after_send`` runs once after each call, to change the world mid-loop."""

    def __init__(self, db: Session, complete: bool = True, per_call: int = 10**9) -> None:
        self.db = db
        self.complete = complete
        self.per_call = per_call
        self.sent: List[str] = []
        self.calls = 0
        self.closed = False
        self.after_send: Optional[Any] = None

    async def dispatch_jobs_batch(
        self,
        jobs,
        pipeline_name,
        rate_limit=None,
        on_sent=None,
        should_stop=None,
        on_unsendable=None,
    ):
        self.calls += 1
        sent = 0
        for payload in jobs[: self.per_call]:
            self.sent.append(payload["job_id"])
            on_sent(payload)
            sent += 1
            if self.complete:
                self.db.query(Job).filter(Job.job_id == payload["job_id"]).update(
                    {"state": "completed"}
                )
                self.db.commit()
        if self.after_send:
            self.after_send()
        return sent

    async def close(self) -> None:
        self.closed = True


class _LocalDispatcher(LocalDispatcher, _Dispatcher):
    """A LocalDispatcher (for the isinstance check) that behaves like _Dispatcher."""

    def __init__(self, db: Session) -> None:
        _Dispatcher.__init__(self, db)

    dispatch_jobs_batch = _Dispatcher.dispatch_jobs_batch
    close = _Dispatcher.close


def _runner(db: Session, dispatcher: Any = None) -> PipelineRunner:
    return PipelineRunner(
        ExecutionManager(db),
        JobManager(db),
        dispatcher if dispatcher is not None else _Dispatcher(db),
    )


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(runner_mod, "CHECKPOINT_POLL_INTERVAL", 0)
    monkeypatch.setattr(runner_mod, "DRIVER_RETRY_SECONDS", 0)


@pytest.fixture
def registry(monkeypatch):
    """Resolve every pipeline name to a stub with the given rate_limit."""
    pipeline = SimpleNamespace(rate_limit=None)
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipeline)
    return pipeline


class _Clock:
    """Frozen replacement for the ``time`` module inside pipeline_runner."""

    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def time(self) -> float:
        return self.now

    def sleep(self, _seconds: float) -> None:
        return None


def _track(at: float, finished: float = -1.0) -> Dict[str, float]:
    return {"finished": finished, "progress_at": at, "warned_at": at, "lease_checked_at": at}


def _step(runner: PipelineRunner, track: Dict[str, float], driver: str = "me") -> Any:
    return asyncio.run(runner._drive_step("e", "p", None, "p:e", driver, track))


# ----- resume_execution -----------------------------------------------------------


def test_resume_of_an_unknown_execution_is_a_no_op(db, registry):
    dispatcher = _Dispatcher(db)
    _runner(db, dispatcher).resume_execution("missing")
    assert dispatcher.calls == 0
    assert db.query(Execution).count() == 0


def test_resume_fails_an_execution_whose_pipeline_is_no_longer_registered(db, monkeypatch):
    monkeypatch.setattr(pipeline_registry, "get", lambda name: None)
    _execution(db)
    _add_jobs(db, 2)
    dispatcher = _Dispatcher(db)

    _runner(db, dispatcher).resume_execution("e")

    execution = _exec(db)
    assert execution.state == "failed"
    assert "Pipeline 'p' not found in registry during recovery" in execution.error_message
    assert execution.completed_at is not None
    assert dispatcher.calls == 0
    assert set(_states(db).values()) == {"pending"}


def test_resume_with_nothing_unfinished_settles_the_state_from_the_jobs(db, registry):
    _execution(db)
    _add_jobs(db, 2, state="completed")
    _add_jobs(db, 1, state="deduplicated")
    dispatcher = _Dispatcher(db)

    _runner(db, dispatcher).resume_execution("e")

    execution = _exec(db)
    assert execution.state == "completed"
    assert (execution.jobs_completed, execution.deduplicated_jobs) == (3, 1)
    assert dispatcher.calls == 0


def test_resume_from_paused_drives_the_rest_to_completion(db, registry):
    """POST /resume flips paused -> running, then the runner restarts the loop."""
    _execution(db, state="paused")
    done = _add_jobs(db, 2, state="completed")
    pending = _add_jobs(db, 3)
    dispatcher = _Dispatcher(db)
    assert ExecutionManager(db).resume_execution("e") is not None

    _runner(db, dispatcher).resume_execution("e")

    assert dispatcher.sent == pending  # finished jobs are not sent again
    assert set(_states(db).values()) == {"completed"}
    execution = _exec(db)
    assert execution.state == "completed"
    assert execution.jobs_completed == len(done) + len(pending)
    assert execution.driver_id is None  # lease released on exit
    assert dispatcher.closed


def test_resume_of_a_still_paused_execution_sends_nothing(db, registry):
    _execution(db, state="paused")
    _add_jobs(db, 3)
    dispatcher = _Dispatcher(db)

    _runner(db, dispatcher).resume_execution("e")

    assert dispatcher.sent == []
    assert _exec(db).state == "paused"
    assert set(_states(db).values()) == {"pending"}


def test_resume_cancelled_mid_loop_stops_and_stays_cancelled(db, registry):
    _execution(db)
    ids = _add_jobs(db, 4)
    dispatcher = _Dispatcher(db, per_call=1)

    def cancel() -> None:
        if ExecutionManager(db).cancel_execution("e"):
            JobManager(db).cancel_unstarted_jobs("e")

    dispatcher.after_send = cancel

    _runner(db, dispatcher).resume_execution("e")

    assert dispatcher.sent == ids[:1]
    states = _states(db)
    assert states[ids[0]] == "completed"
    assert {states[i] for i in ids[1:]} == {"cancelled"}
    execution = _exec(db)
    assert execution.state == "cancelled"  # not overwritten by the loop's finalize
    assert execution.driver_id is None


def test_resume_stops_when_another_loop_takes_over(db, registry):
    _execution(db)
    ids = _add_jobs(db, 4)
    dispatcher = _Dispatcher(db, per_call=1)

    def steal() -> None:
        db.query(Execution).update({"driver_id": "other", "driver_heartbeat_at": _now()})
        db.commit()

    dispatcher.after_send = steal

    _runner(db, dispatcher).resume_execution("e")

    assert dispatcher.sent == ids[:1]
    assert [s for s in _states(db).values()].count("pending") == 3  # left to the new owner
    execution = _exec(db)
    assert execution.state == "running"  # not finalized by the loop that lost the lease
    assert execution.driver_id == "other"  # and its lease is not released by us


@pytest.mark.xfail(
    strict=True,
    reason=(
        "BUG: resume_execution never checks the execution's state. A /resume whose "
        "background task runs after a /cancel finds no unfinished jobs and "
        "_finalize_execution_state overwrites the terminal 'cancelled' with 'failed'."
    ),
)
def test_resume_does_not_overwrite_a_cancelled_execution(db, registry):
    _execution(db)
    _add_jobs(db, 1, state="completed")
    _add_jobs(db, 3)
    ExecutionManager(db).cancel_execution("e")
    JobManager(db).cancel_unstarted_jobs("e")

    _runner(db).resume_execution("e")

    assert _exec(db).state == "cancelled"


def test_resume_of_an_unplanned_execution_that_was_settled_elsewhere_does_nothing(db, registry):
    _execution(db, state="cancelled", plan_complete=False)
    _add_jobs(db, 2, state="cancelled")
    dispatcher = _Dispatcher(db)

    _runner(db, dispatcher).resume_execution("e")

    assert _exec(db).state == "cancelled"
    assert dispatcher.calls == 0


@pytest.mark.parametrize(
    "override, persisted, class_rate, expected",
    [
        (None, None, 12.0, 12.0),  # falls back to the pipeline's class rate_limit
        (None, 30.0, 12.0, 30.0),  # the persisted /run override wins over the class
        (90.0, 30.0, 12.0, 90.0),  # an explicit override wins over both
        (None, None, None, None),  # unlimited
    ],
)
def test_resume_rate_limit_precedence(
    db, registry, monkeypatch, override, persisted, class_rate, expected
):
    registry.rate_limit = class_rate
    _execution(db, rate_limit=persisted)
    _add_jobs(db, 1)
    runner = _runner(db)
    seen: Dict[str, Any] = {}

    def fake_drive(execution_id, pipeline_name, rate):
        seen["rate"] = rate
        return 0, 0, 0

    monkeypatch.setattr(runner, "_drive_execution", fake_drive)
    runner.resume_execution("e", rate_limit_override=override)

    assert seen == {"rate": expected}


def test_resume_in_local_mode_requeues_jobs_that_died_with_the_process(db, registry, monkeypatch):
    _execution(db)
    dispatched = _add_jobs(db, 1, state="dispatched", dispatched_at=_now())
    running = _add_jobs(db, 1, state="running", updated_at=_now())
    done = _add_jobs(db, 1, state="completed")
    runner = _runner(db, _LocalDispatcher(db))
    seen: Dict[str, str] = {}

    def fake_drive(*_args):
        seen.update(_states(db))
        return 0, 0, 0

    monkeypatch.setattr(runner, "_drive_execution", fake_drive)
    runner.resume_execution("e")

    assert seen[dispatched[0]] == "pending"
    assert seen[running[0]] == "pending"
    assert seen[done[0]] == "completed"


def test_resume_in_distributed_mode_leaves_in_flight_jobs_to_their_leases(
    db, registry, monkeypatch
):
    _execution(db)
    running = _add_jobs(db, 1, state="running", updated_at=_now())
    runner = _runner(db)
    seen: Dict[str, str] = {}
    monkeypatch.setattr(
        runner, "_drive_execution", lambda *a: seen.update(_states(db)) or (0, 0, 0)
    )

    runner.resume_execution("e")

    assert seen[running[0]] == "running"


# ----- _wait_for_driver_lease -----------------------------------------------------


def _wait(runner: PipelineRunner, driver: str = "me") -> bool:
    return asyncio.run(runner._wait_for_driver_lease("e", driver))


def test_an_unowned_execution_is_claimed_at_once(db):
    _execution(db)
    assert _wait(_runner(db)) is True
    assert _exec(db).driver_id == "me"


def test_a_stale_lease_is_taken_over_at_once(db):
    _execution(db, driver_id="dead", driver_heartbeat_at=_now() - timedelta(seconds=600))
    assert _wait(_runner(db)) is True
    assert _exec(db).driver_id == "me"


def test_our_own_lease_is_reclaimed(db):
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    assert _wait(_runner(db)) is True


def test_a_fresh_lease_is_waited_on_until_the_execution_stops_running(db, monkeypatch, caplog):
    _execution(db, driver_id="other", driver_heartbeat_at=_now())
    runner = _runner(db)
    real_state = runner._execution_state
    polls = {"n": 0}

    def state(execution_id):
        polls["n"] += 1
        if polls["n"] == 3:
            ExecutionManager(db).pause_execution("e")
        return real_state(execution_id)

    monkeypatch.setattr(runner, "_execution_state", state)
    with caplog.at_level(logging.INFO, logger=runner_mod.__name__):
        assert _wait(runner) is False

    assert polls["n"] == 3  # stood by twice, then saw the pause
    execution = _exec(db)
    assert execution.driver_id == "other"  # never took the live loop's lease
    assert execution.state == "paused"
    standing_by = [r for r in caplog.records if "standing by" in r.getMessage()]
    assert len(standing_by) == 1  # announced once, not on every retry


def test_a_lease_left_by_this_host_is_freed_by_startup_and_claimed_without_waiting(db):
    host = socket.gethostname()
    _execution(db, driver_id=f"{host}:1:dead", driver_heartbeat_at=_now())
    runner = _runner(db)
    assert runner.execution_manager.claim_driver("e", "me", 60) is False  # still fresh

    assert ExecutionManager(db).release_drivers_on_host(host) == 1
    assert _wait(runner) is True
    assert _exec(db).driver_id == "me"


def test_a_database_error_while_claiming_is_retried(db, monkeypatch):
    _execution(db)
    runner = _runner(db)
    real_claim = runner.execution_manager.claim_driver
    calls = {"claim": 0, "rollback": 0}

    def flaky_claim(*args):
        calls["claim"] += 1
        if calls["claim"] <= 2:
            raise _db_error()
        return real_claim(*args)

    real_rollback = runner.job_manager.db.rollback

    def rollback():
        calls["rollback"] += 1
        real_rollback()

    monkeypatch.setattr(runner.execution_manager, "claim_driver", flaky_claim)
    monkeypatch.setattr(runner.job_manager.db, "rollback", rollback)

    assert _wait(runner) is True
    assert calls == {"claim": 3, "rollback": 2}
    assert _exec(db).driver_id == "me"


# ----- _drive_async ---------------------------------------------------------------


def test_drive_loop_that_never_gets_the_lease_returns_synced_counts(db):
    _execution(db, driver_id="other", driver_heartbeat_at=_now())
    ExecutionManager(db).cancel_execution("e")
    _add_jobs(db, 2, state="completed")
    _add_jobs(db, 1, state="failed")
    dispatcher = _Dispatcher(db)

    result = asyncio.run(_runner(db, dispatcher)._drive_async("e", "p", None, "me"))

    assert result == (3, 2, 1)
    assert dispatcher.calls == 0
    execution = _exec(db)
    assert (execution.jobs_completed, execution.jobs_failed) == (2, 1)
    assert execution.state == "cancelled"


def test_database_errors_back_off_exponentially_and_reset_after_a_good_step(db, monkeypatch):
    _execution(db)
    runner = _runner(db)
    # raise x2, one good pass, raise x5, then finish.
    script: List[Any] = [_db_error(), _db_error(), None] + [_db_error()] * 5 + [(1, 1, 0)]

    async def step(*_args):
        item = script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    sleeps: List[float] = []

    async def record_sleep(seconds):
        sleeps.append(seconds)

    def broken_rollback():
        raise RuntimeError("connection gone too")

    monkeypatch.setattr(runner, "_drive_step", step)
    monkeypatch.setattr(runner_mod.asyncio, "sleep", record_sleep)
    monkeypatch.setattr(runner.job_manager.db, "rollback", broken_rollback)

    result = asyncio.run(runner._drive_async("e", "p", None, "me"))

    assert result == (1, 1, 0)
    # 5 s doubling to a 60 s cap, back to 5 s after a step that got through
    assert sleeps == [5.0, 10.0, 5.0, 10.0, 20.0, 40.0, 60.0]


def test_a_dispatcher_that_fails_to_close_does_not_lose_the_result(db, monkeypatch, caplog):
    _execution(db)
    _add_jobs(db, 2)
    dispatcher = _Dispatcher(db)

    async def broken_close():
        raise RuntimeError("producer already gone")

    dispatcher.close = broken_close  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger=runner_mod.__name__):
        result = _runner(db, dispatcher)._drive_execution("e", "p", None)

    assert result == (2, 2, 0)
    assert _exec(db).state == "completed"
    assert any("Could not close dispatcher" in r.getMessage() for r in caplog.records)


def test_a_lease_that_cannot_be_released_does_not_fail_the_run(db, monkeypatch, caplog):
    _execution(db)
    _add_jobs(db, 1)
    runner = _runner(db)

    def broken_release(*_args):
        raise _db_error()

    monkeypatch.setattr(runner.execution_manager, "release_driver", broken_release)
    with caplog.at_level(logging.WARNING, logger=runner_mod.__name__):
        assert runner._drive_execution("e", "p", None) == (1, 1, 0)

    assert _exec(db).state == "completed"
    assert any("Could not release dispatch lease" in r.getMessage() for r in caplog.records)


# ----- _drive_step ----------------------------------------------------------------


def test_drive_step_tops_the_in_flight_window_up_as_jobs_finish(db, monkeypatch):
    monkeypatch.setattr(runner_mod, "MAX_IN_FLIGHT_JOBS", 4)
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    running = _add_jobs(db, 3, state="running", updated_at=_now())
    pending = _add_jobs(db, 5)
    dispatcher = _Dispatcher(db, complete=False)
    runner = _runner(db, dispatcher)
    track = _track(time.time())

    assert _step(runner, track) is None
    assert dispatcher.sent == pending[:1]  # 3 in flight + 1 = the window of 4

    assert _step(runner, track) is None
    assert dispatcher.sent == pending[:1]  # window full: nothing more

    db.query(Job).filter(Job.job_id.in_(running)).update(
        {"state": "completed"}, synchronize_session=False
    )
    db.commit()
    assert _step(runner, track) is None
    assert dispatcher.sent == pending[:4]  # 1 in flight: three free slots
    states = _states(db)
    assert [states[i] for i in pending] == ["dispatched"] * 4 + ["pending"]
    assert _exec(db).jobs_dispatched == 7  # 3 completed + 4 dispatched


def test_drive_step_fetches_at_most_one_page_of_payloads(db, monkeypatch):
    monkeypatch.setattr(runner_mod, "DISPATCH_FETCH_LIMIT", 2)
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    pending = _add_jobs(db, 5)
    dispatcher = _Dispatcher(db, complete=False)

    _step(_runner(db, dispatcher), _track(time.time()))

    assert dispatcher.sent == pending[:2]


def test_drive_step_finalizes_once_nothing_is_left(db):
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    _add_jobs(db, 2, state="completed")
    _add_jobs(db, 1, state="deduplicated")

    assert _step(_runner(db), _track(time.time())) == (3, 3, 0)
    assert _exec(db).state == "completed"


def test_drive_step_stops_when_it_lost_the_lease(db):
    _execution(db, driver_id="other", driver_heartbeat_at=_now())
    _add_jobs(db, 2)
    dispatcher = _Dispatcher(db)

    assert _step(_runner(db, dispatcher), _track(time.time())) == (0, 0, 0)
    assert dispatcher.calls == 0
    assert _exec(db).state == "running"


def test_drive_step_sweeps_expired_leases_once_per_interval(db, monkeypatch, caplog):
    clock = _Clock()
    monkeypatch.setattr(runner_mod, "time", clock)
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    old = _now() - timedelta(hours=2)
    lost_worker = _add_jobs(db, 1, state="running", updated_at=old, attempts=1)
    out_of_attempts = _add_jobs(db, 1, state="running", updated_at=old, attempts=3)
    alive = _add_jobs(db, 1, state="running", updated_at=_now(), attempts=1)
    dispatcher = _Dispatcher(db, complete=False)
    runner = _runner(db, dispatcher)
    counter = metrics.jobs_processed_total.labels(pipeline="p", status="lease_expired")
    before = counter._value.get()
    track = _track(clock.now)
    track["lease_checked_at"] = clock.now - runner_mod.LEASE_CHECK_INTERVAL

    with caplog.at_level(logging.WARNING, logger=runner_mod.__name__):
        _step(runner, track)

    states = _states(db)
    assert states[out_of_attempts[0]] == "failed"
    assert states[alive[0]] == "running"
    # the requeued job went back to pending and was sent again in the same pass
    assert states[lost_worker[0]] == "dispatched"
    assert dispatcher.sent == lost_worker
    assert counter._value.get() == before + 1
    assert track["lease_checked_at"] == clock.now
    assert any("lease expired" in r.getMessage() for r in caplog.records)

    # Within the interval no second sweep runs, even with another stale job.
    db.query(Job).filter(Job.job_id == alive[0]).update({"updated_at": old, "attempts": 3})
    db.commit()
    clock.now += runner_mod.LEASE_CHECK_INTERVAL - 1
    _step(runner, track)
    assert _states(db)[alive[0]] == "running"
    clock.now += 1
    _step(runner, track)
    assert _states(db)[alive[0]] == "failed"
    assert counter._value.get() == before + 2


def test_drive_step_warns_after_ten_minutes_without_progress(db, monkeypatch, caplog):
    clock = _Clock()
    monkeypatch.setattr(runner_mod, "time", clock)
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    running = _add_jobs(db, 2, state="running", updated_at=_now())
    _add_jobs(db, 1, state="completed")
    runner = _runner(db, _Dispatcher(db, complete=False))
    start = clock.now
    track = _track(start)
    track["lease_checked_at"] = 1e18  # keep the lease sweep out of this test

    def warnings() -> List[str]:
        return [r.getMessage() for r in caplog.records if "no job finished" in r.getMessage()]

    with caplog.at_level(logging.WARNING, logger=runner_mod.__name__):
        _step(runner, track)  # first pass records the finished count as progress
        assert track["finished"] == 1 and track["progress_at"] == start

        clock.now = start + runner_mod.STALL_WARN_SECONDS - 1
        _step(runner, track)
        assert warnings() == []

        clock.now = start + runner_mod.STALL_WARN_SECONDS
        _step(runner, track)
        assert len(warnings()) == 1
        assert "no job finished for 10 min with 2 in flight and 0 pending" in warnings()[0]
        assert track["warned_at"] == clock.now

        clock.now += runner_mod.STALL_WARN_SECONDS / 2
        _step(runner, track)
        assert len(warnings()) == 1  # not again until another full interval

        clock.now = start + 2 * runner_mod.STALL_WARN_SECONDS
        _step(runner, track)
        assert len(warnings()) == 2
        assert "for 20 min" in warnings()[1]

        # A job finishing is progress: the clock restarts and nothing is warned.
        db.query(Job).filter(Job.job_id == running[0]).update({"state": "completed"})
        db.commit()
        clock.now += runner_mod.STALL_WARN_SECONDS
        _step(runner, track)
        assert track["finished"] == 2 and track["progress_at"] == clock.now
        assert len(warnings()) == 2


def test_send_jobs_hands_control_back_when_the_chunk_runs_out(db, monkeypatch):
    """A spent token (chunk=False) is still sent; the chunk running out stops the rest."""
    monkeypatch.setattr(runner_mod, "DISPATCH_CHUNK_SECONDS", 0)
    _execution(db, driver_id="me", driver_heartbeat_at=_now())
    _add_jobs(db, 2)
    answers: List[bool] = []

    class _Probe(_Dispatcher):
        async def dispatch_jobs_batch(
            self,
            jobs,
            pipeline_name,
            rate_limit=None,
            on_sent=None,
            should_stop=None,
            on_unsendable=None,
        ):
            answers.append(should_stop())
            answers.append(should_stop(chunk=False))
            return 0

    runner = _runner(db, _Probe(db))
    jobs = runner.job_manager.get_pending_jobs("e", limit=2)
    assert asyncio.run(runner._send_jobs("e", "p:e", None, "me", jobs)) == 0
    assert answers == [True, False]


# ----- _finalize_execution_state --------------------------------------------------


@pytest.mark.parametrize(
    "jobs, expected_state, expected_counts",
    [
        ({"completed": 3}, "completed", (3, 3, 0)),
        ({"completed": 2, "deduplicated": 1}, "completed", (3, 3, 0)),
        ({"completed": 2, "failed": 1}, "failed", (3, 2, 1)),
        ({"failed": 3}, "failed", (3, 0, 3)),
        # Cancelled jobs never finished: the run did not do its work.
        ({"cancelled": 3}, "failed", (0, 0, 0)),
    ],
)
def test_finalize_sets_the_terminal_state_from_the_jobs(db, jobs, expected_state, expected_counts):
    _execution(db)
    for state, n in jobs.items():
        _add_jobs(db, n, state=state)

    result = _runner(db)._finalize_execution_state("e", 3, (0, 0, 0))

    assert result == expected_counts
    execution = _exec(db)
    assert execution.state == expected_state
    assert execution.completed_at is not None
    assert (execution.jobs_completed, execution.jobs_failed) == expected_counts[1:]


def test_finalize_fails_an_execution_with_jobs_missing(db, caplog):
    _execution(db)
    _add_jobs(db, 2, state="completed")
    _add_jobs(db, 1)  # never ran

    with caplog.at_level(logging.WARNING, logger=runner_mod.__name__):
        assert _runner(db)._finalize_execution_state("e", 3, (0, 0, 0)) == (2, 2, 0)

    assert _exec(db).state == "failed"
    assert any("Only 2 of 3 jobs finished" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(
    "fallback, expected_state",
    [((5, 5, 0), "completed"), ((5, 4, 1), "failed"), ((5, 3, 0), "failed")],
)
def test_finalize_falls_back_to_in_memory_counts_when_the_sync_fails(
    db, monkeypatch, fallback, expected_state
):
    _execution(db)
    runner = _runner(db)

    def broken_sync(_execution_id):
        raise _db_error()

    monkeypatch.setattr(runner, "_sync_counts_from_db", broken_sync)

    assert runner._finalize_execution_state("e", 5, fallback) == fallback
    assert _exec(db).state == expected_state


# ----- run_pipeline / run_pipeline_jobs ------------------------------------------


def test_run_pipeline_with_an_unknown_name_creates_nothing(db, monkeypatch):
    monkeypatch.setattr(pipeline_registry, "get", lambda name: None)

    with pytest.raises(ValueError, match="Pipeline 'nope' not found in registry"):
        _runner(db).run_pipeline("nope", {}, "e")

    assert db.query(Execution).count() == 0


def test_run_pipeline_persists_the_override_and_reports_the_final_state(db, registry, monkeypatch):
    runner = _runner(db)
    seen: Dict[str, Any] = {}

    def fake_jobs(execution_id, pipeline_name, runtime_params, rate_limit_override=None):
        seen.update(params=runtime_params, rate=rate_limit_override)
        execution = runner.execution_manager.get_execution(execution_id)
        execution.jobs_dispatched = 4
        runner.execution_manager.update_execution_state(execution_id, "completed")

    monkeypatch.setattr(runner, "run_pipeline_jobs", fake_jobs)

    result = runner.run_pipeline("p", {"x": 1}, "e", rate_limit_override=42.0)

    assert result == {
        "execution_id": "e",
        "pipeline_name": "p",
        "state": "completed",
        "jobs_dispatched": 4,
        "rate_limit": 42.0,
    }
    assert seen == {"params": {"x": 1}, "rate": 42.0}
    execution = _exec(db)
    assert execution.rate_limit == 42.0
    assert execution.runtime_params == {"x": 1}


def test_run_pipeline_marks_the_execution_failed_and_re_raises(db, registry, monkeypatch):
    runner = _runner(db)

    def boom(**_kwargs):
        raise RuntimeError("source unreachable")

    monkeypatch.setattr(runner, "run_pipeline_jobs", boom)

    with pytest.raises(RuntimeError, match="source unreachable"):
        runner.run_pipeline("p", {}, "e")

    execution = _exec(db)
    assert execution.state == "failed"
    assert execution.error_message == "source unreachable"


def test_run_pipeline_re_raises_the_original_error_when_marking_failed_fails(
    db, registry, monkeypatch, caplog
):
    runner = _runner(db)

    def boom(**_kwargs):
        raise RuntimeError("source unreachable")

    def db_down(*_args, **_kwargs):
        raise _db_error()

    monkeypatch.setattr(runner, "run_pipeline_jobs", boom)
    monkeypatch.setattr(runner.execution_manager, "update_execution_state", db_down)

    with caplog.at_level(logging.ERROR, logger=runner_mod.__name__):
        with pytest.raises(RuntimeError, match="source unreachable"):
            runner.run_pipeline("p", {}, "e")

    assert _exec(db).state == "pending"
    assert any("Failed to mark execution e as failed" in r.getMessage() for r in caplog.records)


def test_run_pipeline_jobs_for_an_unregistered_pipeline_raises_and_releases_the_lease(
    db, monkeypatch
):
    monkeypatch.setattr(pipeline_registry, "get", lambda name: None)
    db.add(Execution(execution_id="e", pipeline_name="gone", state="pending", plan_complete=False))
    db.commit()

    with pytest.raises(ValueError, match="'gone' not found"):
        _runner(db).run_pipeline_jobs("e", "gone", {})

    execution = _exec(db)
    assert execution.driver_id is None
    assert execution.plan_complete is False


class _BatchedPlan(AbstractPipeline[RuntimeParams]):
    name = "phase3_runner_batched_plan"
    rate_limit = 30.0

    def define_jobs(self, runtime_params: Any):
        for i in range(5):
            yield job([{"i": i}], current_id=i)

    def define_transformations(self, records: Any, runtime_params: Any):
        return []

    def define_destination(self, records: Any, runtime_params: Any):
        raise AssertionError("not reached")


def test_planning_flushes_one_checkpoint_batch_at_a_time(db, monkeypatch):
    monkeypatch.setattr(runner_mod, "CHECKPOINT_BATCH_SIZE", 2)
    assert pipeline_registry.get(_BatchedPlan.name) is not None
    db.add(
        Execution(
            execution_id="e", pipeline_name=_BatchedPlan.name, state="pending", plan_complete=False
        )
    )
    db.commit()
    runner = _runner(db)
    inserts: List[int] = []
    real_create = runner.job_manager.create_jobs

    def spy_create(rows):
        inserts.append(len(rows))
        real_create(rows)

    seen: Dict[str, Any] = {}
    monkeypatch.setattr(runner.job_manager, "create_jobs", spy_create)
    monkeypatch.setattr(
        runner, "_drive_execution", lambda eid, name, rate: seen.update(rate=rate) or (0, 0, 0)
    )

    runner.run_pipeline_jobs("e", _BatchedPlan.name, {})

    assert inserts == [2, 2, 1]  # streamed, never more than one batch buffered
    db.expire_all()
    jobs = db.query(Job).all()
    by_id = sorted((j.job_payload["job_params"]["current_id"], j.batch_number) for j in jobs)
    assert by_id == [(0, 1), (1, 1), (2, 2), (3, 2), (4, 3)]
    execution = _exec(db)
    assert execution.total_jobs == 5
    assert execution.plan_complete is True
    assert execution.state == "running"
    assert seen == {"rate": 30.0}  # the class rate_limit, no /run override


# ----- _PlanLease -----------------------------------------------------------------


def test_plan_lease_keeps_beating_through_errors_and_survives_a_failed_release(
    engine, db, monkeypatch, caplog
):
    monkeypatch.setattr(runner_mod, "PLAN_HEARTBEAT_SECONDS", 0.005)
    _execution(db, state="pending", plan_complete=False)
    beats = {"n": 0}

    def broken_beat(self, execution_id, owner):
        beats["n"] += 1
        raise _db_error()

    def broken_release(self, execution_id, owner):
        raise _db_error()

    monkeypatch.setattr(ExecutionManager, "heartbeat_driver", broken_beat)
    monkeypatch.setattr(ExecutionManager, "release_driver", broken_release)

    with caplog.at_level(logging.WARNING, logger=runner_mod.__name__):
        lease = _PlanLease(engine, "e")
        lease.start()
        deadline = time.monotonic() + 2
        while beats["n"] < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
        lease.stop()

    assert beats["n"] >= 2  # a failed beat does not stop the thread
    messages = [r.getMessage() for r in caplog.records]
    assert any("Planning heartbeat failed" in m for m in messages)
    assert any("Could not release the planning lease" in m for m in messages)
    assert _exec(db).driver_id.startswith("plan:")  # left to expire


# ----- _run_async -----------------------------------------------------------------


async def _answer() -> int:
    await asyncio.sleep(0)
    return 42


def test_run_async_without_a_running_loop():
    assert runner_mod._run_async(_answer()) == 42


async def test_run_async_inside_a_running_loop_without_nest_asyncio(monkeypatch):
    monkeypatch.setitem(sys.modules, "nest_asyncio", None)  # import -> ImportError
    assert runner_mod._run_async(_answer()) == 42


async def test_run_async_inside_a_uvloop_that_nest_asyncio_cannot_patch(monkeypatch):
    fake = types.ModuleType("nest_asyncio")

    def refuse() -> None:
        raise ValueError("Can't patch loop of type <class 'uvloop.Loop'>")

    fake.apply = refuse  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nest_asyncio", fake)
    assert runner_mod._run_async(_answer()) == 42
