"""ReflowManager service lifecycle in app.py: `_startup`, `_recover_interrupted_executions`,
`_resume_execution_sync` and `_shutdown`, on a real SQLite database.

What recovery must get right after a manager restart: resume what was dispatching, fail a
plan whose planner is gone (a plan cannot be continued), and leave alone a plan whose
planner is still alive (another pod, mid-rollout).
"""

import asyncio
import socket
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
import reflowfy.reflow_manager.pipeline_runner as runner_mod
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner

HOST = socket.gethostname()


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/startup.db",
        connect_args={"timeout": 30, "check_same_thread": False},
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine, monkeypatch):
    f = sessionmaker(bind=engine)
    monkeypatch.setattr(app_module, "SessionLocal", f)
    monkeypatch.setenv("EXECUTION_MODE", "local")
    return f


@pytest.fixture
def db(factory) -> Session:
    session = factory()
    yield session
    session.close()


@pytest.fixture
def registry(monkeypatch):
    """Every pipeline name resolves (recovery fails executions whose pipeline is gone)."""
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: SimpleNamespace(rate_limit=None))


def _execution(db: Session, eid: str, state: str, **cols: Any) -> None:
    db.add(Execution(execution_id=eid, pipeline_name="p", state=state, **cols))
    db.commit()


def _row(db: Session, eid: str) -> Execution:
    db.expire_all()
    return db.query(Execution).filter(Execution.execution_id == eid).one()


# ----- _startup ---------------------------------------------------------------------


class _FakeScheduler:
    def __init__(self) -> None:
        self.synced: List[Any] = []

    def sync_schedules_from_registry(self, db: Session) -> None:
        self.synced.append(db)


@pytest.fixture
def startup_spies(monkeypatch, factory) -> Dict[str, Any]:
    """Replace every side effect of `_startup` with a recorder."""
    from reflowfy.core import pipeline_discovery

    calls: Dict[str, Any] = {"order": [], "scheduler": _FakeScheduler()}

    def record(name: str, result: Any = None):
        def fn(*args: Any, **kwargs: Any) -> Any:
            calls["order"].append(name)
            calls.setdefault(name + "_args", args)
            return result

        return fn

    async def recover() -> None:
        calls["order"].append("recover")

    monkeypatch.setattr(app_module, "_db_ready", False)
    monkeypatch.setattr(app_module, "setup_logging", record("logging"))
    monkeypatch.setattr(app_module, "init_db", record("init_db"))
    monkeypatch.setattr(pipeline_discovery, "discover_and_load_pipelines", record("discover"))
    monkeypatch.setattr(app_module, "_recover_interrupted_executions", recover)
    monkeypatch.setattr(app_module, "init_dlq_scheduler", record("dlq"))
    monkeypatch.setattr(
        app_module, "init_pipeline_scheduler", record("pipeline_scheduler", calls["scheduler"])
    )
    monkeypatch.setattr(app_module, "init_content_dedup_scheduler", record("dedup"))
    return calls


async def test_startup_migrates_loads_recovers_then_starts_the_schedulers(startup_spies):
    await app_module._startup()

    assert startup_spies["order"] == [
        "logging",
        "init_db",
        "discover",
        "recover",
        "dlq",
        "pipeline_scheduler",
        "dedup",
    ]
    assert app_module._db_ready is True  # /health turns healthy only after init_db
    # schedulers build runners on fresh sessions
    assert startup_spies["dlq_args"] == (app_module._pipeline_runner_factory,)
    assert startup_spies["pipeline_scheduler_args"] == (app_module._pipeline_runner_factory,)
    assert len(startup_spies["scheduler"].synced) == 1  # schedules synced from the registry


async def test_startup_survives_a_scheduler_that_fails_to_start(startup_spies, monkeypatch):
    def boom(*args: Any) -> Any:
        raise RuntimeError("no thread for you")

    monkeypatch.setattr(app_module, "init_dlq_scheduler", boom)
    monkeypatch.setattr(app_module, "init_pipeline_scheduler", boom)

    await app_module._startup()

    assert startup_spies["order"][-1] == "dedup"  # the rest still started
    assert app_module._db_ready is True


async def test_startup_fails_loudly_when_the_database_never_comes_up(startup_spies, monkeypatch):
    def no_db(*args: Any) -> None:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr(app_module, "init_db", no_db)

    with pytest.raises(RuntimeError, match="database unreachable"):
        await app_module._startup()
    assert app_module._db_ready is False
    assert "recover" not in startup_spies["order"]


def test_pipeline_runner_factory_builds_on_a_fresh_session(factory):
    runner = app_module._pipeline_runner_factory()
    assert isinstance(runner, PipelineRunner)


# ----- _recover_interrupted_executions ----------------------------------------------


async def test_recovery_frees_own_host_leases_and_schedules_interrupted_runs(db, monkeypatch):
    resumed: List[str] = []
    monkeypatch.setattr(app_module, "_resume_execution_sync", resumed.append)
    stamp = _now()
    _execution(
        db,
        "running",
        "running",
        plan_complete=True,
        driver_id=f"{HOST}:123:abcd",
        driver_heartbeat_at=stamp,
    )
    _execution(
        db,
        "unplanned",
        "pending",
        plan_complete=False,
        driver_id="other-host:9:ffff",
        driver_heartbeat_at=stamp,
    )
    _execution(db, "planned-pending", "pending", plan_complete=True)
    _execution(db, "done", "completed", plan_complete=True)
    _execution(db, "paused", "paused", plan_complete=True)

    await app_module._recover_interrupted_executions()
    await asyncio.sleep(0)  # let the executor threads start
    deadline = time.monotonic() + 5
    while len(resumed) < 2 and time.monotonic() < deadline:
        await asyncio.sleep(0.01)

    assert sorted(resumed) == ["running", "unplanned"]
    # this host's dead loop gave its lease up; another host's lease is its own business
    assert _row(db, "running").driver_id is None
    assert _row(db, "unplanned").driver_id == "other-host:9:ffff"


async def test_recovery_with_nothing_interrupted(db, monkeypatch):
    resumed: List[str] = []
    monkeypatch.setattr(app_module, "_resume_execution_sync", resumed.append)
    _execution(db, "done", "completed", plan_complete=True)

    await app_module._recover_interrupted_executions()

    assert resumed == []


# ----- _resume_execution_sync (what recovery runs per execution) ---------------------


def test_resume_drives_a_running_execution_with_pending_jobs(db, registry, monkeypatch):
    driven: List[Any] = []
    monkeypatch.setattr(
        PipelineRunner, "_drive_execution", lambda self, *a: driven.append(a) or (0, 0, 0)
    )
    _execution(db, "e", "running", plan_complete=True, rate_limit=30)
    db.add(Job(job_id="j1", execution_id="e", job_payload={}, state="pending", batch_number=1))
    db.add(Job(job_id="j2", execution_id="e", job_payload={}, state="running", batch_number=1))
    db.commit()

    app_module._resume_execution_sync("e")

    assert driven == [("e", "p", 30)]  # with the persisted /run rate_limit
    db.expire_all()
    # local mode: whatever was running died with the old process, so it is requeued
    assert db.query(Job).filter(Job.job_id == "j2").one().state == "pending"


def test_resume_settles_an_execution_whose_jobs_all_finished(db, registry, monkeypatch):
    monkeypatch.setattr(
        PipelineRunner, "_drive_execution", lambda *a: pytest.fail("nothing to dispatch")
    )
    _execution(db, "e", "running", plan_complete=True, total_jobs=1)
    db.add(Job(job_id="j1", execution_id="e", job_payload={}, state="completed", batch_number=1))
    db.commit()

    app_module._resume_execution_sync("e")

    assert _row(db, "e").state == "completed"


def test_resume_fails_an_execution_whose_pipeline_is_gone(db, monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: None)
    _execution(db, "e", "running", plan_complete=True)

    app_module._resume_execution_sync("e")

    execution = _row(db, "e")
    assert execution.state == "failed" and "not found in registry" in execution.error_message


def test_resume_fails_a_plan_whose_planner_is_gone(db, registry):
    """Planning lease stale and the execution older than one lease: the planner died."""
    old = _now() - timedelta(seconds=runner_mod.DRIVER_LEASE_SECONDS * 3)
    _execution(
        db,
        "e",
        "pending",
        plan_complete=False,
        created_at=old,
        driver_id="plan:dead-pod:1:aaaa",
        driver_heartbeat_at=old,
    )
    db.add(Job(job_id="j1", execution_id="e", job_payload={}, state="pending", batch_number=1))
    db.commit()

    app_module._resume_execution_sync("e")

    execution = _row(db, "e")
    assert execution.state == "failed"
    assert "stopped while planning" in execution.error_message
    assert "1 of its jobs" in execution.error_message
    assert execution.driver_id is None  # the recovery lease was released
    db.expire_all()
    assert db.query(Job).one().state == "pending"  # the fragment was never dispatched


def test_resume_leaves_a_plan_with_a_live_planner_alone(db, registry, monkeypatch):
    """A fresh planning lease means a planner (another pod) is still at work: recovery
    waits instead of failing the execution or dispatching its partial plan."""

    class _StopWaiting(Exception):
        pass

    waits: List[float] = []

    def sleep(seconds: float) -> None:
        waits.append(seconds)
        raise _StopWaiting  # end the wait loop after its first round

    monkeypatch.setattr(runner_mod, "time", SimpleNamespace(sleep=sleep, monotonic=time.monotonic))
    monkeypatch.setattr(
        PipelineRunner, "_drive_execution", lambda *a: pytest.fail("partial plan dispatched")
    )
    old = _now() - timedelta(seconds=runner_mod.DRIVER_LEASE_SECONDS * 3)
    _execution(
        db,
        "e",
        "pending",
        plan_complete=False,
        created_at=old,
        driver_id="plan:live-pod:1:bbbb",
        driver_heartbeat_at=_now(),
    )

    app_module._resume_execution_sync("e")  # logs the interruption, does not raise

    assert waits == [runner_mod.DRIVER_RETRY_SECONDS]  # it was waiting on the lease
    execution = _row(db, "e")
    assert execution.state == "pending" and execution.error_message is None
    assert execution.driver_id == "plan:live-pod:1:bbbb"  # lease untouched


def test_resume_of_an_unknown_execution_is_a_no_op(db, registry):
    app_module._resume_execution_sync("nope")  # must not raise
    assert db.query(Execution).count() == 0


def test_resume_swallows_and_logs_errors(factory, monkeypatch, caplog):
    def broken(db: Session, mode: Any = None) -> Any:
        raise RuntimeError("cannot build manager")

    monkeypatch.setattr(app_module, "_make_manager", broken)
    app_module._resume_execution_sync("e")  # a recovery thread must not crash
    assert "Failed to resume execution" in caplog.text


# ----- _shutdown --------------------------------------------------------------------


async def test_shutdown_stops_all_three_schedulers(monkeypatch):
    stopped: List[str] = []
    monkeypatch.setattr(app_module, "stop_pipeline_scheduler", lambda: stopped.append("pipeline"))
    monkeypatch.setattr(app_module, "stop_dlq_scheduler", lambda: stopped.append("dlq"))
    monkeypatch.setattr(app_module, "stop_content_dedup_scheduler", lambda: stopped.append("dedup"))

    await app_module._shutdown()

    assert stopped == ["pipeline", "dlq", "dedup"]


async def test_shutdown_really_stops_running_schedulers(monkeypatch):
    """With the real init/stop functions: the module-level singletons are cleared."""
    import reflowfy.reflow_manager.content_dedup_scheduler as dedup_mod
    import reflowfy.reflow_manager.dlq_scheduler as dlq_mod
    import reflowfy.reflow_manager.pipeline_scheduler as pipe_mod

    monkeypatch.setattr(dlq_mod.DLQScheduler, "_run_loop", lambda self: None)
    monkeypatch.setattr(pipe_mod.PipelineScheduler, "_run_loop", lambda self: None)
    monkeypatch.setattr(dedup_mod.ContentDedupScheduler, "_run_loop", lambda self: None)
    # restore the singletons whatever happens below
    monkeypatch.setattr(dlq_mod, "_scheduler", None)
    monkeypatch.setattr(pipe_mod, "_scheduler", None)
    monkeypatch.setattr(dedup_mod, "_sweeper", None)
    dlq_mod.init_dlq_scheduler(lambda: None)
    pipe_mod.init_pipeline_scheduler(lambda: None)
    dedup_mod.init_content_dedup_scheduler()
    assert dlq_mod.get_dlq_scheduler() is not None
    assert pipe_mod.get_pipeline_scheduler() is not None

    await app_module._shutdown()

    assert dlq_mod.get_dlq_scheduler() is None
    assert pipe_mod.get_pipeline_scheduler() is None
    assert dedup_mod._sweeper is None
