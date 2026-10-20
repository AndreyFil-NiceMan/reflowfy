"""A manager killed while planning must not leave a run that looks finished.

Found on an OpenShift cluster: the manager was force-killed ~12 s into planning a
2M-doc execution. The execution stayed `running` with total_jobs=0 and 0 jobs saved;
the replacement manager resumed it, found "no unfinished jobs", and recorded it as
`completed`. With some jobs saved it would have dispatched a fragment of the plan and
completed that. Planning now marks `executions.plan_complete` when the last job is
saved and holds a heartbeated lease while it runs; recovery fails an execution whose
planner is gone and leaves one whose planner is alive alone.
"""

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.pipeline_runner as runner_mod
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner, _PlanLease


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def engine(tmp_path):
    """File-backed: the heartbeat thread opens its own connections to the same DB."""
    eng = create_engine(f"sqlite:///{tmp_path}/plan.db", connect_args={"timeout": 30})
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _execution(db: Session, *, plan_complete, state="running", age_s=600, driver=None, beat=None):
    db.add(
        Execution(
            execution_id="e",
            pipeline_name="p",
            state=state,
            total_jobs=0,
            plan_complete=plan_complete,
            created_at=_now() - timedelta(seconds=age_s),
            driver_id=driver,
            driver_heartbeat_at=beat,
        )
    )
    db.commit()


def _jobs(db: Session, n: int) -> List[str]:
    ids = [f"j{i}" for i in range(n)]
    for job_id in ids:
        db.add(
            Job(job_id=job_id, execution_id="e", job_payload={"job_id": job_id},
                batch_number=1, state="pending")
        )
    db.commit()
    return ids


class _Dispatcher:
    def __init__(self, db: Session):
        self.db = db
        self.sent: List[str] = []

    async def dispatch_jobs_batch(self, jobs, pipeline_name, rate_limit=None,
                                  on_sent=None, should_stop=None, on_unsendable=None):
        for payload in jobs:
            self.sent.append(payload["job_id"])
            on_sent(payload)
            self.db.query(Job).filter(Job.job_id == payload["job_id"]).update(
                {"state": "completed"}
            )
            self.db.commit()
        return len(jobs)

    async def close(self):
        pass


def _runner(db: Session) -> PipelineRunner:
    return PipelineRunner(ExecutionManager(db), JobManager(db), _Dispatcher(db))


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: SimpleNamespace(rate_limit=None))
    monkeypatch.setattr(runner_mod, "CHECKPOINT_POLL_INTERVAL", 0.01)
    monkeypatch.setattr(runner_mod, "DRIVER_RETRY_SECONDS", 0.01)


def _state(db: Session) -> Execution:
    db.expire_all()
    return db.query(Execution).one()


def test_abandoned_empty_plan_is_failed_not_completed(db):
    """The reproduced case: killed during enumeration, nothing saved yet."""
    _execution(db, plan_complete=False)
    runner = _runner(db)

    runner.resume_execution("e")

    execution = _state(db)
    assert execution.state == "failed"
    assert "stopped while planning" in execution.error_message
    assert "0 of its jobs" in execution.error_message
    assert runner.dispatcher.sent == []


def test_abandoned_partial_plan_is_failed_and_its_fragment_is_not_dispatched(db):
    _execution(db, plan_complete=False)
    _jobs(db, 3)
    runner = _runner(db)

    runner.resume_execution("e")

    execution = _state(db)
    assert execution.state == "failed"
    assert "3 of its jobs" in execution.error_message
    assert runner.dispatcher.sent == []  # nothing from the fragment ran


def test_legacy_execution_without_the_flag_still_resumes(db):
    """plan_complete NULL = created before the column existed: unchanged behaviour."""
    _execution(db, plan_complete=None)
    _jobs(db, 2)
    runner = _runner(db)

    runner.resume_execution("e")

    assert sorted(runner.dispatcher.sent) == ["j0", "j1"]
    assert _state(db).state == "completed"


def test_completed_plan_resumes_normally(db):
    _execution(db, plan_complete=True)
    _jobs(db, 2)
    runner = _runner(db)

    runner.resume_execution("e")

    assert sorted(runner.dispatcher.sent) == ["j0", "j1"]
    assert _state(db).state == "completed"


def test_a_live_planner_is_waited_for_not_failed(db, monkeypatch):
    """Rollout overlap: the old pod is still planning, its lease is fresh. Recovery on
    the new pod must wait — and only once the lease goes stale call it abandoned."""
    _execution(db, plan_complete=False, driver="plan:old-pod:1:x", beat=_now())
    waits = {"n": 0}
    real_sleep = time.sleep

    def sleeper(seconds):
        waits["n"] += 1
        if waits["n"] == 4:  # the old pod dies: its heartbeat stops and ages out
            db.query(Execution).update({"driver_heartbeat_at": _now() - timedelta(seconds=600)})
            db.commit()
        real_sleep(0)

    monkeypatch.setattr(runner_mod.time, "sleep", sleeper)

    _runner(db).resume_execution("e")

    assert waits["n"] >= 4  # it really did wait while the lease was fresh
    assert _state(db).state == "failed"


def test_planner_that_finishes_while_we_wait_is_resumed_not_failed(db, monkeypatch):
    _execution(db, plan_complete=False, driver="plan:old-pod:1:x", beat=_now())
    runner = _runner(db)
    waits = {"n": 0}

    def sleeper(seconds):
        waits["n"] += 1
        if waits["n"] == 3:  # the planner saves the last job and flips the flag
            _jobs(db, 2)
            db.query(Execution).update(
                {"plan_complete": True, "driver_id": None, "driver_heartbeat_at": None}
            )
            db.commit()

    monkeypatch.setattr(runner_mod.time, "sleep", sleeper)

    runner.resume_execution("e")

    assert sorted(runner.dispatcher.sent) == ["j0", "j1"]
    assert _state(db).state == "completed"


def test_a_just_created_execution_is_not_mistaken_for_an_orphan(db, monkeypatch):
    """Created 1s ago and not yet owned (the planner has not claimed its lease): wait
    until it is older than a lease before calling it abandoned."""
    monkeypatch.setattr(runner_mod, "DRIVER_LEASE_SECONDS", 2.0)
    # Taken before the row exists: setup time (slow under coverage) must not count
    # against the wait, which is measured from created_at, not from the call.
    before_insert = time.time()
    _execution(db, plan_complete=False, state="pending", age_s=1)

    _runner(db).resume_execution("e")

    assert time.time() - before_insert >= 0.95  # waited out the remainder of the lease
    assert _state(db).state == "failed"


def test_recovery_finds_pending_executions_with_unfinished_plans(db):
    for i, (state, flag) in enumerate(
        [("pending", False), ("pending", True), ("pending", None), ("running", None),
         ("running", False), ("completed", False)]
    ):
        db.add(Execution(execution_id=f"e{i}", pipeline_name="p", state=state, plan_complete=flag))
    db.commit()

    found = {e.execution_id for e in ExecutionManager(db).get_interrupted_executions()}

    assert found == {"e0", "e3", "e4"}  # not pending+True/None, not completed


def test_new_executions_start_unplanned(db):
    execution = ExecutionManager(db).create_execution("e", "p", {})
    assert execution.plan_complete is False
    ExecutionManager(db).mark_plan_complete("e")
    assert _state(db).plan_complete is True


# ----- the planning lease ---------------------------------------------------------


def test_plan_lease_claims_heartbeats_and_releases(engine, db, monkeypatch):
    monkeypatch.setattr(runner_mod, "PLAN_HEARTBEAT_SECONDS", 0.05)
    _execution(db, plan_complete=False)
    lease = _PlanLease(engine, "e")

    lease.start()
    execution = _state(db)
    assert execution.driver_id and execution.driver_id.startswith("plan:")
    first_beat = execution.driver_heartbeat_at

    time.sleep(0.4)
    assert _state(db).driver_heartbeat_at > first_beat  # the thread keeps it fresh

    lease.stop()
    lease.stop()  # idempotent
    execution = _state(db)
    assert execution.driver_id is None and execution.driver_heartbeat_at is None


def test_plan_lease_survives_an_unreachable_database():
    """Planning must go ahead unprotected rather than not at all."""
    lease = _PlanLease(object(), "e")  # not a usable bind
    lease.start()
    lease.stop()


def test_a_finished_plan_flips_the_flag_and_releases_the_lease(db, engine, monkeypatch):
    """run_pipeline_jobs end to end on a real database (dispatch stubbed out)."""
    from reflowfy.core.abstract_pipeline import AbstractPipeline
    from reflowfy.core.registry import pipeline_registry
    from reflowfy.core.runtime_params import RuntimeParams
    from reflowfy.sources.static import StaticSource

    class _Tiny(AbstractPipeline[RuntimeParams]):
        name = "planning_crash_tiny"

        def define_source(self, runtime_params: Any):
            return StaticSource([{"id": 1}, {"id": 2}])

        def define_transformations(self, records: Any, runtime_params: Any):
            return []

        def define_destination(self, records: Any, runtime_params: Any):
            raise AssertionError("not reached")

    monkeypatch.undo()  # use the real registry for this one
    assert pipeline_registry.get("planning_crash_tiny") is not None
    db.add(Execution(execution_id="e", pipeline_name="planning_crash_tiny", state="pending",
                     plan_complete=False))
    db.commit()
    runner = _runner(db)
    monkeypatch.setattr(PipelineRunner, "_drive_execution", lambda *a, **k: (0, 0, 0))

    runner.run_pipeline_jobs("e", "planning_crash_tiny", {})

    execution = _state(db)
    assert execution.plan_complete is True
    assert execution.total_jobs >= 1
    assert execution.driver_id is None  # planning lease released before dispatch
