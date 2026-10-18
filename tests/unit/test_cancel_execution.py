"""Cancelling an execution, the restricted state override, and a planner that stops on cancel.

Before this there was no way to stop a run: `PATCH /executions/{id}/state` set any string and
any transition (`running -> completed` while a loop was dispatching, `completed -> running`
with nothing driving it, a state that does not exist), and a plan that took minutes could not
be abandoned. Now `POST /executions/{id}/cancel` stops dispatching and planning and cancels
every job no worker has started; the PATCH accepts only `failed` and `cancelled`.
"""

import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
import reflowfy.reflow_manager.pipeline_runner as runner_mod
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.execution.job_runner import job
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner
from reflowfy.worker.executor import WorkerExecutor


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path}/c.db", connect_args={"timeout": 30})
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _execution(db: Session, state: str = "running", execution_id: str = "e") -> None:
    db.add(Execution(execution_id=execution_id, pipeline_name="p", state=state, plan_complete=True))
    db.commit()


def _jobs(db: Session, **states: int) -> None:
    n = 0
    for state, count in states.items():
        for _ in range(count):
            db.add(Job(job_id=f"j{n}", execution_id="e", job_payload={"job_id": f"j{n}"},
                       batch_number=1, state=state))
            n += 1
    db.commit()


def _by_state(db: Session) -> dict:
    db.expire_all()
    counts: dict = {}
    for j in db.query(Job).all():
        counts[j.state] = counts.get(j.state, 0) + 1
    return counts


# ----- cancelling -----------------------------------------------------------------


def test_cancel_stops_what_has_not_started_and_leaves_the_rest(db):
    _execution(db)
    _jobs(db, pending=3, dispatched=2, running=1, completed=4, failed=1)
    manager = ReflowManager(db, execution_mode="local")

    result = manager.cancel_execution("e")

    assert result is not None and result.cancelled_jobs == 5  # pending + dispatched
    assert _by_state(db) == {"cancelled": 5, "running": 1, "completed": 4, "failed": 1}
    db.expire_all()
    execution = db.query(Execution).one()
    assert execution.state == "cancelled" and execution.completed_at is not None


@pytest.mark.parametrize("state", ["completed", "failed", "cancelled"])
def test_a_finished_execution_cannot_be_cancelled(db, state):
    _execution(db, state)
    assert ReflowManager(db, execution_mode="local").cancel_execution("e") is None
    db.expire_all()
    assert db.query(Execution).one().state == state


def test_cancelling_a_paused_or_pending_execution_works(db):
    for i, state in enumerate(["paused", "pending"]):
        _execution(db, state, f"x{i}")
        assert ReflowManager(db, execution_mode="local").cancel_execution(f"x{i}") is not None


def test_the_job_counts_report_cancelled(db):
    _execution(db)
    _jobs(db, cancelled=3, completed=1)
    assert JobManager(db).get_job_counts("e")["cancelled"] == 3


class _Dispatcher:
    sent: List[str]

    def __init__(self) -> None:
        self.sent = []

    async def dispatch_jobs_batch(self, jobs, pipeline_name, rate_limit=None, on_sent=None,
                                  should_stop=None, on_unsendable=None):
        for payload in jobs:
            self.sent.append(payload["job_id"])
            on_sent(payload)
        return len(jobs)

    async def close(self):
        pass


def test_a_cancelled_execution_is_not_dispatched_and_stays_cancelled(db, monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: SimpleNamespace(rate_limit=None))
    monkeypatch.setattr(runner_mod, "CHECKPOINT_POLL_INTERVAL", 0.01)
    _execution(db)
    _jobs(db, pending=4)
    ReflowManager(db, execution_mode="local").cancel_execution("e")
    dispatcher = _Dispatcher()

    PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher)._drive_execution("e", "p", None)

    assert dispatcher.sent == []
    db.expire_all()
    assert db.query(Execution).one().state == "cancelled"  # not overwritten with completed/failed


# ----- the worker -----------------------------------------------------------------


@pytest.mark.real_claim
async def test_a_worker_cannot_claim_a_cancelled_job(tmp_path):
    from sqlalchemy.ext.asyncio import create_async_engine

    url = f"sqlite+aiosqlite:///{tmp_path}/w.db"
    engine = create_async_engine(url)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(Execution.__table__.insert().values(
            execution_id="e", pipeline_name="p", state="cancelled", created_at=now, updated_at=now))
        await conn.execute(Job.__table__.insert().values(
            job_id="j", execution_id="e", job_payload={}, state="cancelled", attempts=0,
            created_at=now, updated_at=now))
    await engine.dispose()

    worker = WorkerExecutor(database_url=url)
    try:
        assert await worker._claim_job("j") == "cancelled"  # refused: the job is not run
    finally:
        await worker.close()


async def test_a_cancelled_job_message_is_skipped_counted_and_commits(monkeypatch):
    from reflowfy.core.registry import pipeline_registry
    from reflowfy.observability import metrics

    pipe = SimpleNamespace(name="p", define_transformations=lambda r, p: [],
                           define_destination=lambda r, p: (_ for _ in ()).throw(AssertionError("ran")))
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipe)
    worker = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(worker, "_claim_job", AsyncMock(return_value="cancelled"))
    update = AsyncMock()
    monkeypatch.setattr(worker, "_update_job_in_db", update)

    before = metrics.jobs_processed_total.labels(pipeline="p", status="cancelled")._value.get()
    ok = await worker.execute_job({"schema_version": 2, "execution_id": "e", "job_id": "j",
                                   "pipeline_name": "p", "metadata": {}})

    assert ok is True  # commit the offset: the job is finished as far as Kafka is concerned
    update.assert_not_awaited()  # and the job row keeps its `cancelled` state
    assert metrics.jobs_processed_total.labels(pipeline="p", status="cancelled")._value.get() == before + 1


# ----- the planner ----------------------------------------------------------------


class _SlowPlan(AbstractPipeline[RuntimeParams]):
    name = "cancel_during_plan_test"
    bind: Any = None  # the test's engine (not a runtime param: those get serialized)

    def define_jobs(self, runtime_params: Any):
        yield job([1], job_index=0)
        time.sleep(1.2)  # the planner re-checks the execution about once a second
        with sessionmaker(bind=_SlowPlan.bind)() as other:  # someone cancels meanwhile
            ReflowManager(other, execution_mode="local").cancel_execution("e")
        yield job([2], job_index=1)
        yield job([3], job_index=2)

    def define_transformations(self, records: Any, runtime_params: Any):
        return []

    def define_destination(self, records: Any, runtime_params: Any):
        raise AssertionError("not reached")


def test_planning_stops_when_the_execution_is_cancelled(engine, db, monkeypatch):
    db.add(Execution(execution_id="e", pipeline_name=_SlowPlan.name, state="pending", plan_complete=False))
    db.commit()
    driven: List[Any] = []
    monkeypatch.setattr(PipelineRunner, "_drive_execution", lambda *a, **k: driven.append(1) or (0, 0, 0))
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher=None)  # type: ignore[arg-type]

    _SlowPlan.bind = engine
    runner.run_pipeline_jobs("e", _SlowPlan.name, {})

    db.expire_all()
    execution = db.query(Execution).one()
    assert execution.state == "cancelled"
    assert execution.plan_complete is False  # never declared a whole plan
    assert execution.total_jobs == 0
    assert driven == []  # no dispatch phase after an aborted plan
    assert "pending" not in _by_state(db) and "dispatched" not in _by_state(db)


# ----- the API --------------------------------------------------------------------


@pytest.fixture
def client(engine):
    def override():
        session = sessionmaker(bind=engine)()
        try:
            yield ReflowManager(session, execution_mode="local")
        finally:
            session.close()

    app_module.app.dependency_overrides[app_module.get_reflow_manager] = override
    yield TestClient(app_module.app)
    app_module.app.dependency_overrides.clear()


def test_cancel_endpoint(db, client):
    _execution(db)
    _jobs(db, pending=2, running=1)

    response = client.post("/executions/e/cancel")

    assert response.status_code == 200
    body = response.json()
    assert body["cancelled_jobs"] == 2 and body["execution"]["state"] == "cancelled"


def test_cancel_endpoint_errors(db, client):
    _execution(db, "completed")
    assert client.post("/executions/nope/cancel").status_code == 404
    assert client.post("/executions/e/cancel").status_code == 409


@pytest.mark.parametrize("state", ["completed", "running", "paused", "pending"])
def test_patch_refuses_states_that_have_an_owner(db, client, state):
    """`completed` is set by the system, running/paused by /resume and /pause (which also
    restart the loop), a retry by /retry-failed."""
    _execution(db)

    response = client.patch("/executions/e/state", json={"state": state})

    assert response.status_code == 422
    db.expire_all()
    assert db.query(Execution).one().state == "running"  # untouched


def test_patch_refuses_a_state_that_does_not_exist(db, client):
    _execution(db)
    response = client.patch("/executions/e/state", json={"state": "bogus"})
    assert response.status_code == 422 and "Unknown state" in response.text


def test_patch_can_force_fail_a_stuck_run(db, client):
    _execution(db)
    response = client.patch("/executions/e/state", json={"state": "failed", "error_message": "stuck"})
    assert response.status_code == 200
    db.expire_all()
    execution = db.query(Execution).one()
    assert (execution.state, execution.error_message) == ("failed", "stuck")


def test_patch_cancelled_cancels_the_jobs_too(db, client):
    _execution(db)
    _jobs(db, pending=3)
    assert client.patch("/executions/e/state", json={"state": "cancelled"}).status_code == 200
    assert _by_state(db) == {"cancelled": 3}


def test_patch_cannot_reopen_a_finished_run(db, client):
    _execution(db, "completed")
    assert client.patch("/executions/e/state", json={"state": "failed"}).status_code == 409
    assert client.patch("/executions/nope/state", json={"state": "failed"}).status_code == 404
