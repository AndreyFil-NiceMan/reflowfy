"""Races on the execution and job state machines (docs/state-machines.md, F1–F3).

An execution moves by compare-and-set (``ExecutionState.FROM``), so of two concurrent
transitions only a legal one lands. A worker's writes to a job are fenced by the token
its claim wrote (``jobs.claimed_by``), so a worker whose lease expired cannot overwrite
the job once it was cancelled, failed or claimed by another delivery.
"""

import asyncio
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.job_retry import JobRetryConfig
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.execution.job_runner import job
from reflowfy.observability.metrics import JobOutcome
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner
from reflowfy.store.models import Base, Execution, Job
from reflowfy.store.states import ExecutionState, JobState
from reflowfy.worker.executor import JobStats, WorkerExecutor

# The worker tests below claim jobs in a real (SQLite) database.
pytestmark = pytest.mark.real_claim


@pytest.fixture
def db_path(tmp_path):
    return f"{tmp_path}/t.db"


_workers: list = []


@pytest.fixture
def db(db_path) -> Session:
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()
    while _workers:
        asyncio.run(_workers.pop().close())


def _execution(db: Session, state: str = "running", **kw: Any) -> None:
    kw.setdefault("pipeline_name", "p")
    db.add(Execution(execution_id="e", state=state, **kw))
    db.commit()


def _job(db: Session, state: str, job_id: str = "j", **kw: Any) -> None:
    db.add(Job(job_id=job_id, execution_id="e", job_payload={}, state=state, **kw))
    db.commit()


def _state(db: Session) -> str:
    db.expire_all()
    return db.query(Execution).one().state


def _job_row(db: Session, job_id: str = "j") -> Job:
    db.expire_all()
    return db.query(Job).filter(Job.job_id == job_id).one()


# ----- the transition table -------------------------------------------------------


def test_every_state_has_its_predecessors_declared():
    assert set(ExecutionState.FROM) == set(ExecutionState.ALL)
    for target, sources in ExecutionState.FROM.items():
        # Nothing leaves a terminal state through update_execution_state.
        assert not set(sources) & set(ExecutionState.TERMINAL), target


@pytest.mark.parametrize("terminal", ExecutionState.TERMINAL)
@pytest.mark.parametrize("target", ExecutionState.ALL)
def test_a_settled_execution_is_never_moved(db, terminal, target):
    _execution(db, terminal)
    assert ExecutionManager(db).update_execution_state("e", target) is None
    assert _state(db) == terminal


def test_pausing_a_finished_execution_is_refused(db):
    """A pause racing the end of a run used to turn completed back into paused."""
    _execution(db, "completed")
    assert ExecutionManager(db).pause_execution("e") is None
    assert _state(db) == "completed"


def test_an_unknown_state_is_an_error(db):
    _execution(db)
    with pytest.raises(ValueError, match="Unknown execution state"):
        ExecutionManager(db).update_execution_state("e", "done")


def test_a_refused_transition_tells_no_listener(db, monkeypatch):
    _execution(db, "cancelled")
    told = []
    monkeypatch.setattr(ExecutionManager, "notify_terminal", lambda *a: told.append(a))
    assert ExecutionManager(db).update_execution_state("e", "failed") is None
    assert told == []


# ----- planning start ---------------------------------------------------------------


class _OneJob(AbstractPipeline[RuntimeParams]):
    name = "state_transitions_one_job"

    def define_jobs(self, runtime_params: Any):
        yield job([1], job_index=0)

    def define_transformations(self, records: Any, runtime_params: Any):
        return []

    def define_destination(self, records: Any, runtime_params: Any):
        raise AssertionError("not sent in these tests")


def _plan(db: Session, monkeypatch) -> list:
    driven: list = []
    monkeypatch.setattr(
        PipelineRunner, "_drive_execution", lambda *a, **k: driven.append(1) or (0, 0, 0)
    )
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher=None)  # type: ignore[arg-type]
    runner.run_pipeline_jobs("e", _OneJob.name, {})
    return driven


def test_a_cancel_before_planning_starts_is_kept(db, monkeypatch):
    """It used to be overwritten by the planner's `running`, and the run went ahead."""
    _execution(db, "pending", pipeline_name=_OneJob.name)
    assert ExecutionManager(db).cancel_execution("e") is not None

    driven = _plan(db, monkeypatch)

    assert _state(db) == "cancelled"
    assert db.query(Job).count() == 0  # nothing planned
    assert driven == []


def test_a_pause_before_planning_starts_plans_but_stays_paused(db, monkeypatch):
    _execution(db, "pending", pipeline_name=_OneJob.name)
    assert ExecutionManager(db).pause_execution("e") is not None

    _plan(db, monkeypatch)

    assert _state(db) == "paused"  # /resume starts the dispatch
    assert db.query(Execution).one().plan_complete is True
    assert [j.state for j in db.query(Job)] == ["pending"]


# ----- the drive loop's end ---------------------------------------------------------


def _runner(db: Session) -> PipelineRunner:
    return PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher=None)  # type: ignore[arg-type]


def test_finalize_does_not_overwrite_a_cancel(db):
    """The loop saw `running`, then a /cancel landed before it settled the execution."""
    _execution(db, "cancelled")
    _job(db, "completed")

    assert _runner(db)._finalize_execution_state("e", 1, (1, 1, 0)) == (1, 1, 0)
    assert _state(db) == "cancelled"


def test_finalize_keeps_driving_when_jobs_were_requeued_meanwhile(db):
    """/retry-failed requeued a job after the loop counted none unfinished."""
    _execution(db)
    _job(db, "pending")

    assert _runner(db)._finalize_execution_state("e", 1, (1, 0, 1)) is None
    assert _state(db) == "running"


# ----- /retry-failed and hooks ------------------------------------------------------


@pytest.mark.parametrize("state", ["pending", "running", "paused"])
def test_reopen_leaves_an_open_execution_alone(db, state):
    _execution(db, state)
    assert ExecutionManager(db).reopen_execution("e") is None
    assert _state(db) == state


def test_reopen_moves_a_settled_execution_back_to_running(db):
    _execution(db, "completed")
    assert ExecutionManager(db).reopen_execution("e") is not None
    assert _state(db) == "running"


def test_hooks_are_not_claimed_for_an_open_execution(db):
    """A reopen between the hook thread's state check and its claim must not use up
    the claim the retried run's real end needs."""
    _execution(db, "running")
    em = ExecutionManager(db)
    assert em.claim_hooks("e") is False
    em.update_execution_state("e", "completed")
    assert em.claim_hooks("e") is True
    assert em.claim_hooks("e") is False


# ----- worker: the job's owner ------------------------------------------------------


def _worker(db_path: str) -> WorkerExecutor:
    worker = WorkerExecutor(database_url=f"sqlite+aiosqlite:///{db_path}")
    _workers.append(worker)
    return worker


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _claimed(worker: WorkerExecutor) -> JobStats:
    stats = JobStats()
    assert _run(worker._claim_job("j", stats)) is None
    assert stats.claim_token is not None
    return stats


def test_the_claim_writes_a_token(db, db_path):
    _execution(db)
    _job(db, "dispatched")
    stats = _claimed(_worker(db_path))
    row = _job_row(db)
    assert (row.state, row.claimed_by, row.attempts) == ("running", stats.claim_token, 1)


@pytest.mark.parametrize("taken_to", ["cancelled", "failed", "pending"])
def test_a_worker_that_lost_its_job_does_not_write_it(db, db_path, taken_to):
    """Lease expired: the sweep requeued it (pending), failed it out of attempts, or a
    cancel took it after the requeue. The late worker's outcome used to overwrite."""
    _execution(db)
    _job(db, "pending")
    worker = _worker(db_path)
    stats = _claimed(worker)
    db.query(Job).update({"state": taken_to})
    db.commit()

    stats.success = True
    assert _run(worker._update_job_in_db("e", "j", stats)) is False

    assert _job_row(db).state == taken_to
    assert stats.claim_lost and stats.outcome == JobOutcome.SUPERSEDED


def test_two_deliveries_only_the_owner_writes(db, db_path):
    """A stalled first worker and the one that claimed the requeued job both finish."""
    _execution(db)
    _job(db, "pending")
    first, second = _worker(db_path), _worker(db_path)
    stale = _claimed(first)
    db.query(Job).update({"state": "pending"})  # its lease expired
    db.commit()
    owner = _claimed(second)

    stale.success = False
    stale.error = "late"
    owner.success = True
    assert _run(first._update_job_in_db("e", "j", stale)) is False
    assert _run(second._update_job_in_db("e", "j", owner)) is True

    row = _job_row(db)
    assert (row.state, row.error_message) == ("completed", None)
    assert stale.outcome == JobOutcome.SUPERSEDED and owner.outcome == JobOutcome.COMPLETED


def test_a_stale_heartbeat_does_not_keep_the_new_owners_lease_alive(db, db_path):
    _execution(db)
    _job(db, "pending")
    stale = _claimed(_worker(db_path))
    db.query(Job).update({"claimed_by": "someone-else"})
    db.commit()
    before = _job_row(db).updated_at

    from sqlalchemy import update

    async def beat() -> int:
        worker = _worker(db_path)
        async with worker._async_session() as session:
            result = await session.execute(
                update(Job)
                .where(*WorkerExecutor._owned("j", stale.claim_token))
                .values(updated_at=before.replace(year=before.year + 1))
            )
            await session.commit()
            return int(getattr(result, "rowcount", 0))

    assert _run(beat()) == 0  # the heartbeat's WHERE matches nothing
    assert _job_row(db).updated_at == before


def test_ownership_is_checked_before_the_send(db, db_path):
    _execution(db)
    _job(db, "pending")
    worker = _worker(db_path)
    stats = _claimed(worker)
    assert _run(worker._still_owns("j", stats)) is True

    db.query(Job).update({"state": JobState.CANCELLED})
    db.commit()
    assert _run(worker._still_owns("j", stats)) is False
    assert stats.claim_lost


def test_a_failed_retry_write_of_a_lost_job_writes_nothing(db, db_path):
    """job_retry: the retry write is refused because the job is no longer ours; the
    final `failed` write must not then land either."""
    _execution(db)
    _job(db, "pending")
    worker = _worker(db_path)
    stats = _claimed(worker)
    db.query(Job).update({"state": JobState.CANCELLED})
    db.commit()

    payload = {"metadata": {"job_retry": JobRetryConfig(max_retries=2, backoff_seconds=1).to_dict()}}
    stats.success = False
    stats.error = "boom"
    _run(worker._finish_failed_job(payload, "e", "j", stats, ConnectionError("boom")))

    assert _job_row(db).state == "cancelled"
    assert stats.outcome == JobOutcome.SUPERSEDED


def test_an_untracked_job_still_runs_unfenced(db, db_path):
    """No row: no token, nothing to fence (the at-least-once path from before claims)."""
    _execution(db)
    stats = JobStats()
    worker = _worker(db_path)
    assert _run(worker._claim_job("missing", stats)) is None
    assert stats.claim_token is None
    assert _run(worker._still_owns("missing", stats)) is True


def test_the_ownership_check_fails_open_on_a_database_error(db, db_path, monkeypatch):
    _execution(db)
    _job(db, "pending")
    worker = _worker(db_path)
    stats = _claimed(worker)

    def broken() -> Any:
        raise ConnectionError("database gone")

    monkeypatch.setattr(worker, "_async_session", broken)
    assert _run(worker._still_owns("j", stats)) is True  # goes ahead, as the claim does
    assert not stats.claim_lost


# ----- the routes answer 409 when the compare-and-set loses -------------------------


@pytest.mark.parametrize("target", ["cancelled", "failed"])
def test_patch_state_answers_409_when_overtaken(db, monkeypatch, target):
    """The route checked "not terminal", then the drive loop settled it first."""
    from fastapi import HTTPException

    import reflowfy.reflow_manager.app as app_module
    from reflowfy.reflow_manager.manager import ReflowManager
    from reflowfy.reflow_manager.schemas import UpdateExecutionStateRequest

    _execution(db)
    manager = ReflowManager(db, execution_mode="local")
    real_update = ExecutionManager.update_execution_state

    def settle_then_update(self: ExecutionManager, *args: Any, **kwargs: Any) -> Any:
        db.query(Execution).update({"state": "completed"})  # the loop got there first
        db.commit()
        return real_update(self, *args, **kwargs)

    monkeypatch.setattr(ExecutionManager, "update_execution_state", settle_then_update)
    request = UpdateExecutionStateRequest(state=target, error_message="stuck")
    with pytest.raises(HTTPException) as caught:
        asyncio.run(app_module.update_execution_state("e", request, manager))
    assert caught.value.status_code == 409
    assert _state(db) == "completed"


def test_once_lost_nothing_more_is_written(db, db_path):
    _execution(db)
    _job(db, "pending")
    worker = _worker(db_path)
    stats = _claimed(worker)
    stats.claim_lost = True
    stats.success = True
    assert _run(worker._update_job_in_db("e", "j", stats)) is False
    assert _job_row(db).state == "running"  # still the claim's; nothing written
