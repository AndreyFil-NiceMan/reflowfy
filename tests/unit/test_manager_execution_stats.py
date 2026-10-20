"""`ReflowManager.get_execution_stats` / `_get_batch_checkpoint_stats` and the
`ExecutionManager` record operations (counts, pause/resume, dedup count, reopen)."""

from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.reflow_manager.models import Base, Execution, Job


@pytest.fixture
def db(tmp_path) -> Session:
    engine = create_engine(f"sqlite:///{tmp_path}/m.db", connect_args={"timeout": 30})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()


def _execution(db: Session, state: str = "running", eid: str = "e", **cols: Any) -> None:
    db.add(Execution(execution_id=eid, pipeline_name="p", state=state, **cols))
    db.commit()


def _jobs(db: Session, batch: Any, eid: str = "e", **states: int) -> None:
    for state, count in states.items():
        for i in range(count):
            db.add(
                Job(
                    job_id=f"{eid}-b{batch}-{state}-{i}",
                    execution_id=eid,
                    job_payload={},
                    batch_number=batch,
                    state=state,
                )
            )
    db.commit()


def _manager(db: Session) -> ReflowManager:
    return ReflowManager(db, execution_mode="local")


# ----- get_execution_stats / _get_batch_checkpoint_stats -----------------------------


def test_stats_of_an_unknown_execution_is_none(db):
    assert _manager(db).get_execution_stats("nope") is None


def test_stats_with_no_jobs_yet(db):
    _execution(db, "pending", runtime_params={"a": 1})
    stats = _manager(db).get_execution_stats("e")
    assert stats is not None
    assert stats["checkpoints"] == [] and stats["current_checkpoint"] is None
    assert stats["jobs_dispatched"] == stats["jobs_pending"] == stats["jobs_cancelled"] == 0
    assert stats["runtime_params"] == {"a": 1}
    assert stats["completed_at"] is None and stats["created_at"] is not None


def test_batch_states_across_several_batches(db):
    _execution(db)
    _jobs(db, 1, completed=3)  # all done
    _jobs(db, 2, completed=1, failed=1, pending=1)  # any failure marks the batch failed
    _jobs(db, 3, completed=2, running=1)  # some done
    _jobs(db, 4, pending=2, dispatched=1)  # nothing done
    _jobs(db, None, pending=5)  # no batch number: not a checkpoint
    _execution(db, eid="other")
    _jobs(db, 1, eid="other", failed=4)  # another execution's jobs are not counted

    batches = _manager(db)._get_batch_checkpoint_stats("e")

    assert batches == [
        {
            "batch_number": 1,
            "total_jobs": 3,
            "pending": 0,
            "completed": 3,
            "failed": 0,
            "state": "completed",
        },
        {
            "batch_number": 2,
            "total_jobs": 3,
            "pending": 1,
            "completed": 1,
            "failed": 1,
            "state": "failed",
        },
        {
            "batch_number": 3,
            "total_jobs": 3,
            "pending": 0,
            "completed": 2,
            "failed": 0,
            "state": "in_progress",
        },
        {
            "batch_number": 4,
            "total_jobs": 3,
            "pending": 2,
            "completed": 0,
            "failed": 0,
            "state": "pending",
        },
    ]


def test_execution_stats_count_every_job_state(db):
    _execution(db, total_jobs=12, deduplicated_jobs=1, error_message=None)
    _jobs(db, 1, completed=3)
    _jobs(db, 2, pending=2, dispatched=1, running=1, failed=1, deduplicated=1, cancelled=3)

    stats = _manager(db).get_execution_stats("e")

    assert stats is not None
    assert stats["execution_id"] == "e" and stats["pipeline_name"] == "p"
    assert stats["total_jobs"] == 12
    assert stats["jobs_pending"] == 2
    assert stats["jobs_running"] == 1
    assert stats["jobs_completed"] == 3
    assert stats["jobs_failed"] == 1
    assert stats["jobs_cancelled"] == 3
    # everything that left `pending` and was not cancelled has been sent
    assert stats["jobs_dispatched"] == 1 + 1 + 3 + 1 + 1
    assert stats["deduplicated_jobs"] == 1
    assert (
        stats["jobs_dispatched"] + stats["jobs_pending"] + stats["jobs_cancelled"]
        == stats["total_jobs"]
    )
    # batch 1 is done, batch 2 holds the failure: the current checkpoint is the first unfinished
    assert stats["current_checkpoint"] == 2
    assert [c["state"] for c in stats["checkpoints"]] == ["completed", "failed"]


def test_current_checkpoint_when_every_batch_completed(db):
    _execution(db, "completed")
    _jobs(db, 1, completed=2)
    _jobs(db, 2, completed=2)
    _jobs(db, 3, completed=1)
    assert _manager(db).get_execution_stats("e")["current_checkpoint"] == 3  # type: ignore[index]


def test_get_checkpoints_delegates_to_the_job_table(db):
    _execution(db)
    _jobs(db, 1, completed=1, failed=2)
    manager = _manager(db)
    assert len(manager.get_checkpoints("e")) == 3
    assert {j.state for j in manager.get_checkpoints("e", "failed")} == {"failed"}


# ----- ExecutionManager -------------------------------------------------------------


def test_update_job_counts_increments_without_committing(db):
    _execution(db, jobs_dispatched=1, jobs_completed=2, jobs_failed=0)
    em = ExecutionManager(db)

    em.update_job_counts("e", jobs_dispatched=4, jobs_completed=3)
    execution = em.update_job_counts("e", jobs_failed=2)

    assert execution is not None
    assert (execution.jobs_dispatched, execution.jobs_completed, execution.jobs_failed) == (5, 5, 2)
    db.rollback()  # the caller commits; nothing was persisted yet
    db.expire_all()
    persisted = em.get_execution("e")
    assert persisted is not None
    assert (persisted.jobs_dispatched, persisted.jobs_completed, persisted.jobs_failed) == (1, 2, 0)


def test_update_job_counts_via_manager_and_unknown(db):
    _execution(db, jobs_dispatched=0, jobs_completed=0, jobs_failed=0)
    manager = _manager(db)
    assert manager.update_job_counts("nope", jobs_completed=1) is None
    execution = manager.update_job_counts("e", jobs_dispatched=1, jobs_completed=1, jobs_failed=1)
    assert execution is not None
    db.commit()
    assert (execution.jobs_dispatched, execution.jobs_completed, execution.jobs_failed) == (1, 1, 1)


@pytest.mark.parametrize(
    "state, paused",
    [
        ("pending", True),
        ("running", True),
        ("paused", False),
        ("completed", False),
        ("failed", False),
        ("cancelled", False),
    ],
)
def test_pause_transitions(db, state, paused):
    _execution(db, state)
    result = ExecutionManager(db).pause_execution("e")
    assert (result is not None) is paused
    db.expire_all()
    execution = db.query(Execution).one()
    assert execution.state == ("paused" if paused else state)
    assert execution.completed_at is None  # pausing never looks like finishing


@pytest.mark.parametrize(
    "state, resumed",
    [
        ("paused", True),
        ("pending", False),
        ("running", False),
        ("completed", False),
        ("failed", False),
        ("cancelled", False),
    ],
)
def test_resume_transitions(db, state, resumed):
    _execution(db, state)
    result = ExecutionManager(db).resume_execution("e")
    assert (result is not None) is resumed
    db.expire_all()
    assert db.query(Execution).one().state == ("running" if resumed else state)


def test_pause_and_resume_unknown(db):
    em = ExecutionManager(db)
    assert em.pause_execution("nope") is None
    assert em.resume_execution("nope") is None


def test_update_deduplicated_count_sets_not_adds(db):
    _execution(db, deduplicated_jobs=2)
    em = ExecutionManager(db)
    em.update_deduplicated_count("e", 7)
    em.update_deduplicated_count("nope", 3)  # unknown: ignored
    db.expire_all()
    assert db.query(Execution).one().deduplicated_jobs == 7


def test_update_execution_state_terminal_sets_completed_at(db):
    _execution(db)
    em = ExecutionManager(db)
    assert em.update_execution_state("nope", "failed") is None

    running = em.update_execution_state("e", "running", error_message=None)
    assert running is not None and running.completed_at is None

    failed = em.update_execution_state("e", "failed", error_message="boom")
    assert failed is not None
    assert failed.completed_at is not None and failed.error_message == "boom"

    # a later update without a message keeps the old one
    again = em.update_execution_state("e", "failed")
    assert again is not None and again.error_message == "boom"


def test_reopen_execution_clears_the_outcome(db):
    _execution(db, "failed", error_message="boom")
    em = ExecutionManager(db)
    em.update_execution_state("e", "failed", error_message="boom")

    reopened = em.reopen_execution("e")

    assert reopened is not None
    assert reopened.state == "running"
    assert reopened.completed_at is None and reopened.error_message is None
    assert em.reopen_execution("nope") is None


def test_create_execution_starts_pending_and_unplanned(db):
    execution = ExecutionManager(db).create_execution("new", "p", None, rate_limit=5.0)
    assert execution.state == "pending" and execution.plan_complete is False
    assert execution.runtime_params == {} and execution.rate_limit == 5.0


def test_get_execution_for_update(db):
    _execution(db)
    execution = ExecutionManager(db).get_execution("e", for_update=True)
    assert execution is not None and execution.execution_id == "e"


def test_heartbeat_driver_only_for_the_owner(db):
    _execution(db)
    em = ExecutionManager(db)
    assert em.claim_driver("e", "a", lease_seconds=60) is True
    assert em.heartbeat_driver("e", "a") is True
    assert em.heartbeat_driver("e", "b") is False  # someone else's lease
    assert em.claim_driver("e", "b", lease_seconds=60) is False  # a's lease is fresh
    em.release_driver("e", "b")  # not the owner: no effect
    db.expire_all()
    assert db.query(Execution).one().driver_id == "a"
    em.release_driver("e", "a")
    db.expire_all()
    assert db.query(Execution).one().driver_id is None
