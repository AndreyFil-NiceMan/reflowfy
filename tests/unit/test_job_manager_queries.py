"""JobManager queries the routes and the drive loop rely on, on SQLite.

Leases, cancel and requeue are covered by test_dispatch_recovery_fixes.py and
test_cancel_execution.py; this file covers listing, single-job updates, reading what
other sessions (workers) wrote, and the Postgres-only total_batches back-fill.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.models import Base, Execution, Job

T0 = datetime(2026, 10, 20, 12, 0, 0)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path}/jobs.db")
    Base.metadata.create_all(eng)
    with sessionmaker(bind=eng)() as s:
        s.add(Execution(execution_id="e", pipeline_name="p", state="running"))
        s.add(Execution(execution_id="other", pipeline_name="p", state="running"))
        s.commit()
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _job(
    db: Session, job_id: str, state: str = "pending", execution_id: str = "e", age_s: int = 0
) -> None:
    db.add(
        Job(
            job_id=job_id,
            execution_id=execution_id,
            job_payload={"job_id": job_id},
            state=state,
            batch_number=1,
            created_at=T0 - timedelta(seconds=age_s),
        )
    )
    db.commit()


def test_get_jobs_lists_an_executions_jobs_oldest_first(db):
    _job(db, "b", "completed", age_s=10)
    _job(db, "a", "failed", age_s=20)
    _job(db, "c", "completed", age_s=0)
    _job(db, "x", "completed", execution_id="other")
    jm = JobManager(db)

    assert [j.job_id for j in jm.get_jobs("e")] == ["a", "b", "c"]
    assert [j.job_id for j in jm.get_jobs("e", state="completed")] == ["b", "c"]
    assert jm.get_jobs("e", state="running") == []
    assert jm.get_jobs("missing") == []


def test_update_job_state_records_the_workers_result(db):
    _job(db, "j")

    job = JobManager(db).update_job_state(
        "j", "completed", processed_records=120, stats={"records_in": 120, "skip_reason": None}
    )

    assert job is not None
    assert (job.state, job.processed_records) == ("completed", 120)
    assert job.stats == {"records_in": 120, "skip_reason": None}
    assert job.completed_at is not None
    assert job.error_message is None


def test_update_job_state_to_failed_keeps_the_error(db):
    _job(db, "j")

    job = JobManager(db).update_job_state("j", "failed", error_message="destination 503")

    assert (job.state, job.error_message) == ("failed", "destination 503")
    assert job.completed_at is not None
    assert job.processed_records == 0  # not touched when not given


def test_update_job_state_to_a_non_terminal_state_sets_no_completion_time(db):
    _job(db, "j")
    job = JobManager(db).update_job_state("j", "running")
    assert (job.state, job.completed_at) == ("running", None)


def test_update_job_state_of_an_unknown_job_returns_none(db):
    assert JobManager(db).update_job_state("ghost", "completed") is None
    assert db.query(Job).count() == 0


def test_get_job_states_sees_what_workers_wrote_from_their_own_sessions(engine, db):
    _job(db, "a")
    _job(db, "b")
    jm = JobManager(db)
    assert jm.get_job_states(["a", "b"]) == {"a": "pending", "b": "pending"}
    jm.get_job("a")  # now cached in the manager's session

    with sessionmaker(bind=engine)() as worker:
        worker.query(Job).filter(Job.job_id == "a").update({"state": "completed"})
        worker.commit()

    assert jm.get_job_states(["a", "b", "ghost"]) == {"a": "completed", "b": "pending"}
    assert jm.get_job_states([]) == {}


def test_get_job_counts_includes_every_state_and_the_total(db):
    for i, state in enumerate(
        [
            "pending",
            "dispatched",
            "running",
            "completed",
            "completed",
            "failed",
            "deduplicated",
            "cancelled",
        ]
    ):
        _job(db, f"j{i}", state)
    _job(db, "x", "completed", execution_id="other")

    assert JobManager(db).get_job_counts("e") == {
        "total": 8,
        "pending": 1,
        "dispatched": 1,
        "running": 1,
        "completed": 2,
        "failed": 1,
        "deduplicated": 1,
        "cancelled": 1,
    }


def test_create_jobs_with_nothing_to_insert_does_not_touch_the_database():
    class _NoDb:
        def __getattr__(self, name: str) -> Any:
            raise AssertionError(f"db.{name} used")

    JobManager(_NoDb()).create_jobs([])  # type: ignore[arg-type]


def test_bulk_set_total_batches_patches_the_payload_as_jsonb():
    """Postgres-only SQL (jsonb_set on a json column): checked for the statement it sends."""

    class _Recorder:
        def __init__(self) -> None:
            self.executed: List[Any] = []
            self.commits = 0

        def execute(self, statement: Any, params: Dict[str, Any]) -> None:
            self.executed.append((str(statement), params))

        def commit(self) -> None:
            self.commits += 1

    db = _Recorder()
    JobManager(db).bulk_set_total_batches("e", 7)  # type: ignore[arg-type]

    [(sql, params)] = db.executed
    assert "jsonb_set(job_payload::jsonb, '{metadata,total_batches}'" in sql
    assert "WHERE execution_id = :eid" in sql
    assert params == {"val": "7", "eid": "e"}
    assert db.commits == 1
