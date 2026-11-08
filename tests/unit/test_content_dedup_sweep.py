from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from reflowfy.reflow_manager.models import Base, Execution, Job, ProcessedRecord
from reflowfy.reflow_manager.content_dedup_scheduler import purge_expired_content, purge_expired_executions


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_purge_removes_only_expired(session):
    now = datetime(2026, 6, 30, 12, 0, 0)
    session.add(ProcessedRecord(
        record_hash=b"o" * 32, job_id="j1", created_at=now - timedelta(hours=25),
    ))
    session.add(ProcessedRecord(
        record_hash=b"f" * 32, job_id="j2", created_at=now - timedelta(hours=1),
    ))
    session.commit()

    deleted = purge_expired_content(session, retention_hours=24, now=now)
    session.commit()

    remaining = {r.record_hash for r in session.query(ProcessedRecord).all()}
    assert deleted == 1
    assert remaining == {b"f" * 32}


def test_purge_executions_removes_old_finished_with_jobs(session):
    now = datetime(2026, 6, 30, 12, 0, 0)
    for eid, done in [("old", now - timedelta(hours=8)), ("new", now - timedelta(hours=1)), ("running", None)]:
        session.add(Execution(execution_id=eid, pipeline_name="p", state="completed",
                              created_at=now - timedelta(days=30), completed_at=done))
        session.add(Job(job_id=f"j-{eid}", execution_id=eid, job_payload={}, state="completed"))
    session.commit()

    assert purge_expired_executions(session, retention_hours=0, now=now) == 0
    assert purge_expired_executions(session, retention_hours=7, now=now) == 1
    session.commit()

    assert {e.execution_id for e in session.query(Execution)} == {"new", "running"}
    assert {j.job_id for j in session.query(Job)} == {"j-new", "j-running"}


def test_purge_deletes_expired_record_claims_in_chunks(session):
    """4M-record runs leave 4M rows: the purge commits every chunk instead of
    deleting them in one transaction, and keeps going until none are left."""
    now = datetime(2026, 6, 30, 12, 0, 0)
    for i in range(23):
        session.add(ProcessedRecord(record_hash=bytes([i]) * 32, job_id="j",
                                    created_at=now - timedelta(hours=30)))
    session.add(ProcessedRecord(record_hash=b"\xff" * 32, job_id="j",
                                created_at=now - timedelta(hours=1)))
    session.commit()

    assert purge_expired_content(session, retention_hours=24, now=now, chunk_size=5) == 23
    assert [r.record_hash for r in session.query(ProcessedRecord)] == [b"\xff" * 32]
