from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from reflowfy.reflow_manager.models import Base, ProcessedContent
from reflowfy.reflow_manager.content_dedup_scheduler import purge_expired_content


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_purge_removes_only_expired(session):
    now = datetime(2026, 6, 30, 12, 0, 0)
    session.add(ProcessedContent(
        content_hash="old", pipeline_name="p", job_id="j1", execution_id="e1",
        created_at=now - timedelta(hours=25),
    ))
    session.add(ProcessedContent(
        content_hash="fresh", pipeline_name="p", job_id="j2", execution_id="e2",
        created_at=now - timedelta(hours=1),
    ))
    session.commit()

    deleted = purge_expired_content(session, retention_hours=24, now=now)
    session.commit()

    remaining = {r.content_hash for r in session.query(ProcessedContent).all()}
    assert deleted == 1
    assert remaining == {"fresh"}
