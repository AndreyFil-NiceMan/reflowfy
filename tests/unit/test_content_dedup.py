from reflowfy.reflow_manager.models import ProcessedContent


def test_processed_content_table_shape():
    cols = {c.name for c in ProcessedContent.__table__.columns}
    assert {"content_hash", "pipeline_name", "job_id", "execution_id", "created_at"} <= cols
    assert ProcessedContent.__table__.primary_key.columns.keys() == ["content_hash"]


from reflowfy.execution.content_dedup import compute_content_hash


def test_hash_is_stable_for_same_content():
    a = compute_content_hash("p", ["t1", "t2"], [{"id": 1, "v": "x"}])
    b = compute_content_hash("p", ["t2", "t1"], [{"id": 1, "v": "x"}])  # order-insensitive transforms
    assert a == b
    assert len(a) == 64


def test_hash_changes_with_records():
    a = compute_content_hash("p", [], [{"id": 1, "v": "x"}])
    b = compute_content_hash("p", [], [{"id": 1, "v": "y"}])
    assert a != b


def test_hash_changes_with_pipeline_name():
    a = compute_content_hash("p1", [], [{"id": 1}])
    b = compute_content_hash("p2", [], [{"id": 1}])
    assert a != b


import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from reflowfy.reflow_manager.models import Base
from reflowfy.execution.content_dedup import claim_content_hash, release_content_hash


@pytest.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def test_first_claim_wins_second_loses(session_factory):
    won1 = await claim_content_hash(session_factory, "h1", "pipe", "job1", "ex1")
    won2 = await claim_content_hash(session_factory, "h1", "pipe", "job2", "ex2")
    assert won1 is True
    assert won2 is False


async def test_release_allows_reclaim(session_factory):
    assert await claim_content_hash(session_factory, "h2", "pipe", "jobA", "exA") is True
    await release_content_hash(session_factory, "h2", "jobA")
    assert await claim_content_hash(session_factory, "h2", "pipe", "jobB", "exB") is True


async def test_release_only_removes_own_claim(session_factory):
    assert await claim_content_hash(session_factory, "h3", "pipe", "owner", "exO") is True
    await release_content_hash(session_factory, "h3", "not-owner")  # no-op
    assert await claim_content_hash(session_factory, "h3", "pipe", "x", "exX") is False
