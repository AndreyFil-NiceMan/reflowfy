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


def test_hash_changes_with_per_job_params():
    """Same records, different job — e.g. the same rows for another tenant.

    Without this the second job is claimed as a duplicate of the first and its
    output is silently never written.
    """
    a = compute_content_hash("p", [], [{"id": 1}], {"tenant": "a"})
    b = compute_content_hash("p", [], [{"id": 1}], {"tenant": "b"})
    assert a != b


def test_hash_is_unchanged_when_there_are_no_per_job_params():
    """Jobs that carry no params must keep the hash they already have, so
    enable_duplicate_jobs=False stays idempotent across this upgrade."""
    baseline = compute_content_hash("p", ["t"], [{"id": 1}])

    assert compute_content_hash("p", ["t"], [{"id": 1}], None) == baseline
    assert compute_content_hash("p", ["t"], [{"id": 1}], {}) == baseline


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


def test_destination_changes_hash():
    """Same records to a different topic is new work, not a duplicate."""
    from reflowfy.destinations.kafka import kafka_destination
    from reflowfy.execution.content_dedup import destination_identity

    a = kafka_destination(bootstrap_servers="k:9092", topic="a")
    b = kafka_destination(bootstrap_servers="k:9092", topic="b")
    ha = compute_content_hash("p", [], [{"id": 1}], None, destination_identity(a))
    hb = compute_content_hash("p", [], [{"id": 1}], None, destination_identity(b))
    assert ha != hb
    same = kafka_destination(bootstrap_servers="k:9092", topic="a")
    assert compute_content_hash("p", [], [{"id": 1}], None, destination_identity(same)) == ha


def test_destination_identity_ignores_credentials():
    """Rotating a password/token must not make written content look new."""
    from reflowfy.destinations.api import api_destination
    from reflowfy.execution.content_dedup import destination_identity

    a = api_destination(url="http://x/y", auth_type="bearer", auth_token="old",
                        headers={"Authorization": "Bearer old", "X-Tenant": "t1"})
    b = api_destination(url="http://x/y", auth_type="bearer", auth_token="new",
                        headers={"Authorization": "Bearer new", "X-Tenant": "t1"})
    assert destination_identity(a) == destination_identity(b)
    ident = destination_identity(a)
    assert "auth_token" not in ident["config"]
    assert ident["config"]["headers"] == {"X-Tenant": "t1"}


async def test_same_job_reclaims_its_own_hash(session_factory):
    """A job re-run after its worker died must not be a duplicate of itself."""
    assert await claim_content_hash(session_factory, "h4", "pipe", "job1", "ex1") is True
    # claim never released (SIGKILL) — the lease re-dispatch runs job1 again
    assert await claim_content_hash(session_factory, "h4", "pipe", "job1", "ex1") is True
    assert await claim_content_hash(session_factory, "h4", "pipe", "job2", "ex1") is False


async def test_legacy_claim_blocks_other_jobs_during_transition(session_factory, monkeypatch):
    """A claim recorded before destinations joined the hash still counts, so an
    upgrade does not rewrite everything the previous version already wrote."""
    legacy = "legacy-h"
    assert await claim_content_hash(session_factory, legacy, "pipe", "old-job", "ex0") is True

    # same content, new-style hash (destination included): blocked by the legacy claim
    assert (
        await claim_content_hash(
            session_factory, "new-h", "pipe", "job1", "ex1", legacy_hash=legacy
        )
        is False
    )
    # nothing was claimed under the new hash
    assert await claim_content_hash(session_factory, "new-h", "pipe", "job2", "ex1") is True


async def test_legacy_claim_by_same_job_does_not_block(session_factory):
    legacy = "legacy-h2"
    assert await claim_content_hash(session_factory, legacy, "pipe", "job1", "ex") is True
    assert (
        await claim_content_hash(session_factory, "new-h2", "pipe", "job1", "ex", legacy_hash=legacy)
        is True
    )


async def test_legacy_claims_can_be_switched_off(session_factory, monkeypatch):
    monkeypatch.setenv("DEDUP_HONOR_LEGACY_HASH", "false")
    legacy = "legacy-h3"
    assert await claim_content_hash(session_factory, legacy, "pipe", "old-job", "ex0") is True
    assert (
        await claim_content_hash(
            session_factory, "new-h3", "pipe", "job1", "ex1", legacy_hash=legacy
        )
        is True
    )


def _api(body, identity=None, url="http://s/in", params=None):
    from reflowfy.destinations.api import api_destination

    return api_destination(url=url, body=body, params=params, dedup_identity=identity)


def test_a_changing_request_body_is_new_work_by_default():
    """Safe side: a timestamp in the body makes two otherwise identical writes look
    different — a duplicate write, never a lost one."""
    from reflowfy.execution.content_dedup import destination_identity

    a = destination_identity(_api({"ts": 1, "records": [1]}))
    b = destination_identity(_api({"ts": 2, "records": [1]}))
    assert a != b


def test_a_declared_dedup_identity_replaces_the_whole_config():
    from reflowfy.execution.content_dedup import destination_identity

    a = destination_identity(_api({"ts": 1}, identity={"url": "http://s/in", "tenant": "t1"}))
    b = destination_identity(_api({"ts": 2}, identity={"url": "http://s/in", "tenant": "t1"}))
    other = destination_identity(_api({"ts": 1}, identity={"url": "http://s/in", "tenant": "t2"}))
    assert a == b  # the volatile body no longer matters
    assert a != other  # but a different declared target is still different work
    assert "ts" not in str(a)


def test_a_declared_identity_is_still_scrubbed_of_credentials():
    from reflowfy.execution.content_dedup import destination_identity

    one = destination_identity(_api({}, identity={"url": "u", "auth_token": "old"}))
    two = destination_identity(_api({}, identity={"url": "u", "auth_token": "new"}))
    assert one == two
