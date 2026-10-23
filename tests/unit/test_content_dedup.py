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
from reflowfy.execution.content_dedup import (
    claim_record_hashes,
    compute_record_hash,
    held_by_other_job,
    record_identity,
    release_record_hashes,
)
from reflowfy.reflow_manager.models import ProcessedContent, ProcessedRecord


@pytest.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


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


# --- per-record dedup -------------------------------------------------------


def test_processed_records_table_shape():
    cols = {c.name for c in ProcessedRecord.__table__.columns}
    assert cols == {"record_hash", "job_id", "created_at"}
    assert ProcessedRecord.__table__.primary_key.columns.keys() == ["record_hash"]


def _h(record, key=None, params=None, destination=None, job_params=None, pipeline="p"):
    return compute_record_hash(
        pipeline, record_identity(record, key, params or {}), destination, job_params
    )


def test_record_hash_is_a_stable_raw_digest():
    assert _h({"id": 1, "v": "x"}) == _h({"v": "x", "id": 1})
    assert len(_h({"id": 1})) == 32  # bytea, not hex
    assert _h({"id": 1, "v": "x"}) != _h({"id": 1, "v": "y"})
    assert _h({"id": 1}, pipeline="a") != _h({"id": 1}, pipeline="b")


def test_record_hash_changes_with_destination_not_credentials():
    from reflowfy.destinations.api import api_destination
    from reflowfy.execution.content_dedup import destination_identity

    a = destination_identity(api_destination(url="http://x/a", auth_type="bearer", auth_token="1"))
    rotated = destination_identity(
        api_destination(url="http://x/a", auth_type="bearer", auth_token="2")
    )
    other = destination_identity(api_destination(url="http://x/b"))
    assert _h({"id": 1}, destination=a) == _h({"id": 1}, destination=rotated)
    assert _h({"id": 1}, destination=a) != _h({"id": 1}, destination=other)


def test_a_field_key_ignores_the_other_fields():
    a = _h({"order_id": 7, "processed_at": "10:00"}, key="order_id")
    b = _h({"order_id": 7, "processed_at": "10:05"}, key="order_id")
    assert a == b
    assert a != _h({"order_id": 8, "processed_at": "10:00"}, key="order_id")


def test_a_missing_key_field_raises():
    with pytest.raises(KeyError):
        record_identity({"id": 1}, "order_id", {})


def test_a_key_function_gets_the_record_and_runtime_params():
    def key(record, params):
        return (params.get("tenant"), record["order_id"])

    a = _h({"order_id": 7, "x": 1}, key=key, params={"tenant": "a"})
    b = _h({"order_id": 7, "x": 2}, key=key, params={"tenant": "a"})
    other_tenant = _h({"order_id": 7, "x": 1}, key=key, params={"tenant": "b"})
    assert a == b
    assert a != other_tenant


def test_job_params_count_but_slice_params_do_not():
    """The same rows for another tenant (reflowfy.job(rows, tenant=...)) are new work,
    but overlapping slices — the same record under different current_ids — dedup."""
    assert _h({"id": 1}, job_params={"tenant": "a"}) != _h({"id": 1}, job_params={"tenant": "b"})
    assert _h({"id": 1}, job_params={"current_ids": [1, 2], "current_id": 1}) == _h({"id": 1})
    assert _h({"id": 1}, job_params={}) == _h({"id": 1})


async def test_a_second_job_wins_only_the_new_records(session_factory):
    first = [_h({"id": i}) for i in range(1, 101)]
    second = [_h({"id": i}) for i in range(50, 151)]
    assert await claim_record_hashes(session_factory, first, "job-a") == set(first)
    won = await claim_record_hashes(session_factory, second, "job-b")
    assert won == {_h({"id": i}) for i in range(101, 151)}


async def test_a_job_rewins_its_own_claims(session_factory):
    """A job re-run after its worker died must not be a duplicate of itself."""
    hashes = [_h({"id": 1}), _h({"id": 2})]
    assert await claim_record_hashes(session_factory, hashes, "job1") == set(hashes)
    assert await claim_record_hashes(session_factory, hashes, "job1") == set(hashes)
    assert await claim_record_hashes(session_factory, hashes, "job2") == set()


async def test_release_frees_only_own_claims(session_factory):
    mine, theirs = _h({"id": 1}), _h({"id": 2})
    await claim_record_hashes(session_factory, [mine], "job1")
    await claim_record_hashes(session_factory, [theirs], "job2")
    await release_record_hashes(session_factory, [mine, theirs], "job1")
    assert await claim_record_hashes(session_factory, [mine, theirs], "job3") == {mine}


async def test_claims_are_chunked(session_factory, monkeypatch):
    from reflowfy.execution import content_dedup

    monkeypatch.setattr(content_dedup, "CLAIM_CHUNK_SIZE", 7)
    hashes = [_h({"id": i}) for i in range(30)]
    assert await claim_record_hashes(session_factory, hashes + hashes[:3], "j") == set(hashes)
    assert await claim_record_hashes(session_factory, hashes, "other") == set()
    await release_record_hashes(session_factory, hashes, "j")
    assert await claim_record_hashes(session_factory, hashes, "other") == set(hashes)
    assert await claim_record_hashes(session_factory, [], "j") == set()


async def _old_claim(session_factory, content_hash, job_id):
    async with session_factory() as db:
        db.add(ProcessedContent(
            content_hash=content_hash, pipeline_name="p", job_id=job_id, execution_id="e0"
        ))
        await db.commit()


async def test_a_job_hash_from_the_previous_release_blocks_other_jobs(session_factory):
    await _old_claim(session_factory, "old-h", "old-job")
    assert await held_by_other_job(session_factory, ["x", "old-h"], "job1") is True
    assert await held_by_other_job(session_factory, ["old-h"], "old-job") is False
    assert await held_by_other_job(session_factory, ["x"], "job1") is False
    assert await held_by_other_job(session_factory, [], "job1") is False

