"""Per-record dedup through the real WorkerExecutor and a real (SQLite) claim table.

enable_duplicate_jobs=False claims one hash per record about to be sent; records
another job already sent are dropped before the send. Checked on all three
observability channels: the log line, JobStats on the job row, and metrics.
"""

from typing import Any, Dict, List

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.ext.asyncio import create_async_engine

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.transformations.base import BaseTransformation
from reflowfy.worker.executor import JobStats, WorkerExecutor
from reflowfy.reflow_manager.models import Base
from test_worker_executor_paths import (  # type: ignore[import-not-found]
    FakeDest,
    FakePipeline,
    _payload,
    capture_logs,
    processed,
    register,  # noqa: F401  (fixture)
)


def deduplicated_records(pipeline: str) -> float:
    return (
        REGISTRY.get_sample_value("reflowfy_records_deduplicated_total", {"pipeline": pipeline})
        or 0.0
    )


class KeyedPipeline(FakePipeline):
    def __init__(self, name: str, dest: FakeDest, dedup_key: Any = None, **kw: Any):
        super().__init__(name, dest, **kw)
        self.dedup_key = dedup_key


class StampTime(BaseTransformation):
    """Makes every run's output differ, the way a processed_at stamp does."""

    name = "rd_stamp"
    calls = 0

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        StampTime.calls += 1
        return [{**r, "processed_at": StampTime.calls} for r in records]


class DropAll(BaseTransformation):
    name = "rd_drop_all"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        return []


@pytest.fixture
async def worker(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path}/dedup.db"
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    w = WorkerExecutor(database_url=url)
    updates: List[JobStats] = []

    async def record(execution_id: str, job_id: str, stats: JobStats) -> None:
        updates.append(stats)

    w._update_job_in_db = record  # type: ignore[method-assign]
    w.updates = updates  # type: ignore[attr-defined]
    yield w
    await w.close()


def _ids(n_from: int, n_to: int) -> List[Dict[str, Any]]:
    return [{"id": i} for i in range(n_from, n_to + 1)]


async def _run(worker, pipeline: str, job_id: str, records: List[Any], **extra: Any) -> JobStats:
    await worker.execute_job(
        _payload(pipeline, job_id=job_id, records=records, dedup_check=True, **extra)
    )
    return worker.updates[-1]


async def test_overlapping_jobs_send_each_record_once(register, worker):  # noqa: F811
    dest = FakeDest()
    register(FakePipeline("rd-overlap", dest))
    before = deduplicated_records("rd-overlap")

    first = await _run(worker, "rd-overlap", "a", _ids(1, 100))
    with capture_logs() as logs:
        second = await _run(worker, "rd-overlap", "b", _ids(50, 150))

    assert [r["id"] for r in dest.sent] == list(range(1, 151))
    assert (first.records_output, first.records_deduplicated) == (100, 0)
    assert (second.records_output, second.records_deduplicated) == (50, 51)
    assert second.success and not second.deduplicated  # partly new: a completed job
    assert second.to_dict()["records_deduplicated"] == 51
    assert "51 of 101 records already processed — dropped" in logs
    assert deduplicated_records("rd-overlap") == before + 51


async def test_a_job_with_nothing_new_is_deduplicated(register, worker):  # noqa: F811
    dest = FakeDest()
    register(FakePipeline("rd-full", dest))
    before = processed("rd-full", "deduplicated")

    await _run(worker, "rd-full", "a", _ids(1, 3))
    again = await _run(worker, "rd-full", "b", list(reversed(_ids(1, 3))))  # order is irrelevant

    assert len(dest.sent) == 3
    assert again.deduplicated and again.records_output == 0 and again.records_deduplicated == 3
    assert processed("rd-full", "deduplicated") == before + 1


async def test_a_rerun_of_the_same_job_is_not_its_own_duplicate(register, worker):  # noqa: F811
    """Claims left by a SIGKILL'd worker belong to the job; its re-run sends again."""
    dest = FakeDest()
    register(FakePipeline("rd-rerun", dest))
    await _run(worker, "rd-rerun", "a", _ids(1, 2))
    rerun = await _run(worker, "rd-rerun", "a", _ids(1, 2))
    assert rerun.records_output == 2 and len(dest.sent) == 4


async def test_duplicates_within_one_job_are_sent_once(register, worker):  # noqa: F811
    dest = FakeDest()
    register(FakePipeline("rd-within", dest))
    stats = await _run(worker, "rd-within", "a", [{"id": 1}, {"id": 1}, {"id": 2}])
    assert dest.sent == [{"id": 1}, {"id": 2}]
    assert stats.records_deduplicated == 1


async def test_a_failed_send_releases_the_claims_for_the_retry(register, worker):  # noqa: F811
    from reflowfy.destinations.base import DestinationError

    failing = FakeDest(send_error=DestinationError("x", "down"))
    register(FakePipeline("rd-retry", failing))
    assert not (await _run(worker, "rd-retry", "a", _ids(1, 2))).success

    failing.send_error = None
    retried = await _run(worker, "rd-retry", "b", _ids(1, 2))
    assert retried.records_output == 2 and failing.sent == _ids(1, 2)


async def test_a_stamped_field_defeats_whole_record_dedup(register, worker):  # noqa: F811
    dest = FakeDest()
    register(FakePipeline("rd-stamped", dest, transformations=[StampTime()]))
    await _run(worker, "rd-stamped", "a", _ids(1, 2))
    await _run(worker, "rd-stamped", "b", _ids(1, 2))
    assert len(dest.sent) == 4  # what is sent differs, so it is new work


async def test_a_field_key_dedups_records_whose_other_fields_differ(register, worker):  # noqa: F811
    dest = FakeDest()
    register(KeyedPipeline("rd-key", dest, dedup_key="id", transformations=[StampTime()]))
    await _run(worker, "rd-key", "a", _ids(1, 2))
    second = await _run(worker, "rd-key", "b", _ids(1, 3))
    assert [r["id"] for r in dest.sent] == [1, 2, 3]
    assert second.records_deduplicated == 2


async def test_a_key_function_sees_runtime_params(register, worker):  # noqa: F811
    dest = FakeDest()
    register(KeyedPipeline("rd-tenant", dest, dedup_key=lambda r, p: (p.get("tenant"), r["id"])))

    def tenant(t: str) -> Dict[str, Any]:
        return {"metadata": {"runtime_params": {"tenant": t}}}

    await _run(worker, "rd-tenant", "a", _ids(1, 2), **tenant("x"))
    await _run(worker, "rd-tenant", "b", _ids(1, 2), **tenant("y"))
    again = await _run(worker, "rd-tenant", "c", _ids(1, 2), **tenant("x"))
    assert len(dest.sent) == 4
    assert again.deduplicated


async def test_jobs_tagged_for_another_tenant_are_new_work(register, worker):  # noqa: F811
    """reflowfy.job(rows, tenant=...) params count; slice params do not."""
    dest = FakeDest()
    register(FakePipeline("rd-jobparams", dest))
    await _run(worker, "rd-jobparams", "a", _ids(1, 1), job_params={"tenant": "x", "current_id": 1})
    await _run(worker, "rd-jobparams", "b", _ids(1, 1), job_params={"tenant": "y", "current_id": 2})
    same = await _run(
        worker, "rd-jobparams", "c", _ids(1, 1), job_params={"tenant": "x", "current_id": 3}
    )
    assert len(dest.sent) == 2
    assert same.deduplicated


async def test_a_job_whose_transformations_drop_everything_is_not_a_duplicate(
    register, worker  # noqa: F811
):
    dest = FakeDest()
    register(FakePipeline("rd-empty", dest, transformations=[DropAll()]))
    stats = await _run(worker, "rd-empty", "a", _ids(1, 2))
    assert stats.success and not stats.deduplicated and stats.records_deduplicated == 0


class _Keyed(AbstractPipeline[RuntimeParams]):
    def define_source(self, params):  # pragma: no cover - never planned
        raise NotImplementedError

    def define_transformations(self, records, params):  # pragma: no cover
        return []

    def define_destination(self, records, params):  # pragma: no cover
        raise NotImplementedError


def _pipeline(**attrs: Any) -> Any:
    return type("RdPipe", (_Keyed,), {"name": None, **attrs})


def test_dedup_key_without_dedup_enabled_is_rejected():
    with pytest.raises(ValueError, match="enable_duplicate_jobs"):
        _pipeline()(dedup_key="id")


def test_dedup_key_must_be_a_field_or_a_function():
    with pytest.raises(ValueError, match="field name or a function"):
        _pipeline()(enable_duplicate_jobs=False, dedup_key=3)


def test_a_staticmethod_key_resolves_to_a_function_of_record_and_params():
    cls = _pipeline(
        name="rd_staticmethod_key",
        enable_duplicate_jobs=False,
        dedup_key=staticmethod(lambda r, p: (p["tenant"], r["id"])),
    )
    pipe = cls()
    assert pipe.dedup_key({"id": 1}, {"tenant": "t"}) == ("t", 1)
    assert pipe.to_dict()["dedup_key"] == "<callable>"
    assert cls(enable_duplicate_jobs=False, dedup_key="id").to_dict()["dedup_key"] == "id"


async def test_the_destination_is_rebuilt_from_the_records_kept(register, worker):  # noqa: F811
    """A destination that bakes the records in (an api_destination body is sent
    verbatim) must not carry the dropped ones."""

    class BodyDest(FakeDest):
        async def send_with_retry(self, records: List[Any], params: Dict[str, Any]) -> None:
            self.sent.extend(self.config["body"])

    built: List[BodyDest] = []

    class BodyPipeline(FakePipeline):
        def define_destination(self, records: List[Any], params: Dict[str, Any]) -> Any:
            dest = BodyDest()
            dest.config = {"url": "http://x"}  # identity, without the body
            dest.dedup_identity = {"url": "http://x"}
            dest.config["body"] = list(records)
            built.append(dest)
            return dest

    register(BodyPipeline("rd-body", FakeDest()))
    await _run(worker, "rd-body", "a", _ids(1, 2))
    await _run(worker, "rd-body", "b", _ids(2, 3))

    assert [r["id"] for d in built for r in d.sent] == [1, 2, 3]
    assert len(built) == 3 and built[1].closed  # resolved again for job b, first one closed
