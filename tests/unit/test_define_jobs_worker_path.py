"""The define_jobs job plan must survive the distributed (Kafka) worker path.

The E2E stack runs EXECUTION_MODE=local, where LocalDispatcher hands the
in-memory payload straight to WorkerExecutor — so the Kafka hop
(json.dumps -> bytes -> json.loads) and KafkaJobConsumer's schema_version
gate are never exercised there. This walks a payload built by the real
manager code through that hop and into the real consumer.
"""

import json
from types import SimpleNamespace

import pytest

from reflowfy import chunk
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.execution.job_runner import plan_slices
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.reflow_manager.pipeline_runner import build_job_payload
from reflowfy.core.serialization import to_json_safe
from reflowfy.worker.consumer import KafkaJobConsumer
from reflowfy.worker.executor import WorkerExecutor

RECORDS = [{"name": "obj1", "value": 1}, {"name": "obj2", "value": 2}, {"name": "obj3", "value": 3}]

_METADATA = {
    "batch_id": "b",
    "created_at": "t",
    "batch_number": 1,
    "total_batches": 1,
    "retry_count": 0,
    "is_retry": False,
    "runtime_params": {},
    "source_metadata": None,
}


class _FakeDest:
    def __init__(self, sink):
        self.sink = sink

    async def health_check(self):
        return True

    async def send_with_retry(self, records, params):
        self.sink.extend(records)

    async def close(self):
        pass


class _FakePipeline:
    name = "p"

    def __init__(self, sink):
        self.sink = sink

    def define_transformations(self, records, params):
        return []

    def define_destination(self, records, params):
        return _FakeDest(self.sink)


async def _async_noop(*a, **k):
    return None


def _payloads_over_the_wire(job_plan):
    """Build job payloads exactly as the manager does, then Kafka-encode them.

    Mirrors pipeline_runner.run_pipeline_jobs phase 1 (plan_slices ->
    build_job_payload -> to_json_safe) followed by KafkaDispatcher's
    json.dumps and the consumer's json.loads.
    """
    for index, sub_source in enumerate(plan_slices(job_plan, {})):
        payload = build_job_payload("e", f"j{index}", "p", sub_source, dict(_METADATA))
        wire = json.dumps(to_json_safe(payload)).encode("utf-8")
        yield json.loads(wire.decode("utf-8"))


def test_chunked_plan_serializes_and_rebuilds_over_the_wire():
    """Each chunk must survive JSON as a StaticSource carrying its own records."""
    payloads = list(_payloads_over_the_wire(chunk(RECORDS, size=1)))

    assert len(payloads) == 3
    for payload, record in zip(payloads, RECORDS):
        assert payload["schema_version"] == 2
        assert payload["source"]["type"] == "StaticSource"
        rebuilt = SourceFactory.create(payload["source"]["type"], payload["source"]["config"])
        assert rebuilt.fetch({}) == [record]


def test_job_size_two_puts_the_right_records_in_each_job():
    payloads = list(_payloads_over_the_wire(chunk(RECORDS, size=2)))

    fetched = [
        SourceFactory.create(p["source"]["type"], p["source"]["config"]).fetch({})
        for p in payloads
    ]
    assert fetched == [RECORDS[:2], RECORDS[2:]]


@pytest.mark.parametrize("job_size,expected_jobs", [(1, 3), (2, 2), (3, 1)])
async def test_consumer_processes_wire_payload_end_to_end(monkeypatch, job_size, expected_jobs):
    """Full distributed path: manager plan -> Kafka bytes -> KafkaJobConsumer
    -> WorkerExecutor -> destination, with only the DB and broker stubbed."""
    from reflowfy.core.registry import pipeline_registry

    sink = []
    monkeypatch.setattr(pipeline_registry, "get", lambda name: _FakePipeline(sink))

    executor = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(executor, "_update_job_in_db", _async_noop)

    # Real consumer, stubbed broker: only _process_message is under test.
    consumer = KafkaJobConsumer.__new__(KafkaJobConsumer)
    consumer.executor = executor

    committed = []

    class _StubKafka:
        async def commit(self):
            committed.append(True)

    consumer.consumer = _StubKafka()

    payloads = list(_payloads_over_the_wire(chunk(RECORDS, size=job_size)))
    assert len(payloads) == expected_jobs

    for payload in payloads:
        await consumer._process_message(
            SimpleNamespace(value=json.dumps(payload).encode("utf-8"))
        )

    # Every record arrives exactly once, and every offset is committed.
    assert sink == RECORDS
    assert len(committed) == expected_jobs


# ---------------------------------------------------------------------------
# IdBasedPipeline over the same wire
#
# IdBasedPipeline plans through AbstractPipeline.define_jobs, and each planned
# source carries `job_params` so a job gets only the IDs it handles. That
# attribute lives on the source object and must NOT reach the worker — what
# must reach it is the narrowed params, inside the payload metadata.
# ---------------------------------------------------------------------------

_SEEN_PARAMS: list = []


class _IdSyncPipeline(IdBasedPipeline):
    name = "worker_path_id_sync"
    ids_batch_size = 2
    sink: list = []

    def define_source(self, runtime_params):
        # An enrichment: a key define_source adds must reach the worker, and
        # must be this batch's value rather than a later batch's.
        runtime_params["batch_tag"] = f"tag-{runtime_params['current_ids'][0]}"
        return [{"id": i} for i in runtime_params["current_ids"]]

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        _SEEN_PARAMS.append(dict(runtime_params))
        return _FakeDest(type(self).sink)


def _id_based_payloads_over_the_wire(ids):
    """Real manager Phase 1 for an IdBasedPipeline, then the Kafka encode/decode."""
    from unittest.mock import MagicMock, patch

    from reflowfy.reflow_manager.pipeline_runner import PipelineRunner

    runner = PipelineRunner(
        execution_manager=MagicMock(), job_manager=MagicMock(), dispatcher=MagicMock()
    )
    with patch.object(PipelineRunner, "_dispatch_and_wait_batches", return_value=(0, 0, 0)):
        runner.run_pipeline_jobs(
            execution_id="e",
            pipeline_name=_IdSyncPipeline.name,
            runtime_params={"ids": list(ids), "env": "prod"},
        )

    rows = [r for call in runner.job_manager.create_jobs.call_args_list for r in call.args[0]]
    for row in rows:
        wire = json.dumps(row["job_payload"]).encode("utf-8")
        yield json.loads(wire.decode("utf-8"))


def test_id_based_plan_survives_the_wire_with_only_its_own_ids():
    payloads = list(_id_based_payloads_over_the_wire(range(6)))

    assert len(payloads) == 3  # 6 ids, ids_batch_size=2
    for payload, expected_ids in zip(payloads, ([0, 1], [2, 3], [4, 5])):
        assert payload["schema_version"] == 2
        rebuilt = SourceFactory.create(payload["source"]["type"], payload["source"]["config"])
        assert rebuilt.fetch({}) == [{"id": i} for i in expected_ids]

        params = payload["metadata"]["runtime_params"]
        assert payload["metadata"]["current_ids"] == expected_ids
        assert params["current_ids"] == expected_ids
        assert params["batch_tag"] == f"tag-{expected_ids[0]}"   # this batch's enrichment
        assert params["env"] == "prod"                           # other params still travel
        assert "ids" not in params                               # not all 6

    # job_params is a planning-time attribute; it must not ride on the wire.
    assert all("job_params" not in p["source"]["config"] for p in payloads)


async def test_worker_processes_an_id_based_job_end_to_end(monkeypatch):
    """Manager plan -> Kafka bytes -> KafkaJobConsumer -> WorkerExecutor -> destination."""
    _IdSyncPipeline.sink = []
    _SEEN_PARAMS.clear()

    executor = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(executor, "_update_job_in_db", _async_noop)

    consumer = KafkaJobConsumer.__new__(KafkaJobConsumer)
    consumer.executor = executor

    class _StubKafka:
        async def commit(self):
            pass

    consumer.consumer = _StubKafka()

    for payload in _id_based_payloads_over_the_wire(range(6)):
        await consumer._process_message(
            SimpleNamespace(value=json.dumps(payload).encode("utf-8"))
        )

    # Every ID delivered exactly once, in order.
    assert _IdSyncPipeline.sink == [{"id": i} for i in range(6)]

    # The worker resolved the destination with this job's IDs, not the whole run.
    assert [p["current_ids"] for p in _SEEN_PARAMS] == [[0, 1], [2, 3], [4, 5]]
    assert [p["batch_tag"] for p in _SEEN_PARAMS] == ["tag-0", "tag-2", "tag-4"]
    assert all("ids" not in p for p in _SEEN_PARAMS)
