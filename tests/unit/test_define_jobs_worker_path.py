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
