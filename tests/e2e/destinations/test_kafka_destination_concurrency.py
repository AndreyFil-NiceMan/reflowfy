"""
E2E tests for ``kafka_destination(concurrency=...)`` against a real broker.

Runs ``e2e_kafka_concurrency_test`` and consumes its topic: every record of this test's
``run`` must arrive exactly once, and with ``concurrency=1`` in the order the job sent
them (the topic has one partition).

Prerequisites:
    - Kafka on localhost:9095 (SASL_PLAINTEXT, from docker-compose.e2e-infra.yml)
    - ReflowManager on localhost:8002

Run:
    pytest tests/e2e/destinations/test_kafka_destination_concurrency.py -v
"""

import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict, List

import httpx
import pytest
from aiokafka import AIOKafkaConsumer

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095")
TOPIC = os.getenv("E2E_KAFKA_CONCURRENCY_TOPIC", "e2e-kafka-concurrency")
PIPELINE_NAME = "e2e_kafka_concurrency_test"
MAX_WAIT = 120


@pytest.fixture(scope="module")
def manager():
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as client:
        yield client


def _run_and_wait(manager: httpx.Client, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = manager.post("/run", json={"pipeline_name": PIPELINE_NAME, "runtime_params": params})
    assert resp.status_code == 202, f"/run failed {resp.status_code}: {resp.text}"
    execution_id = resp.json()["execution_id"]
    deadline = time.time() + MAX_WAIT
    stats: Dict[str, Any] = {}
    while time.time() < deadline:
        stats = manager.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(1)
    return stats


async def _consume_run(run: str, expected: int, timeout: float = 60.0) -> List[int]:
    """Every id of ``run`` on the topic, in topic order, read from the beginning."""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"e2e-concurrency-{uuid.uuid4().hex}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        sasl_plain_username=os.getenv("KAFKA_SASL_USERNAME", "admin"),
        sasl_plain_password=os.getenv("KAFKA_SASL_PASSWORD", "admin-secret"),
    )
    await consumer.start()
    ids: List[int] = []
    try:
        deadline = time.time() + timeout
        quiet_since = None
        while time.time() < deadline:
            batch = await consumer.getmany(timeout_ms=1000, max_records=1000)
            got_any = False
            for messages in batch.values():
                for msg in messages:
                    value = json.loads(msg.value.decode("utf-8"))
                    if isinstance(value, dict) and value.get("run") == run:
                        ids.append(value["id"])
                        got_any = True
            # Keep reading a little past the expected count, so a duplicate shows up.
            if len(ids) >= expected:
                quiet_since = quiet_since or time.time()
                if not got_any and time.time() - quiet_since > 3:
                    break
            await asyncio.sleep(0)
    finally:
        await consumer.stop()
    return ids


@pytest.mark.parametrize("concurrency", [1, 16])
async def test_every_record_arrives_exactly_once(manager, concurrency):
    run = f"kafka-conc-{concurrency}-{uuid.uuid4().hex[:8]}"
    stats = _run_and_wait(
        manager, {"run": run, "count": 200, "batch_size": 50, "concurrency": concurrency}
    )

    assert stats.get("state") == "completed", stats
    assert (stats["total_jobs"], stats["jobs_completed"]) == (4, 4), stats
    ids = await _consume_run(run, expected=200)
    assert len(ids) == 200, f"{len(ids)} messages, {len(set(ids))} unique"
    assert sorted(ids) == list(range(1, 201))


async def test_concurrency_one_keeps_each_jobs_order(manager):
    run = f"kafka-order-{uuid.uuid4().hex[:8]}"
    stats = _run_and_wait(manager, {"run": run, "count": 100, "batch_size": 25})

    assert stats.get("state") == "completed", stats
    ids = await _consume_run(run, expected=100)
    assert sorted(ids) == list(range(1, 101))
    # Jobs may interleave with each other, but within a job (25 consecutive ids) the
    # messages are on the single partition in the order they were sent.
    for start in range(1, 101, 25):
        job_ids = [i for i in ids if start <= i < start + 25]
        assert job_ids == list(range(start, start + 25)), job_ids
