"""
E2E tests for ``kafka_destination(headers=...)`` against a real broker.

Runs ``e2e_kafka_headers_test`` and consumes its topic: every message of this test's
``run`` must carry the headers the pipeline configured, filled from that record.

Prerequisites:
    - Kafka on localhost:9095 (SASL_PLAINTEXT, from docker-compose.e2e-infra.yml)
    - ReflowManager on localhost:8002

Run:
    pytest tests/e2e/destinations/test_kafka_destination_headers.py -v
"""

import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pytest
from aiokafka import AIOKafkaConsumer

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095")
TOPIC = os.getenv("E2E_KAFKA_HEADERS_TOPIC", "e2e-kafka-headers")
PIPELINE_NAME = "e2e_kafka_headers_test"
MAX_WAIT = 120

Headers = Dict[str, Optional[bytes]]


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


async def _consume_run(run: str, expected: int, timeout: float = 60.0) -> List[Tuple[int, Headers]]:
    """(id, headers) of every message of ``run`` on the topic, read from the beginning."""
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"e2e-headers-{uuid.uuid4().hex}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        sasl_plain_username=os.getenv("KAFKA_SASL_USERNAME", "admin"),
        sasl_plain_password=os.getenv("KAFKA_SASL_PASSWORD", "admin-secret"),
    )
    await consumer.start()
    found: List[Tuple[int, Headers]] = []
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
                        found.append((value["id"], dict(msg.headers or ())))
                        got_any = True
            # Keep reading a little past the expected count, so a duplicate shows up.
            # expected=0 just reads until the topic has been quiet for a while.
            if len(found) >= expected:
                quiet_since = quiet_since or time.time()
                if not got_any and time.time() - quiet_since > 3:
                    break
            await asyncio.sleep(0)
    finally:
        await consumer.stop()
    return found


async def test_static_headers_with_placeholders(manager):
    run = f"kafka-hdr-static-{uuid.uuid4().hex[:8]}"
    stats = _run_and_wait(
        manager, {"run": run, "mode": "static", "count": 20}
    )

    assert stats.get("state") == "completed", stats
    assert (stats["total_jobs"], stats["jobs_completed"]) == (2, 2), stats
    found = await _consume_run(run, expected=20)
    assert sorted(i for i, _ in found) == list(range(1, 21))
    for record_id, headers in found:
        assert headers["source"] == b"reflowfy-e2e"
        assert headers["run"] == run.encode()
        assert headers["record-id"] == str(record_id).encode()


async def test_function_headers_bytes_and_null(manager):
    run = f"kafka-hdr-fn-{uuid.uuid4().hex[:8]}"
    stats = _run_and_wait(manager, {"run": run, "mode": "function", "count": 20})

    assert stats.get("state") == "completed", stats
    found = await _consume_run(run, expected=20)
    assert sorted(i for i, _ in found) == list(range(1, 21))
    for record_id, headers in found:
        assert headers["record-id"] == str(record_id).encode()
        assert headers["parity"] == (b"even" if record_id % 2 == 0 else b"odd")
        assert headers["raw"] == b"\x00\xff"
        assert "nullable" in headers and headers["nullable"] is None


async def test_missing_placeholder_field_fails_and_writes_nothing(manager):
    run = f"kafka-hdr-missing-{uuid.uuid4().hex[:8]}"
    stats = _run_and_wait(manager, {"run": run, "mode": "missing", "count": 20})

    assert stats.get("state") in ("completed", "failed"), stats
    assert stats["jobs_failed"] == stats["total_jobs"] == 2, stats
    assert await _consume_run(run, expected=0, timeout=10.0) == []
