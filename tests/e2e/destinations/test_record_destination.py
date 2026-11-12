"""
E2E tests for ``@record_destination`` against a real broker.

Runs ``e2e_record_destination_test``, whose jobs each write to three topics (one per
``engine``), and consumes all three: every record of this test's ``run`` must land
exactly once, on its own engine's topic.

Prerequisites:
    - Kafka on localhost:9095 (SASL_PLAINTEXT, from docker-compose.e2e-infra.yml)
    - ReflowManager on localhost:8002

Run:
    pytest tests/e2e/destinations/test_record_destination.py -v
"""

import asyncio
import json
import os
import time
import uuid
from typing import Any, Dict, List, Tuple

import httpx
from aiokafka import AIOKafkaConsumer

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095")
PIPELINE_NAME = "e2e_record_destination_test"
ENGINES = ("whisper", "google", "azure")
TOPICS = {f"e2e-rd-{engine}": engine for engine in ENGINES}
MAX_WAIT = 120


def _run_and_wait(params: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as manager:
        resp = manager.post("/run", json={"pipeline_name": PIPELINE_NAME, "runtime_params": params})
        assert resp.status_code == 202, f"/run failed {resp.status_code}: {resp.text}"
        execution_id = resp.json()["execution_id"]
        deadline = time.time() + MAX_WAIT
        stats: Dict[str, Any] = {}
        while time.time() < deadline:
            stats = manager.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") in ("completed", "failed"):
                break
            time.sleep(1)
    return execution_id, stats


def _job_stats(execution_id: str) -> List[Dict[str, Any]]:
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as manager:
        resp = manager.get(f"/executions/{execution_id}/jobs")
    assert resp.status_code == 200, resp.text
    return [job.get("stats") or {} for job in resp.json()]


async def _consume_run(run: str, expected: int, timeout: float = 60.0) -> List[Tuple[str, Dict]]:
    """(topic, record) of every message of ``run`` on the three topics, from the start."""
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"e2e-rd-{uuid.uuid4().hex}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        sasl_plain_username=os.getenv("KAFKA_SASL_USERNAME", "admin"),
        sasl_plain_password=os.getenv("KAFKA_SASL_PASSWORD", "admin-secret"),
    )
    await consumer.start()
    found: List[Tuple[str, Dict]] = []
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
                        found.append((msg.topic, value))
                        got_any = True
            # Read a little past the expected count, so a duplicate shows up.
            if len(found) >= expected:
                quiet_since = quiet_since or time.time()
                if not got_any and time.time() - quiet_since > 3:
                    break
            await asyncio.sleep(0)
    finally:
        await consumer.stop()
    return found


async def test_each_record_lands_once_on_its_engine_topic():
    run = f"rd-route-{uuid.uuid4().hex[:8]}"
    _, stats = _run_and_wait({"run": run, "scenario": "route", "count": 30, "batch_size": 15})

    assert stats.get("state") == "completed", stats
    assert (stats["total_jobs"], stats["jobs_completed"]) == (2, 2), stats
    found = await _consume_run(run, expected=30)
    assert sorted(r["id"] for _, r in found) == list(range(1, 31))  # exactly once
    for topic, record in found:
        assert TOPICS[topic] == record["engine"]
    # Each job of 15 records wrote to all three topics.
    assert {topic for topic, _ in found} == set(TOPICS)


async def test_unknown_engines_are_skipped_and_the_rest_is_sent():
    run = f"rd-skip-{uuid.uuid4().hex[:8]}"
    execution_id, stats = _run_and_wait({"run": run, "scenario": "skip", "count": 30})

    assert stats.get("state") == "completed", stats
    assert stats["jobs_completed"] == 2, stats
    found = await _consume_run(run, expected=24)
    assert sorted(r["id"] for _, r in found) == [i for i in range(1, 31) if i % 5 != 0]
    # ids 5, 10, 15 in the first job and 20, 25, 30 in the second.
    job_stats = _job_stats(execution_id)
    assert [s.get("records_skipped") for s in job_stats] == [3, 3], job_stats
    assert all(s.get("record_skip_reasons") == {"KeyError": 3} for s in job_stats), job_stats
    assert all(s.get("destinations") == 3 for s in job_stats), job_stats


async def test_unknown_engines_fail_the_job_with_nothing_written():
    run = f"rd-fail-{uuid.uuid4().hex[:8]}"
    _, stats = _run_and_wait({"run": run, "scenario": "fail", "count": 30})

    assert stats.get("state") in ("completed", "failed"), stats
    assert stats["jobs_failed"] == stats["total_jobs"] == 2, stats
    assert await _consume_run(run, expected=0, timeout=10.0) == []
