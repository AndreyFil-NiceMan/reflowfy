"""
E2E Tests: Kafka Destination Lag Health Check.

Two scenarios:
  1. High lag  → execution fails, no jobs dispatched, DLQ entry created.
  2. Low lag   → execution completes, jobs are dispatched normally.

Prerequisites:
  - Kafka running on localhost:9094 (PLAINTEXT, no SASL)
  - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/destinations/test_kafka_lag_health_check.py -v
"""

import os
import time
from typing import Any, Dict

import httpx
import pytest
from aiokafka import AIOKafkaProducer

# ── Configuration ─────────────────────────────────────────────────────────────

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "admin")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "admin-secret")
LAG_TEST_TOPIC = os.getenv("E2E_LAG_TEST_TOPIC", "e2e-lag-health-check")
# Fixed group that never commits — so lag always equals end-offset of the topic.
LAG_TEST_GROUP = os.getenv("E2E_LAG_TEST_GROUP", "e2e-lag-test-consumer-group")

TIMEOUT = 90.0
POLL_INTERVAL = 2


# ── Shared aiokafka connection kwargs ─────────────────────────────────────────

_KAFKA_CONN: Dict[str, Any] = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "security_protocol": KAFKA_SECURITY_PROTOCOL,
    "sasl_mechanism": KAFKA_SASL_MECHANISM,
    "sasl_plain_username": KAFKA_SASL_USERNAME,
    "sasl_plain_password": KAFKA_SASL_PASSWORD,
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def poll_until_terminal(client: httpx.Client, execution_id: str, timeout: int = 90) -> str:
    """Poll execution stats until state is terminal. Returns final state string."""
    start = time.time()
    while time.time() - start < timeout:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        state = stats.get("state")
        if state in ("completed", "failed"):
            return state
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Execution {execution_id} did not reach terminal state within {timeout}s")


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as c:
        yield c


@pytest.fixture(scope="module")
async def ensure_lag_test_topic():
    """
    Create the lag-test topic if it doesn't exist yet.

    Uses AIOKafkaAdminClient so no extra dependency is needed.
    Topic-already-exists errors are silently ignored.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    admin = AIOKafkaAdminClient(**_KAFKA_CONN)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=LAG_TEST_TOPIC, num_partitions=1, replication_factor=1)]
        )
        print(f"✅ Created topic: {LAG_TEST_TOPIC}")
    except Exception as exc:
        # TopicAlreadyExistsError or similar — perfectly fine
        print(f"ℹ️  Topic setup: {exc}")
    finally:
        await admin.close()


@pytest.fixture
async def kafka_producer(ensure_lag_test_topic):
    """Temporary producer for flooding the lag-test topic."""
    producer = AIOKafkaProducer(**_KAFKA_CONN)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


# ── Tests ──────────────────────────────────────────────────────────────────────

class TestKafkaLagHealthCheck:
    """End-to-end tests for Kafka destination lag health check."""

    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is accessible."""
        resp = client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_high_lag_blocks_dispatch(self, client, kafka_producer):
        """
        Jobs should NOT be dispatched when consumer lag exceeds the threshold.

        Strategy:
          - LAG_TEST_GROUP never commits offsets to LAG_TEST_TOPIC, so
            measured lag == current end-offset of the topic.
          - Produce 10 000 messages → end-offset grows to ≥ 10 000.
          - Run pipeline with lag_threshold=100 (100 << 10 000) → must fail.
        """
        # 1. Flood the topic — LAG_TEST_GROUP has no committed offsets,
        #    so lag = end_offset of the topic after flooding.
        flood_count = 10_000
        for _ in range(flood_count):
            await kafka_producer.send_and_wait(LAG_TEST_TOPIC, b'{"x":1}')
        await kafka_producer.flush()

        # 2. Trigger pipeline with a very low threshold
        resp = client.post("/run", json={
            "pipeline_name": "e2e_kafka_lag_health_check",
            "runtime_params": {"lag_threshold": 100},
        })
        assert resp.status_code == 202, resp.text
        execution_id = resp.json()["execution_id"]

        # 3. Wait for terminal state
        final_state = poll_until_terminal(client, execution_id, timeout=60)

        # 4. Assertions
        stats = client.get(f"/executions/{execution_id}/stats").json()
        assert final_state == "failed", (
            f"Expected 'failed', got '{final_state}'. Stats: {stats}"
        )
        assert stats["jobs_dispatched"] == 0, (
            f"Expected 0 dispatched jobs, got {stats['jobs_dispatched']}"
        )

        # DLQ entry should have been created for later retry
        dlq_resp = client.get(
            "/dlq/jobs",
            params={"pipeline_name": "e2e_kafka_lag_health_check"},
        )
        assert dlq_resp.status_code == 200, dlq_resp.text
        dlq_data = dlq_resp.json()
        pending = [j for j in dlq_data.get("jobs", []) if j["status"] == "pending"]
        assert len(pending) >= 1, (
            f"Expected ≥1 pending DLQ entry, got: {dlq_data}"
        )

        print(
            f"✅ High-lag test passed: state={final_state}, "
            f"dispatched={stats['jobs_dispatched']}, dlq_entries={len(pending)}"
        )

    @pytest.mark.asyncio
    async def test_low_lag_allows_dispatch(self, client, ensure_lag_test_topic):
        """
        Jobs SHOULD be dispatched when consumer lag is below the threshold.

        lag_threshold=999_999 ensures the check always passes regardless of
        how many messages accumulated in the topic during the high-lag test.
        """
        resp = client.post("/run", json={
            "pipeline_name": "e2e_kafka_lag_health_check",
            "runtime_params": {"lag_threshold": 999_999},
        })
        assert resp.status_code == 202, resp.text
        execution_id = resp.json()["execution_id"]

        final_state = poll_until_terminal(client, execution_id, timeout=120)

        stats = client.get(f"/executions/{execution_id}/stats").json()
        assert final_state == "completed", (
            f"Expected 'completed', got '{final_state}'. Stats: {stats}"
        )
        assert stats["jobs_dispatched"] > 0, "Expected at least one dispatched job"
        assert stats["jobs_failed"] == 0, (
            f"Expected 0 failed jobs, got {stats['jobs_failed']}"
        )

        print(
            f"✅ Low-lag test passed: state={final_state}, "
            f"dispatched={stats['jobs_dispatched']}, failed={stats['jobs_failed']}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
