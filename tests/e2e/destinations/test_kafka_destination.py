"""
E2E Tests for Kafka Destination.

Tests the KafkaDestination connector by running a pipeline that uses
mock source data and sends to a Kafka topic.

Prerequisites:
    - Kafka running on localhost:9094 (from docker-compose.e2e.yml)
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/destinations/test_kafka_destination.py -v
"""

import os
import json
import time
import asyncio
import pytest
import httpx
from aiokafka import AIOKafkaConsumer

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095")
KAFKA_TOPIC = os.getenv("E2E_KAFKA_DEST_TOPIC", "e2e-test-destination")
# SASL Configuration
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "admin")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "admin-secret")

TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="function")
async def kafka_consumer():
    """Create a Kafka consumer for verifying messages."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"e2e-test-consumer-{time.time()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        security_protocol=KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=KAFKA_SASL_MECHANISM,
        sasl_plain_username=KAFKA_SASL_USERNAME,
        sasl_plain_password=KAFKA_SASL_PASSWORD,
    )
    
    await consumer.start()
    try:
        yield consumer
    finally:
        await consumer.stop()


class TestKafkaDestinationPipeline:
    """Test Kafka destination pipeline."""
    
    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is running."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    @pytest.mark.asyncio
    async def test_kafka_topic_exists(self, kafka_consumer):
        """Verify Kafka topic exists (wait for it if necessary)."""
        # Since we run with explicit topic creation in run_e2e_tests.sh, it should exist.
        # But we retry checking for it just in case.
        start = time.time()
        topics = set()
        while time.time() - start < 30:
            topics = await kafka_consumer.topics()
            if KAFKA_TOPIC in topics:
                break
            await asyncio.sleep(1)
            
        assert KAFKA_TOPIC in topics, f"Topic {KAFKA_TOPIC} not found. Found: {topics}"
    
    def test_pipeline_starts(self, client):
        """Test that pipeline can start."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_kafka_dest_test",
        })
        
        assert response.status_code == 202
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_kafka_dest_test"
    
    def test_pipeline_completes(self, client):
        """Test that pipeline runs to completion."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_kafka_dest_test",
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        max_wait = 120
        start = time.time()
        final_state = None
        stats = {}
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            final_state = stats.get("state")
            
            if final_state in ["completed", "failed"]:
                break
            
            time.sleep(POLL_INTERVAL)
        
        # Verify completion
        assert final_state == "completed", f"Expected completed, got {final_state}"
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0
        
        print(f"✅ Pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs")
    
    @pytest.mark.asyncio
    async def test_messages_in_kafka(self, client, kafka_consumer):
        """Test that messages are sent to Kafka topic."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_kafka_dest_test",
        })
        
        execution_id = response.json()["execution_id"]
        
        # Wait for completion (simpler check here, or assume async consumer waits)
        # We can just start consuming immediately
        
        messages = []
        consume_start = time.time()
        
        # Consume for up to 30 seconds or until we get 10 messages
        while time.time() - consume_start < 30:
            result = await kafka_consumer.getmany(timeout_ms=1000, max_records=10)
            for tp, msgs in result.items():
                for msg in msgs:
                    try:
                        value = json.loads(msg.value.decode("utf-8"))
                        messages.append(value)
                    except json.JSONDecodeError:
                        pass
            
            if len(messages) >= 10:
                break
                
            # If pipeline fails, stop
            stats = client.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") == "failed":
                pytest.fail("Pipeline failed execution")
            
            await asyncio.sleep(1)
        
        # Verify we received messages
        assert len(messages) > 0, "Expected messages in Kafka topic"
        
        # Check message format
        sample_message = messages[0]
        assert "_destination_type" in sample_message
        assert sample_message["_destination_type"] == "kafka"
        assert "_test_pipeline" in sample_message
        
        print(f"✅ Consumed {len(messages)} messages from Kafka topic")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
