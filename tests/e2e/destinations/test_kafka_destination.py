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
import pytest
import httpx
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("E2E_KAFKA_DEST_TOPIC", "e2e-test-destination")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="module")
def check_kafka():
    """Verify Kafka is available and create test topic."""
    admin_client = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    })
    
    try:
        # Check cluster is reachable
        metadata = admin_client.list_topics(timeout=5.0)
        
        # Create test topic if it doesn't exist
        if KAFKA_TOPIC not in metadata.topics:
            topic = NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            futures = admin_client.create_topics([topic])
            
            for topic_name, future in futures.items():
                try:
                    future.result()
                    print(f"✅ Created Kafka topic: {topic_name}")
                except KafkaException as e:
                    if "already exists" not in str(e):
                        pytest.skip(f"Failed to create topic: {e}")
        else:
            print(f"✅ Kafka topic exists: {KAFKA_TOPIC}")
        
        print("✅ Kafka is running")
        
    except Exception as e:
        pytest.skip(f"Kafka not available at {KAFKA_BOOTSTRAP_SERVERS}: {e}")


@pytest.fixture
def kafka_consumer(check_kafka):
    """Create a Kafka consumer for verifying messages."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"e2e-test-consumer-{time.time()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    
    consumer.subscribe([KAFKA_TOPIC])
    
    yield consumer
    
    consumer.close()


class TestKafkaDestinationPipeline:
    """Test Kafka destination pipeline."""
    
    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is running."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_kafka_topic_exists(self, check_kafka):
        """Verify Kafka topic was created."""
        admin_client = AdminClient({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        })
        
        metadata = admin_client.list_topics(timeout=5.0)
        assert KAFKA_TOPIC in metadata.topics, f"Topic {KAFKA_TOPIC} not found"
    
    def test_pipeline_starts(self, client, check_kafka):
        """Test that pipeline can start."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_kafka_dest_test",
        })
        
        assert response.status_code == 202
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_kafka_dest_test"
    
    def test_pipeline_completes(self, client, check_kafka):
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
    
    def test_messages_in_kafka(self, client, kafka_consumer, check_kafka):
        """Test that messages are sent to Kafka topic."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_kafka_dest_test",
        })
        
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        max_wait = 120
        start = time.time()
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") in ["completed", "failed"]:
                break
            time.sleep(POLL_INTERVAL)
        
        # Consume messages from topic
        messages = []
        consume_start = time.time()
        
        while time.time() - consume_start < 10:  # 10 second timeout
            msg = kafka_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                value = json.loads(msg.value().decode("utf-8"))
                messages.append(value)
            except json.JSONDecodeError:
                pass
            
            # Stop after getting some messages
            if len(messages) >= 10:
                break
        
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
