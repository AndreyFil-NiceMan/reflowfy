"""
Kafka Lag Health Check Test Pipeline.

Pipeline used for E2E testing of the Kafka destination lag health check feature.
The lag_threshold is driven at runtime via runtime_params so a single pipeline
definition covers both the "lag blocks dispatch" and "lag allows dispatch" scenarios.
"""

import os

from reflowfy import AbstractPipeline
from reflowfy.destinations.kafka import kafka_destination
from tests.e2e.test_pipelines.sources import e2e_mock

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",  # Docker-internal PLAINTEXT (set by run script)
    os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095"),
)
LAG_TEST_TOPIC = os.getenv("E2E_LAG_TEST_TOPIC", "e2e-lag-health-check")
LAG_TEST_GROUP = os.getenv("E2E_LAG_TEST_GROUP", "e2e-lag-test-consumer-group")


class E2EKafkaLagHealthCheckPipeline(AbstractPipeline):
    """E2E test pipeline for Kafka destination lag health check."""

    name = "e2e_kafka_lag_health_check"
    rate_limit = 10

    def define_source(self, runtime_params):
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, records, runtime_params):
        threshold = runtime_params.get("lag_threshold", 5000)
        return kafka_destination(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=LAG_TEST_TOPIC,
            lag_health_check_enabled=True,
            consumer_group_id=LAG_TEST_GROUP,
            lag_threshold=int(threshold),
            lag_check_timeout=15.0,
        )

    def define_transformations(self, records, runtime_params):
        return []
