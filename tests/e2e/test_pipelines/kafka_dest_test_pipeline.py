"""
Kafka Destination Test Pipeline.

Pipeline that uses mock source and sends to Kafka topic.
Used for E2E testing of the KafkaDestination connector.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_kafka
from tests.e2e.test_pipelines.transformations import kafka_add_dest_info


class E2EKafkaDestTestPipeline(AbstractPipeline):
    """E2E test pipeline for Kafka destination."""

    name = "e2e_kafka_dest_test"
    rate_limit = 10

    def define_source(self, params):
        return e2e_mock(count=100, batch_size=10)

    def define_destination(self, params):
        return e2e_kafka()

    def define_transformations(self, params):
        return [kafka_add_dest_info()]
