"""
Kafka Destination Test Pipeline.

Pipeline that uses mock source and sends to Kafka topic.
Used for E2E testing of the KafkaDestination connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source, generate_sample_data
from reflowfy.destinations.kafka import kafka_destination


class AddDestinationInfo(BaseTransformation):
    """Add destination metadata to records."""
    
    name = "kafka_add_dest_info"
    
    def apply(self, records, context):
        """Add destination identification to records."""
        execution_id = context.get("execution_id", "unknown")
        for record in records:
            record["_destination_type"] = "kafka"
            record["_test_pipeline"] = "kafka_dest_test"
            record["_execution_id"] = execution_id
        return records


# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("E2E_KAFKA_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("E2E_KAFKA_DEST_TOPIC", "e2e-test-destination")
SAMPLE_DATA = generate_sample_data(count=100)


class E2EKafkaDestTestPipeline(AbstractPipeline):
    """E2E test pipeline for Kafka destination."""
    
    name = "e2e_kafka_dest_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return mock_source(
            data=SAMPLE_DATA,
            batch_size=10,
        )
    
    def define_destination(self, params):
        return kafka_destination(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            compression_type="gzip",
            batch_size=16384,
            linger_ms=10,
        )
    
    def define_transformations(self, params):
        return [AddDestinationInfo()]


pipeline_registry.register(E2EKafkaDestTestPipeline())
