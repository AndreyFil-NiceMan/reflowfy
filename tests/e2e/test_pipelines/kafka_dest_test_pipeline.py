"""
Kafka Destination Test Pipeline.

Pipeline that uses mock source and sends to Kafka topic.
Used for E2E testing of the KafkaDestination connector.
"""

from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_kafka


@transformation("kafka_add_dest_info")
def kafka_add_dest_info(records, context):
    """Add destination metadata to records."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_destination_type"] = "kafka"
        record["_test_pipeline"] = "kafka_dest_test"
        record["_execution_id"] = execution_id
    return records


class E2EKafkaDestTestPipeline(AbstractPipeline):
    """E2E test pipeline for Kafka destination."""
    
    name = "e2e_kafka_dest_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return e2e_mock(count=100, batch_size=10)
    
    def define_destination(self, params):
        return e2e_kafka()
    
    def define_transformations(self, params):
        return [kafka_add_dest_info()]
