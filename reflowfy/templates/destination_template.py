"""
Example reusable destination configuration.

Use the @destination decorator to create named destination configurations
that can be reused across multiple pipelines.
"""

from typing import Any
import os
from reflowfy import destination, kafka_destination


@destination("example_kafka")
def example_kafka(**overrides: Any):
    """
    Example Kafka destination configuration.

    Usage in a pipeline:
        from destinations.example_destination import example_kafka

        def define_destination(self, records, runtime_params):
            return example_kafka(topic="my-output-topic")
    """
    return kafka_destination(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        topic=overrides.get("topic", "default-output"),
        compression_type=overrides.get("compression_type", "gzip"),
    )
