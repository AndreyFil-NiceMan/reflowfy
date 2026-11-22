"""
Example reusable destination configuration.

A plain function that returns a configured destination: import it from any
pipeline. No decorator needed. (To send each record to its own destination, use
@destination(per_record=True) — see the README.)
"""

from typing import Any
import os
from reflowfy import BaseDestination, get_logger, kafka_destination

logger = get_logger(__name__)


def example_kafka(**overrides: Any) -> BaseDestination:
    """
    Example Kafka destination configuration.

    Usage in a pipeline:
        from destinations.example_destination import example_kafka

        def define_destination(
            self, records: Records, runtime_params: MyParams
        ) -> BaseDestination:
            return example_kafka(topic="my-output-topic")
    """
    logger.debug(
        "Building kafka destination for topic %s", overrides.get("topic", "default-output")
    )
    return kafka_destination(
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:9092"),
        topic=overrides.get("topic", "default-output"),
        compression_type=overrides.get("compression_type", "gzip"),
    )
