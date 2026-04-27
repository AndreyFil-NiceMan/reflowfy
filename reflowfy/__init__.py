"""
Reflowfy - A horizontally scalable data movement and transformation framework.

This framework enables users to define pipelines that:
- Fetch data from sources (Elastic, DBs, APIs, S3, etc.)
- Apply custom transformations
- Send data to destinations (Kafka, APIs, queues, etc.)

It is Kafka-based, Kubernetes-native, and order-independent for maximum parallelism.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.registry import pipeline_registry
from reflowfy.transformations.base import BaseTransformation
from reflowfy.sources.elastic import elastic_source
from reflowfy.sources.sql import sql_source
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.console import console_destination

# Decorators for reusable components
from reflowfy.sources.decorators import source, source_registry
from reflowfy.destinations.decorators import destination, destination_registry
from reflowfy.transformations.decorators import transformation

__version__ = "0.38"

__all__ = [
    "AbstractPipeline",
    "IdBasedPipeline",
    "PipelineParameter",
    "pipeline_registry",
    "BaseTransformation",
    "elastic_source",
    "sql_source",
    "kafka_destination",
    "api_destination",
    "console_destination",
    # Decorators
    "source",
    "destination",
    "transformation",
    "source_registry",
    "destination_registry",
]

