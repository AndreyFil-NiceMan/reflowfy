"""
Reflowfy - A horizontally scalable data movement and transformation framework.

This framework enables users to define pipelines that:
- Fetch data from sources (Elastic, DBs, APIs, S3, etc.)
- Apply custom transformations
- Send data to destinations (Kafka, APIs, queues, etc.)

It is Kafka-based, Kubernetes-native, and order-independent for maximum parallelism.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.core.registry import pipeline_registry
from reflowfy.transformations.base import BaseTransformation
from reflowfy.sources.elastic import elastic_source
from reflowfy.sources.sql import sql_source
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.http import http_destination
from reflowfy.destinations.console import console_destination

__version__ = "0.1.34"

__all__ = [
    "AbstractPipeline",
    "PipelineParameter",
    "pipeline_registry",
    "BaseTransformation",
    "elastic_source",
    "sql_source",
    "kafka_destination",
    "http_destination",
    "console_destination",
]

