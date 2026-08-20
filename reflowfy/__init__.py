"""
Reflowfy - A horizontally scalable data movement and transformation framework.

This framework enables users to define pipelines that:
- Fetch data from sources (Elastic, DBs, APIs, S3, etc.)
- Apply custom transformations
- Send data to destinations (Kafka, APIs, queues, etc.)

It is Kafka-based, Kubernetes-native, and order-independent for maximum parallelism.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.core.exceptions import PipelineError
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.types import Record, Records, Transformations
from reflowfy.core.query_loader import load_query, load_query_text
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import Param, RuntimeParams
from reflowfy.execution.job_runner import chunk, job
from reflowfy.observability.logging import get_logger
from reflowfy.transformations.base import BaseTransformation, TransformationError
from reflowfy.sources.base import BaseSource
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import SourceError
from reflowfy.destinations.base import DestinationError
from reflowfy.sources.elastic import elastic_source
from reflowfy.sources.sql import sql_source
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.console import console_destination

# Decorators for reusable components
from reflowfy.sources.decorators import source, source_registry
from reflowfy.destinations.decorators import destination, destination_registry
from reflowfy.transformations.decorators import transformation

__version__ = "1.0.31"

__all__ = [
    "load_query",
    "load_query_text",
    "AbstractPipeline",
    "IdBasedPipeline",
    "PipelineParameter",
    # Typed runtime_params: subclass RuntimeParams, pass it as
    # AbstractPipeline[MyParams], annotate defaults with Param.
    "RuntimeParams",
    # Annotating your hooks: `Records` == List[Dict[str, Any]], and the
    # base classes the hooks return.
    "Record",
    "Records",
    "Transformations",
    "BaseSource",
    "BaseDestination",
    "Param",
    "pipeline_registry",
    "chunk",
    "job",
    "BaseTransformation",
    # Logging — module-level, works anywhere (no `self` needed):
    #   logger = get_logger(__name__)
    "get_logger",
    # Exceptions
    "PipelineError",
    "SourceError",
    "DestinationError",
    "TransformationError",
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
