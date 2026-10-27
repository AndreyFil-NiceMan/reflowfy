"""
Reflowfy - A horizontally scalable data movement and transformation framework.

This framework enables users to define pipelines that:
- Fetch data from sources (Elastic, DBs, APIs, S3, etc.)
- Apply custom transformations
- Send data to destinations (Kafka, APIs, queues, etc.)

It is Kafka-based, Kubernetes-native, and order-independent for maximum parallelism.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter, ScheduledRun
from reflowfy.core.exceptions import PipelineError, RecordValidationError, SkipJob, SkipRecord
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.types import Record, Records, Transformations, TypedRecord
from reflowfy.core.query_loader import load_query, load_query_text
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import Param, RuntimeParams
from reflowfy.execution.job_runner import chunk, job
from reflowfy.observability.logging import get_logger
from reflowfy.transformations.base import (
    AsyncTransformation,
    BaseTransformation,
    RecordTransformation,
    StepOutputError,
    TransformationError,
)
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
from reflowfy.transformations.decorators import record_transformation, transformation

__version__ = "1.1.1"

__all__ = [
    "load_query",
    "load_query_text",
    "AbstractPipeline",
    "IdBasedPipeline",
    "PipelineParameter",
    # Declaring `schedules = [ScheduledRun(...), ...]` on a pipeline.
    "ScheduledRun",
    # Typed runtime_params: subclass RuntimeParams, pass it as
    # AbstractPipeline[MyParams], annotate defaults with Param.
    "RuntimeParams",
    # Annotating your hooks: `Records` == List[Dict[str, Any]], and the
    # base classes the hooks return.
    "Record",
    "Records",
    "Transformations",
    # A record with a known shape, {"data": T}; also what record_type checks.
    "TypedRecord",
    "BaseSource",
    "BaseDestination",
    "Param",
    "pipeline_registry",
    "chunk",
    "job",
    "BaseTransformation",
    # Async transformations: `async def apply_async`, or one coroutine per record
    # run concurrently (`@record_transformation`).
    "AsyncTransformation",
    "RecordTransformation",
    # Logging — module-level, works anywhere (no `self` needed):
    #   logger = get_logger(__name__)
    "get_logger",
    # Exceptions
    "PipelineError",
    "SkipJob",
    "SkipRecord",
    "RecordValidationError",
    "SourceError",
    "DestinationError",
    "TransformationError",
    "StepOutputError",
    "elastic_source",
    "sql_source",
    "kafka_destination",
    "api_destination",
    "console_destination",
    # Decorators
    "source",
    "destination",
    "transformation",
    "record_transformation",
    "source_registry",
    "destination_registry",
]
