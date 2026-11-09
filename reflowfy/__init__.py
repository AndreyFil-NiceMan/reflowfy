"""
Reflowfy - A horizontally scalable data movement and transformation framework.

This framework enables users to define pipelines that:
- Fetch data from sources (Elastic, DBs, APIs, S3, etc.)
- Apply custom transformations
- Send data to destinations (Kafka, APIs, queues, etc.)

It is Kafka-based, Kubernetes-native, and order-independent for maximum parallelism.
"""

from typing import TYPE_CHECKING, Any

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter, ScheduledRun
from reflowfy.core.exceptions import PipelineError, RecordValidationError, SkipJob, SkipRecord
from reflowfy.core.execution_summary import ExecutionSummary
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.job_retry import JobRetryConfig
from reflowfy.core.types import Record, Records, Transformations, TypedRecord
from reflowfy.core.query_loader import load_query, load_query_text
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import Param, RuntimeParams
from reflowfy.core.secrets import SecretError, secret
from reflowfy.core.planning import chunk
from reflowfy.execution.job_runner import job
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
from reflowfy.destinations.base import DestinationError, PermanentDestinationError, RetryConfig
from reflowfy.factories.source_factory import SourceRoundTripError
from reflowfy.sources.api import id_based_api_source
from reflowfy.sources.elastic import elastic_source
from reflowfy.sources.mock import mock_source
from reflowfy.sources.sql import SqlTemplateError, sql_source
from reflowfy.destinations.fan_out import fan_out
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.console import console_destination

# Decorators for reusable components
from reflowfy.sources.decorators import source, source_registry
from reflowfy.destinations.decorators import (
    destination,
    destination_registry,
    record_destination,
)
from reflowfy.destinations.record import RecordDestination, RecordDestinationError
from reflowfy.transformations.decorators import record_transformation, transformation

if TYPE_CHECKING:
    from reflowfy.reflow_manager.lifecycle import run_pipeline
    from reflowfy.sources.s3 import s3_source

# Loaded on first use: s3_source needs the optional boto3, and run_pipeline pulls in
# the manager's database layer, which a worker or `reflowfy test` never needs.
_LAZY = {
    "s3_source": "reflowfy.sources.s3",
    "run_pipeline": "reflowfy.reflow_manager.lifecycle",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module 'reflowfy' has no attribute {name!r}")
    import importlib

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        if name == "s3_source":
            raise ImportError(
                "reflowfy.s3_source needs boto3: pip install boto3"
            ) from exc
        raise
    value = getattr(module, name)
    globals()[name] = value
    return value


__version__ = "1.1.4"

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
    "RecordDestinationError",
    "TransformationError",
    "StepOutputError",
    "PermanentDestinationError",
    "SecretError",
    "SourceRoundTripError",
    "SqlTemplateError",
    # Retry knobs: in-job destination sends, and whole jobs re-run (opt-in).
    "RetryConfig",
    "JobRetryConfig",
    # Lifecycle hooks: what on_execution_* receive, and chaining another run.
    "ExecutionSummary",
    "run_pipeline",
    # A credential read from the worker's environment, never serialized.
    "secret",
    "elastic_source",
    "sql_source",
    "s3_source",
    "id_based_api_source",
    "mock_source",
    "kafka_destination",
    "fan_out",
    "api_destination",
    "console_destination",
    # Decorators
    "source",
    "destination",
    "record_destination",
    "RecordDestination",
    "transformation",
    "record_transformation",
    "source_registry",
    "destination_registry",
]
