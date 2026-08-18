"""
Example S3 Pipeline

Demonstrates using S3Source to read JSON files from an S3 bucket,
apply transformations, and output to console.
"""

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import Param, RuntimeParams
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.sources.s3 import s3_source
from reflowfy.destinations.console import ConsoleDestination


class S3JsonParams(RuntimeParams, total=False):
    """Parameters for :class:`S3JsonPipeline`."""

    bucket: Annotated[Required[str], Param("S3 bucket name")]
    prefix: Annotated[NotRequired[str], Param("Object key prefix filter", default="")]
    file_pattern: Annotated[
        NotRequired[str],
        Param("Glob pattern for filtering files (e.g., *.json)", default="*.json"),
    ]


class S3JsonPipeline(AbstractPipeline[S3JsonParams]):
    """
    Example pipeline that reads JSON files from S3.

    Usage:
        POST /pipelines/s3_json_pipeline/run
        {
            "bucket": "my-data-bucket",
            "prefix": "logs/2024-01-15/",
            "file_pattern": "*.json"
        }
    """

    name = "s3_json_pipeline"
    rate_limit = 10

    def define_source(self, runtime_params):
        return s3_source(
            bucket=runtime_params.get("bucket"),
            prefix=runtime_params.get("prefix", ""),
            file_pattern=runtime_params.get("file_pattern", "*.json"),
            page_size=100,
            read_content=True,
            content_type="json",
        )

    def define_destination(self, records, runtime_params):
        return ConsoleDestination()

    def define_transformations(self, records, runtime_params):
        # Add your transformations here
        return []
