"""
Example S3 Pipeline

Demonstrates using S3Source to read JSON files from an S3 bucket,
apply transformations, and output to console.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.sources.s3 import s3_source
from reflowfy.destinations.console import ConsoleDestination


class S3JsonPipeline(AbstractPipeline):
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
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return [
            PipelineParameter(
                name="bucket",
                description="S3 bucket name",
                param_type=str,
                required=True,
            ),
            PipelineParameter(
                name="prefix",
                description="Object key prefix filter",
                param_type=str,
                required=False,
                default="",
            ),
            PipelineParameter(
                name="file_pattern",
                description="Glob pattern for filtering files (e.g., *.json)",
                param_type=str,
                required=False,
                default="*.json",
            ),
        ]
    
    def define_source(self, runtime_params):
        return s3_source(
            bucket=runtime_params.get("bucket"),
            prefix=runtime_params.get("prefix", ""),
            file_pattern=runtime_params.get("file_pattern", "*.json"),
            page_size=100,
            read_content=True,
            content_type="json",
        )
    
    def define_destination(self, runtime_params):
        return ConsoleDestination()
    
    def define_transformations(self, runtime_params):
        # Add your transformations here
        return []


# Register the pipeline
s3_json_pipeline = S3JsonPipeline()
