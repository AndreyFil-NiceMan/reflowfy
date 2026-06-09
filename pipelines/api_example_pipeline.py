"""
Example API Pipeline

Demonstrates using IDBasedAPISource to fetch data from a REST API,
apply transformations, and output to console.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.sources.api import id_based_api_source
from reflowfy.destinations.console import ConsoleDestination


class APIPipeline(AbstractPipeline):
    """
    Example pipeline that fetches data from a REST API using ID-based source.

    Usage:
        POST /pipelines/api_pipeline/run
        {
            "base_url": "https://jsonplaceholder.typicode.com",
            "ids": [1, 2, 3, 4, 5],
            "batch_size": 2
        }
    """

    name = "api_pipeline"
    rate_limit = 10

    def define_parameters(self):
        return [
            PipelineParameter(
                name="base_url",
                description="Base API URL",
                param_type=str,
                required=False,
                default="https://jsonplaceholder.typicode.com",
            ),
            PipelineParameter(
                name="ids",
                description="List of IDs to fetch",
                param_type=list,
                required=False,
                default=[1, 2, 3, 4, 5],
            ),
            PipelineParameter(
                name="batch_size",
                description="Number of IDs per request batch",
                param_type=int,
                required=False,
                default=2,
            ),
        ]

    def define_source(self, runtime_params):
        return id_based_api_source(
            base_url=runtime_params.get("base_url", "https://jsonplaceholder.typicode.com"),
            endpoint_template="/posts/{id}",
            ids=runtime_params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=runtime_params.get("batch_size", 2),
        )

    def define_destination(self, runtime_params):
        return ConsoleDestination()

    def define_transformations(self, runtime_params):
        # Add your transformations here
        return []
