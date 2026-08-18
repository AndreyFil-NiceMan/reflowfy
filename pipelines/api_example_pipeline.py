"""
Example API Pipeline

Demonstrates using IDBasedAPISource to fetch data from a REST API,
apply transformations, and output to console.
"""

from typing import Any, List

from typing_extensions import Annotated, NotRequired

from reflowfy import BaseDestination, Param, Records, RuntimeParams, Transformations
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.sources.api import id_based_api_source
from reflowfy.destinations.console import ConsoleDestination


class APIParams(RuntimeParams, total=False):
    """Parameters for :class:`APIPipeline`."""

    base_url: Annotated[
        NotRequired[str],
        Param("Base API URL", default="https://jsonplaceholder.typicode.com"),
    ]
    ids: Annotated[NotRequired[List[Any]], Param("List of IDs to fetch", default=[1, 2, 3, 4, 5])]
    batch_size: Annotated[NotRequired[int], Param("Number of IDs per request batch", default=2)]


class APIPipeline(AbstractPipeline[APIParams]):
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

    def define_source(self, runtime_params):
        return id_based_api_source(
            base_url=runtime_params.get("base_url", "https://jsonplaceholder.typicode.com"),
            endpoint_template="/posts/{id}",
            ids=runtime_params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=runtime_params.get("batch_size", 2),
        )

    def define_destination(self, records: Records, runtime_params: APIParams) -> BaseDestination:
        return ConsoleDestination()

    def define_transformations(
        self, records: Records, runtime_params: APIParams
    ) -> Transformations:
        # Add your transformations here
        return []
