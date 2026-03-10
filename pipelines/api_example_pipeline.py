"""
Example API Pipeline

Demonstrates using PaginatedAPISource to fetch data from a REST API,
apply transformations, and output to console.
"""

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.sources.api import paginated_api_source
from reflowfy.destinations.console import ConsoleDestination


class APIPipeline(AbstractPipeline):
    """
    Example pipeline that fetches data from a paginated REST API.
    
    Usage:
        POST /pipelines/api_pipeline/run
        {
            "base_url": "https://api.example.com",
            "endpoint": "/users",
            "page_size": 50
        }
    """
    
    name = "api_pipeline"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return [
            PipelineParameter(
                name="base_url",
                description="Base API URL",
                param_type=str,
                required=True,
            ),
            PipelineParameter(
                name="endpoint",
                description="API endpoint path",
                param_type=str,
                required=True,
            ),
            PipelineParameter(
                name="page_size",
                description="Records per page",
                param_type=int,
                required=False,
                default=100,
            ),
            PipelineParameter(
                name="pagination_type",
                description="Pagination style (offset, page, cursor)",
                param_type=str,
                required=False,
                default="offset",
            ),
            PipelineParameter(
                name="data_key",
                description="JSON key containing records",
                param_type=str,
                required=False,
                default="data",
            ),
        ]
    
    def define_source(self, runtime_params):
        return paginated_api_source(
            base_url=runtime_params.get("base_url"),
            endpoint=runtime_params.get("endpoint"),
            pagination_type=runtime_params.get("pagination_type", "offset"),
            page_size=runtime_params.get("page_size", 100),
            data_key=runtime_params.get("data_key", "data"),
        )
    
    def define_destination(self, runtime_params):
        return ConsoleDestination()
    
    def define_transformations(self, runtime_params):
        # Add your transformations here
        return []

