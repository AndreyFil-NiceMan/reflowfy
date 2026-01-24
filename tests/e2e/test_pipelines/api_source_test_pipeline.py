"""
Paginated API Source Test Pipeline.

Pipeline that reads from a paginated API and outputs to console.
Used for E2E testing of the PaginatedAPISource connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.api import paginated_api_source
from reflowfy.destinations.console import console_destination


class AddAPISourceInfo(BaseTransformation):
    """Add source metadata to records."""
    
    name = "api_add_source_info"
    
    def apply(self, records, context):
        """Add source identification to records."""
        for record in records:
            record["_source_type"] = "api"
            record["_test_pipeline"] = "e2e_api_source_test"
        return records


class LogRecordCount(BaseTransformation):
    """Log the number of records processed."""
    
    name = "api_log_record_count"
    
    def apply(self, records, context):
        """Log record count."""
        print(f"  📊 API Source: Processing {len(records)} records")
        return records


# Configuration from environment
# Inside Docker, this will be http://e2e-mock-api:8092
# Outside Docker (e.g., running tests locally), it defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class E2EApiSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for paginated API source."""
    
    name = "e2e_api_source_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return [
            PipelineParameter(
                name="base_url",
                description="Base API URL",
                param_type=str,
                required=False,
                default=MOCK_API_URL,
            ),
            PipelineParameter(
                name="endpoint",
                description="API endpoint path",
                param_type=str,
                required=False,
                default="/users",
            ),
            PipelineParameter(
                name="page_size",
                description="Records per page",
                param_type=int,
                required=False,
                default=10,
            ),
        ]
    
    def define_source(self, params):
        return paginated_api_source(
            base_url=params.get("base_url", MOCK_API_URL),
            endpoint=params.get("endpoint", "/users"),
            pagination_type="offset",
            page_size=params.get("page_size", 10),
            data_key="data",
            total_key="total",
            offset_param="offset",
            limit_param="limit",
        )
    
    def define_destination(self, params):
        return console_destination(
            pretty_print=True,
            max_records_display=5,
        )
    
    def define_transformations(self, params):
        return [
            LogRecordCount(),
            AddAPISourceInfo(),
        ]


pipeline_registry.register(E2EApiSourceTestPipeline())
