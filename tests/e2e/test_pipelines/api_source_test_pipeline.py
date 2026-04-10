"""
Paginated API Source Test Pipeline.

Pipeline that reads from a paginated API and outputs to console.
Used for E2E testing of the PaginatedAPISource connector.
"""

import os


from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_destinations import e2e_console
from tests.e2e.test_pipelines.shared_sources import e2e_paginated_api


@transformation("api_add_source_info")
def api_add_source_info(records, context):
    """Add source metadata to records."""
    for record in records:
        record["_source_type"] = "api"
        record["_test_pipeline"] = "e2e_api_source_test"
    return records


@transformation("api_log_record_count")
def api_log_record_count(records, context):
    """Log the number of records processed."""
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

    def define_source(self, runtime_params):
        return e2e_paginated_api(
            base_url=runtime_params.get("base_url", MOCK_API_URL),
            endpoint=runtime_params.get("endpoint", "/users"),
            page_size=runtime_params.get("page_size", 10),
        )

    def define_destination(self, runtime_params):
        return e2e_console()

    def define_transformations(self, runtime_params):
        return [
            api_log_record_count(),
            api_add_source_info(),
        ]
