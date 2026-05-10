"""
Paginated API Source Test Pipeline.

Pipeline that reads from a paginated API and outputs to console.
Used for E2E testing of the PaginatedAPISource connector.
"""

import os

from reflowfy import AbstractPipeline, PipelineParameter
from tests.e2e.test_pipelines.sources import e2e_paginated_api
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.transformations import (
    api_log_record_count,
    api_add_source_info,
)

# Inside Docker: http://e2e-mock-api:8092 — outside Docker defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class E2EApiSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for paginated API source."""

    name = "e2e_api_source_test"
    rate_limit = 10

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

    def define_destination(self, records, runtime_params):
        return e2e_console()

    def define_transformations(self, records, runtime_params):
        return [
            api_log_record_count(),
            api_add_source_info(),
        ]
