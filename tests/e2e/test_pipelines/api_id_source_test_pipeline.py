"""
ID-Based API Source Test Pipeline.

Pipeline that fetches resources by ID from an API and outputs to console.
Used for E2E testing of the IDBasedAPISource connector.
"""

import os

from reflowfy import AbstractPipeline, PipelineParameter
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_id_based_api
from tests.e2e.test_pipelines.transformations import (
    api_id_add_source_info,
    api_id_log_record_count,
)

# Inside Docker: http://e2e-mock-api:8092 — outside Docker defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class E2EApiIdSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for ID-based API source."""

    name = "e2e_api_id_source_test"
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
                name="ids",
                description="List of user IDs to fetch",
                param_type=list,
                required=False,
                default=[1, 2, 3, 4, 5],
            ),
            PipelineParameter(
                name="batch_size",
                description="IDs per job batch",
                param_type=int,
                required=False,
                default=2,
            ),
        ]

    def define_source(self, runtime_params):
        return e2e_id_based_api(
            base_url=runtime_params.get("base_url", MOCK_API_URL),
            ids=runtime_params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=runtime_params.get("batch_size", 2),
        )

    def define_destination(self, runtime_params):
        return e2e_console()

    def define_transformations(self, runtime_params):
        return [
            api_id_log_record_count(),
            api_id_add_source_info(),
        ]
