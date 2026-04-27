"""
API Destination Test Pipeline.

Pipeline that uses mock source and sends to the API destination endpoint.
Demonstrates runtime_params flowing into ApiDestination params and body.
"""

from reflowfy import AbstractPipeline, PipelineParameter
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import api_add_dest_info

import os

MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")


class E2EApiDestTestPipeline(AbstractPipeline):
    """
    E2E test pipeline for API destination.

    Accepts runtime params that flow directly into the API destination's
    URL query string (params) and request body (body).
    """

    name = "e2e_api_dest_test"
    rate_limit = 50

    def define_parameters(self):
        return [
            PipelineParameter(
                name="tenant_id",
                description="Tenant identifier sent as a URL query param",
                required=True,
                param_type=str,
            ),
            PipelineParameter(
                name="env",
                description="Environment sent as a URL query param",
                required=False,
                param_type=str,
                choices=["staging", "production"],
                default="staging",
            ),
            PipelineParameter(
                name="app_name",
                description="Application name merged into the request body",
                required=False,
                param_type=str,
                default="reflowfy",
            ),
        ]

    def define_source(self, runtime_params):
        return e2e_mock(count=100, batch_size=10)

    def define_destination(self, runtime_params):
        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            batch_requests=True,
            timeout=30.0,
            # URL query params built from runtime_params
            params={
                "tenant_id": runtime_params["tenant_id"],
                "env": runtime_params["env"],
            },
            # Static + dynamic body fields merged into every request
            body={
                "source": "reflowfy",
                "app_name": runtime_params["app_name"],
            },
        )

    def define_transformations(self, runtime_params):
        return [api_add_dest_info()]
