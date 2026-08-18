"""
API Destination Test Pipeline.

Pipeline that uses mock source and sends to the API destination endpoint.
Demonstrates runtime_params flowing into ApiDestination params and body.
"""

from typing import Literal

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Records,
    RuntimeParams,
    Transformations,
)
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import api_add_dest_info

import os

MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")


class ApiDestParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EApiDestTestPipeline`."""

    tenant_id: Annotated[Required[str], Param("Tenant identifier sent as a URL query param")]
    env: Annotated[
        NotRequired[Literal["staging", "production"]],
        Param("Environment sent as a URL query param", default="staging"),
    ]
    app_name: Annotated[
        NotRequired[str],
        Param("Application name merged into the request body", default="reflowfy"),
    ]


class E2EApiDestTestPipeline(AbstractPipeline[ApiDestParams]):
    """
    E2E test pipeline for API destination.

    Accepts runtime params that flow directly into the API destination's
    URL query string (params) and request body (body). The pipeline builds
    the request body explicitly in define_destination, combining records
    with static and dynamic fields.
    """

    name = "e2e_api_dest_test"
    rate_limit = 3000  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_mock(count=100, batch_size=10)

    def define_destination(
        self, records: Records, runtime_params: ApiDestParams
    ) -> BaseDestination:
        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            timeout=30.0,
            # URL query params built from runtime_params
            params={
                "tenant_id": runtime_params["tenant_id"],
                "env": runtime_params.get("env", "staging"),
            },
            # Body includes records plus static and dynamic fields.
            # execution_id is included so E2E tests can scope mock-server
            # assertions to exactly this run (the mock server's /stats batch
            # window and counters are shared/global across the test module).
            body={
                "records": records,
                "source": "reflowfy",
                "app_name": runtime_params.get("app_name", "reflowfy"),
                "execution_id": runtime_params.get("execution_id"),
            },
        )

    def define_transformations(
        self, records: Records, runtime_params: ApiDestParams
    ) -> Transformations:
        return [api_add_dest_info()]
