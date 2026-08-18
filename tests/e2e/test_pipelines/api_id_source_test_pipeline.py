"""
ID-Based API Source Test Pipeline.

Pipeline that fetches resources by ID from an API and outputs to console.
Used for E2E testing of the IDBasedAPISource connector.
"""

import os

from typing import Any, List

from typing_extensions import Annotated, NotRequired

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Records,
    RuntimeParams,
    Transformations,
)
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_id_based_api
from tests.e2e.test_pipelines.transformations import (
    api_id_add_source_info,
    api_id_log_record_count,
)

# Inside Docker: http://e2e-mock-api:8092 — outside Docker defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class ApiIdSourceParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EApiIdSourceTestPipeline`."""

    base_url: Annotated[NotRequired[str], Param("Base API URL", default=MOCK_API_URL)]
    ids: Annotated[
        NotRequired[List[Any]], Param("List of user IDs to fetch", default=[1, 2, 3, 4, 5])
    ]
    batch_size: Annotated[NotRequired[int], Param("IDs per job batch", default=2)]


class E2EApiIdSourceTestPipeline(AbstractPipeline[ApiIdSourceParams]):
    """E2E test pipeline for ID-based API source."""

    name = "e2e_api_id_source_test"
    rate_limit = 600  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_id_based_api(
            base_url=runtime_params.get("base_url", MOCK_API_URL),
            ids=runtime_params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=runtime_params.get("batch_size", 2),
        )

    def define_destination(
        self, records: Records, runtime_params: ApiIdSourceParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: Records, runtime_params: ApiIdSourceParams
    ) -> Transformations:
        return [
            api_id_log_record_count(),
            api_id_add_source_info(),
        ]
