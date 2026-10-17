"""
Elasticsearch range-window Test Pipeline.

``window_mode="range"`` cuts the index into value ranges of the first sort key and
each job opens its own PIT. Sorting by ``user_id`` — a random integer from 1 to 100
over the 500 seeded docs, so about five documents share every value — makes equal
values straddle the window boundaries, which is where a range window could lose or
repeat documents. Every doc must reach the mock webhook exactly once.
"""

import os
from typing import Any, Dict

from reflowfy import AbstractPipeline, BaseDestination, Records, RuntimeParams, Transformations
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_elastic

INDEX_NAME = "e2e-test-events"
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")

QUERY: Dict[str, Any] = {"query": {"match_all": {}}, "sort": [{"user_id": "asc"}]}


class E2EElasticRangeWindowsPipeline(AbstractPipeline[RuntimeParams]):
    """E2E pipeline exercising window_mode="range" with heavily tied sort values."""

    name = "e2e_elastic_range_windows_test"
    rate_limit = 6000  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query=QUERY,
            size=40,
            docs_per_job=60,
            window_mode="range",
        )

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            health_check_enabled=False,
            body={"records": records},
        )

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []
