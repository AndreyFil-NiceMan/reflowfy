"""
Elasticsearch routed-destination E2E pipeline.

Reads from Elasticsearch via scroll, enriches each record with metadata,
and routes each job to one of two destinations based on transformation output.
"""

import json
import os
from pathlib import Path

from reflowfy import AbstractPipeline, PipelineParameter
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_elastic
from tests.e2e.test_pipelines.transformations import elastic_add_metadata_and_route

QUERIES_DIR = Path(__file__).parent / "queries"
ELASTIC_QUERY = json.loads((QUERIES_DIR / "events_by_timestamp.json").read_text())
INDEX_NAME = "e2e-test-events"
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")


class E2EElasticRoutedDestinationsPipeline(AbstractPipeline):
    """E2E pipeline that routes each elastic scroll job to one of two destinations."""

    name = "e2e_elastic_routed_destinations"
    rate_limit = 20

    def define_parameters(self):
        return [
            PipelineParameter(name="start_time", required=True),
            PipelineParameter(name="end_time", required=True),
        ]

    def define_source(self, runtime_params):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query=ELASTIC_QUERY,
            scroll="2m",
            size=40,
        )

    def define_transformations(self, records, runtime_params):
        return [elastic_add_metadata_and_route()]

    def define_destination(self, records, runtime_params):
        route_target = records[0].get("_route_target", "primary") if records else "primary"

        if route_target == "secondary":
            return api_destination(
                url=MOCK_HTTP_URL,
                method="POST",
                auth_type="bearer",
                auth_token="test-webhook-token",
                timeout=30.0,
                params={"route": "secondary"},
                body={"records": records, "destination_name": "secondary", "runtime_params": runtime_params},
            )

        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            timeout=30.0,
            params={"route": "primary"},
            body={"records": records, "destination_name": "primary", "runtime_params": runtime_params},
        )
