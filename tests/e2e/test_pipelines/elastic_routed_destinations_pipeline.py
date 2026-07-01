"""
Elasticsearch routed-destination E2E pipeline.

Reads from Elasticsearch via sliced scroll (worker-side sourcing: each
``ElasticSource.split()`` slice is one job), enriches each record with
metadata, and routes each job to one of two destinations based on the
majority content-based route hint among the job's own records.
"""

import os

from reflowfy import AbstractPipeline, PipelineParameter
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_elastic
from tests.e2e.test_pipelines.transformations import elastic_add_metadata_and_route

INDEX_NAME = "e2e-test-events"
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")

# v2 worker-side sourcing: ElasticSource.split() yields one job per slice
# (not one job per scroll page as in v1). Use multiple slices so this
# pipeline still produces multiple parallel jobs to route across both
# destinations. 8 slices keeps the (rare, ~0.8%) chance that every slice's
# content-based majority lands on the same side negligible for CI.
NUM_SLICES = 8


class E2EElasticRoutedDestinationsPipeline(AbstractPipeline):
    """E2E pipeline that routes each elastic slice-job to one of two destinations."""

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
            base_query=self.load_query("events_by_timestamp.json"),
            scroll="2m",
            size=40,
            num_slices=NUM_SLICES,
        )

    def define_transformations(self, records, runtime_params):
        return [elastic_add_metadata_and_route()]

    def define_destination(self, records, runtime_params):
        # v2: no per-job "page_num" is available anymore (worker-side sourcing
        # only passes one source descriptor + execution metadata per job), so
        # the route hint is computed per-record from content (see
        # elastic_add_metadata_and_route). A job is one ElasticSource slice
        # containing many records, so route the *whole job* to whichever
        # destination the majority of its own records point to — this keeps
        # exactly one destination call per job (no data loss) while still
        # exercising both destinations across the N slices.
        if records:
            secondary_votes = sum(1 for r in records if r.get("_route_target") == "secondary")
            route_target = "secondary" if secondary_votes * 2 > len(records) else "primary"
        else:
            route_target = "primary"

        if route_target == "secondary":
            return api_destination(
                url=MOCK_HTTP_URL,
                method="POST",
                auth_type="bearer",
                auth_token="test-webhook-token",
                timeout=30.0,
                params={"route": "secondary"},
                body={
                    "records": records,
                    "destination_name": "secondary",
                    "runtime_params": runtime_params,
                },
            )

        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            timeout=30.0,
            params={"route": "primary"},
            body={
                "records": records,
                "destination_name": "primary",
                "runtime_params": runtime_params,
            },
        )
