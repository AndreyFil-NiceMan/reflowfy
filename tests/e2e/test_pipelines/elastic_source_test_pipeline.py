"""
Elasticsearch Source Test Pipeline.

Pipeline that reads from Elasticsearch and outputs to console.
Uses a JSON query template loaded from queries/events_by_timestamp.json.
"""

from typing_extensions import Required

from reflowfy import AbstractPipeline, BaseDestination, Records, RuntimeParams, Transformations
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_elastic
from tests.e2e.test_pipelines.transformations import add_source_info

INDEX_NAME = "e2e-test-events"


class ElasticSourceParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EElasticSourceTestPipeline`."""

    start_time: Required[str]
    end_time: Required[str]


class E2EElasticSourceTestPipeline(AbstractPipeline[ElasticSourceParams]):
    """E2E test pipeline for Elasticsearch source."""

    name = "e2e_elastic_source_test"
    rate_limit = 600  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query=self.load_query("events_by_timestamp.json"),
            scroll="2m",
            size=50,
        )

    def define_destination(
        self, records: Records, runtime_params: ElasticSourceParams
    ) -> BaseDestination:
        return e2e_console(max_records_display=5)

    def define_transformations(
        self, records: Records, runtime_params: ElasticSourceParams
    ) -> Transformations:
        return [add_source_info()]
