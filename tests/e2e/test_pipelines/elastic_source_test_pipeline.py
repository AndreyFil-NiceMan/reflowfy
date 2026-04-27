"""
Elasticsearch Source Test Pipeline.

Pipeline that reads from Elasticsearch and outputs to console.
Uses a JSON query template loaded from queries/events_by_timestamp.json.
"""

import json
from pathlib import Path

from reflowfy import AbstractPipeline, PipelineParameter
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_elastic
from tests.e2e.test_pipelines.transformations import add_source_info

QUERIES_DIR = Path(__file__).parent / "queries"
ELASTIC_QUERY = json.loads((QUERIES_DIR / "events_by_timestamp.json").read_text())
INDEX_NAME = "e2e-test-events"


class E2EElasticSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for Elasticsearch source."""

    name = "e2e_elastic_source_test"
    rate_limit = 10

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
            size=50,
        )

    def define_destination(self, runtime_params):
        return e2e_console(max_records_display=5)

    def define_transformations(self, runtime_params):
        return [add_source_info()]
