"""
Elasticsearch Source Test Pipeline.

Pipeline that reads from Elasticsearch and outputs to console.
Uses a JSON query template loaded from queries/events_by_timestamp.json.
Used for E2E testing of the ElasticSource connector.
"""

import json
from pathlib import Path

from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_destinations import e2e_console
from tests.e2e.test_pipelines.shared_sources import e2e_elastic

# Load query from the queries/ folder
QUERIES_DIR = Path(__file__).parent / "queries"
ELASTIC_QUERY = json.loads((QUERIES_DIR / "events_by_timestamp.json").read_text())
INDEX_NAME = "e2e-test-events"


@transformation("add_source_info")
def add_source_info(records, context):
    """Add source metadata to records."""
    for record in records:
        record["_source_type"] = "elasticsearch"
        record["_test_pipeline"] = "elastic_source_test"
    return records


class E2EElasticSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for Elasticsearch source."""

    name = "e2e_elastic_source_test"
    rate_limit = {"jobs_per_second": 10}

    def define_parameters(self):
        return [
            PipelineParameter(name="start_time", required=True),
            PipelineParameter(name="end_time", required=True),
        ]

    def define_source(self, params):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query=ELASTIC_QUERY,
            scroll="2m",
            size=50,
        )

    def define_destination(self, params):
        return e2e_console(max_records_display=5)

    def define_transformations(self, params):
        return [add_source_info()]
