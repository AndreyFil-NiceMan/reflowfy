"""
Elasticsearch docs_per_job Test Pipeline.

Reads all documents from Elasticsearch with ``docs_per_job`` set, so the
manager fans the query out into ``ceil(count / DOCS_PER_JOB)`` parallel
sliced-scroll jobs instead of a single job.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_elastic
from tests.e2e.test_pipelines.transformations import add_source_info

INDEX_NAME = "e2e-test-events"

# 500 seeded docs / 50 => 10 jobs. The test computes the expected job count
# from the live ES doc count, so this stays correct if the seed size changes.
DOCS_PER_JOB = 50


class E2EElasticDocsPerJobPipeline(AbstractPipeline):
    """E2E test pipeline exercising count-derived slicing (docs_per_job)."""

    name = "e2e_elastic_docs_per_job_test"
    rate_limit = 10

    def define_source(self, runtime_params):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query={"query": {"match_all": {}}},
            scroll="2m",
            size=50,
            docs_per_job=DOCS_PER_JOB,
        )

    def define_destination(self, records, runtime_params):
        return e2e_console(max_records_display=5)

    def define_transformations(self, records, runtime_params):
        return [add_source_info()]
