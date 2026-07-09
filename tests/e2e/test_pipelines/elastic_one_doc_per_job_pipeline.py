"""
Elasticsearch one-doc-per-job Test Pipeline.

Exercises deterministic positional windowing: with ``docs_per_job=1`` the
manager cuts the matched docs into exactly one job per document via a
``search_after`` pre-scan (not hash-partitioned sliced scroll), so there are
no empty jobs and every doc is delivered exactly once.

Reads a deterministic subset of the seeded index (``metadata.batch == 0``,
which is the first 100 seeded docs) and writes each single-doc job to the mock
webhook server so the test can assert an exact record cover.
"""

import os

from reflowfy import AbstractPipeline
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_elastic
from tests.e2e.test_pipelines.transformations import add_source_info

INDEX_NAME = "e2e-test-events"
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")

# Deterministic subset: seed assigns metadata.batch = i // 100, so batch 0 is
# exactly the first 100 docs regardless of the total seed size. Keeps the
# one-job-per-doc run bounded while staying deterministic.
SUBSET_QUERY = {"query": {"term": {"metadata.batch": 0}}}


class E2EElasticOneDocPerJobPipeline(AbstractPipeline):
    """E2E pipeline exercising docs_per_job=1 (exactly one doc per job)."""

    name = "e2e_elastic_one_doc_per_job_test"
    rate_limit = 100

    def define_source(self, runtime_params):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query=SUBSET_QUERY,
            scroll="2m",
            size=100,
            docs_per_job=1,
        )

    def define_destination(self, records, runtime_params):
        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            # One tiny POST per single-doc job; skip the per-send HEAD probe so
            # a hundred jobs stay fast.
            health_check_enabled=False,
            body={"records": records},
        )

    def define_transformations(self, records, runtime_params):
        return [add_source_info()]
