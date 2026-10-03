"""
Elasticsearch plain-hits edge cases.

- ``e2e_elastic_script_fields_test``  ``script_fields`` + ``fields`` with ``_source: false``.
  There is no ``_source`` in such a hit; values come back in ``hit["fields"]``.
- ``e2e_elastic_search_after_test``   the query brings its OWN ``sort`` + ``search_after``
  cursor. Elasticsearch rejects that inside a scroll, so the source must page it itself.
"""

import os
from typing import Any, Dict

from reflowfy import AbstractPipeline, BaseDestination, Records, RuntimeParams, Transformations
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_elastic

INDEX_NAME = "e2e-test-events"
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")

# seed assigns metadata.batch = i // 100, so batch 0 is exactly 100 docs
SCRIPT_FIELDS_QUERY: Dict[str, Any] = {
    "query": {"term": {"metadata.batch": 0}},
    "_source": False,
    "fields": ["user_id"],
    "script_fields": {"double_id": {"script": {"source": "doc['user_id'].value * 2"}}},
}

# Resume after user_id == AFTER_ID. ``_doc`` is the unique tiebreaker; the huge
# second cursor value skips every doc whose user_id equals AFTER_ID, so the
# expected set is exactly ``user_id > AFTER_ID``.
AFTER_ID = 10
SEARCH_AFTER_QUERY: Dict[str, Any] = {
    "query": {"match_all": {}},
    "sort": [{"user_id": "asc"}, {"_doc": "asc"}],
    "search_after": [AFTER_ID, 2147483647],
}


class _Base(AbstractPipeline[RuntimeParams]):
    rate_limit = 600

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


class E2EElasticScriptFieldsPipeline(_Base):
    name = "e2e_elastic_script_fields_test"

    def define_source(self, runtime_params: RuntimeParams):
        return e2e_elastic(index=INDEX_NAME, base_query=SCRIPT_FIELDS_QUERY, size=30)


class E2EElasticSearchAfterPipeline(_Base):
    name = "e2e_elastic_search_after_test"

    def define_source(self, runtime_params: RuntimeParams):
        # size=50 over ~450 docs -> many search_after pages in one job
        return e2e_elastic(index=INDEX_NAME, base_query=SEARCH_AFTER_QUERY, size=50)
