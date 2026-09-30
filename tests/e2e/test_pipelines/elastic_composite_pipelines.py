"""
Elasticsearch composite-aggregation test pipelines.

A composite aggregation returns its rows in ``aggregations``, not ``hits``; the
source pages it with ``after_key`` and emits one record per bucket
(``{"data": bucket}``). The grouping key is a Painless *script*, which is the
case that originally returned 0 records.

- ``e2e_elastic_composite_test``        no ``docs_per_job`` -> ONE job pages everything
- ``e2e_elastic_composite_paged_test``  ``docs_per_job=PAGE`` -> one job per page of buckets
- ``e2e_elastic_composite_nested_test`` composite nested under a ``filter`` agg, paged
"""

import os
from typing import Any, Dict

from typing_extensions import Annotated, NotRequired

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Records,
    RuntimeParams,
    Transformations,
)
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_elastic

INDEX_NAME = "e2e-test-events"
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")

# Buckets per request/job. The seed has 6 event types x 4 statuses = 24 groups,
# so PAGE=5 gives several ``after_key`` hops and ceil(24 / 5) = 5 paged jobs.
PAGE = 5

GROUP_SCRIPT = "doc['event_type'].value + '|' + doc['status'].value"


def composite_aggs(size: int | None = None) -> Dict[str, Any]:
    """Script-keyed composite with a sub-aggregation (kept inside each bucket)."""
    composite: Dict[str, Any] = {
        "sources": [{"group": {"terms": {"script": {"source": GROUP_SCRIPT, "lang": "painless"}}}}]
    }
    if size:
        composite["size"] = size
    return {"by_group": {"composite": composite, "aggs": {"total": {"sum": {"field": "amount"}}}}}


class _Base(AbstractPipeline[RuntimeParams]):
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


class E2EElasticCompositePipeline(_Base):
    name = "e2e_elastic_composite_test"
    rate_limit = 600

    def define_source(self, runtime_params: RuntimeParams):
        # composite ``size`` = PAGE, no docs_per_job -> one job, several after_key hops
        return e2e_elastic(index=INDEX_NAME, base_query={"aggs": composite_aggs(PAGE)})


class E2EElasticCompositePagedPipeline(_Base):
    name = "e2e_elastic_composite_paged_test"
    rate_limit = 600

    def define_source(self, runtime_params: RuntimeParams):
        # docs_per_job sets the page size and fans the aggregation into jobs
        return e2e_elastic(
            index=INDEX_NAME, base_query={"aggs": composite_aggs()}, docs_per_job=PAGE
        )


class NestedParams(RuntimeParams, total=False):
    status: Annotated[NotRequired[str], Param(default="active")]


class E2EElasticCompositeNestedPipeline(AbstractPipeline[NestedParams]):
    name = "e2e_elastic_composite_nested_test"
    rate_limit = 600

    def define_source(self, runtime_params: NestedParams):
        return e2e_elastic(
            index=INDEX_NAME,
            base_query={
                "aggs": {
                    "only": {
                        "filter": {"term": {"status": "{{ status }}"}},
                        "aggs": composite_aggs(),
                    }
                }
            },
            docs_per_job=4,
        )

    def define_destination(self, records: Records, runtime_params: NestedParams) -> BaseDestination:
        return api_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            auth_type="bearer",
            auth_token="test-webhook-token",
            health_check_enabled=False,
            body={"records": records},
        )

    def define_transformations(
        self, records: Records, runtime_params: NestedParams
    ) -> Transformations:
        return []
