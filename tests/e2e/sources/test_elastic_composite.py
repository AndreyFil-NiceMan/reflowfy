"""
E2E tests for Elasticsearch composite aggregations (``after_key`` paging).

Each test runs a pipeline through the full stack against a real Elasticsearch
and compares what reached the destination with a ground-truth composite query
issued straight at Elasticsearch: every group delivered exactly once, with the
right ``doc_count`` and sub-aggregation value.

Prerequisites: same as ``test_elastic_source.py`` (seeded ``e2e-test-events``,
ReflowManager on :8002, mock HTTP server on :8091).
"""

import math
import os
import time
from typing import Any, Dict, List, Optional

import httpx
import pytest

from tests.e2e.test_pipelines.elastic_composite_pipelines import (
    GROUP_SCRIPT,
    PAGE,
)

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091").replace("/webhook", "")
INDEX = "e2e-test-events"
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


@pytest.fixture(scope="module")
def truth() -> Dict[str, Dict[str, Any]]:
    """group -> {doc_count, total}, computed directly by Elasticsearch."""
    from elasticsearch import Elasticsearch

    es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
    try:
        if not es.indices.exists(index=INDEX) or es.count(index=INDEX)["count"] == 0:
            pytest.skip("Seeded index 'e2e-test-events' missing. Run init_elastic_test_data.py.")
        return _groups(es)
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Elasticsearch not available: {e}")
    finally:
        es.close()


def _groups(es: Any, status: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    aggs = {
        "g": {
            "composite": {
                "size": 10000,
                "sources": [{"group": {"terms": {"script": {"source": GROUP_SCRIPT}}}}],
            },
            "aggs": {"total": {"sum": {"field": "amount"}}},
        }
    }
    query = {"term": {"status": status}} if status else {"match_all": {}}
    resp = es.search(index=INDEX, body={"size": 0, "query": query, "aggs": aggs})
    return {
        b["key"]["group"]: {"doc_count": b["doc_count"], "total": b["total"]["value"]}
        for b in resp["aggregations"]["g"]["buckets"]
    }


@pytest.fixture(autouse=True)
def reset_mock():
    try:
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
    except httpx.RequestError:
        pytest.skip(f"Mock HTTP server not available at {MOCK_HTTP_URL}")
    yield


def _run(client: httpx.Client, pipeline: str, params: Optional[dict] = None) -> Dict[str, Any]:
    resp = client.post("/run", json={"pipeline_name": pipeline, "runtime_params": params or {}})
    assert resp.status_code == 202, resp.text
    execution_id = resp.json()["execution_id"]
    deadline = time.time() + 120
    stats: Dict[str, Any] = {}
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            break
        time.sleep(POLL_INTERVAL)
    assert stats.get("state") == "completed", f"{pipeline}: {stats}"
    assert stats["jobs_failed"] == 0
    return stats


def _delivered(expected: int) -> List[Dict[str, Any]]:
    """Bucket records that reached the destination (waits for ``expected``)."""
    records: List[Dict[str, Any]] = []
    deadline = time.time() + 20
    while time.time() < deadline:
        body = httpx.get(f"{MOCK_HTTP_URL}/records", params={"limit": 5000}, timeout=10).json()
        records = [r["data"] for r in body.get("records", [])]
        if len(records) >= expected:
            break
        time.sleep(1)
    return records


def _assert_exact_cover(records: List[Dict[str, Any]], expected: Dict[str, Dict[str, Any]]) -> None:
    keys = [r["key"]["group"] for r in records]
    assert len(keys) == len(set(keys)), f"duplicate buckets delivered: {sorted(keys)}"
    assert set(keys) == set(
        expected
    ), f"missing={set(expected) - set(keys)} extra={set(keys) - set(expected)}"
    for r in records:
        want = expected[r["key"]["group"]]
        assert r["doc_count"] == want["doc_count"]
        # sub-aggregation rides along inside each bucket
        assert r["total"]["value"] == pytest.approx(want["total"])


class TestElasticComposite:
    def test_script_composite_single_job_pages_all_buckets(self, client, truth):
        """Script-keyed composite: one job walks every ``after_key`` hop."""
        assert len(truth) > PAGE, "needs more groups than one page to exercise after_key"
        stats = _run(client, "e2e_elastic_composite_test")

        assert stats["total_jobs"] == 1
        _assert_exact_cover(_delivered(len(truth)), truth)

    def test_docs_per_job_fans_out_one_job_per_page(self, client, truth):
        """``docs_per_job`` pages the aggregation into jobs; union is still exact."""
        stats = _run(client, "e2e_elastic_composite_paged_test")

        assert stats["total_jobs"] == math.ceil(len(truth) / PAGE)
        assert stats["jobs_completed"] == stats["total_jobs"]
        _assert_exact_cover(_delivered(len(truth)), truth)

    def test_composite_nested_under_filter(self, client, truth):
        """Composite under a single-bucket ``filter`` parent is found and paged."""
        from elasticsearch import Elasticsearch

        es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
        try:
            expected = _groups(es, status="active")
        finally:
            es.close()
        assert expected, "seed has no active docs"

        stats = _run(client, "e2e_elastic_composite_nested_test", {"status": "active"})

        assert stats["total_jobs"] == math.ceil(len(expected) / 4)
        _assert_exact_cover(_delivered(len(expected)), expected)

    def test_empty_composite_creates_no_jobs(self, client, truth):
        """A filter matching nothing yields no buckets, hence no jobs."""
        stats = _run(client, "e2e_elastic_composite_nested_test", {"status": "no-such-status"})

        assert stats["total_jobs"] == 0
        assert httpx.get(f"{MOCK_HTTP_URL}/stats", timeout=10).json()["total_records"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
