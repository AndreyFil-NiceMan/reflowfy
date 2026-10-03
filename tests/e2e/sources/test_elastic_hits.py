"""
E2E tests for plain-hits Elasticsearch queries that used to fail:

- ``script_fields`` / ``fields`` with no ``_source`` (was ``KeyError: '_source'``)
- a query with its own ``search_after`` (was ``search_after cannot be used in a
  scroll context``)

Prerequisites: same as ``test_elastic_source.py``.
"""

import os
import time
from typing import Any, Dict, List

import httpx
import pytest

from tests.e2e.test_pipelines.elastic_hits_pipelines import AFTER_ID

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091").replace("/webhook", "")
INDEX = "e2e-test-events"


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


@pytest.fixture(scope="module")
def es():
    from elasticsearch import Elasticsearch

    es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
    try:
        if not es.indices.exists(index=INDEX) or es.count(index=INDEX)["count"] == 0:
            pytest.skip("Seeded index 'e2e-test-events' missing. Run init_elastic_test_data.py.")
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"Elasticsearch not available: {e}")
    yield es
    es.close()


@pytest.fixture(autouse=True)
def reset_mock():
    try:
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
    except httpx.RequestError:
        pytest.skip(f"Mock HTTP server not available at {MOCK_HTTP_URL}")
    yield


def _run(client: httpx.Client, pipeline: str) -> Dict[str, Any]:
    resp = client.post("/run", json={"pipeline_name": pipeline, "runtime_params": {}})
    assert resp.status_code == 202, resp.text
    execution_id = resp.json()["execution_id"]
    deadline = time.time() + 120
    stats: Dict[str, Any] = {}
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            break
        time.sleep(2)
    assert stats.get("state") == "completed", f"{pipeline}: {stats}"
    assert stats["jobs_failed"] == 0
    return stats


def _delivered(expected: int) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    deadline = time.time() + 20
    while time.time() < deadline:
        body = httpx.get(f"{MOCK_HTTP_URL}/records", params={"limit": 5000}, timeout=10).json()
        records = body.get("records", [])
        if len(records) >= expected:
            break
        time.sleep(1)
    return records


class TestElasticHitsEdgeCases:
    def test_script_fields_without_source(self, client, es):
        """No ``_source``: values arrive in ``fields`` and the job still completes."""
        expected = es.count(index=INDEX, body={"query": {"term": {"metadata.batch": 0}}})["count"]
        assert expected > 30, "needs more docs than one scroll page"

        stats = _run(client, "e2e_elastic_script_fields_test")

        assert stats["total_jobs"] == 1
        records = _delivered(expected)
        assert len(records) == expected
        for r in records:
            assert r["data"] == {}  # no _source requested
            assert r["fields"]["double_id"][0] == 2 * r["fields"]["user_id"][0]

    def test_user_search_after_pages_from_cursor(self, client, es):
        """The query's own ``search_after`` resumes after it, across many pages."""
        expected = es.count(index=INDEX, body={"query": {"range": {"user_id": {"gt": AFTER_ID}}}})[
            "count"
        ]
        assert expected > 100, "needs several search_after pages"

        stats = _run(client, "e2e_elastic_search_after_test")

        assert stats["total_jobs"] == 1
        records = _delivered(expected)
        assert len(records) == expected, f"got {len(records)}, expected {expected}"
        assert all(r["data"]["user_id"] > AFTER_ID for r in records)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
