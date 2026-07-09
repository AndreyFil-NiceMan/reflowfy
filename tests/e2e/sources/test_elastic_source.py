"""
E2E Tests for Elasticsearch Source.

Tests the ElasticSource connector by running a pipeline that fetches
from Elasticsearch and outputs to console.

Prerequisites:
    - Elasticsearch running on localhost:9201 (from docker-compose.e2e.yml)
    - Test data initialized via init_elastic_test_data.py
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/sources/test_elastic_source.py -v
"""

import math
import os
import time
import pytest
import httpx

from tests.e2e.test_pipelines.elastic_docs_per_job_pipeline import DOCS_PER_JOB
from tests.e2e.test_pipelines.elastic_one_doc_per_job_pipeline import SUBSET_QUERY

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091").replace("/webhook", "")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="module")
def check_elasticsearch():
    """Verify Elasticsearch is available and has test data."""
    from elasticsearch import Elasticsearch
    
    es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
    
    try:
        health = es.cluster.health()
        if health["status"] not in ["green", "yellow"]:
            pytest.skip(f"Elasticsearch cluster unhealthy: {health['status']}")
        
        # Check for test data
        if not es.indices.exists(index="e2e-test-events"):
            pytest.skip("Test index 'e2e-test-events' not found. Run init_elastic_test_data.py first.")
        
        count = es.count(index="e2e-test-events")["count"]
        if count == 0:
            pytest.skip("Test index has no data. Run init_elastic_test_data.py first.")
        
        print(f"✅ Elasticsearch ready with {count} test documents")
        
    except Exception as e:
        pytest.skip(f"Elasticsearch not available: {e}")
    finally:
        es.close()


class TestElasticSourcePipeline:
    """Test Elasticsearch source pipeline."""
    
    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is running."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_pipeline_starts(self, client, check_elasticsearch):
        """Test that pipeline can start."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_source_test",
            "runtime_params": {
                "start_time": "2020-01-01T00:00:00",
                "end_time": "2030-12-31T23:59:59",
            },
        })
        
        assert response.status_code == 202
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_elastic_source_test"
        assert data["state"] == "pending"
    
    def test_pipeline_creates_jobs(self, client, check_elasticsearch):
        """Test that pipeline creates jobs from Elasticsearch data."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_source_test",
            "runtime_params": {
                "start_time": "2020-01-01T00:00:00",
                "end_time": "2030-12-31T23:59:59",
            },
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for jobs to be created
        max_wait = 30
        start = time.time()
        total_jobs = 0
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            total_jobs = stats.get("total_jobs", 0)
            
            if total_jobs > 0:
                break
            
            time.sleep(POLL_INTERVAL)
        
        assert total_jobs > 0, f"Expected jobs to be created, got {total_jobs}"
        print(f"✅ Pipeline created {total_jobs} jobs")
    
    def test_pipeline_completes(self, client, check_elasticsearch):
        """Test that pipeline runs to completion."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_source_test",
            "runtime_params": {
                "start_time": "2020-01-01T00:00:00",
                "end_time": "2030-12-31T23:59:59",
            },
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        max_wait = 120
        start = time.time()
        final_state = None
        stats = {}
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            final_state = stats.get("state")
            
            if final_state in ["completed", "failed"]:
                break
            
            time.sleep(POLL_INTERVAL)
        
        # Verify completion
        assert final_state == "completed", f"Expected completed, got {final_state}"
        
        # Verify job counts
        assert stats["total_jobs"] > 0
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0
        
        print(f"✅ Pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs")

    def test_empty_query_creates_no_jobs(self, client, check_elasticsearch):
        """A query matching no documents must create 0 jobs (no no-op job).

        Seeded @timestamps are within the last 90 days, so a 1990 range
        matches nothing — ElasticSource.split() must yield no slices.
        """
        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_source_test",
            "runtime_params": {
                "start_time": "1990-01-01T00:00:00",
                "end_time": "1990-01-02T00:00:00",
            },
        })

        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        max_wait = 60
        start = time.time()
        stats = {}
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") in ("completed", "failed"):
                break
            time.sleep(POLL_INTERVAL)

        assert stats.get("state") == "completed", f"Expected completed, got {stats}"
        assert stats.get("total_jobs", -1) == 0, (
            f"empty Elastic query must create 0 jobs, got {stats.get('total_jobs')}"
        )
        print("✅ Empty Elastic query created 0 jobs")

    def test_docs_per_job_splits_into_multiple_jobs(self, client, check_elasticsearch):
        """docs_per_job must fan the query into ceil(count / docs_per_job) jobs.

        The pipeline sets docs_per_job=DOCS_PER_JOB over a match_all query, so
        the manager derives the slice count from the matched-document count.
        Job count is deterministic (unlike per-slice doc counts), so we assert
        it exactly against the live ES doc count.
        """
        from elasticsearch import Elasticsearch

        es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
        try:
            doc_count = es.count(index="e2e-test-events")["count"]
        finally:
            es.close()

        expected_jobs = math.ceil(doc_count / DOCS_PER_JOB)
        assert expected_jobs > 1, (
            f"test needs >1 expected job to be meaningful; got {expected_jobs} "
            f"from {doc_count} docs / {DOCS_PER_JOB}"
        )

        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_docs_per_job_test",
            "runtime_params": {},
        })
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        max_wait = 120
        start = time.time()
        stats = {}
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") in ("completed", "failed"):
                break
            time.sleep(POLL_INTERVAL)

        assert stats.get("state") == "completed", f"Expected completed, got {stats}"
        assert stats.get("total_jobs") == expected_jobs, (
            f"docs_per_job={DOCS_PER_JOB} over {doc_count} docs must create "
            f"{expected_jobs} jobs, got {stats.get('total_jobs')}"
        )
        assert stats["jobs_completed"] == expected_jobs
        assert stats["jobs_failed"] == 0
        print(f"✅ docs_per_job created {expected_jobs} jobs from {doc_count} docs")


class TestElasticOneDocPerJob:
    """docs_per_job=1: exactly one job per doc, no empty jobs, exact record cover.

    This is the regression suite for count-derived positional windowing. The old
    sliced-scroll path hash-partitioned docs, so ``docs_per_job=1`` produced
    uneven slices with empty jobs (which then failed in the transformation).
    Windowing must instead yield exactly one non-empty job per matched doc and
    deliver every doc to the destination exactly once.
    """

    def _mock_up(self):
        try:
            r = httpx.get(f"{MOCK_HTTP_URL}/health", timeout=5.0)
            return r.status_code == 200
        except httpx.RequestError:
            return False

    def _subset_count(self):
        from elasticsearch import Elasticsearch

        es = Elasticsearch(hosts=[ELASTICSEARCH_URL])
        try:
            return es.count(index="e2e-test-events", body=SUBSET_QUERY)["count"]
        finally:
            es.close()

    def test_one_job_per_doc_no_empties_exact_cover(self, client, check_elasticsearch):
        if not self._mock_up():
            pytest.skip(f"Mock HTTP server not available at {MOCK_HTTP_URL}")

        expected = self._subset_count()
        assert 1 < expected <= 200, (
            f"subset must be a bounded, splittable set for this test; got {expected}"
        )

        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)

        response = client.post("/run", json={
            "pipeline_name": "e2e_elastic_one_doc_per_job_test",
            "runtime_params": {},
        })
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]

        max_wait = 180
        start = time.time()
        stats = {}
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") in ("completed", "failed"):
                break
            time.sleep(POLL_INTERVAL)

        assert stats.get("state") == "completed", f"Expected completed, got {stats}"
        # Exactly one job per matched doc — deterministic, unlike hash slicing.
        assert stats.get("total_jobs") == expected, (
            f"docs_per_job=1 must create one job per doc ({expected}), "
            f"got {stats.get('total_jobs')}"
        )
        # No empty-slice jobs failing in the transformation (the bug this fixes).
        assert stats["jobs_completed"] == expected
        assert stats["jobs_failed"] == 0

        # Exact cover on the destination: every doc delivered exactly once. One
        # record per job means batch and record totals both equal the doc count
        # — proving no loss, no duplication, and no empty jobs.
        mock = {}
        deadline = time.time() + 15
        while time.time() < deadline:
            mock = httpx.get(f"{MOCK_HTTP_URL}/stats", timeout=10.0).json()
            if mock.get("total_records", 0) >= expected:
                break
            time.sleep(1)

        assert mock["total_records"] == expected, (
            f"expected {expected} records delivered exactly once, got {mock['total_records']}"
        )
        assert mock["total_batches"] == expected, (
            f"one doc per job -> {expected} single-record batches, got {mock['total_batches']}"
        )
        print(f"✅ one-doc-per-job: {expected} jobs, exact cover, no empties/loss/dup")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
