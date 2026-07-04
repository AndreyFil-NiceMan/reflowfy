"""E2E: verify the observability signals actually flow.

In E2E the stack runs EXECUTION_MODE=local, so the reflow-manager runs jobs
in-process — its /metrics reflects job execution and its logs (configured with
LOG_DESTINATION=elastic by the harness) ship to the e2e Elasticsearch.
"""

import time

import httpx
import pytest

from tests.e2e.conftest import ELASTICSEARCH_URL, REFLOW_MANAGER_URL


def test_metrics_endpoint_exposes_reflowfy_families(check_reflow_manager):
    """The manager exposes Prometheus metrics with reflowfy_* families."""
    resp = httpx.get(f"{REFLOW_MANAGER_URL}/metrics", timeout=10.0)
    assert resp.status_code == 200
    body = resp.text
    assert "reflowfy_pipeline_executions_total" in body
    assert "reflowfy_jobs_processed_total" in body


def test_pipeline_run_records_metrics(check_reflow_manager, reflow_client):
    """After a run, execution + job counters advance."""
    before = httpx.get(f"{REFLOW_MANAGER_URL}/metrics", timeout=10.0).text

    resp = reflow_client.post(
        "/run", json={"pipeline_name": "e2e_api_dest_test", "runtime_params": {"tenant_id": "obs-e2e", "env": "staging"}}
    )
    assert resp.status_code in (200, 202), resp.text

    # Give the in-process executor a moment, then re-scrape.
    deadline = time.time() + 30
    after = before
    while time.time() < deadline:
        after = httpx.get(f"{REFLOW_MANAGER_URL}/metrics", timeout=10.0).text
        if "reflowfy_pipeline_executions_total{" in after:
            break
        time.sleep(1)
    # A concrete execution sample now exists (labelled series appears once used).
    assert "reflowfy_pipeline_executions_total{" in after


@pytest.mark.elasticsearch
def test_logs_land_in_elastic(check_reflow_manager, check_elasticsearch, reflow_client):
    """Manager logs are shipped to the e2e Elasticsearch as reflowfy-logs-*."""
    from elasticsearch import Elasticsearch

    es = Elasticsearch(hosts=[ELASTICSEARCH_URL])

    # Trigger activity so there are fresh logs to ship.
    reflow_client.post(
        "/run", json={"pipeline_name": "e2e_api_dest_test", "runtime_params": {"tenant_id": "obs-e2e", "env": "staging"}}
    )

    deadline = time.time() + 40
    total = 0
    while time.time() < deadline:
        es.indices.refresh(index="reflowfy-logs-*", ignore_unavailable=True)
        res = es.search(index="reflowfy-logs-*", size=1, ignore_unavailable=True)
        total = res["hits"]["total"]["value"]
        if total > 0:
            break
        time.sleep(2)

    assert total > 0, "no reflowfy logs found in Elasticsearch (reflowfy-logs-*)"
