"""
E2E Tests for HTTP Destination.

Tests the HttpDestination connector by running a pipeline that uses
mock source data and sends to a mock HTTP webhook server.

Prerequisites:
    - Mock HTTP server running on localhost:8091 (run mock_http_server.py)
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/destinations/test_http_destination.py -v
"""

import os
import time
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="module")
def check_mock_http():
    """Verify mock HTTP server is running."""
    try:
        response = httpx.get(f"{MOCK_HTTP_URL}/health", timeout=5.0)
        if response.status_code != 200:
            pytest.skip(f"Mock HTTP server unhealthy: {response.status_code}")
        
        print("✅ Mock HTTP server is running")
        
    except httpx.RequestError as e:
        pytest.skip(f"Mock HTTP server not available at {MOCK_HTTP_URL}: {e}")


@pytest.fixture(autouse=True)
def reset_mock_http(check_mock_http):
    """Reset mock HTTP server data before each test."""
    try:
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
    except:
        pass
    yield


class TestHttpDestinationPipeline:
    """Test HTTP destination pipeline."""
    
    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is running."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_mock_http_server_ready(self, check_mock_http):
        """Verify mock HTTP server can receive data."""
        response = httpx.post(
            f"{MOCK_HTTP_URL}/webhook",
            json={
                "records": [{"test": "data"}],
                "metadata": {"test": True},
            },
            headers={"Authorization": "Bearer test-webhook-token"},
            timeout=10.0,
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "received"
        assert data["record_count"] == 1
    
    def test_pipeline_starts(self, client, check_mock_http):
        """Test that pipeline can start."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
        })
        
        assert response.status_code == 202
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_http_dest_test"
    
    def test_pipeline_sends_to_http(self, client, check_mock_http):
        """Test that pipeline sends records to HTTP endpoint."""
        # Reset mock server
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
        
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
        })
        
        assert response.status_code == 202
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        max_wait = 120
        start = time.time()
        final_state = None
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            final_state = stats.get("state")
            
            if final_state in ["completed", "failed"]:
                break
            
            time.sleep(POLL_INTERVAL)
        
        # Verify completion
        assert final_state == "completed", f"Expected completed, got {final_state}"
        
        # Verify mock server received records
        mock_stats = httpx.get(f"{MOCK_HTTP_URL}/stats", timeout=10.0).json()
        
        assert mock_stats["total_records"] > 0, "Expected records to be sent to mock server"
        assert mock_stats["total_batches"] > 0, "Expected batches to be received"
        
        print(f"✅ Mock server received {mock_stats['total_records']} records in {mock_stats['total_batches']} batches")
    
    def test_records_have_correct_format(self, client, check_mock_http):
        """Test that sent records have the expected format."""
        # Reset mock server
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
        
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
        })
        
        execution_id = response.json()["execution_id"]
        
        # Wait for completion
        max_wait = 120
        start = time.time()
        
        while time.time() - start < max_wait:
            stats = client.get(f"/executions/{execution_id}/stats").json()
            if stats.get("state") in ["completed", "failed"]:
                break
            time.sleep(POLL_INTERVAL)
        
        # Get received records
        records_response = httpx.get(
            f"{MOCK_HTTP_URL}/records",
            params={"limit": 10},
            timeout=10.0,
        ).json()
        
        assert records_response["total"] > 0
        
        # Check record format
        sample_record = records_response["records"][0]
        
        # Should have transformation-added fields
        assert "_destination_type" in sample_record
        assert sample_record["_destination_type"] == "http"
        assert "_test_pipeline" in sample_record
        assert sample_record["_test_pipeline"] == "http_dest_test"
        
        print(f"✅ Records have correct format with _destination_type='http'")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
