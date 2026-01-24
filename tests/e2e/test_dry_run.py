"""E2E tests for dry run mode."""

import os
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8002")
TIMEOUT = 60.0


@pytest.fixture
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


class TestDryRunMode:
    """Test dry run functionality."""
    
    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is running."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_dry_run_returns_preview(self, client):
        """Test that dry_run=true returns preview without creating execution."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_sql_source_test",
            "runtime_params": {
                "start_time": "2023-01-01T00:00:00",
                "end_time": "2024-01-01T00:00:00",
            },
            "dry_run": True,
        })
        
        assert response.status_code == 202
        data = response.json()
        
        # Verify dry run response structure
        assert data["dry_run"] is True
        assert data["pipeline_name"] == "e2e_sql_source_test"
        assert "sample_records" in data
        assert "transformations" in data
        assert "destination" in data
        assert data["message"] == "Dry run complete. No jobs were dispatched."
    
    def test_dry_run_shows_destination_type(self, client):
        """Test that dry run includes destination configuration."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_http_dest_test",
            "dry_run": True,
        })
        
        assert response.status_code == 202
        data = response.json()
        
        # Verify destination info is included
        assert "destination" in data
        assert "type" in data["destination"]
        assert "config" in data["destination"]
    
    def test_dry_run_shows_transformations(self, client):
        """Test that dry run includes transformation info."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_sql_source_test",
            "runtime_params": {
                "start_time": "2023-01-01T00:00:00",
                "end_time": "2024-01-01T00:00:00",
            },
            "dry_run": True,
        })
        
        assert response.status_code == 202
        data = response.json()
        
        # Verify transformations are listed
        assert "transformations" in data
        assert isinstance(data["transformations"], list)
    
    def test_dry_run_does_not_create_execution(self, client):
        """Test that dry run does not create an execution record."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_sql_source_test",
            "runtime_params": {
                "start_time": "2023-01-01T00:00:00",
                "end_time": "2024-01-01T00:00:00",
            },
            "dry_run": True,
        })
        
        assert response.status_code == 202
        data = response.json()
        
        # Dry run should NOT return execution_id or status_url
        assert "execution_id" not in data
        assert "status_url" not in data
    
    def test_dry_run_invalid_pipeline_returns_404(self, client):
        """Test that dry run with invalid pipeline returns 404."""
        response = client.post("/run", json={
            "pipeline_name": "nonexistent_pipeline",
            "dry_run": True,
        })
        
        assert response.status_code == 404
    
    def test_dry_run_with_runtime_params(self, client):
        """Test that dry run accepts runtime parameters."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_api_source_test",
            "runtime_params": {
                "endpoint": "/users",
                "page_size": 10,
            },
            "dry_run": True,
        })
        
        assert response.status_code == 202
        data = response.json()
        
        # Verify runtime params are echoed back
        assert data["runtime_params"]["endpoint"] == "/users"
        assert data["runtime_params"]["page_size"] == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
