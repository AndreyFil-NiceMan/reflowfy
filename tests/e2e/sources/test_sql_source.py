"""
E2E Tests for SQL Source.

Tests the SqlSource connector by running a pipeline that fetches
from PostgreSQL and outputs to console.

Prerequisites:
    - PostgreSQL running on localhost:5433 (from docker-compose.e2e.yml)
    - Test data initialized via init_sql_test_data.py
    - ReflowManager running on localhost:8002

Run with:
    pytest tests/e2e/sources/test_sql_source.py -v
"""

import os
import time
import pytest
import httpx

# Configuration
REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
SQL_CONNECTION_URL = os.getenv("SQL_CONNECTION_URL", "postgresql://reflowfy:reflowfy@localhost:5433/reflowfy_e2e")
TIMEOUT = 60.0
POLL_INTERVAL = 2


@pytest.fixture(scope="module")
def client():
    """HTTP client for ReflowManager API."""
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="module")
def check_postgres():
    """Verify PostgreSQL is available and has test data."""
    from sqlalchemy import create_engine, text
    
    engine = create_engine(SQL_CONNECTION_URL)
    
    try:
        with engine.connect() as conn:
            # Check table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'test_events'
                )
            """))
            exists = result.scalar()
            
            if not exists:
                pytest.skip("Test table 'test_events' not found. Run init_sql_test_data.py first.")
            
            # Check for data
            result = conn.execute(text("SELECT COUNT(*) FROM test_events"))
            count = result.scalar()
            
            if count == 0:
                pytest.skip("Test table has no data. Run init_sql_test_data.py first.")
            
            print(f"✅ PostgreSQL ready with {count} test records")
            
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    finally:
        engine.dispose()


class TestSqlSourcePipeline:
    """Test SQL source pipeline."""
    
    def test_reflow_manager_health(self, client):
        """Verify ReflowManager is running."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_pipeline_starts(self, client, check_postgres):
        """Test that pipeline can start."""
        response = client.post("/run", json={
            "pipeline_name": "e2e_sql_source_test",
            "runtime_params": {
                "start_time": "2020-01-01T00:00:00",
                "end_time": "2030-12-31T23:59:59",
            },
        })
        
        assert response.status_code == 202
        data = response.json()
        assert "execution_id" in data
        assert data["pipeline_name"] == "e2e_sql_source_test"
    
    def test_pipeline_creates_jobs_with_id_ranges(self, client, check_postgres):
        """Test that pipeline creates jobs with ID range splitting."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_sql_source_test",
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
    
    def test_pipeline_completes(self, client, check_postgres):
        """Test that pipeline runs to completion."""
        # Start pipeline
        response = client.post("/run", json={
            "pipeline_name": "e2e_sql_source_test",
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
        assert stats["jobs_completed"] == stats["total_jobs"]
        assert stats["jobs_failed"] == 0
        
        print(f"✅ Pipeline completed: {stats['jobs_completed']}/{stats['total_jobs']} jobs")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
