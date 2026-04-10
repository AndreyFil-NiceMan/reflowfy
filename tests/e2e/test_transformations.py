"""
E2E Tests for Transformation Verification.

Tests that user-defined transformations are correctly applied to records
and that transformation chains execute in the correct order.

Prerequisites:
    - ReflowManager running on localhost:8002
    - Mock HTTP server running on localhost:8091

Run with:
    pytest tests/e2e/test_transformations.py -v
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
def client(check_reflow_manager):
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
    except httpx.RequestError as e:
        pytest.skip(f"Mock HTTP server not available at {MOCK_HTTP_URL}: {e}")


@pytest.fixture(autouse=True)
def reset_mock_http(check_mock_http):
    """Reset mock HTTP server data before each test."""
    try:
        httpx.delete(f"{MOCK_HTTP_URL}/reset", timeout=5.0)
    except Exception:
        pass
    yield


def _run_transformation_pipeline(client):
    """Start the transformation test pipeline and wait for completion."""
    response = client.post("/run", json={
        "pipeline_name": "e2e_transformation_test",
    })
    
    if response.status_code == 404:
        pytest.skip("e2e_transformation_test pipeline not registered")
    
    assert response.status_code == 202
    execution_id = response.json()["execution_id"]
    
    # Wait for completion
    max_wait = 120
    start = time.time()
    
    while time.time() - start < max_wait:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ["completed", "failed"]:
            return execution_id, stats
        time.sleep(POLL_INTERVAL)
    
    raise TimeoutError(
        f"Transformation pipeline {execution_id} did not complete within {max_wait}s"
    )


class TestTransformationChain:
    """Tests for transformation chain execution."""
    
    def test_transformation_adds_expected_fields(self, client, check_mock_http):
        """
        Verify that transformations add the expected fields to records.
        
        The transformation chain adds:
        - Step 1: _processed_at, _execution_id, _transform_step_1
        - Step 2: _transform_step_2, _transform_chain_verified, _computed_category
        """
        execution_id, stats = _run_transformation_pipeline(client)
        
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        
        # Get records from mock server
        records_response = httpx.get(
            f"{MOCK_HTTP_URL}/records",
            params={"limit": 10},
            timeout=10.0,
        ).json()
        
        assert records_response["total"] > 0, "No records received by mock server"
        
        sample_record = records_response["records"][0]
        
        # Verify Step 1 fields
        assert "_processed_at" in sample_record, "Missing _processed_at from step 1"
        assert "_execution_id" in sample_record, "Missing _execution_id from step 1"
        assert sample_record.get("_transform_step_1") is True, "Step 1 not applied"
        
        # Verify Step 2 fields
        assert sample_record.get("_transform_step_2") is True, "Step 2 not applied"
        assert "_computed_category" in sample_record, "Missing _computed_category from step 2"
        assert sample_record["_computed_category"] in ["even", "odd"]
        
        print("✅ All transformation fields present in records")
    
    def test_transformation_chain_order(self, client, check_mock_http):
        """
        Verify that transformations execute in the correct order.
        
        Step 2 sets _transform_chain_verified=True only if Step 1 ran first.
        """
        execution_id, stats = _run_transformation_pipeline(client)
        
        assert stats["state"] == "completed", f"Pipeline failed: {stats}"
        
        # Get records and check chain verification
        records_response = httpx.get(
            f"{MOCK_HTTP_URL}/records",
            params={"limit": 20},
            timeout=10.0,
        ).json()
        
        assert records_response["total"] > 0
        
        # Check ALL returned records have chain verified
        for record in records_response["records"]:
            assert record.get("_transform_chain_verified") is True, (
                f"Transformation chain order broken: step 2 didn't see step 1's output. "
                f"Record: {record}"
            )
        
        print(f"✅ Transformation chain order verified for {len(records_response['records'])} records")
    
    def test_transformation_computed_field_correctness(self, client, check_mock_http):
        """
        Verify that the computed field (_computed_category) has correct values
        based on the record's ID.
        """
        execution_id, stats = _run_transformation_pipeline(client)
        
        assert stats["state"] == "completed"
        
        # Get records
        records_response = httpx.get(
            f"{MOCK_HTTP_URL}/records",
            params={"limit": 20},
            timeout=10.0,
        ).json()
        
        assert records_response["total"] > 0
        
        verified_count = 0
        for record in records_response["records"]:
            record_id = record.get("id")
            category = record.get("_computed_category")
            
            if record_id is not None and category is not None:
                expected = "even" if record_id % 2 == 0 else "odd"
                assert category == expected, (
                    f"Wrong category for id={record_id}: expected '{expected}', got '{category}'"
                )
                verified_count += 1
        
        assert verified_count > 0, "No records had both 'id' and '_computed_category' fields"
        print(f"✅ Computed field correctness verified for {verified_count} records")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
