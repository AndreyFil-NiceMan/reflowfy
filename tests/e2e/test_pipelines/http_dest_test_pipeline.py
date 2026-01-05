"""
HTTP Destination Test Pipeline.

Pipeline that uses mock source and sends to HTTP endpoint.
Used for E2E testing of the HttpDestination connector.
"""

import os
from reflowfy import (
    build_pipeline,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source, generate_sample_data
from reflowfy.destinations.http import http_destination


class AddDestinationInfo(BaseTransformation):
    """Add destination metadata to records."""
    
    name = "http_add_dest_info"
    
    def apply(self, records, context):
        """Add destination identification to records."""
        execution_id = context.get("execution_id", "unknown")
        for record in records:
            record["_destination_type"] = "http"
            record["_test_pipeline"] = "http_dest_test"
            record["_execution_id"] = execution_id
        return records


# Configuration from environment
# Mock HTTP server URL - started separately during tests
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")

# Generate sample data
sample_data = generate_sample_data(count=100)

# Create mock source
source = mock_source(
    data=sample_data,
    batch_size=10,
)

# Create HTTP destination
destination = http_destination(
    url=MOCK_HTTP_URL,
    method="POST",
    headers={"Content-Type": "application/json"},
    auth_type="bearer",
    auth_token="test-webhook-token",
    batch_requests=True,  # Send all records in one request per batch
    timeout=30.0,
)

# Build and register pipeline
pipeline = build_pipeline(
    name="e2e_http_dest_test",
    source=source,
    transformations=[AddDestinationInfo()],
    destination=destination,
    rate_limit={"jobs_per_second": 10},
)

pipeline_registry.register(pipeline)
