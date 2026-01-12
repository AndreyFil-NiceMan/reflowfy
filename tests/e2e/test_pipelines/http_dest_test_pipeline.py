"""
HTTP Destination Test Pipeline.

Pipeline that uses mock source and sends to HTTP endpoint.
Used for E2E testing of the HttpDestination connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
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
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")
SAMPLE_DATA = generate_sample_data(count=100)


class E2EHttpDestTestPipeline(AbstractPipeline):
    """E2E test pipeline for HTTP destination."""
    
    name = "e2e_http_dest_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return mock_source(
            data=SAMPLE_DATA,
            batch_size=10,
        )
    
    def define_destination(self, params):
        return http_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            headers={"Content-Type": "application/json"},
            auth_type="bearer",
            auth_token="test-webhook-token",
            batch_requests=True,
            timeout=30.0,
        )
    
    def define_transformations(self, params):
        return [AddDestinationInfo()]


pipeline_registry.register(E2EHttpDestTestPipeline())
