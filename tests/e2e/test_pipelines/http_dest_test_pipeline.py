"""
HTTP Destination Test Pipeline.

Pipeline that uses mock source and sends to HTTP endpoint.
Used for E2E testing of the HttpDestination connector.
"""

from reflowfy import (
    AbstractPipeline,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http


@transformation("http_add_dest_info")
def http_add_dest_info(records, context):
    """Add destination metadata to records."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_destination_type"] = "http"
        record["_test_pipeline"] = "http_dest_test"
        record["_execution_id"] = execution_id
    return records


class E2EHttpDestTestPipeline(AbstractPipeline):
    """E2E test pipeline for HTTP destination."""
    
    name = "e2e_http_dest_test"
    # High rate for fast normal tests - crash recovery test uses 0.5/sec override
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return e2e_mock(count=100, batch_size=10)
    
    def define_destination(self, params):
        return e2e_http()
    
    def define_transformations(self, params):
        return [http_add_dest_info()]
