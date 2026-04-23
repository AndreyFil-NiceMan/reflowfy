"""
HTTP Destination Test Pipeline.

Pipeline that uses mock source and sends to HTTP endpoint.
Used for E2E testing of the HttpDestination connector.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.transformations import http_add_dest_info


class E2EHttpDestTestPipeline(AbstractPipeline):
    """E2E test pipeline for HTTP destination."""

    name = "e2e_http_dest_test"
    # High rate for fast normal tests — crash recovery test uses 0.5/sec override
    rate_limit = 50

    def define_source(self, params):
        return e2e_mock(count=100, batch_size=10)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [http_add_dest_info()]
