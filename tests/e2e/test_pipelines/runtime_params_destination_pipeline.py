"""
E2E pipeline to verify runtime_params are appended through transforms
and used by the destination payload/headers.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.destinations import e2e_http_runtime_params
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import params_step1_enrich, params_step2_verify


class E2ERuntimeParamsDestinationPipeline(AbstractPipeline):
    """
    Pipeline that pushes runtime_params through two transformations
    and ensures destination uses them.
    """

    name = "e2e_runtime_params_destination"
    rate_limit = 50

    def define_source(self, runtime_params):
        runtime_params["source_marker"] = "mock_source"
        runtime_params["tenant"] = "acme"
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, records, runtime_params):
        body = {"records": records, "runtime_params": runtime_params}
        return e2e_http_runtime_params(body=body)

    def define_transformations(self, records, runtime_params):
        return [params_step1_enrich(), params_step2_verify()]
