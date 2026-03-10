"""
DLQ Test Pipeline.

Used for testing DLQ mechanics in E2E tests.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_console


class DLQTestPipelineAuto(AbstractPipeline):
    """Pipeline for testing automatic DLQ processing."""
    
    name = "test_pipeline_auto"
    rate_limit = {"jobs_per_second": 100}
    
    def define_source(self, params):
        return e2e_mock(data=[{"dlq_test": True}])
    
    def define_destination(self, params):
        return e2e_console(pretty_print=False)
    
    def define_transformations(self, params):
        return []



class DLQTestPipelineBatch(AbstractPipeline):
    """Pipeline for testing batch dispatch."""
    
    name = "test_pipeline_batch_dispatch"
    
    def define_source(self, params):
        return e2e_mock(data=[{"batch_test": True}])
    
    def define_destination(self, params):
        return e2e_console(pretty_print=False)
    
    def define_transformations(self, params):
        return []



class DLQTestPipelineDispatch(AbstractPipeline):
    """Pipeline for testing single dispatch."""
    
    name = "test_pipeline_dispatch"
    
    def define_source(self, params):
        return e2e_mock(data=[{"batch_test": True}])
    
    def define_destination(self, params):
        return e2e_console(pretty_print=False)
    
    def define_transformations(self, params):
        return []
