"""
DLQ Test Pipeline.

Used for testing DLQ mechanics in E2E tests.
"""

from reflowfy import (
    AbstractPipeline,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source
from reflowfy.destinations.console import console_destination


class DLQTestPipelineAuto(AbstractPipeline):
    """Pipeline for testing automatic DLQ processing."""
    
    name = "test_pipeline_auto"
    rate_limit = {"jobs_per_second": 100}
    
    def define_source(self, params):
        # If this is a DLQ retry, the params contain the original job payload
        # We just want to process it successfully.
        # Mock source generates data based on count.
        # If we are in DLQ mode, we want to simulate processing the failed record.
        # But for simplicity, we can just return a mock source that yields 1 record.
        return mock_source(
            data=[{"dlq_test": True}]
        )
    
    def define_destination(self, params):
        return console_destination()
    
    def define_transformations(self, params):
        return []

pipeline_registry.register(DLQTestPipelineAuto())


class DLQTestPipelineBatch(AbstractPipeline):
    """Pipeline for testing batch dispatch."""
    
    name = "test_pipeline_batch_dispatch"
    
    def define_source(self, params):
        return mock_source(data=[{"batch_test": True}])
    
    def define_destination(self, params):
        return console_destination()
    
    def define_transformations(self, params):
        return []

pipeline_registry.register(DLQTestPipelineBatch())


class DLQTestPipelineDispatch(AbstractPipeline):
    """Pipeline for testing single dispatch."""
    
    name = "test_pipeline_dispatch"
    
    def define_source(self, params):
        return mock_source(data=[{"batch_test": True}])
    
    def define_destination(self, params):
        return console_destination()
    
    def define_transformations(self, params):
        return []

pipeline_registry.register(DLQTestPipelineDispatch())
