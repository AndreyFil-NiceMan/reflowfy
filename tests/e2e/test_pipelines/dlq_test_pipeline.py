"""
DLQ Test Pipeline.

Used for testing DLQ mechanics in E2E tests.
"""

from reflowfy import AbstractPipeline, BaseDestination, Records, RuntimeParams, Transformations
from tests.e2e.test_pipelines.shared_destinations import e2e_console
from tests.e2e.test_pipelines.shared_sources import e2e_mock


class DLQTestPipelineAuto(AbstractPipeline[RuntimeParams]):
    """Pipeline for testing automatic DLQ processing."""

    name = "test_pipeline_auto"
    rate_limit = 6000  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_mock(data=[{"dlq_test": True}])

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False)

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []


class DLQTestPipelineBatch(AbstractPipeline[RuntimeParams]):
    """Pipeline for testing batch dispatch."""

    name = "test_pipeline_batch_dispatch"

    def define_source(self, runtime_params):
        return e2e_mock(data=[{"batch_test": True}])

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False)

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []


class DLQTestPipelineDispatch(AbstractPipeline[RuntimeParams]):
    """Pipeline for testing single dispatch."""

    name = "test_pipeline_dispatch"

    def define_source(self, runtime_params):
        return e2e_mock(data=[{"batch_test": True}])

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False)

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []
