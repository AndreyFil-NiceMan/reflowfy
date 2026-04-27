"""
E2E test pipeline that always fails via a transformation error.

Used by test_dx_improvements.py to verify that:
- Failed jobs store an error_message in the DB
- GET /executions/{id}/errors returns the error details
"""

from reflowfy import AbstractPipeline, BaseTransformation
from reflowfy.destinations.console import console_destination
from tests.e2e.test_pipelines.sources import e2e_mock


class AlwaysFailTransformation(BaseTransformation):
    """Raises RuntimeError on every record batch for error-handling tests."""

    name = "always_fail_transform"

    def apply(self, records, runtime_params):
        raise RuntimeError("Intentional failure: error_pipeline_test transformation")


class ErrorPipelineTest(AbstractPipeline):
    """Pipeline whose transformation always raises — used to test error reporting."""

    name = "error_pipeline_test"
    rate_limit = 10
    enable_duplicate_jobs = True

    def define_source(self, runtime_params):
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, runtime_params):
        return console_destination()

    def define_transformations(self, runtime_params):
        return [AlwaysFailTransformation()]
