"""
Transformation Verification Test Pipeline.

Pipeline that applies a two-step transformation chain to verify that
transformations are applied correctly and in order.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.transformations import (
    transform_add_timestamp,
    transform_enrich_record,
)


class E2ETransformationTestPipeline(AbstractPipeline):
    """E2E test pipeline for verifying transformation chains."""

    name = "e2e_transformation_test"
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=50, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [
            transform_add_timestamp(),
            transform_enrich_record(),
        ]
