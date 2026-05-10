"""
Advanced Transformation Test Pipelines.

Four pipelines for testing transformation context features:
- E2EContextProbePipeline:    stamps all ExecutionContext keys onto each record
- E2ERuntimeParamsPipeline:   reads runtime_params from context and computes values
- E2EErrorTolerantPipeline:   two-step chain; id==999 would raise but never appears
- E2EBatchIdentityPipeline:   stamps batch_id per record to verify uniqueness per batch
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import (
    ctx_batch_id,
    ctx_enrich,
    ctx_maybe_fail,
    ctx_probe,
    ctx_runtime_params,
)


class E2EContextProbePipeline(AbstractPipeline):
    """Stamps all 4 ExecutionContext keys onto records."""

    name = "e2e_context_probe"
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=20, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [ctx_probe()]


class E2ERuntimeParamsPipeline(AbstractPipeline):
    """Reads runtime_params; test sends env=staging and multiplier=3."""

    name = "e2e_runtime_params"
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [ctx_runtime_params()]


class E2EErrorTolerantPipeline(AbstractPipeline):
    """Two-step transformation chain; mock data never contains id=999 so always completes."""

    name = "e2e_error_tolerant"
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [ctx_enrich(), ctx_maybe_fail()]


class E2EBatchIdentityPipeline(AbstractPipeline):
    """30 records across 3 batches of 10; each batch must carry a distinct batch_id."""

    name = "e2e_batch_identity"
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=30, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [ctx_batch_id()]
