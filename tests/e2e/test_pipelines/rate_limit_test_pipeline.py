"""
Rate Limiting Test Pipelines.

Three pipelines for verifying rate limiter behaviour:
- E2ESlowRatePipeline: rate=1 job/s — 5 batches should take >= 3 s
- E2EFastRatePipeline: rate=500 jobs/s — 5 batches should finish in < 5 s
- E2ERateLimitOverridePipeline: class default=500; test sends rate_limit=1 override

All use e2e_mock → e2e_http with rl_passthrough that stamps dispatch timestamp
and batch_id for timing verification.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.transformations import rl_passthrough


class E2ESlowRatePipeline(AbstractPipeline):
    """Rate = 1 job/s — 5 single-record batches should take >= 3 s."""

    name = "e2e_slow_rate"
    rate_limit = 1

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=1)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [rl_passthrough()]


class E2EFastRatePipeline(AbstractPipeline):
    """Rate = 500 jobs/s — 50 records in 5 batches should complete quickly."""

    name = "e2e_fast_rate"
    rate_limit = 500

    def define_source(self, runtime_params):
        return e2e_mock(count=50, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [rl_passthrough()]


class E2ERateLimitOverridePipeline(AbstractPipeline):
    """Default rate = 500 jobs/s; test overrides to 1 via RunPipelineRequest.rate_limit."""

    name = "e2e_rate_override"
    rate_limit = 500

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=1)

    def define_destination(self, records, runtime_params):
        return e2e_http()

    def define_transformations(self, records, runtime_params):
        return [rl_passthrough()]
