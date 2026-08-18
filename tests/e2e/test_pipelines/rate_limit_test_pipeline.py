"""
Rate Limiting Test Pipelines.

Three pipelines for verifying rate limiter behaviour:
- E2ESlowRatePipeline: rate=60 jobs/min (1/s) — 5 batches should take >= 3 s
- E2EFastRatePipeline: rate=30000 jobs/min — 5 batches should finish in < 5 s
- E2ERateLimitOverridePipeline: class default=30000; test sends rate_limit=60 override

All use e2e_mock → e2e_http with rl_passthrough that stamps dispatch timestamp
and batch_id for timing verification.
"""

from reflowfy import AbstractPipeline, BaseDestination, Records, RuntimeParams, Transformations
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.transformations import rl_passthrough


class E2ESlowRatePipeline(AbstractPipeline[RuntimeParams]):
    """Rate = 60 jobs/min (1/s) — 5 single-record batches should take >= 3 s."""

    name = "e2e_slow_rate"
    rate_limit = 60  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=1)

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return [rl_passthrough()]


class E2EFastRatePipeline(AbstractPipeline[RuntimeParams]):
    """Rate = 30000 jobs/min — 50 records in 5 batches should complete quickly."""

    name = "e2e_fast_rate"
    rate_limit = 30000  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_mock(count=50, batch_size=10)

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return [rl_passthrough()]


class E2ERateLimitOverridePipeline(AbstractPipeline[RuntimeParams]):
    """Default rate = 30000 jobs/min; test overrides to 60 via RunPipelineRequest.rate_limit."""

    name = "e2e_rate_override"
    rate_limit = 30000  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=1)

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return [rl_passthrough()]
