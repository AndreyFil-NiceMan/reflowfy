"""
Rate Limiting Test Pipelines.

Three pipelines for verifying rate limiter behaviour:
- E2ESlowRatePipeline: rate=1 job/s — 5 batches should take ≥ 3 s
- E2EFastRatePipeline: rate=500 jobs/s — 5 batches should finish in < 5 s
- E2ERateLimitOverridePipeline: class default=500; test sends rate_limit=1 override

All use e2e_mock → e2e_http with a passthrough transformation that stamps
the dispatch timestamp and batch_id for timing verification.
"""

from datetime import datetime

from reflowfy import AbstractPipeline, transformation
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http


@transformation("rl_passthrough")
def rl_passthrough(records, context):
    """Stamp dispatch timestamp and batch_id for rate-limit timing tests."""
    ts = datetime.utcnow().isoformat()
    for record in records:
        record["_rl_dispatched_at"] = ts
        record["_rl_batch_id"] = context.get("batch_id", "")
    return records


class E2ESlowRatePipeline(AbstractPipeline):
    """Rate = 1 job/s — 5 single-record batches should take ≥ 3 s."""

    name = "e2e_slow_rate"
    rate_limit = {"jobs_per_second": 1}

    def define_source(self, params):
        return e2e_mock(count=5, batch_size=1)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [rl_passthrough()]


class E2EFastRatePipeline(AbstractPipeline):
    """Rate = 500 jobs/s — 50 records in 5 batches should complete quickly."""

    name = "e2e_fast_rate"
    rate_limit = {"jobs_per_second": 500}

    def define_source(self, params):
        return e2e_mock(count=50, batch_size=10)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [rl_passthrough()]


class E2ERateLimitOverridePipeline(AbstractPipeline):
    """Default rate = 500 jobs/s; test overrides to 1 via RunPipelineRequest.rate_limit."""

    name = "e2e_rate_override"
    rate_limit = {"jobs_per_second": 500}

    def define_source(self, params):
        return e2e_mock(count=5, batch_size=1)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [rl_passthrough()]
