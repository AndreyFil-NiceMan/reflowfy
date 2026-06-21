"""
Scheduled Test Pipeline.

Pipeline that fires on a cron schedule — used for E2E schedule tests.
A very frequent cron (every minute) lets tests observe auto-triggering
without waiting long.
"""

import uuid

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http

# Unique per service startup so stale hashes from previous runs never block run 1,
# but stable within a single service lifetime so run 2 sees run 1's hashes.
_SERVICE_RUN_ID = str(uuid.uuid4())

_NO_DUP_FIXED_DATA = [
    {"id": i, "name": f"record-{i}", "value": i * 10, "_run": _SERVICE_RUN_ID} for i in range(1, 6)
]


class E2EScheduledTestPipeline(AbstractPipeline):
    """E2E scheduled pipeline — fires every minute."""

    name = "e2e_scheduled_test"
    schedule = "* * * * *"

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=5)

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return []


class E2EScheduledSlowPipeline(AbstractPipeline):
    """E2E scheduled pipeline with a less frequent schedule."""

    name = "e2e_scheduled_slow_test"
    schedule = "0 * * * *"

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=5)

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return []


class E2EScheduledNoDuplicatesPipeline(AbstractPipeline):
    """E2E scheduled pipeline with duplicate jobs disabled."""

    name = "e2e_scheduled_no_duplicates_test"
    schedule = "0 0 1 1 *"  # once a year — never auto-fires during tests
    enable_duplicate_jobs = False

    def define_source(self, runtime_params):
        return e2e_mock(data=_NO_DUP_FIXED_DATA, batch_size=5)

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return []
