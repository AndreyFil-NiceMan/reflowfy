"""
Deduplication Test Pipelines.

Two pipeline variants for testing the enable_duplicate_jobs flag:

- E2EDedupOffPipeline (name="e2e_dedup_off"): enable_duplicate_jobs=False
  Each unique job runs at most once — second run skips all jobs.

- E2EDedupOnPipeline (name="e2e_dedup_on"): enable_duplicate_jobs=True (default)
  Jobs run every time — same as current baseline behavior.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_mock

_FIXED_DATA = [
    {"id": 1, "name": "alice", "value": 100},
    {"id": 2, "name": "bob", "value": 200},
    {"id": 3, "name": "carol", "value": 300},
    {"id": 4, "name": "dave", "value": 400},
    {"id": 5, "name": "eve", "value": 500},
]


class E2EDedupOffPipeline(AbstractPipeline):
    """Pipeline with deduplication enabled (enable_duplicate_jobs=False).

    The same job (identified by its content hash) will never run twice.
    """

    name = "e2e_dedup_off"
    enable_duplicate_jobs = False
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=5, data=_FIXED_DATA)

    def define_destination(self, records, runtime_params):
        return e2e_console()

    def define_transformations(self, records, runtime_params):
        return []


class E2EDedupOnPipeline(AbstractPipeline):
    """Pipeline with deduplication disabled (enable_duplicate_jobs=True, default).

    Jobs run every time regardless of prior executions — baseline behavior.
    """

    name = "e2e_dedup_on"
    enable_duplicate_jobs = True
    rate_limit = 50

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=5, data=_FIXED_DATA)

    def define_destination(self, records, runtime_params):
        return e2e_console()

    def define_transformations(self, records, runtime_params):
        return []
