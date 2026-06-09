"""
Crash Recovery Test Pipeline.

Dedicated pipeline for crash recovery testing with many jobs and a slow
rate limit override to ensure the pipeline runs long enough to be
interrupted and recovered.
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import crash_recovery_add_info


class CrashRecoveryTestPipeline(AbstractPipeline):
    """Test pipeline for crash recovery scenarios."""

    name = "crash_recovery_test"
    # High default rate — crash recovery test overrides to slow via RunPipelineRequest
    rate_limit = 50

    def define_source(self, runtime_params):
        # 500 items / 10 batch_size = 50 jobs. At 0.5 jobs/sec override ≈ 100 seconds
        return e2e_mock(count=500, batch_size=10)

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return [crash_recovery_add_info()]
