"""
Crash Recovery Test Pipeline.

Dedicated pipeline for crash recovery testing with many jobs and a slow
rate limit override to ensure the pipeline runs long enough to be
interrupted and recovered.
"""

from reflowfy import (
    AbstractPipeline,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http


@transformation("crash_recovery_add_info")
def crash_recovery_add_info(records, context):
    """Add crash recovery metadata to records."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_test_pipeline"] = "crash_recovery"
        record["_execution_id"] = execution_id
    return records


class CrashRecoveryTestPipeline(AbstractPipeline):
    """Test pipeline for crash recovery scenarios."""
    
    name = "crash_recovery_test"
    # High default rate - crash recovery test will use slow override
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        # 500 items / 10 batch_size = 50 jobs. At 0.5 jobs/sec override = ~100 seconds
        return e2e_mock(count=500, batch_size=10)
    
    def define_destination(self, params):
        return e2e_http()
    
    def define_transformations(self, params):
        return [crash_recovery_add_info()]
