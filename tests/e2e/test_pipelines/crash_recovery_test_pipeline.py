"""
Crash Recovery Test Pipeline.

Dedicated pipeline for crash recovery testing with many jobs and a slow
rate limit override to ensure the pipeline runs long enough to be
interrupted and recovered.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source, generate_sample_data


class AddCrashRecoveryInfo(BaseTransformation):
    """Add crash recovery metadata to records."""
    
    name = "crash_recovery_add_info"
    
    def apply(self, records, context):
        """Add metadata to records."""
        execution_id = context.get("execution_id", "unknown")
        for record in records:
            record["_test_pipeline"] = "crash_recovery"
            record["_execution_id"] = execution_id
        return records


# Configuration from environment
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")
# 500 items / 10 batch_size = 50 jobs. At 0.5 jobs/sec override = ~100 seconds
SAMPLE_DATA = generate_sample_data(count=500)


class CrashRecoveryTestPipeline(AbstractPipeline):
    """Test pipeline for crash recovery scenarios."""
    
    name = "crash_recovery_test"
    # High default rate - crash recovery test will use slow override
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return mock_source(
            data=SAMPLE_DATA,
            batch_size=10,
        )
    
    def define_destination(self, params):
        # Use HTTP destination like the other test pipeline
        from reflowfy.destinations.http import http_destination
        return http_destination(
            url=MOCK_HTTP_URL,
            method="POST",
            headers={"Content-Type": "application/json"},
            auth_type="bearer",
            auth_token="test-webhook-token",
            batch_requests=True,
            timeout=30.0,
        )
    
    def define_transformations(self, params):
        return [AddCrashRecoveryInfo()]


pipeline_registry.register(CrashRecoveryTestPipeline())
