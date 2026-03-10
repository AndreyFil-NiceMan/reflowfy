"""
Transformation Verification Test Pipeline.

Pipeline that applies a chain of two transformations to verify
that transformations are applied correctly and in order.

Used for E2E testing of the transformation system.
"""

from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http


@transformation("transform_add_timestamp")
def transform_add_timestamp(records, context):
    """First transformation: adds a processing timestamp and source marker."""
    from datetime import datetime
    
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_processed_at"] = datetime.utcnow().isoformat()
        record["_execution_id"] = execution_id
        record["_transform_step_1"] = True
    return records


@transformation("transform_enrich_record")
def transform_enrich_record(records, context):
    """
    Second transformation: enriches records with computed fields.
    
    Depends on _transform_step_1 being set by the first transformation
    to verify ordering.
    """
    for record in records:
        # Verify step 1 ran first
        record["_transform_step_2"] = True
        record["_transform_chain_verified"] = record.get("_transform_step_1", False)
        
        # Add a computed field from existing data
        record_id = record.get("id", 0)
        record["_computed_category"] = "even" if record_id % 2 == 0 else "odd"
        record["_destination_type"] = "http"
        record["_test_pipeline"] = "transformation_verify"
    return records


class E2ETransformationTestPipeline(AbstractPipeline):
    """E2E test pipeline for verifying transformation chains."""
    
    name = "e2e_transformation_test"
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return e2e_mock(count=50, batch_size=10)
    
    def define_destination(self, params):
        return e2e_http()
    
    def define_transformations(self, params):
        return [
            transform_add_timestamp(),
            transform_enrich_record(),
        ]
