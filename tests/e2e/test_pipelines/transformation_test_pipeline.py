"""
Transformation Verification Test Pipeline.

Pipeline that applies a chain of two transformations to verify
that transformations are applied correctly and in order.

Used for E2E testing of the transformation system.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source, generate_sample_data
from reflowfy.destinations.http import http_destination


class AddTimestampTransformation(BaseTransformation):
    """First transformation: adds a processing timestamp and source marker."""
    
    name = "transform_add_timestamp"
    
    def apply(self, records, context):
        """Add processing metadata to each record."""
        from datetime import datetime
        
        execution_id = context.get("execution_id", "unknown")
        for record in records:
            record["_processed_at"] = datetime.utcnow().isoformat()
            record["_execution_id"] = execution_id
            record["_transform_step_1"] = True
        return records


class EnrichRecordTransformation(BaseTransformation):
    """
    Second transformation: enriches records with computed fields.
    
    Depends on _transform_step_1 being set by the first transformation
    to verify ordering.
    """
    
    name = "transform_enrich_record"
    
    def apply(self, records, context):
        """Enrich records with computed fields."""
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


# Configuration
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")
SAMPLE_DATA = generate_sample_data(count=50)


class E2ETransformationTestPipeline(AbstractPipeline):
    """E2E test pipeline for verifying transformation chains."""
    
    name = "e2e_transformation_test"
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        return []
    
    def define_source(self, params):
        return mock_source(
            data=SAMPLE_DATA,
            batch_size=10,
        )
    
    def define_destination(self, params):
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
        return [
            AddTimestampTransformation(),
            EnrichRecordTransformation(),
        ]


pipeline_registry.register(E2ETransformationTestPipeline())
