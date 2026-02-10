"""
IdBasedPipeline E2E Test Pipeline.

Pipeline that uses IdBasedPipeline to process multiple IDs dynamically.
Each ID is used to source data from the mock API (per-ID endpoint),
apply transformations, and output to console.

Used for E2E testing of the IdBasedPipeline feature.
"""

import os
from reflowfy import (
    IdBasedPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source


class AddIdMetadataTransformation(BaseTransformation):
    """Add the current_id and processing metadata to each record."""
    
    name = "id_pipeline_add_metadata"
    
    def apply(self, records, context):
        """Add ID metadata to each record."""
        current_id = context.get("current_id", "unknown")
        execution_id = context.get("execution_id", "unknown")
        for record in records:
            record["_processed_by_id_pipeline"] = True
            record["_current_id"] = current_id
            record["_execution_id"] = execution_id
        return records


class EnrichWithIdTransformation(BaseTransformation):
    """Enrich records with computed fields based on ID processing."""
    
    name = "id_pipeline_enrich"
    
    def apply(self, records, context):
        """Enrich records."""
        for record in records:
            record["_id_pipeline_verified"] = record.get("_processed_by_id_pipeline", False)
            record["_test_pipeline"] = "e2e_id_based_pipeline_test"
        return records


# Configuration
MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")


class E2EIdBasedPipelineTest(IdBasedPipeline):
    """
    E2E test pipeline for IdBasedPipeline feature.
    
    Each ID generates mock data and processes it independently.
    This tests the core IdBasedPipeline flow: per-ID source resolution,
    per-ID transformations, and shared destination.
    """
    
    name = "e2e_id_based_pipeline_test"
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        return [
            PipelineParameter(
                name="records_per_id",
                description="Number of mock records to generate per ID",
                param_type=int,
                required=False,
                default=10,
            ),
        ]
    
    def define_source(self, params, current_id):
        """Create a mock source with data unique to this ID."""
        records_per_id = params.get("records_per_id", 10)
        
        # Generate mock data specific to this ID
        data = [
            {
                "id": f"{current_id}_record_{i}",
                "entity_id": current_id,
                "value": f"data_for_{current_id}_{i}",
                "index": i,
            }
            for i in range(records_per_id)
        ]
        
        return mock_source(data=data, batch_size=5)
    
    def define_destination(self, params):
        from reflowfy.destinations.console import console_destination
        return console_destination(
            pretty_print=False,
            max_records_display=3,
        )
    
    def define_transformations(self, params, current_id):
        return [
            AddIdMetadataTransformation(),
            EnrichWithIdTransformation(),
        ]


pipeline_registry.register(E2EIdBasedPipelineTest())
