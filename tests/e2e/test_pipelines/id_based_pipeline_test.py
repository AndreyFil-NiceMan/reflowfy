"""
IdBasedPipeline E2E Test Pipeline.

Pipeline that uses IdBasedPipeline to process multiple IDs dynamically.
Each ID is used to source data from the mock API (per-ID endpoint),
apply transformations, and output to console.

Used for E2E testing of the IdBasedPipeline feature.
"""

from reflowfy import (
    IdBasedPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_console


@transformation("id_pipeline_add_metadata")
def id_pipeline_add_metadata(records, context):
    """Add the current_ids and processing metadata to each record."""
    current_ids = context.get("current_ids", [])
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_processed_by_id_pipeline"] = True
        record["_current_ids"] = current_ids
        record["_execution_id"] = execution_id
    return records


@transformation("id_pipeline_enrich")
def id_pipeline_enrich(records, context):
    """Enrich records with computed fields based on ID processing."""
    for record in records:
        record["_id_pipeline_verified"] = record.get("_processed_by_id_pipeline", False)
        record["_test_pipeline"] = "e2e_id_based_pipeline_test"
    return records


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
    
    def define_source(self, params, current_ids):
        """Create a mock source with data for each ID in the batch."""
        records_per_id = params.get("records_per_id", 10)

        data = []
        for current_id in current_ids:
            data += [
                {
                    "id": f"{current_id}_record_{i}",
                    "entity_id": current_id,
                    "value": f"data_for_{current_id}_{i}",
                    "index": i,
                }
                for i in range(records_per_id)
            ]

        return e2e_mock(data=data, batch_size=5)

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, params, current_ids):
        return [
            id_pipeline_add_metadata(),
            id_pipeline_enrich(),
        ]


class E2EIdBasedBatchPipelineTest(IdBasedPipeline):
    """
    E2E test pipeline for ids_batch_size > 1.

    Uses ids_batch_size=2 so every source resolution receives 2 IDs at once.
    """

    name = "e2e_id_based_batch_pipeline_test"
    rate_limit = {"jobs_per_second": 50}
    ids_batch_size = 2

    def define_parameters(self):
        return [
            PipelineParameter(
                name="records_per_id",
                description="Number of mock records to generate per ID",
                param_type=int,
                required=False,
                default=5,
            ),
        ]

    def define_source(self, params, current_ids):
        """Create a mock source with data for all IDs in the batch."""
        records_per_id = params.get("records_per_id", 5)

        data = []
        for current_id in current_ids:
            data += [
                {
                    "id": f"{current_id}_record_{i}",
                    "entity_id": current_id,
                    "batch_ids": current_ids,
                }
                for i in range(records_per_id)
            ]

        return e2e_mock(data=data, batch_size=5)

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, params, current_ids):
        return [
            id_pipeline_add_metadata(),
            id_pipeline_enrich(),
        ]
