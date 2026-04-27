"""
IdBasedPipeline E2E Test Pipelines.

Two pipelines for testing the IdBasedPipeline feature:
- E2EIdBasedPipelineTest:      ids_batch_size=1 (default), per-ID source resolution
- E2EIdBasedBatchPipelineTest: ids_batch_size=2, two IDs per source call
"""

from reflowfy import IdBasedPipeline, PipelineParameter
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_console, e2e_http
from tests.e2e.test_pipelines.transformations import (
    id_pipeline_add_metadata,
    id_pipeline_enrich,
)


class E2EIdBasedPipelineTest(IdBasedPipeline):
    """
    E2E test pipeline for IdBasedPipeline feature.

    Each ID generates mock data and processes it independently.
    Tests per-ID source resolution, per-ID transformations, and shared destination.
    """

    name = "e2e_id_based_pipeline_test"
    rate_limit = 50

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
        records_per_id = params.get("records_per_id", 10)
        data = [
            {
                "id": f"{current_id}_record_{i}",
                "entity_id": current_id,
                "value": f"data_for_{current_id}_{i}",
                "index": i,
            }
            for current_id in current_ids
            for i in range(records_per_id)
        ]
        return e2e_mock(data=data, batch_size=5)

    def define_destination(self, params):
        return e2e_http()

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
    rate_limit = 50
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
        records_per_id = params.get("records_per_id", 5)
        data = [
            {
                "id": f"{current_id}_record_{i}",
                "entity_id": current_id,
                "batch_ids": current_ids,
            }
            for current_id in current_ids
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
