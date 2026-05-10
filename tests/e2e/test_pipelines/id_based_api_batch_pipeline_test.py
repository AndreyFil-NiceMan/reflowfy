"""
IdBasedPipeline E2E Test Pipeline — Batch POST API source.

Pipeline that uses IdBasedPipeline with ids_batch_size=10 to fetch user data
from the mock API's POST /users/batch endpoint. Every 10 IDs are grouped
into a single POST request so define_source is called once per batch.
"""

from reflowfy import IdBasedPipeline, PipelineParameter
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_id_based_api
from tests.e2e.test_pipelines.transformations import (
    api_batch_add_metadata,
    api_batch_filter_active,
)


class E2EIdBasedAPIBatchPipelineTest(IdBasedPipeline):
    """
    E2E test pipeline for IdBasedPipeline + IDBasedAPISource in batch POST mode.

    - ids_batch_size=10: groups every 10 IDs into one POST /users/batch call
    - Two transformations: metadata stamp + active-user filter
    - Console destination
    """

    name = "e2e_id_based_api_batch_pipeline_test"
    rate_limit = 20
    ids_batch_size = 10

    def define_parameters(self):
        return [
            PipelineParameter(
                name="batch_size",
                description="Records per SourceJob (controls job count per ID-batch)",
                param_type=int,
                required=False,
                default=5,
            ),
        ]

    def define_source(self, runtime_params):
        current_ids = runtime_params.get("current_ids", [])
        return e2e_id_based_api(
            endpoint_template="/users/batch",
            ids=current_ids,
            method="POST",
            batch_size=runtime_params.get("batch_size", 5),
            batch_id_key="ids",
            data_key="users",
        )

    def define_destination(self, records, runtime_params):
        return e2e_console(pretty_print=False, max_records_display=5)

    def define_transformations(self, records, runtime_params):
        return [
            api_batch_add_metadata(),
            api_batch_filter_active(),
        ]
