"""
IdBasedPipeline E2E Test Pipeline — Batch POST API source.

Pipeline that uses IdBasedPipeline with ids_batch_size=10 to fetch user data
from the mock API's POST /users/batch endpoint.  Every 10 IDs are grouped
into a single POST request so define_source is called once per batch.

Used for E2E testing of the IdBasedPipeline with IDBasedAPISource in batch POST mode.
"""

from reflowfy import (
    IdBasedPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_id_based_api
from tests.e2e.test_pipelines.shared_destinations import e2e_console


@transformation("api_batch_add_metadata")
def api_batch_add_metadata(records, context):
    """Stamp each record with the IDs batch and execution context."""
    current_ids = context.get("current_ids", [])
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_batch_ids"] = current_ids
        record["_execution_id"] = execution_id
        record["_source"] = "api_batch_post"
    return records


@transformation("api_batch_filter_active")
def api_batch_filter_active(records, context):
    """Keep only active users and add a computed display_name field."""
    result = []
    for record in records:
        if record.get("active", True):
            record["display_name"] = f"{record.get('name', '')} <{record.get('email', '')}>"
            result.append(record)
    return result


class E2EIdBasedAPIBatchPipelineTest(IdBasedPipeline):
    """
    E2E test pipeline for IdBasedPipeline + IDBasedAPISource in batch POST mode.

    - ids_batch_size=10: groups every 10 IDs into one POST /users/batch call
    - Two transformations: metadata stamp + active-user filter
    - Console destination
    """

    name = "e2e_id_based_api_batch_pipeline_test"
    rate_limit = {"jobs_per_second": 20}
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

    def define_source(self, params, current_ids):
        """POST current_ids to /users/batch and return matching user records."""
        batch_size = params.get("batch_size", 5)
        return e2e_id_based_api(
            endpoint_template="/users/batch",   # no {id} → batch mode auto-detected
            ids=current_ids,
            method="POST",
            batch_size=batch_size,
            batch_id_key="ids",
            data_key="users",
        )

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=5)

    def define_transformations(self, params, current_ids):
        return [
            api_batch_add_metadata(),
            api_batch_filter_active(),
        ]
