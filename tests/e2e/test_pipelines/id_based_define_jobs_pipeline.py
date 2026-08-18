"""
IdBasedPipeline + custom define_jobs E2E test pipeline.

The one-input-ID-fans-out-into-many-jobs case: define_jobs owns the splitting,
so define_source never runs and each child ID becomes its own job with its own
transformations, destination write and retry.
"""

from reflowfy import IdBasedPipeline, RuntimeParams, job
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.transformations import id_fanout_stamp

CHILDREN_PER_ID = 3


class E2EIdBasedDefineJobsPipeline(IdBasedPipeline[RuntimeParams]):
    """Each input ID expands into CHILDREN_PER_ID jobs, each tagged with its ID."""

    name = "e2e_id_based_define_jobs"
    rate_limit = 3000  # jobs per minute

    def define_jobs(self, runtime_params):
        for parent_id in runtime_params.get("ids", []):
            for child in range(CHILDREN_PER_ID):
                yield job(
                    [{"parent_id": parent_id, "child_id": f"{parent_id}-{child}"}],
                    current_id=parent_id,
                    current_ids=[parent_id],
                )

    # define_source is deliberately absent: overriding define_jobs means it is
    # never called, and an IdBasedPipeline must not require a dummy stub.

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return [id_fanout_stamp()]
