"""
ID-Based API Source Test Pipeline.

Pipeline that fetches resources by ID from an API and outputs to console.
Used for E2E testing of the IDBasedAPISource connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_id_based_api
from tests.e2e.test_pipelines.shared_destinations import e2e_console


@transformation("api_id_add_source_info")
def api_id_add_source_info(records, context):
    """Add source metadata to records."""
    for record in records:
        record["_source_type"] = "api_id"
        record["_test_pipeline"] = "e2e_api_id_source_test"
    return records


@transformation("api_id_log_record_count")
def api_id_log_record_count(records, context):
    """Log the number of records processed."""
    print(f"  📊 ID-Based API Source: Processing {len(records)} records")
    return records


# Configuration from environment
# Inside Docker, this will be http://e2e-mock-api:8092
# Outside Docker (e.g., running tests locally), it defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class E2EApiIdSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for ID-based API source."""
    
    name = "e2e_api_id_source_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return [
            PipelineParameter(
                name="base_url",
                description="Base API URL",
                param_type=str,
                required=False,
                default=MOCK_API_URL,
            ),
            PipelineParameter(
                name="ids",
                description="List of user IDs to fetch",
                param_type=list,
                required=False,
                default=[1, 2, 3, 4, 5],
            ),
            PipelineParameter(
                name="batch_size",
                description="IDs per job batch",
                param_type=int,
                required=False,
                default=2,
            ),
        ]
    
    def define_source(self, params):
        return e2e_id_based_api(
            base_url=params.get("base_url", MOCK_API_URL),
            ids=params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=params.get("batch_size", 2),
        )
    
    def define_destination(self, params):
        return e2e_console()
    
    def define_transformations(self, params):
        return [
            api_id_log_record_count(),
            api_id_add_source_info(),
        ]
