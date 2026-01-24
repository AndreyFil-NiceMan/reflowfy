"""
ID-Based API Source Test Pipeline.

Pipeline that fetches resources by ID from an API and outputs to console.
Used for E2E testing of the IDBasedAPISource connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.api import id_based_api_source
from reflowfy.destinations.console import console_destination


class AddIDSourceInfo(BaseTransformation):
    """Add source metadata to records."""
    
    name = "api_id_add_source_info"
    
    def apply(self, records, context):
        """Add source identification to records."""
        for record in records:
            record["_source_type"] = "api_id"
            record["_test_pipeline"] = "e2e_api_id_source_test"
        return records


class LogIDRecordCount(BaseTransformation):
    """Log the number of records processed."""
    
    name = "api_id_log_record_count"
    
    def apply(self, records, context):
        """Log record count."""
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
        return id_based_api_source(
            base_url=params.get("base_url", MOCK_API_URL),
            endpoint_template="/users/{id}",
            ids=params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=params.get("batch_size", 2),
        )
    
    def define_destination(self, params):
        return console_destination(
            pretty_print=True,
            max_records_display=5,
        )
    
    def define_transformations(self, params):
        return [
            LogIDRecordCount(),
            AddIDSourceInfo(),
        ]


pipeline_registry.register(E2EApiIdSourceTestPipeline())
