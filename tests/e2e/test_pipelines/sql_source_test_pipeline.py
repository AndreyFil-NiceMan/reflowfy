"""
SQL Source Test Pipeline.

Pipeline that reads from PostgreSQL and outputs to console.
Used for E2E testing of the SqlSource connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
    sql_source,
)
from reflowfy.destinations.console import console_destination


class AddSourceInfo(BaseTransformation):
    """Add source metadata to records."""
    
    name = "sql_add_source_info"
    
    def apply(self, records, context):
        """Add source identification to records."""
        for record in records:
            record["_source_type"] = "sql"
            record["_test_pipeline"] = "sql_source_test"
        return records


class FilterByStatus(BaseTransformation):
    """Filter records by status."""
    
    name = "sql_filter_by_status"
    
    def apply(self, records, context):
        """Keep only active records."""
        status_filter = context.get("runtime_params", {}).get("filter_status", "active")
        filtered = [r for r in records if r.get("status") == status_filter]
        print(f"  📊 SQL Filter: {len(records)} → {len(filtered)} records (status={status_filter})")
        return filtered


# Configuration from environment
SQL_CONNECTION_URL = os.getenv(
    "SQL_CONNECTION_URL", 
    "postgresql://reflowfy:reflowfy@localhost:5433/reflowfy_e2e"
)


class E2ESqlSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for SQL source."""
    
    name = "e2e_sql_source_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return [
            PipelineParameter(name="start_time", required=True),
            PipelineParameter(name="end_time", required=True),
            PipelineParameter(name="filter_status", required=False, default="active"),
        ]
    
    def define_source(self, params):
        return sql_source(
            connection_url=SQL_CONNECTION_URL,
            query="""
                SELECT id, event_type, user_id, user_name, status, amount, created_at, metadata
                FROM test_events
                WHERE created_at >= '{{ start_time }}'::timestamp
                  AND created_at <= '{{ end_time }}'::timestamp
                ORDER BY id
            """,
            id_column="id",
            batch_size=50,
        )
    
    def define_destination(self, params):
        return console_destination(
            pretty_print=True,
            max_records_display=5,
        )
    
    def define_transformations(self, params):
        return [
            FilterByStatus(),
            AddSourceInfo(),
        ]


pipeline_registry.register(E2ESqlSourceTestPipeline())
