"""
SQL Source Test Pipeline.

Pipeline that reads from PostgreSQL and outputs to console.
Uses a SQL query template loaded from queries/events_by_date.sql.
Used for E2E testing of the SqlSource connector.
"""

from pathlib import Path

from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_sql
from tests.e2e.test_pipelines.shared_destinations import e2e_console


# Load query from the queries/ folder
QUERIES_DIR = Path(__file__).parent / "queries"
SQL_QUERY = (QUERIES_DIR / "events_by_date.sql").read_text()


@transformation("sql_add_source_info")
def sql_add_source_info(records, context):
    """Add source metadata to records."""
    for record in records:
        record["_source_type"] = "sql"
        record["_test_pipeline"] = "sql_source_test"
    return records


@transformation("sql_filter_by_status")
def sql_filter_by_status(records, context):
    """Filter records by status."""
    status_filter = context.get("runtime_params", {}).get("filter_status", "active")
    filtered = [r for r in records if r.get("status") == status_filter]
    print(f"  📊 SQL Filter: {len(records)} → {len(filtered)} records (status={status_filter})")
    return filtered


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
        return e2e_sql(
            query=SQL_QUERY,
            id_column="id",
            batch_size=50,
        )
    
    def define_destination(self, params):
        return e2e_console(pretty_print=True, max_records_display=5)
    
    def define_transformations(self, params):
        return [
            sql_filter_by_status(),
            sql_add_source_info(),
        ]
