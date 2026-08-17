"""
SQL Source Test Pipeline.

Pipeline that reads from PostgreSQL and outputs to console.
Uses a SQL query template loaded from queries/events_by_date.sql.
"""

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import AbstractPipeline, Param, RuntimeParams
from tests.e2e.test_pipelines.sources import e2e_sql
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.transformations import (
    sql_filter_by_status,
    sql_add_source_info,
)


class SqlSourceParams(RuntimeParams, total=False):
    """Parameters for :class:`E2ESqlSourceTestPipeline`."""

    start_time: Required[str]
    end_time: Required[str]
    filter_status: Annotated[NotRequired[str], Param(default="active")]


class E2ESqlSourceTestPipeline(AbstractPipeline[SqlSourceParams]):
    """E2E test pipeline for SQL source."""

    name = "e2e_sql_source_test"
    rate_limit = 600  # jobs per minute

    def define_source(self, runtime_params):
        return e2e_sql(
            query=self.load_query("events_by_date.sql"),
            id_column="id",
            batch_size=50,
        )

    def define_destination(self, records, runtime_params):
        return e2e_console(pretty_print=True, max_records_display=5)

    def define_transformations(self, records, runtime_params):
        return [
            sql_filter_by_status(),
            sql_add_source_info(),
        ]
