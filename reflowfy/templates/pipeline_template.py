"""
Simple test pipeline that works without any external dependencies.

Auto-registered — no need to call pipeline_registry.register().

Perfect for testing the Reflowfy framework locally:
- No Elasticsearch needed
- No Kafka needed
- No databases needed
- Uses mock data source
- Prints to console

Just run the API and call the /test endpoint!
"""

from typing import Any, Dict

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    BaseSource,
    BaseTransformation,
    Records,
    RuntimeParams,
    Transformations,
    get_logger,
)

# Uncomment when you declare parameters on SimpleTestParams below:
# from typing import Literal
# from typing_extensions import Annotated, NotRequired, Required
# from reflowfy import Param
from reflowfy.destinations.console import console_destination
from reflowfy.sources.mock import generate_sample_data, mock_source

# Logging: works from anywhere — transformations, @source functions, plain helpers.
# No `self` needed. execution_id / job_id / pipeline_name are attached automatically.
logger = get_logger(__name__)

# ============================================================================
# Transformations
# ============================================================================


class UppercaseNames(BaseTransformation):
    """Transform names to uppercase."""

    name = "uppercase_names"

    def apply(self, records: Records, runtime_params: Dict[str, Any]) -> Records:
        """Convert first_name and last_name to uppercase."""
        transformed: Records = []

        for record in records:
            new_record = record.copy()

            if "first_name" in new_record:
                new_record["first_name"] = new_record["first_name"].upper()

            if "last_name" in new_record:
                new_record["last_name"] = new_record["last_name"].upper()

            transformed.append(new_record)

        return transformed


class FilterActiveUsers(BaseTransformation):
    """Filter only active users."""

    name = "filter_active_users"

    def apply(self, records: Records, runtime_params: Dict[str, Any]) -> Records:
        """Keep only records where active=True."""
        return [r for r in records if r.get("active", False)]


class AddProcessingInfo(BaseTransformation):
    """Add processing metadata to each record."""

    name = "add_processing_info"

    def apply(self, records: Records, runtime_params: Dict[str, Any]) -> Records:
        """Add processing information from context."""
        execution_id = runtime_params.get("execution_id", "unknown")
        logger.info("Tagging %d records for execution %s", len(records), execution_id)

        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id

        return records


# ============================================================================
# Pipeline Definition
# ============================================================================

# Pre-generate sample data
SAMPLE_DATA = generate_sample_data(count=500)


class SimpleTestParams(RuntimeParams, total=False):
    """The parameters this pipeline accepts, declared once as types.

    Subclassing RuntimeParams means the framework's execution-context keys
    (execution_id, batch_id, pipeline_name, created_at, ...) come along for free.
    Add your own below; each becomes a validated, documented pipeline parameter:

        env: Required[Literal["dev", "prod"]]        # required, with choices
        limit: Annotated[NotRequired[int], Param("Max rows", default=100)]

    Declare here, too, any key your own code *writes* into runtime_params.
    """


class SimpleTestPipeline(AbstractPipeline[SimpleTestParams]):
    """
    Simple test pipeline with mock source and console destination.

    No parameters required - uses pre-generated mock data.
    """

    name = "simple_test_pipeline"
    rate_limit = 600  # jobs per minute

    # define_parameters() is derived from SimpleTestParams — no need to write it
    # by hand. Override it if you would rather build the list yourself.

    def define_source(self, runtime_params: SimpleTestParams) -> BaseSource:
        """Return mock data source."""
        # Tip: load query templates from the queries/ folder with no boilerplate.
        # self.load_query() finds the file (recursively, so subfolders work) and
        # parses .json to a dict, returning .sql/.txt as text:
        #
        #   from reflowfy.sources.sql import sql_source
        #   return sql_source(query=self.load_query("example_query.sql"), id_column="id")
        return mock_source(
            data=SAMPLE_DATA,
            batch_size=10,
        )

    # To split the work into jobs yourself, implement define_jobs INSTEAD of
    # define_source — each element of the returned list is one job:
    #
    #   def define_jobs(self, runtime_params: SimpleTestParams) -> Iterable[Any]:
    #       from reflowfy import chunk
    #       rows = httpx.get(runtime_params["url"]).json()["items"]
    #       return chunk(rows, size=10)   # 10 records per job

    def define_destination(
        self, records: Records, runtime_params: SimpleTestParams
    ) -> BaseDestination:
        """Return console destination."""
        # Raise PipelineError from any define_* hook to fail the job deliberately —
        # see transformations/example_transformation.py. The message, type and
        # traceback land on the job record, and the error names the failing step.
        logger.debug("Resolving destination for %d records", len(records))
        return console_destination(
            pretty_print=True,
            max_records_display=10,
        )

    def define_transformations(
        self, records: Records, runtime_params: SimpleTestParams
    ) -> Transformations:
        """Return transformation pipeline."""
        return [
            FilterActiveUsers(),
            UppercaseNames(),
            AddProcessingInfo(),
        ]
