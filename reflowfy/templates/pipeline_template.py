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

from typing import Any, Dict, List
from reflowfy import (
    AbstractPipeline,
    BaseTransformation,
)
from reflowfy.destinations.console import console_destination
from reflowfy.sources.mock import generate_sample_data, mock_source

# ============================================================================
# Transformations
# ============================================================================


class UppercaseNames(BaseTransformation):
    """Transform names to uppercase."""

    name = "uppercase_names"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """Convert first_name and last_name to uppercase."""
        transformed = []

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

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """Keep only records where active=True."""
        return [r for r in records if r.get("active", False)]


class AddProcessingInfo(BaseTransformation):
    """Add processing metadata to each record."""

    name = "add_processing_info"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """Add processing information from context."""
        execution_id = runtime_params.get("execution_id", "unknown")

        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id

        return records


# ============================================================================
# Pipeline Definition
# ============================================================================

# Pre-generate sample data
SAMPLE_DATA = generate_sample_data(count=500)


class SimpleTestPipeline(AbstractPipeline):
    """
    Simple test pipeline with mock source and console destination.

    No parameters required - uses pre-generated mock data.
    """

    name = "simple_test_pipeline"
    rate_limit = 10

    def define_parameters(self):
        """No parameters needed for this simple test pipeline."""
        return []

    def define_source(self, runtime_params: Dict[str, Any]) -> Any:
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

    def define_destination(self, records: List[Any], runtime_params: Dict[str, Any]) -> Any:
        """Return console destination."""
        return console_destination(
            pretty_print=True,
            max_records_display=10,
        )

    def define_transformations(
        self, records: List[Any], runtime_params: Dict[str, Any]
    ) -> List[Any]:
        """Return transformation pipeline."""
        return [
            FilterActiveUsers(),
            UppercaseNames(),
            AddProcessingInfo(),
        ]
