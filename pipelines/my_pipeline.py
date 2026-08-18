"""
Simple test pipeline that works without any external dependencies.

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
    Transformations,
    BaseDestination,
    Records,
    AbstractPipeline,
    RuntimeParams,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source, generate_sample_data
from reflowfy.destinations.console import console_destination

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

        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id

        return records


# ============================================================================
# Pipeline Definition
# ============================================================================

# Pre-generate sample data
SAMPLE_DATA = generate_sample_data(count=500)


class SimpleTestPipeline(AbstractPipeline[RuntimeParams]):
    """
    Simple test pipeline with mock source and console destination.

    No parameters required - uses pre-generated mock data.
    """

    name = "simple_test_pipeline"
    rate_limit = 10

    def define_source(self, runtime_params):
        """Return mock data source."""
        return mock_source(
            data=SAMPLE_DATA,
            batch_size=10,
        )

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        """Return console destination."""
        return console_destination(
            pretty_print=True,
            max_records_display=10,
        )

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        """Return transformation pipeline."""
        return [
            FilterActiveUsers(),
            UppercaseNames(),
            AddProcessingInfo(),
        ]
