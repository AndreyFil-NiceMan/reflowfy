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

from reflowfy import (
    build_pipeline,
    pipeline_registry,
    BaseTransformation,
)
from reflowfy.sources.mock import mock_source, generate_sample_data
from reflowfy.destinations.console import console_destination


# 1. Define a simple transformation
class UppercaseNames(BaseTransformation):
    """Transform names to uppercase."""
    
    name = "uppercase_names"
    
    def apply(self, records, context):
        """
        Convert first_name and last_name to uppercase.
        
        Args:
            records: List of records
            context: Execution context
        
        Returns:
            Transformed records
        """
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
    
    def apply(self, records, context):
        """
        Keep only records where active=True.
        
        Args:
            records: List of records
            context: Execution context
        
        Returns:
            Filtered records
        """
        return [r for r in records if r.get("active", False)]


class AddProcessingInfo(BaseTransformation):
    """Add processing metadata to each record."""
    
    name = "add_processing_info"
    
    def apply(self, records, context):
        """
        Add processing information from context.
        
        Args:
            records: List of records
            context: Execution context
        
        Returns:
            Enhanced records
        """
        execution_id = context.get("execution_id", "unknown")
        
        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id
        
        return records


# 2. Generate sample data
sample_data = generate_sample_data(count=50)

# 3. Configure source (mock data - no external dependencies!)
source = mock_source(
    data=sample_data,
    batch_size=10,
)

# 4. Configure destination (console - just prints to stdout!)
destination = console_destination(
    pretty_print=True,
    max_records_display=5,
)

# 5. Build and register pipeline
pipeline = build_pipeline(
    name="simple_test_pipeline",
    source=source,
    transformations=[
        FilterActiveUsers(),
        UppercaseNames(),
        AddProcessingInfo(),
    ],
    destination=destination,
    rate_limit=10,
)

pipeline_registry.register(pipeline)

# That's it! Test it with:
# POST http://localhost:8000/pipelines/simple_test_pipeline/test
# (No query parameters needed - mock source doesn't use runtime params)
