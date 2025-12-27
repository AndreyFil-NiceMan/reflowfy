"""
Example pipeline for testing Reflofy with Elasticsearch source.

This pipeline:
1. Fetches data from Elasticsearch test index
2. Filters records by status and date range
3. Enriches records with processing metadata
4. Outputs to console for easy testing

Usage:
    # Start Elasticsearch:
    docker-compose -f docker-compose.elastic.yml up -d
    
    # Initialize test data:
    python examples/init_elastic_test_data.py
    
    # Test locally (synchronous, limited data):
    POST http://localhost:8000/pipelines/elastic_test_pipeline/test
    Query params:
      - start_time: 2024-01-01T00:00:00
      - end_time: 2024-12-31T23:59:59
      - filter_status: active  (optional, defaults to 'active')
    
    # Run distributed (async via Kafka):
    POST http://localhost:8000/pipelines/elastic_test_pipeline/run
    Same query params as above
"""

from reflowfy import (
    build_pipeline,
    pipeline_registry,
    BaseTransformation,
    elastic_source,
)
from reflowfy.destinations.console import console_destination


# ============================================================================
# Transformations
# ============================================================================

class FilterByStatus(BaseTransformation):
    """Filter records by status field."""
    
    name = "filter_by_status"
    
    def __init__(self, allowed_status="active"):
        """
        Initialize filter.
        
        Args:
            allowed_status: Status value to keep (default: 'active')
        """
        self.allowed_status = allowed_status
    
    def apply(self, records, context):
        """
        Filter records by status.
        
        Args:
            records: List of records
            context: Execution context
        
        Returns:
            Filtered records
        """
        # Get filter status from runtime params if available
        filter_status = context.get("runtime_params", {}).get(
            "filter_status", 
            self.allowed_status
        )
        
        filtered = [
            r for r in records 
            if r.get("status") == filter_status
        ]
        
        print(f"  📊 Filtered: {len(records)} → {len(filtered)} records (status={filter_status})")
        
        return filtered


class EnrichWithProcessingInfo(BaseTransformation):
    """Add processing metadata to records."""
    
    name = "enrich_processing_info"
    
    def apply(self, records, context):
        """
        Add processing metadata.
        
        Args:
            records: List of records
            context: Execution context
        
        Returns:
            Enriched records
        """
        from datetime import datetime
        
        execution_id = context.get("execution_id", "unknown")
        pipeline_name = context.get("pipeline_name", "unknown")
        
        for record in records:
            # Add processing info
            record["_reflofy_processed"] = {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "processed_at": datetime.utcnow().isoformat(),
                "framework": "reflofy",
            }
        
        return records


class FormatEventData(BaseTransformation):
    """Format event data for better readability."""
    
    name = "format_event_data"
    
    def apply(self, records, context):
        """
        Format event data fields.
        
        Args:
            records: List of records
            context: Execution context
        
        Returns:
            Formatted records
        """
        for record in records:
            # Add a formatted summary field
            event_type = record.get("event_type", "unknown")
            user_name = record.get("user_name", "unknown")
            timestamp = record.get("@timestamp", "unknown")
            
            record["_summary"] = f"{event_type} by {user_name} at {timestamp}"
            
            # Format event data based on type
            event_data = record.get("event_data", {})
            if event_type == "purchase" and "amount" in event_data:
                event_data["formatted_amount"] = f"${event_data['amount']:.2f}"
            
        return records


# ============================================================================
# Pipeline Configuration
# ============================================================================

import os

# Configure Elasticsearch source
# Use environment variable for URL (supports both local and Docker)
elasticsearch_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")

source = elastic_source(
    url=elasticsearch_url,
    index="reflofy-test-data",
    base_query={
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": "{{ start_time }}",
                                "lte": "{{ end_time }}",
                            }
                        }
                    }
                ],
            }
        },
        "sort": [
            {"@timestamp": {"order": "desc"}}
        ],
    },
    scroll="2m",
    size=1,  # 100 docs per scroll page
)

# Configure console destination (prints to stdout)
destination = console_destination(
    pretty_print=True,
    max_records_display=10,  # Show first 10 records
)

# Build and register pipeline
pipeline = build_pipeline(
    name="elastic_test_pipeline",
    source=source,
    transformations=[
        FilterByStatus(allowed_status="active"),
        EnrichWithProcessingInfo(),
        FormatEventData(),
    ],
    destination=destination,
    rate_limit={"jobs_per_second": 20},
)

pipeline_registry.register(pipeline)

# ============================================================================
# Pipeline Ready!
# ============================================================================
# 
# The API will automatically create these routes:
# - POST /pipelines/elastic_test_pipeline/run   (distributed mode)
# - POST /pipelines/elastic_test_pipeline/test  (local mode)
# - GET  /pipelines/elastic_test_pipeline/status
#
# Example requests:
#
# Local test (synchronous):
#   curl -X POST "http://localhost:8000/pipelines/elastic_test_pipeline/test?start_time=2024-01-01T00:00:00&end_time=2024-12-31T23:59:59&filter_status=active"
#
# Distributed run (async):
#   curl -X POST "http://localhost:8000/pipelines/elastic_test_pipeline/run?start_time=2024-01-01T00:00:00&end_time=2024-12-31T23:59:59"
#
# Or use Swagger UI at http://localhost:8000/docs
# ============================================================================
