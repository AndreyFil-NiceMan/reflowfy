"""
Example pipeline for testing Reflofy with Elasticsearch source.

This pipeline:
1. Fetches data from Elasticsearch test index
2. Filters records by status and date range
3. Enriches records with processing metadata
4. Outputs to console for easy testing

Usage:
    # Start Elasticsearch:
    docker compose -f docker-compose.elastic.yml up -d
    
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

import os
from datetime import datetime
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
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
        self.allowed_status = allowed_status
    
    def apply(self, records, context):
        """Filter records by status."""
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
        """Add processing metadata."""
        execution_id = context.get("execution_id", "unknown")
        pipeline_name = context.get("pipeline_name", "unknown")
        
        for record in records:
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
        """Format event data fields."""
        for record in records:
            event_type = record.get("event_type", "unknown")
            user_name = record.get("user_name", "unknown")
            timestamp = record.get("@timestamp", "unknown")
            
            record["_summary"] = f"{event_type} by {user_name} at {timestamp}"
            
            event_data = record.get("event_data", {})
            if event_type == "purchase" and "amount" in event_data:
                event_data["formatted_amount"] = f"${event_data['amount']:.2f}"
            
        return records


# ============================================================================
# Pipeline Definition
# ============================================================================

class ElasticTestPipeline(AbstractPipeline):
    """
    Elasticsearch-based test pipeline.
    
    Demonstrates:
    - Dynamic source configuration with runtime parameters
    - Conditional filtering based on parameters
    - Exposed parameters for API documentation
    """
    
    name = "elastic_test_pipeline"
    rate_limit = {"jobs_per_second": 20}
    
    def define_parameters(self):
        """Define runtime parameters for this pipeline."""
        return [
            PipelineParameter(
                name="start_time",
                description="Start of time range (ISO format)",
                required=True,
                param_type=str,
            ),
            PipelineParameter(
                name="end_time",
                description="End of time range (ISO format)",
                required=True,
                param_type=str,
            ),
            PipelineParameter(
                name="filter_status",
                description="Status to filter by",
                required=False,
                param_type=str,
                default="active",
                choices=["active", "inactive", "pending"],
            ),
        ]
    
    def define_source(self, params):
        """Configure Elasticsearch source with runtime parameters."""
        elasticsearch_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
        
        return elastic_source(
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
            size=1,
        )
    
    def define_destination(self, params):
        """Configure console destination."""
        return console_destination(
            pretty_print=True,
            max_records_display=10,
        )
    
    def define_transformations(self, params):
        """Build transformation pipeline."""
        filter_status = params.get("filter_status", "active")
        
        return [
            FilterByStatus(allowed_status=filter_status),
            EnrichWithProcessingInfo(),
            FormatEventData(),
        ]

