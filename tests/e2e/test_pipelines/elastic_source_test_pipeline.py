"""
Elasticsearch Source Test Pipeline.

Pipeline that reads from Elasticsearch and outputs to console.
Used for E2E testing of the ElasticSource connector.
"""

import os
from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
    pipeline_registry,
    BaseTransformation,
    elastic_source,
)
from reflowfy.destinations.console import console_destination


class AddSourceInfo(BaseTransformation):
    """Add source metadata to records."""
    
    name = "add_source_info"
    
    def apply(self, records, context):
        """Add source identification to records."""
        for record in records:
            record["_source_type"] = "elasticsearch"
            record["_test_pipeline"] = "elastic_source_test"
        return records


# Configuration from environment
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201")
INDEX_NAME = "e2e-test-events"


class E2EElasticSourceTestPipeline(AbstractPipeline):
    """E2E test pipeline for Elasticsearch source."""
    
    name = "e2e_elastic_source_test"
    rate_limit = {"jobs_per_second": 10}
    
    def define_parameters(self):
        return [
            PipelineParameter(name="start_time", required=True),
            PipelineParameter(name="end_time", required=True),
        ]
    
    def define_source(self, params):
        return elastic_source(
            url=ELASTICSEARCH_URL,
            index=INDEX_NAME,
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
                "sort": [{"@timestamp": {"order": "desc"}}],
            },
            scroll="2m",
            size=50,
        )
    
    def define_destination(self, params):
        return console_destination(
            pretty_print=True,
            max_records_display=5,
        )
    
    def define_transformations(self, params):
        return [AddSourceInfo()]


pipeline_registry.register(E2EElasticSourceTestPipeline())
