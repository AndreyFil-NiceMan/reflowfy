"""
E2E Test Pipelines Package.

This package contains pipeline definitions for E2E testing:
- Source tests (Elasticsearch, SQL) → Console destination
- Destination tests (HTTP, Kafka) ← Mock source
"""

# Import and register all test pipelines
# These are imported when the package is loaded by the ReflowManager/Worker

from tests.e2e.test_pipelines.elastic_source_test_pipeline import pipeline as elastic_source_pipeline
from tests.e2e.test_pipelines.sql_source_test_pipeline import pipeline as sql_source_pipeline
from tests.e2e.test_pipelines.http_dest_test_pipeline import pipeline as http_dest_pipeline
from tests.e2e.test_pipelines.kafka_dest_test_pipeline import pipeline as kafka_dest_pipeline

__all__ = [
    "elastic_source_pipeline",
    "sql_source_pipeline",
    "http_dest_pipeline",
    "kafka_dest_pipeline",
]

