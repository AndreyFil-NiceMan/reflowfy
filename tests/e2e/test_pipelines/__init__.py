"""
E2E Test Pipelines Package.

This package contains pipeline definitions for E2E testing:
- Source tests (Elasticsearch, SQL, API) → Console destination
- Destination tests (HTTP, Kafka) ← Mock source

Pipelines are automatically registered via metaclass when their module is imported.
"""

# Import all test pipeline modules - registration happens automatically on class definition
from tests.e2e.test_pipelines import elastic_source_test_pipeline as elastic_source_test_pipeline
from tests.e2e.test_pipelines import sql_source_test_pipeline as sql_source_test_pipeline
from tests.e2e.test_pipelines import http_dest_test_pipeline as http_dest_test_pipeline
from tests.e2e.test_pipelines import kafka_dest_test_pipeline as kafka_dest_test_pipeline
from tests.e2e.test_pipelines import api_source_test_pipeline as api_source_test_pipeline
from tests.e2e.test_pipelines import api_id_source_test_pipeline as api_id_source_test_pipeline
from tests.e2e.test_pipelines import crash_recovery_test_pipeline as crash_recovery_test_pipeline
from tests.e2e.test_pipelines import dlq_test_pipeline as dlq_test_pipeline
from tests.e2e.test_pipelines import transformation_test_pipeline as transformation_test_pipeline
from tests.e2e.test_pipelines import id_based_pipeline_test as id_based_pipeline_test
from tests.e2e.test_pipelines import id_based_api_batch_pipeline_test as id_based_api_batch_pipeline_test
from tests.e2e.test_pipelines import id_based_api_advanced_pipeline_test as id_based_api_advanced_pipeline_test
from tests.e2e.test_pipelines import shared_sources as shared_sources
from tests.e2e.test_pipelines import shared_destinations as shared_destinations
from tests.e2e.test_pipelines import rate_limit_test_pipeline as rate_limit_test_pipeline
from tests.e2e.test_pipelines import advanced_transformation_pipeline as advanced_transformation_pipeline
from tests.e2e.test_pipelines import dedup_test_pipeline as dedup_test_pipeline
