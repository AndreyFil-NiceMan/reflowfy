"""
E2E Test Pipelines Package.

This package contains pipeline definitions for E2E testing:
- Source tests (Elasticsearch, SQL, API) → Console destination
- Destination tests (HTTP, Kafka) ← Mock source

Pipelines are automatically registered when the module is imported.
"""

# Import all test pipeline modules - registration happens on import
from tests.e2e.test_pipelines import elastic_source_test_pipeline
from tests.e2e.test_pipelines import sql_source_test_pipeline
from tests.e2e.test_pipelines import http_dest_test_pipeline
from tests.e2e.test_pipelines import kafka_dest_test_pipeline
from tests.e2e.test_pipelines import api_source_test_pipeline
from tests.e2e.test_pipelines import api_id_source_test_pipeline
from tests.e2e.test_pipelines import crash_recovery_test_pipeline
from tests.e2e.test_pipelines import dlq_test_pipeline
from tests.e2e.test_pipelines import transformation_test_pipeline
from tests.e2e.test_pipelines import id_based_pipeline_test

# Export the pipeline classes for direct access if needed
from tests.e2e.test_pipelines.elastic_source_test_pipeline import E2EElasticSourceTestPipeline
from tests.e2e.test_pipelines.sql_source_test_pipeline import E2ESqlSourceTestPipeline
from tests.e2e.test_pipelines.http_dest_test_pipeline import E2EHttpDestTestPipeline
from tests.e2e.test_pipelines.kafka_dest_test_pipeline import E2EKafkaDestTestPipeline
from tests.e2e.test_pipelines.api_source_test_pipeline import E2EApiSourceTestPipeline
from tests.e2e.test_pipelines.api_id_source_test_pipeline import E2EApiIdSourceTestPipeline
from tests.e2e.test_pipelines.crash_recovery_test_pipeline import CrashRecoveryTestPipeline
from tests.e2e.test_pipelines.transformation_test_pipeline import E2ETransformationTestPipeline
from tests.e2e.test_pipelines.id_based_pipeline_test import E2EIdBasedPipelineTest

__all__ = [
    "E2EElasticSourceTestPipeline",
    "E2ESqlSourceTestPipeline",
    "E2EHttpDestTestPipeline",
    "E2EKafkaDestTestPipeline",
    "E2EApiSourceTestPipeline",
    "E2EApiIdSourceTestPipeline",
    "CrashRecoveryTestPipeline",
    "E2ETransformationTestPipeline",
    "E2EIdBasedPipelineTest",
]

