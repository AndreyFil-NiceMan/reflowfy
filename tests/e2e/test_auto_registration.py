"""
E2E Tests for pipeline auto-registration.

Verifies that the PipelineMeta metaclass correctly registers
pipelines on definition, eliminating the need for manual registration.
"""

import pytest
from reflowfy import pipeline_registry, AbstractPipeline

pytestmark = [pytest.mark.e2e]


def test_pipelines_auto_registered():
    """Verify that E2E test pipelines are auto-registered."""
    # The tests/e2e/test_pipelines package is loaded during test module collection
    # or by the ReflowManager itself. We just check the registry.
    pipelines = pipeline_registry.list_all()
    
    # Check that our main test pipelines are present
    pipeline_names = [p.name for p in pipelines]
    assert "e2e_elastic_source_test" in pipeline_names
    assert "e2e_api_dest_test" in pipeline_names
    assert "e2e_transformation_test" in pipeline_names


def test_pipeline_names_match_class_attribute():
    """Verify registry names match the name attribute."""
    pipeline = pipeline_registry.get("e2e_elastic_source_test")
    assert pipeline is not None
    assert pipeline.name == "e2e_elastic_source_test"
    

def test_dynamic_auto_registration():
    """Verify we can auto-register a pipeline dynamically."""
    
    # Store initial count
    initial_count = len(pipeline_registry.list_all())
    
    # Define a new pipeline class
    class DynamicTestPipeline(AbstractPipeline):
        name = "dynamic_auto_test_123"
        def define_source(self, params): pass
        def define_destination(self, params): pass
        def define_transformations(self, params): return []
        
    # Verify it was added
    new_count = len(pipeline_registry.list_all())
    assert new_count == initial_count + 1
    
    # Verify it's retrievable
    retrieved = pipeline_registry.get("dynamic_auto_test_123")
    assert retrieved is not None
    assert retrieved.name == "dynamic_auto_test_123"
    assert isinstance(retrieved, DynamicTestPipeline)


def test_no_duplicate_registrations():
    """Verify idempotent registration prevents duplicates."""
    
    class DuplicateTestPipeline(AbstractPipeline):
        name = "duplicate_test_123"
        def define_source(self, params): pass
        def define_destination(self, params): pass
        def define_transformations(self, params): return []
        
    # Auto-registered once on class definition
    initial_count = len(pipeline_registry.list_all())
    
    # Manually register again (simulating old pattern)
    pipeline_registry.register(DuplicateTestPipeline())
    
    # Count should not change
    after_count = len(pipeline_registry.list_all())
    assert initial_count == after_count
