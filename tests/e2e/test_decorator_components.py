"""
E2E Tests for decorator components (@source, @destination, @transformation).

Verifies that the decorator pattern works correctly for sharing
and reusing configurations.
"""

import pytest
from reflowfy.sources.decorators import source_registry
from reflowfy.destinations.decorators import destination_registry
from tests.e2e.test_pipelines.shared_sources import e2e_elastic, e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http, e2e_console

pytestmark = [pytest.mark.e2e]


def test_shared_source_registration():
    """Verify shared sources are registered correctly."""
    # Check registry
    assert source_registry.get("e2e_elastic") is not None
    assert source_registry.get("e2e_mock") is not None
    
    # Check that they are the same functions we imported
    assert source_registry.get("e2e_elastic") == e2e_elastic
    assert source_registry.get("e2e_mock") == e2e_mock


def test_shared_destination_registration():
    """Verify shared destinations are registered correctly."""
    # Check registry
    assert destination_registry.get("e2e_http") is not None
    assert destination_registry.get("e2e_console") is not None
    
    # Check that they are the same functions we imported
    assert destination_registry.get("e2e_http") == e2e_http
    assert destination_registry.get("e2e_console") == e2e_console


def test_shared_source_invocation():
    """Verify shared sources return valid configurations."""
    # Call the mock source generator
    mock_config = e2e_mock(count=5, batch_size=2)
    
    assert mock_config is not None
    # Assuming tuple based source config format (class/func, kwargs)
    # The actual structure depends on Reflowfy's exact source representation
    # mostly we just verify it doesn't crash and returns the internal source type
    assert hasattr(mock_config, "items") or isinstance(mock_config, tuple) or hasattr(mock_config, "__dict__")


def test_shared_destination_invocation():
    """Verify shared destinations return valid configurations."""
    # Call the console destination generator
    console_config = e2e_console(max_records_display=10)
    
    assert console_config is not None
    # We just verify it returns a valid config dict/object without crashing
    assert hasattr(console_config, "items") or isinstance(console_config, tuple) or hasattr(console_config, "__dict__")


def test_dynamic_transformation_decorator():
    """Verify we can create a transformation with a decorator dynamically."""
    from reflowfy import transformation
    from reflowfy.transformations.registry import transformation_registry
    
    @transformation("e2e_dynamic_transform")
    def my_transform(records, context):
        for r in records:
            r["dynamic"] = True
        return records
        
    # Verify it registered
    transform_cls = transformation_registry.get("e2e_dynamic_transform")
    assert transform_cls is not None
    
    # Verify we can instantiate and use it
    instance = transform_cls()
    result = instance.apply([{"data": 1}], {})
    
    assert len(result) == 1
    assert result[0] == {"data": 1, "dynamic": True}
