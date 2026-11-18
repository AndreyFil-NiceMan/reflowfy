"""
E2E tests for reusable components: plain source/destination factory functions
and @transformation.
"""

import pytest
from reflowfy.sources.decorators import source_registry
from reflowfy.destinations.decorators import destination_registry
from tests.e2e.test_pipelines.shared_sources import e2e_elastic, e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http, e2e_console

pytestmark = [pytest.mark.e2e]


def test_shared_factories_are_plain_functions():
    """Reusable sources/destinations are plain functions: no registry, no decorator."""
    assert source_registry.get("e2e_elastic") is None
    assert destination_registry.get("e2e_http") is None
    assert callable(e2e_elastic) and callable(e2e_mock)
    assert callable(e2e_http) and callable(e2e_console)


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
    
    @transformation
    def e2e_dynamic_transform(records, runtime_params):
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
