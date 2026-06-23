"""Unit tests for ExecutionContext serialization helpers."""

from reflowfy.core.execution_context import (
    ExecutionContext,
    build_flat_runtime_params_from_metadata,
)


def test_to_dict_has_no_nested_metadata_key():
    ctx = ExecutionContext(execution_id="e1", pipeline_name="p", runtime_params={"x": 1})
    d = ctx.to_dict()
    assert "metadata" not in d           # nested dead field removed
    assert d["execution_id"] == "e1"
    assert d["runtime_params"] == {"x": 1}


def test_build_flat_params_still_works_without_nested_metadata():
    ctx = ExecutionContext(execution_id="e1", pipeline_name="p", runtime_params={"x": 1})
    flat = build_flat_runtime_params_from_metadata(ctx.to_dict())
    assert flat["x"] == 1
    assert flat["execution_id"] == "e1"
    assert flat["pipeline_name"] == "p"
