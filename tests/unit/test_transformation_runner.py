"""Unit tests for the iterative transformation runner."""

import pytest

from reflowfy.execution.transformation_runner import apply_transformations_iteratively
from reflowfy.transformations.base import BaseTransformation, TransformationError


class AppendMarker(BaseTransformation):
    """Appends its own name to the record list (so we can see ordering)."""

    name = "append_marker"

    def apply(self, records, runtime_params):
        return records + [self.name]


class SetFlag(BaseTransformation):
    name = "set_flag"

    def apply(self, records, runtime_params):
        runtime_params["should_add_2"] = True
        return records + [self.name]


class Marker2(BaseTransformation):
    name = "marker2"

    def apply(self, records, runtime_params):
        return records + [self.name]


class SetK2(BaseTransformation):
    name = "set_k2"

    def apply(self, records, runtime_params):
        runtime_params["k2"] = True
        return records + [self.name]


class Marker3(BaseTransformation):
    name = "marker3"

    def apply(self, records, runtime_params):
        return records + [self.name]


class BoomOnApply(BaseTransformation):
    name = "boom"

    def apply(self, records, runtime_params):
        raise ValueError("kaboom")


class FakePipeline:
    """Minimal stand-in: the helper only needs `name` + `define_transformations`."""

    name = "fake_pipeline"

    def __init__(self, fn):
        self._fn = fn

    def define_transformations(self, records, runtime_params):
        return self._fn(records, runtime_params)


def test_static_list_applies_each_once():
    pipeline = FakePipeline(lambda records, params: [AppendMarker(), Marker2()])
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    assert result == ["append_marker", "marker2"]
    assert [name for name, _ in applied] == ["append_marker", "marker2"]


def test_midchain_param_reveals_next_transformation():
    def define(records, params):
        trans = [SetFlag()]
        if params.get("should_add_2"):
            trans.append(Marker2())
        return trans

    pipeline = FakePipeline(define)
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    assert result == ["set_flag", "marker2"]
    assert [name for name, _ in applied] == ["set_flag", "marker2"]


def test_three_deep_chain():
    def define(records, params):
        trans = [SetFlag()]
        if params.get("should_add_2"):
            trans.append(SetK2())
        if params.get("k2"):
            trans.append(Marker3())
        return trans

    pipeline = FakePipeline(define)
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    assert [name for name, _ in applied] == ["set_flag", "set_k2", "marker3"]


def test_runaway_append_raises():
    # define_transformations always returns one more transformation than the
    # previous pass — an unbounded append that the call counter keeps growing.
    state = {"n": 0}

    def grow(records, params):
        state["n"] += 1
        return [AppendMarker() for _ in range(state["n"] + 1)]

    pipeline = FakePipeline(grow)
    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, [], {}, max_steps=5)
    assert "max_steps" in str(exc.value)


def test_prefix_change_is_ignored():
    # On the second pass the element at index 0 differs, but index 0 is already
    # applied, so it must not be re-applied; only the appended tail runs.
    calls = {"n": 0}

    def define(records, params):
        calls["n"] += 1
        if calls["n"] == 1:
            return [AppendMarker()]
        return [Marker2(), Marker3()]  # index 0 changed AppendMarker->Marker2

    pipeline = FakePipeline(define)
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    # First applied is the original index-0 (append_marker); then the new tail (marker3).
    assert [name for name, _ in applied] == ["append_marker", "marker3"]


def test_apply_error_is_wrapped():
    pipeline = FakePipeline(lambda records, params: [BoomOnApply()])
    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, [], {})
    assert exc.value.transformation_name == "boom"
