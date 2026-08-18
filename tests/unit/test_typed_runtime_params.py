"""Typed runtime_params: params declared as a TypedDict drive the runtime.

The point of `AbstractPipeline[MyParams]` is that one declaration types the
hooks *and* replaces `define_parameters()`. These tests pin the second half —
that the derived declaration is equivalent to the hand-written list it replaces,
and that pipelines which declare nothing keep working exactly as before.
"""

import warnings
from typing import Any, Dict, List, Literal, Optional

import pytest
from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    IdBasedPipeline,
    Param,
    PipelineParameter,
    RuntimeParams,
    pipeline_registry,
)
from reflowfy.core.abstract_pipeline import params_from_typeddict


class SampleParams(RuntimeParams, total=False):
    env: Required[Literal["dev", "prod"]]
    limit: Annotated[NotRequired[int], Param("Max rows", default=10)]
    ratio: NotRequired[float]
    enabled: Annotated[NotRequired[bool], Param(default=False)]
    tags: NotRequired[List[str]]
    extra: NotRequired[Dict[str, Any]]
    maybe: NotRequired[Optional[str]]
    # An enrichment key: types the dict, but is not a caller-supplied parameter.
    written_at_runtime: Annotated[str, Param(internal=True)]


def _by_name(params: List[PipelineParameter]) -> Dict[str, PipelineParameter]:
    return {p.name: p for p in params}


class TestParamsFromTypedDict:
    def test_required_and_choices_from_literal(self):
        env = _by_name(params_from_typeddict(SampleParams))["env"]
        assert env.required is True
        assert env.choices == ["dev", "prod"]
        # str, not Literal — PipelineParameter._check_type calls isinstance().
        assert env.param_type is str

    def test_annotated_param_supplies_default_and_description(self):
        limit = _by_name(params_from_typeddict(SampleParams))["limit"]
        assert limit.param_type is int
        assert limit.required is False
        assert limit.default == 10
        assert limit.description == "Max rows"

    def test_param_types_are_bare_runtime_types(self):
        """`isinstance()` and the body/query split need `list`, not `list[str]`."""
        derived = _by_name(params_from_typeddict(SampleParams))
        assert derived["ratio"].param_type is float
        assert derived["enabled"].param_type is bool
        assert derived["enabled"].default is False
        assert derived["tags"].param_type is list
        assert derived["extra"].param_type is dict
        assert derived["maybe"].param_type is str

    def test_derived_params_validate_real_values(self):
        derived = _by_name(params_from_typeddict(SampleParams))
        assert derived["limit"].validate(5) is None
        assert derived["limit"].validate("nope") is not None
        assert derived["env"].validate("dev") is None
        assert derived["env"].validate("staging") is not None
        assert derived["env"].validate(None) == "Missing required parameter: env"

    def test_framework_keys_are_not_user_params(self):
        """RuntimeParams' own keys are the execution context, not parameters."""
        names = {p.name for p in params_from_typeddict(SampleParams)}
        assert "execution_id" not in names
        assert "current_ids" not in names
        assert "env" in names

    def test_bare_runtime_params_derives_nothing(self):
        assert params_from_typeddict(RuntimeParams) == []

    def test_internal_keys_are_not_parameters(self):
        """Enrichment keys must not leak into the API schema or CLI prompts.

        They are written by the pipeline at runtime, never supplied by a caller,
        so declaring one to make the write type-check must not make it settable.
        """
        names = {p.name for p in params_from_typeddict(SampleParams)}
        assert "written_at_runtime" not in names
        assert "limit" in names, "a normal Param() key is still derived"


class TestDerivedDefineParameters:
    def test_declared_type_becomes_define_parameters(self):
        class DerivedPipeline(AbstractPipeline[SampleParams]):
            name = "unit_typed_params_derived"

            def define_source(self, runtime_params: SampleParams) -> List[Any]:
                return []

            def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return None

            def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return []

        pipeline = DerivedPipeline()
        assert pipeline._params_type is SampleParams
        assert _by_name(pipeline.define_parameters())["limit"].default == 10
        # Defaults flow through the normal machinery, unchanged by derivation.
        assert pipeline.apply_defaults({"env": "dev"})["limit"] == 10
        assert pipeline.validate_parameters({"env": "dev", "limit": 1}) == []
        assert pipeline.validate_parameters({}) != []

    def test_hand_written_define_parameters_still_wins(self):
        hand_written = [PipelineParameter(name="only_mine", param_type=int, default=7)]

        class OverridePipeline(AbstractPipeline[SampleParams]):
            name = "unit_typed_params_override"

            def define_parameters(self) -> List[PipelineParameter]:
                return hand_written

            def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return None

            def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return []

        assert OverridePipeline().define_parameters() == hand_written

    def test_undeclared_pipeline_warns_but_still_works(self):
        """Untyped runtime_params is deprecated — a warning, never a failure."""
        with pytest.warns(DeprecationWarning, match="does not declare a params type"):

            class WarnsPipeline(AbstractPipeline):
                name = "unit_typed_params_warns"

                def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                    return None

                def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                    return []

        assert pipeline_registry.get("unit_typed_params_warns") is not None

    def test_declared_pipeline_does_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)

            class QuietPipeline(AbstractPipeline[SampleParams]):
                name = "unit_typed_params_quiet"

                def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                    return None

                def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                    return []

    def test_undeclared_pipeline_is_unchanged(self):
        """The backward-compat contract: no type argument, no behavior change."""

        class LegacyPipeline(AbstractPipeline):
            name = "unit_typed_params_legacy"

            def define_source(self, runtime_params):  # type: ignore[no-untyped-def]
                return []

            def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return None

            def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return []

        pipeline = LegacyPipeline()
        assert pipeline._params_type is None
        assert pipeline.define_parameters() == []
        assert pipeline.apply_defaults({"anything": 1}) == {"anything": 1}

    def test_id_based_pipeline_derives_and_keeps_ids(self):
        """IdBasedPipeline prepends its own `ids` parameter to the derived ones."""

        class IdParams(RuntimeParams, total=False):
            region: Annotated[NotRequired[str], Param("Region", default="eu")]

        class IdPipeline(IdBasedPipeline[IdParams]):
            name = "unit_typed_params_id_based"

            def define_source(self, runtime_params: IdParams) -> List[Any]:
                return []

            def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return None

            def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return []

        params = _by_name(IdPipeline().get_all_parameters())
        assert params["ids"].required is True
        assert params["region"].default == "eu"

    def test_subclass_inherits_declared_params(self):
        class Parent(AbstractPipeline[SampleParams]):
            name = "unit_typed_params_parent"

            def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return None

            def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
                return []

        class Child(Parent):
            name = "unit_typed_params_child"

        assert Child()._params_type is SampleParams
