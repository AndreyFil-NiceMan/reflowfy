"""Behaviour of the core pipeline model that the main suites leave unexercised.

Parameter coercion corner cases, the pipeline constructor's validation, the
"call resolve() first" guards, the registry singleton, JSON normalisation,
Jinja parameter extraction, ID-based pipeline guards, discovery's sys.path
handling and the query-dir fallback.
"""

import logging
import sys
import threading
import types
from pathlib import Path
from typing import Any, Dict, Iterator, List, Union

import pytest

from reflowfy.core import pipeline_discovery
from reflowfy.core.abstract_pipeline import (
    AbstractPipeline,
    PipelineParameter,
    params_from_typeddict,
)
from reflowfy.core.execution_context import ExecutionContext, ParameterResolver
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.registry import PipelineRegistry, pipeline_registry
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.base import ExecutionState, ExecutionStatus
from reflowfy.transformations.registry import TransformationRegistry, transformation_registry


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Pipelines defined in a test register globally; put the registry back after."""
    saved = dict(pipeline_registry._pipelines)
    saved_failures = dict(pipeline_registry._failures)
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)
        pipeline_registry._failures.clear()
        pipeline_registry._failures.update(saved_failures)


def _new_class(name: str, base: Any, namespace: Dict[str, Any]) -> type:
    """`class name(base): ...` — new_class keeps the subscripted base's params type."""
    body = {"__module__": __name__, **namespace}
    return types.new_class(name, (base,), exec_body=lambda ns: ns.update(body))


def _pipeline_class(name: str = "", **attrs: Any) -> type:
    """A minimal concrete pipeline class (registers itself when `name` is set)."""

    def define_source(self: Any, runtime_params: Any) -> Any:
        return [{"id": 1}]

    def define_transformations(self: Any, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self: Any, records: Any, runtime_params: Any) -> Any:
        return None

    namespace: Dict[str, Any] = {
        "define_source": define_source,
        "define_transformations": define_transformations,
        "define_destination": define_destination,
        **attrs,
    }
    if name:
        namespace["name"] = name
    return _new_class("GapPipeline", AbstractPipeline[RuntimeParams], namespace)


# ----- PipelineParameter: type checks and coercion ---------------------------------


class TestFloatParameter:
    def test_int_is_accepted_as_float(self) -> None:
        assert PipelineParameter("ratio", param_type=float).validate(3) is None

    def test_bool_is_not_a_number(self) -> None:
        error = PipelineParameter("ratio", param_type=float).validate(True)
        assert error == "Parameter 'ratio' must be number, got bool"

    def test_string_is_coerced(self) -> None:
        assert PipelineParameter("ratio", param_type=float).coerce("2.5") == 2.5


class TestCoerce:
    def test_none_becomes_the_default(self) -> None:
        assert PipelineParameter("n", param_type=int, default=7).coerce(None) == 7

    @pytest.mark.parametrize("raw", ["false", "0", "no", "OFF"])
    def test_falsy_strings_become_false(self, raw: str) -> None:
        assert PipelineParameter("flag", param_type=bool).coerce(raw) is False

    def test_unrecognised_value_falls_back_to_truthiness(self) -> None:
        flag = PipelineParameter("flag", param_type=bool)
        assert flag.coerce("maybe") is True
        assert flag.coerce(0) is False

    def test_non_string_becomes_a_string(self) -> None:
        assert PipelineParameter("s", param_type=str).coerce(42) == "42"

    def test_json_object_string_becomes_a_dict(self) -> None:
        assert PipelineParameter("d", param_type=dict).coerce('{"a": 1}') == {"a": 1}

    def test_unparseable_value_is_returned_unchanged(self) -> None:
        """A failed coercion leaves the value for validate() to reject with a message."""
        param = PipelineParameter("n", param_type=int)
        assert param.coerce("abc") == "abc"
        assert param.validate("abc") == "Parameter 'n' must be integer, got str"

    def test_bad_json_dict_is_returned_unchanged(self) -> None:
        assert PipelineParameter("d", param_type=dict).coerce("{oops") == "{oops"

    def test_non_string_for_a_list_is_returned_unchanged(self) -> None:
        assert PipelineParameter("xs", param_type=list).coerce((1, 2)) == (1, 2)


def test_a_union_of_several_types_derives_a_string_parameter() -> None:
    """Optional[X] collapses to X; a real union has no single runtime type, so str."""

    class _UnionParams(RuntimeParams, total=False):
        either: Union[int, str]

    (param,) = params_from_typeddict(_UnionParams)
    assert (param.name, param.param_type) == ("either", str)


# ----- AbstractPipeline constructor and helpers --------------------------------------


class TestConstructor:
    def test_constructor_arguments_override_the_class_attributes(
        self, isolated_registry: None
    ) -> None:
        cls = _pipeline_class("gaps_ctor_pipeline", rate_limit=10.0)
        pipeline = cls(rate_limit=99.0, config={"k": "v"}, enable_duplicate_jobs=False)

        assert pipeline.rate_limit == 99.0
        assert pipeline.config == {"k": "v"}
        assert pipeline.enable_duplicate_jobs is False
        # ...without touching the registered instance's values
        assert pipeline_registry.get("gaps_ctor_pipeline").rate_limit == 10.0  # type: ignore[union-attr]

    def test_a_pipeline_without_a_name_cannot_be_built(self) -> None:
        cls = _pipeline_class()  # no name: never registered either
        with pytest.raises(ValueError, match="must define a 'name' attribute"):
            cls()

    def test_an_invalid_name_fails_registration_and_is_remembered(
        self, isolated_registry: None
    ) -> None:
        _pipeline_class("bad name!")

        assert not pipeline_registry.exists("bad name!")
        assert "must contain only alphanumeric" in pipeline_registry.describe_missing("bad name!")


class TestDefaultsAndHelpers:
    @pytest.fixture
    def pipeline(self, isolated_registry: None) -> Any:
        cls = _pipeline_class(
            "gaps_helpers_pipeline",
            rate_limit=12.0,
            define_parameters=lambda self: [
                PipelineParameter("env", required=True),
                PipelineParameter("limit", param_type=int, default=5),
            ],
        )
        return cls()

    def test_default_rate_limit_hook_returns_the_static_limit(self, pipeline: Any) -> None:
        assert pipeline.define_rate_limit({}) == 12.0

    def test_every_transformation_applies_by_default(self, pipeline: Any) -> None:
        assert pipeline.should_apply_transformation(object(), {}, []) is True

    def test_required_parameter_names(self, pipeline: Any) -> None:
        assert pipeline.get_required_parameters() == {"env"}

    def test_resolve_rejects_invalid_parameters(self, pipeline: Any) -> None:
        with pytest.raises(ValueError, match="Invalid parameters: Missing required parameter: env"):
            pipeline.fork().resolve({})

    def test_repr_names_the_pipeline(self, pipeline: Any) -> None:
        assert repr(pipeline) == "GapPipeline(name='gaps_helpers_pipeline')"

    def test_transformation_names_are_empty_when_the_hook_needs_real_records(
        self, isolated_registry: None
    ) -> None:
        def needs_records(self: Any, records: List[Any], runtime_params: Any) -> Any:
            return [records[0]["kind"]]  # IndexError on the empty discovery call

        cls = _pipeline_class("gaps_tx_names_pipeline", define_transformations=needs_records)
        assert cls().get_transformation_names() == []
        assert cls().to_dict()["transformations"] == []


class TestUnresolvedGuards:
    @pytest.fixture
    def pipeline(self, isolated_registry: None) -> Any:
        return _pipeline_class("gaps_unresolved_pipeline")()

    @pytest.mark.parametrize("attr", ["source", "destination", "transformations"])
    def test_reading_the_plan_before_resolve_says_so(self, pipeline: Any, attr: str) -> None:
        with pytest.raises(RuntimeError, match=r"Call pipeline.resolve\(runtime_params\)"):
            getattr(pipeline, attr)

    def test_destination_and_transformations_once_set(self, pipeline: Any) -> None:
        pipeline._destination = "dest"
        pipeline._transformations = ["t"]
        assert pipeline.destination == "dest"
        assert pipeline.transformations == ["t"]

    def test_resolve_clears_per_batch_state(self, pipeline: Any) -> None:
        resolved = pipeline.fork().resolve({})
        assert resolved.source == [{"id": 1}]
        with pytest.raises(RuntimeError):
            resolved.destination


# ----- IdBasedPipeline --------------------------------------------------------------


def _id_pipeline_class(name: str, **attrs: Any) -> type:
    def define_source(self: Any, runtime_params: Any) -> Any:
        return [{"id": i} for i in runtime_params["current_ids"]]

    def define_transformations(self: Any, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self: Any, records: Any, runtime_params: Any) -> Any:
        return None

    namespace: Dict[str, Any] = {
        "name": name,
        "define_source": define_source,
        "define_transformations": define_transformations,
        "define_destination": define_destination,
        **attrs,
    }
    return _new_class("GapIdPipeline", IdBasedPipeline[RuntimeParams], namespace)


class TestIdBasedPipeline:
    def test_declaring_input_ids_yourself_is_rejected(self, isolated_registry: None) -> None:
        cls = _id_pipeline_class(
            "gaps_id_input_ids", define_parameters=lambda self: [PipelineParameter("input_ids")]
        )
        with pytest.raises(ValueError, match="Do not define 'input_ids'"):
            cls().get_all_parameters()

    def test_transformation_names_survive_a_hook_that_raises(self, isolated_registry: None) -> None:
        def explode(self: Any, records: Any, runtime_params: Any) -> Any:
            raise KeyError("needs a real record")

        cls = _id_pipeline_class("gaps_id_tx_names", define_transformations=explode)
        assert cls().get_transformation_names() == []

    def test_repr_marks_the_pipeline_as_id_based(self, isolated_registry: None) -> None:
        pipeline = _id_pipeline_class("gaps_id_repr")()
        assert repr(pipeline) == "GapIdPipeline(name='gaps_id_repr', type='id_based')"

    def test_resolve_for_id_wraps_a_single_id(self, isolated_registry: None) -> None:
        pipeline = _id_pipeline_class("gaps_id_single")()
        resolved = pipeline.resolve_for_id({"input_ids": [5]}, 5)
        assert resolved["current_ids"] == [5]
        assert resolved["source"].config["records"] == [{"id": 5}]  # list -> StaticSource


# ----- PipelineRegistry / TransformationRegistry singletons -------------------------


class _RacingLock:
    """A lock whose acquisition lets another thread 'win' the singleton race."""

    def __init__(self, cls: type, winner: Any) -> None:
        self.cls = cls
        self.winner = winner

    def __enter__(self) -> None:
        self.cls._instance = self.winner  # type: ignore[attr-defined]

    def __exit__(self, *exc: Any) -> None:
        return None


@pytest.mark.parametrize(
    "cls, instance",
    [(PipelineRegistry, pipeline_registry), (TransformationRegistry, transformation_registry)],
)
class TestSingletons:
    def test_constructing_again_returns_the_same_registry(self, cls: type, instance: Any) -> None:
        assert cls() is instance

    def test_double_checked_lock_keeps_the_instance_another_thread_created(
        self, cls: type, instance: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(cls, "_instance", None)
        monkeypatch.setattr(cls, "_lock", _RacingLock(cls, instance))

        assert cls() is instance  # not a second, empty registry


def test_registering_a_name_twice_keeps_the_first(isolated_registry: None) -> None:
    first = _pipeline_class("gaps_dup_pipeline")
    registered = pipeline_registry.get("gaps_dup_pipeline")
    assert isinstance(registered, first)

    pipeline_registry.register(first())  # a second instance under the same name
    assert pipeline_registry.get("gaps_dup_pipeline") is registered


def test_unregister_and_clear(isolated_registry: None) -> None:
    _pipeline_class("gaps_unregister_pipeline")
    pipeline_registry.record_failure("gaps_failed", RuntimeError("x"))

    assert pipeline_registry.unregister("gaps_unregister_pipeline") is True
    assert pipeline_registry.unregister("gaps_unregister_pipeline") is False

    pipeline_registry.clear()
    assert pipeline_registry.list_names() == []
    assert pipeline_registry.describe_missing("gaps_failed") == ""


def test_registry_is_safe_under_concurrent_registration(isolated_registry: None) -> None:
    pipeline_registry.clear()
    cls = _pipeline_class("gaps_concurrent_pipeline")
    pipeline_registry.clear()
    instances = [cls() for _ in range(20)]

    threads = [threading.Thread(target=pipeline_registry.register, args=(p,)) for p in instances]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert pipeline_registry.list_names() == ["gaps_concurrent_pipeline"]


# ----- to_json_safe -------------------------------------------------------------------


class _WithToDict:
    def to_dict(self) -> Dict[str, Any]:
        return {"when": (1, 2)}


class _Plain:
    def __init__(self) -> None:
        self.a = 1
        self.nested = _WithToDict()


def test_objects_with_to_dict_are_serialised_through_it() -> None:
    assert to_json_safe({"x": _WithToDict()}) == {"x": {"when": [1, 2]}}


def test_plain_objects_are_serialised_by_their_attributes() -> None:
    assert to_json_safe([_Plain()]) == [{"a": 1, "nested": {"when": [1, 2]}}]


def test_objects_without_attributes_become_strings() -> None:
    assert to_json_safe({"n": 3 + 4j}) == {"n": "(3+4j)"}


# ----- ExecutionContext / ParameterResolver -----------------------------------------


def test_an_explicit_batch_id_is_kept() -> None:
    assert ExecutionContext(batch_id="b-1").batch_id == "b-1"


class TestParameterResolver:
    def test_bad_template_syntax_is_a_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid template syntax"):
            ParameterResolver().resolve({"q": "{{ unclosed "}, {})

    def test_strict_undefined_errors_are_reported_as_missing_parameters(self) -> None:
        # Undefined attributes on a defined value raise UndefinedError.
        with pytest.raises(ValueError, match="Missing required parameter"):
            ParameterResolver().resolve("{{ day.missing.deeper }}", {"day": {}})

    def test_extract_parameters_walks_dicts_and_lists(self) -> None:
        config = {
            "index": "logs-{{ env }}",
            "query": {"range": [{"gte": "{{start}}"}, {"lte": "{{ end }}"}, 3]},
            "plain": "no template",
        }
        assert ParameterResolver().extract_parameters(config) == {"env", "start", "end"}


# ----- ExecutionStatus ----------------------------------------------------------------


def test_success_rate_of_an_execution_without_jobs_is_zero() -> None:
    assert ExecutionStatus("e", "p", ExecutionState.COMPLETED).success_rate == 0.0


def test_success_rate_and_completion() -> None:
    status = ExecutionStatus("e", "p", ExecutionState.RUNNING, total_jobs=4, completed_jobs=3)
    assert status.success_rate == 0.75
    assert status.is_complete is False

    status.state = ExecutionState.PARTIALLY_FAILED
    assert status.is_complete is True


# ----- pipeline discovery: sys.path handling -----------------------------------------


@pytest.fixture
def scans(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """Record the directories discovery scans instead of importing anything."""
    seen: List[str] = []

    def fake_scan(module_name: str, label: str) -> int:
        seen.append(module_name)
        return 0

    monkeypatch.setattr(pipeline_discovery, "_scan_directory", fake_scan)
    monkeypatch.setattr(sys, "path", list(sys.path))  # restored after the test
    return seen


def test_discovery_puts_the_project_and_its_src_dir_on_sys_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scans: List[str]
) -> None:
    (tmp_path / "src").mkdir()
    monkeypatch.chdir(tmp_path)
    cwd = str(Path.cwd())
    while cwd in sys.path:
        sys.path.remove(cwd)

    pipeline_discovery.discover_and_load_pipelines("my_pipes")

    assert sys.path[0] == str(Path(cwd) / "src")
    assert cwd in sys.path
    # reusable components first, the pipelines last
    assert scans == ["sources", "destinations", "transformations", "my_pipes"]


def test_discovery_warns_when_nothing_was_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scans: List[str], caplog: Any
) -> None:
    monkeypatch.chdir(tmp_path)
    with caplog.at_level(logging.WARNING, logger="reflowfy.core.pipeline_discovery"):
        # the 'reflowfy' logger may have propagation turned off by setup_logging
        monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)
        assert pipeline_discovery.discover_and_load_pipelines("my_pipes") == 0

    assert "No component files found" in caplog.text
    assert str(tmp_path / "src") not in sys.path  # no src/ dir, nothing added


# ----- query directory fallback -------------------------------------------------------


def test_queries_dir_falls_back_to_cwd_when_the_module_has_no_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_registry: None
) -> None:
    pipeline = _pipeline_class("gaps_queries_pipeline", __module__="gaps_repl_module")()
    monkeypatch.setitem(sys.modules, "gaps_repl_module", None)  # e.g. defined in a REPL
    monkeypatch.chdir(tmp_path)

    assert pipeline._resolve_queries_dir() == Path.cwd() / "queries"
