"""The unified decorator API: ``@transformation`` (batch / ``per_record=True``) and
``@destination(per_record=True)``, and the removal of the old spellings.

- ``@transformation`` works bare, names the step after the function, and a step
  without options can be listed bare in ``define_transformations``.
- ``per_record=True`` accepts a plain ``def``: its records run in order in one
  thread, with every per-record rule (None, lists, SkipRecord, on_error, contract).
- Knobs that cannot apply fail at decoration, saying why.
- The old spellings (``@record_transformation``, ``@record_destination``,
  ``@source("x")``, ``@destination("x")`` and their registries) are gone.
"""

import importlib
import threading
from typing import Any, Dict, List, Optional

import pytest
from typing_extensions import TypedDict

from reflowfy import (
    AbstractPipeline,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    TransformationError,
    TypedRecord,
    destination,
    transformation,
)
from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.destinations.fan_out import as_destination
from reflowfy.destinations.record import RecordDestination
from reflowfy.execution.transformation_runner import (
    StepMode,
    apply_transformations_iteratively_async,
)
from reflowfy.sources.mock import MockSource
from reflowfy.testing import CaptureDestination, run_pipeline
from reflowfy.transformations.base import (
    RecordTransformation,
    SyncRecordTransformation,
    as_transformation,
)
from reflowfy.transformations.output_type import step_output_type
from reflowfy.transformations.registry import transformation_registry


def _run(steps: List[Any], records: List[Any], params: Optional[Dict[str, Any]] = None) -> Any:
    class _Pipeline:
        name = "ut_dx_pipeline"
        validate_steps = False

        def define_transformations(self, records: List[Any], runtime_params: Any) -> List[Any]:
            return list(steps)

    return run_coroutine_sync(
        apply_transformations_iteratively_async(_Pipeline(), records, params or {})
    )


def _records(n: int) -> List[Dict[str, Any]]:
    return [{"id": i, "v": i} for i in range(n)]


# ---------------------------------------------------------------------------
# @transformation: bare, named, default name
# ---------------------------------------------------------------------------


@transformation
def ut_dx_double(records: List[Any], runtime_params: Any) -> List[Any]:
    return [{**r, "v": r["v"] * 2} for r in records]


@transformation(name="ut_dx_renamed")
def ut_dx_plus_one(records: List[Any], runtime_params: Any) -> List[Any]:
    return [{**r, "v": r["v"] + 1} for r in records]


@transformation()
def ut_dx_empty_parens(records: List[Any], runtime_params: Any) -> List[Any]:
    return records


@transformation("ut_dx_scale")
def ut_dx_scale(records: List[Any], runtime_params: Any, *, factor: int) -> List[Any]:
    return [{**r, "v": r["v"] * factor} for r in records]


class TestNaming:
    def test_bare_decorator_names_the_step_after_the_function(self):
        assert ut_dx_double.name == "ut_dx_double"
        assert transformation_registry.get("ut_dx_double") is ut_dx_double

    def test_name_keyword_and_positional_name_both_set_it(self):
        assert ut_dx_plus_one.name == "ut_dx_renamed"
        assert ut_dx_scale.name == "ut_dx_scale"
        assert ut_dx_empty_parens.name == "ut_dx_empty_parens"

    def test_a_duplicate_name_says_how_to_fix_it(self):
        with pytest.raises(ValueError, match="give one of them another name"):

            @transformation
            def ut_dx_double(records: List[Any], runtime_params: Any) -> List[Any]:  # noqa: F811
                return records

    def test_a_non_string_name_is_rejected(self):
        with pytest.raises(TypeError, match="name must be a string"):
            transformation(3)  # type: ignore[call-overload]


class TestBareSteps:
    def test_a_step_without_options_can_be_listed_bare(self):
        out, applied = _run([ut_dx_double, ut_dx_plus_one()], _records(3))
        assert [r["v"] for r in out] == [1, 3, 5]
        assert [s.name for s in applied] == ["ut_dx_double", "ut_dx_renamed"]

    def test_a_bare_step_with_a_required_option_fails_naming_it(self):
        with pytest.raises(TypeError, match="factor"):
            _run([ut_dx_scale], _records(1))

    def test_as_transformation_leaves_instances_and_other_objects_alone(self):
        step = ut_dx_scale(factor=2)
        assert as_transformation(step) is step
        assert as_transformation(dict) is dict
        assert isinstance(as_transformation(ut_dx_double), ut_dx_double)

    def test_get_transformation_names_reads_bare_steps(self):
        class Named(AbstractPipeline[RuntimeParams]):
            name = "ut_dx_named_pipeline"

            def define_source(self, runtime_params: RuntimeParams) -> Any:
                return MockSource(data=[])

            def define_transformations(self, records: List[Any], runtime_params: Any) -> Any:
                return [ut_dx_double, ut_dx_plus_one]

            def define_destination(self, records: List[Any], runtime_params: Any) -> Any:
                return CaptureDestination()

        assert Named().get_transformation_names() == ["ut_dx_double", "ut_dx_renamed"]

    def test_reflowfy_testing_runs_bare_steps(self):
        class Bare(AbstractPipeline[RuntimeParams]):
            name = "ut_dx_bare_pipeline"

            def define_source(self, runtime_params: RuntimeParams) -> Any:
                return MockSource(data=[{"id": 1, "v": 1}])

            def define_transformations(self, records: List[Any], runtime_params: Any) -> Any:
                return [ut_dx_double, ut_dx_drop_odd]

            def define_destination(self, records: List[Any], runtime_params: Any) -> Any:
                return CaptureDestination()

        result = run_pipeline(Bare, records=[{"id": 1, "v": 1}, {"id": 2, "v": 2}], wrap=False)
        assert result.ok
        assert [s.name for s in result.steps] == ["ut_dx_double", "ut_dx_drop_odd"]
        assert result.sent == [{"id": 1, "v": 2}, {"id": 2, "v": 4}]


class TestKnobChecks:
    @pytest.mark.parametrize(
        "kwargs, named",
        [
            ({"concurrency": 2}, "concurrency only applies"),
            ({"timeout": 1.0}, "timeout only applies"),
            ({"on_error": "skip"}, "on_error only applies"),
            ({"concurrency": 2, "timeout": 1.0}, "concurrency, timeout only apply"),
        ],
    )
    def test_per_record_knobs_on_a_batch_step_fail_at_decoration(
        self, kwargs: Dict[str, Any], named: str
    ):
        with pytest.raises(TypeError, match=f"{named} to a per-record step; add per_record=True"):
            transformation("ut_dx_batch_knobs", **kwargs)

    @pytest.mark.parametrize("knob", [{"concurrency": 2}, {"timeout": 1.0}])
    def test_concurrency_and_timeout_need_an_async_per_record_function(self, knob: Dict[str, Any]):
        with pytest.raises(TypeError, match="needs an `async def` function"):

            @transformation("ut_dx_sync_knob", per_record=True, **knob)
            def sync_step(record: Any, runtime_params: Any) -> Any:
                return record

    def test_bad_on_error_is_rejected(self):
        with pytest.raises(ValueError, match="on_error must be 'fail' or 'skip'"):
            transformation(per_record=True, on_error="ignore")  # type: ignore[call-overload]

    @pytest.mark.parametrize("knob", ["concurrency", "timeout"])
    def test_a_sync_record_subclass_rejects_concurrency_and_timeout(self, knob: str):
        with pytest.raises(TypeError, match=f"{knob} does not apply"):
            type(
                "BadSync",
                (SyncRecordTransformation,),
                {
                    knob: 2,
                    "apply_record_sync": lambda self, record, params: record,
                },
            )


# ---------------------------------------------------------------------------
# per_record=True with a plain def
# ---------------------------------------------------------------------------


class Order(TypedDict):
    amount: float


@transformation(per_record=True)
def ut_dx_drop_odd(record: Any, runtime_params: Any) -> Optional[Dict[str, Any]]:
    return record if record["id"] % 2 == 0 or record["v"] % 2 == 0 else None


ran_on: List[str] = []


@transformation("ut_dx_sync_rules", per_record=True)
def ut_dx_sync_rules(record: Any, runtime_params: Any, *, explode: int = 2) -> Any:
    ran_on.append(threading.current_thread().name)
    if record["id"] == 1:
        return None
    if record["id"] == 2:
        return [{**record, "part": n} for n in range(explode)]
    if record["id"] == 3:
        raise SkipRecord("not wanted")
    return {**record, "seen": True}


@transformation("ut_dx_sync_raises", per_record=True)
def ut_dx_sync_raises(record: Any, runtime_params: Any) -> Any:
    if record["id"] == 2:
        raise KeyError("missing")
    return record


@transformation("ut_dx_sync_skips", per_record=True, on_error="skip")
def ut_dx_sync_skips(record: Any, runtime_params: Any) -> Any:
    if record["id"] == 2:
        raise KeyError("missing")
    return record


@transformation("ut_dx_sync_bad_return", per_record=True, on_error="skip")
def ut_dx_sync_bad_return(record: Any, runtime_params: Any) -> Any:
    return "not a record" if record["id"] == 1 else record


@transformation("ut_dx_sync_skip_job", per_record=True)
def ut_dx_sync_skip_job(record: Any, runtime_params: Any) -> Any:
    raise SkipJob("nothing to do")


@transformation("ut_dx_sync_typed", per_record=True)
def ut_dx_sync_typed(record: Any, runtime_params: Any) -> Optional[TypedRecord[Order]]:
    return record


@transformation("ut_dx_async_per_record", per_record=True, concurrency=3)
async def ut_dx_async_per_record(record: Any, runtime_params: Any) -> Any:
    return record


class TestSyncPerRecord:
    def test_it_builds_a_sync_record_transformation(self):
        step = ut_dx_sync_rules()
        assert isinstance(step, SyncRecordTransformation)
        assert isinstance(step, RecordTransformation)

    def test_every_per_record_rule_holds_and_order_is_kept(self):
        ran_on.clear()
        out, applied = _run([ut_dx_sync_rules(explode=3)], _records(5))
        assert out == [
            {"id": 0, "v": 0, "seen": True},
            {"id": 2, "v": 2, "part": 0},
            {"id": 2, "v": 2, "part": 1},
            {"id": 2, "v": 2, "part": 2},
            {"id": 4, "v": 4, "seen": True},
        ]
        (step,) = applied
        assert step.mode == StepMode.PER_RECORD
        assert step.concurrency is None
        assert step.skip_reasons == {"not wanted": 1}
        assert (step.records_in, step.records_out) == (5, 5)

    def test_records_run_in_one_worker_thread_off_the_loop(self):
        ran_on.clear()
        _run([ut_dx_sync_rules], _records(5))
        assert len(set(ran_on)) == 1
        assert ran_on[0] != threading.main_thread().name

    def test_a_raising_record_fails_the_job_naming_its_index(self):
        with pytest.raises(TransformationError) as info:
            _run([ut_dx_sync_raises], _records(3))
        assert info.value.culprit_index == 2
        assert isinstance(info.value.original_error, KeyError)

    def test_on_error_skip_drops_the_record_with_its_exception_type(self):
        out, (step,) = _run([ut_dx_sync_skips], _records(3))
        assert [r["id"] for r in out] == [0, 1]
        assert step.skip_reasons == {"KeyError": 1}

    def test_a_wrong_return_type_fails_whatever_on_error_says(self):
        with pytest.raises(TransformationError, match="returned str"):
            _run([ut_dx_sync_bad_return], _records(2))

    def test_skip_job_drops_the_job(self):
        with pytest.raises(SkipJob):
            _run([ut_dx_sync_skip_job], _records(2))

    def test_an_empty_batch_never_calls_the_function(self):
        ran_on.clear()
        out, _ = _run([ut_dx_sync_rules], [])
        assert out == [] and ran_on == []

    def test_its_return_annotation_is_the_promised_record_type(self):
        assert step_output_type(ut_dx_sync_typed()) == TypedRecord[Order]

    def test_direct_apply_works_for_callers_that_are_not_async(self):
        assert ut_dx_sync_rules().apply([{"id": 0, "v": 0}], {}) == [
            {"id": 0, "v": 0, "seen": True}
        ]

    def test_apply_record_and_effective_concurrency_keep_the_base_contract(self):
        step = ut_dx_sync_rules()
        assert step.effective_concurrency() == 1
        out = run_coroutine_sync(step.apply_record({"id": 0, "v": 0}, {}))
        assert out == {"id": 0, "v": 0, "seen": True}

    def test_an_async_per_record_step_still_reports_its_concurrency(self):
        _, (step,) = _run([ut_dx_async_per_record], _records(4))
        assert step.mode == StepMode.PER_RECORD and step.concurrency == 3


# ---------------------------------------------------------------------------
# @destination(per_record=True)
# ---------------------------------------------------------------------------


@destination(per_record=True)
def ut_dx_route(record: Any, runtime_params: Any) -> Any:
    return CaptureDestination()


class TestDestination:
    def test_per_record_builds_a_record_destination_named_after_the_function(self):
        assert issubclass(ut_dx_route, RecordDestination)
        assert ut_dx_route.name == "ut_dx_route"

    def test_a_bare_record_destination_is_built_by_as_destination(self):
        built = as_destination(ut_dx_route)
        assert isinstance(built, ut_dx_route)

    def test_returning_another_destination_class_says_to_call_it(self):
        with pytest.raises(TypeError, match="call it with its arguments"):
            as_destination(CaptureDestination)

    def test_bare_decorator_says_per_record_is_needed(self):
        with pytest.raises(TypeError, match="needs per_record=True"):

            @destination  # type: ignore[arg-type]
            def no_mode(record: Any, runtime_params: Any) -> Any:
                return CaptureDestination()

    @pytest.mark.parametrize("kwargs", [{}, {"per_record": False}])
    def test_without_per_record_it_says_a_plain_function_will_do(self, kwargs: Dict[str, Any]):
        with pytest.raises(TypeError, match="plain function"):
            destination(**kwargs)  # type: ignore[call-overload]

    def test_route_knobs_without_per_record_are_rejected(self):
        with pytest.raises(TypeError, match="pass per_record=True"):
            destination("ut_dx_legacy_knobs", on_error="skip")  # type: ignore[call-overload]

    def test_timeout_needs_an_async_function(self):
        with pytest.raises(TypeError, match="timeout needs an `async def`"):

            @destination(per_record=True, timeout=1.0)
            def sync_route(record: Any, runtime_params: Any) -> Any:
                return CaptureDestination()

    def test_a_non_string_name_is_rejected(self):
        with pytest.raises(TypeError, match="name must be a string"):
            destination(3, per_record=True)  # type: ignore[call-overload]


# The old spellings are gone
# ---------------------------------------------------------------------------


class TestRemovedSpellings:
    @pytest.mark.parametrize(
        "name",
        [
            "record_transformation",
            "record_destination",
            "source",
            "source_registry",
            "destination_registry",
        ],
    )
    def test_is_no_longer_exported(self, name: str):
        import reflowfy

        assert not hasattr(reflowfy, name)
        assert name not in reflowfy.__all__

    def test_the_source_decorator_module_is_gone(self):
        with pytest.raises(ImportError):
            importlib.import_module("reflowfy.sources.decorators")

    def test_a_named_destination_without_per_record_is_an_error(self):
        with pytest.raises(TypeError, match="pass per_record=True"):
            destination("ut_dx_old_named")
