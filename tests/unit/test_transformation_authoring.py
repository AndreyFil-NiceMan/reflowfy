"""Authoring transformations: options, ``on_error``, the per-record return contract and
``should_apply_transformation``.

- Options: keyword arguments given when a decorated step is built are bound against the
  function's signature right then and passed to every call.
- ``@record_transformation(on_error="skip")`` / ``RecordTransformation.on_error``: a record
  whose call raised is dropped, its exception type as the skip reason.
- A per-record step returns one record (a ``dict``) or ``None``; anything else fails the
  job, whatever ``on_error`` says.
- ``should_apply_transformation`` is asked before each step; a declined step keeps its
  position and is reported with ``mode="skipped"``.
"""

import asyncio
import logging
import threading
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest
from typing_extensions import TypedDict

from reflowfy import (
    AbstractPipeline,
    BaseTransformation,
    RecordTransformation,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    TransformationError,
    TypedRecord,
    record_transformation,
    transformation,
)
from reflowfy.transformations.output_type import step_output_type
from reflowfy.core.registry import pipeline_registry
from reflowfy.execution.job_runner import run_job_records
from reflowfy.execution.transformation_runner import (
    SKIPPED_MODE,
    AppliedStep,
    apply_transformations_iteratively_async,
)
from reflowfy.sources.static import StaticSource
from reflowfy.core.async_utils import run_coroutine_sync


def apply_transformations_iteratively(*args, **kwargs):
    return run_coroutine_sync(apply_transformations_iteratively_async(*args, **kwargs))


class _Pipeline:
    """Stand-in pipeline: a fixed (or computed) transformation list, no hook."""

    name = "ut_authoring_pipeline"
    validate_steps = False

    def __init__(self, steps: Any):
        self._steps = steps

    def define_transformations(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
        return self._steps(params) if callable(self._steps) else list(self._steps)


def _records(n: int) -> List[Dict[str, Any]]:
    return [{"id": i, "v": i} for i in range(n)]


# ---------------------------------------------------------------------------
# #10 Options on @transformation / @record_transformation
# ---------------------------------------------------------------------------


@transformation("ut_opt_multiply")
def multiply(records: List[Any], runtime_params: Any, *, factor: int) -> List[Any]:
    return [{**r, "v": r["v"] * factor} for r in records]


@transformation("ut_opt_add_async")
async def add_async(records: List[Any], runtime_params: Any, *, by: int = 1) -> List[Any]:
    await asyncio.sleep(0)
    return [{**r, "v": r["v"] + by} for r in records]


@record_transformation("ut_opt_label")
async def label(record: Any, runtime_params: Any, *, prefix: str) -> Optional[Dict[str, Any]]:
    return {**record, "label": f"{prefix}{record['id']}"}


class Order(TypedDict):
    order_id: int


@transformation("ut_opt_typed")
def typed_with_option(
    records: List[Any], runtime_params: Any, *, keep: bool = True
) -> List[TypedRecord[Order]]:
    return records if keep else []


@record_transformation("ut_opt_typed_record")
async def typed_record_with_option(
    record: Any, runtime_params: Any, *, tag: str
) -> Optional[TypedRecord[Order]]:
    return record


class TestOptions:
    def test_a_sync_step_gets_its_options(self):
        out, _ = apply_transformations_iteratively(_Pipeline([multiply(factor=3)]), _records(3), {})
        assert [r["v"] for r in out] == [0, 3, 6]

    def test_an_async_step_gets_its_options_and_defaults(self):
        steps = [add_async(by=10), add_async()]
        out, _ = apply_transformations_iteratively(_Pipeline(steps), _records(2), {})
        assert [r["v"] for r in out] == [11, 12]

    def test_a_per_record_step_gets_its_options(self):
        out, _ = apply_transformations_iteratively(
            _Pipeline([label(prefix="u-")]), _records(2), {}
        )
        assert [r["label"] for r in out] == ["u-0", "u-1"]

    def test_options_are_kept_on_the_instance(self):
        assert multiply(factor=2).options == {"factor": 2}
        assert add_async().options == {}

    def test_decorated_steps_are_decorated_transformations(self):
        # The decorators are typed to return type[DecoratedTransformation], whose
        # constructor takes **options, so `multiply(factor=2)` type-checks for users.
        from reflowfy.transformations.base import AsyncTransformation, DecoratedTransformation

        assert isinstance(multiply(factor=2), DecoratedTransformation)
        assert isinstance(add_async(), DecoratedTransformation)
        assert isinstance(add_async(), AsyncTransformation)
        assert isinstance(label(prefix="x"), RecordTransformation)

    def test_a_hand_written_subclass_without_bind_options_keeps_its_options(self):
        from reflowfy.transformations.base import DecoratedTransformation

        class Manual(DecoratedTransformation):
            name = "authoring_manual_decorated"

            def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
                return records

        assert Manual(anything=1).options == {"anything": 1}

    def test_two_instances_keep_their_own_options(self):
        steps = [multiply(factor=2), multiply(factor=5)]
        out, _ = apply_transformations_iteratively(_Pipeline(steps), _records(2), {})
        assert [r["v"] for r in out] == [0, 10]

    def test_an_unknown_option_fails_at_construction(self):
        with pytest.raises(TypeError) as info:
            multiply(factor=2, factr=3)
        message = str(info.value)
        assert "'ut_opt_multiply'" in message and "multiply" in message
        assert "factr" in message

    def test_a_missing_option_fails_at_construction(self):
        with pytest.raises(TypeError, match="ut_opt_multiply.*factor"):
            multiply()

    def test_a_missing_per_record_option_fails_at_construction(self):
        with pytest.raises(TypeError, match="ut_opt_label.*prefix"):
            label()

    def test_options_cannot_be_positional(self):
        with pytest.raises(TypeError):
            multiply(2)  # type: ignore[call-arg]

    def test_a_step_without_options_still_builds_with_none(self):
        @transformation("ut_opt_plain")
        def plain(records: List[Any], runtime_params: Any) -> List[Any]:
            return records

        assert plain().options == {}
        with pytest.raises(TypeError, match="ut_opt_plain"):
            plain(anything=1)

    def test_validate_steps_still_reads_the_return_annotation(self):
        assert step_output_type(typed_with_option(keep=False)) == TypedRecord[Order]
        assert step_output_type(typed_record_with_option(tag="x")) == TypedRecord[Order]


# ---------------------------------------------------------------------------
# #11 on_error="skip"
# ---------------------------------------------------------------------------


@record_transformation("ut_onerr_skip", on_error="skip", concurrency=2)
async def fails_on_odd(record: Any, runtime_params: Any) -> Any:
    if record["id"] % 2:
        raise ValueError(f"secret value {record['v']}")
    return record


@record_transformation("ut_onerr_fail")
async def fails_on_odd_default(record: Any, runtime_params: Any) -> Any:
    if record["id"] % 2:
        raise ValueError("boom")
    return record


class TestOnError:
    async def test_skip_drops_records_that_raised_with_the_type_as_reason(self):
        out, applied = await apply_transformations_iteratively_async(
            _Pipeline([fails_on_odd()]), _records(5), {}
        )
        assert [r["id"] for r in out] == [0, 2, 4]
        [step] = applied
        assert step.mode == "per_record"
        # The type only: the message can carry record values.
        assert step.skip_reasons == {"ValueError": 2}
        assert step.records_skipped == 2

    async def test_fail_is_the_default(self):
        assert fails_on_odd_default.on_error == "fail"  # type: ignore[attr-defined]
        with pytest.raises(TransformationError) as info:
            await apply_transformations_iteratively_async(
                _Pipeline([fails_on_odd_default()]), _records(3), {}
            )
        assert info.value.culprit_index == 1

    async def test_skip_counts_a_timeout_as_raising(self):
        @record_transformation("ut_onerr_timeout", on_error="skip", timeout=0.05)
        async def hang_on_1(record: Any, runtime_params: Any) -> Any:
            if record["id"] == 1:
                await asyncio.sleep(10)
            return record

        out, applied = await apply_transformations_iteratively_async(
            _Pipeline([hang_on_1()]), _records(3), {}
        )
        assert [r["id"] for r in out] == [0, 2]
        assert applied[0].skip_reasons == {"TimeoutError": 1}

    async def test_skip_record_keeps_its_own_reason(self):
        @record_transformation("ut_onerr_skiprecord", on_error="skip")
        async def skipper(record: Any, runtime_params: Any) -> Any:
            if record["id"] == 0:
                raise SkipRecord("not wanted")
            if record["id"] == 1:
                raise KeyError("x")
            return record

        out, applied = await apply_transformations_iteratively_async(
            _Pipeline([skipper()]), _records(3), {}
        )
        assert [r["id"] for r in out] == [2]
        assert applied[0].skip_reasons == {"not wanted": 1, "KeyError": 1}

    async def test_skip_job_still_drops_the_job(self):
        @record_transformation("ut_onerr_skipjob", on_error="skip")
        async def drop(record: Any, runtime_params: Any) -> Any:
            raise SkipJob("nothing today")

        with pytest.raises(SkipJob):
            await apply_transformations_iteratively_async(_Pipeline([drop()]), _records(2), {})

    def test_an_unknown_value_fails_at_decoration(self):
        with pytest.raises(ValueError, match="on_error must be 'fail' or 'skip'"):
            record_transformation("ut_onerr_bad", on_error="ignore")  # type: ignore[arg-type]

    async def test_the_subclass_form(self):
        class Lenient(RecordTransformation):
            name = "ut_onerr_subclass"
            on_error = "skip"

            async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Any:
                if record["id"] == 0:
                    raise RuntimeError("down")
                return record

        out, applied = await apply_transformations_iteratively_async(
            _Pipeline([Lenient()]), _records(2), {}
        )
        assert [r["id"] for r in out] == [1]
        assert applied[0].skip_reasons == {"RuntimeError": 1}

    def test_a_subclass_with_an_unknown_value_fails_at_definition(self):
        with pytest.raises(ValueError, match="Strict.on_error"):

            class Strict(RecordTransformation):  # pyright: ignore[reportUnusedClass]
                name = "ut_onerr_subclass_bad"
                on_error = "never"  # type: ignore[assignment]

                async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Any:
                    return record  # pragma: no cover


# ---------------------------------------------------------------------------
# #14 A per-record step returns a record, a list of records, or None
# ---------------------------------------------------------------------------


class _Returns(RecordTransformation):
    """Returns ``value`` for record 1, the record itself for the others."""

    name = "ut_contract_returns"

    def __init__(self, value: Any, on_error: str = "fail"):
        self.value = value
        self.on_error = on_error  # type: ignore[misc]

    async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Any:
        return self.value if record["id"] == 1 else record


class TestPerRecordStepContract:
    @pytest.mark.parametrize(
        "value, returned",
        [
            (({"id": 1},), "tuple"),
            ("x", "str"),
            (7, "int"),
            ([{"id": 1}, "x"], "list with str at position 1"),
            ([None], "list with NoneType at position 0"),
            ([[{"id": 1}]], "list with list at position 0"),
        ],
    )
    async def test_a_non_record_fails_the_job_naming_position_and_type(
        self, value: Any, returned: str
    ):
        with pytest.raises(TransformationError) as info:
            await apply_transformations_iteratively_async(
                _Pipeline([_Returns(value)]), _records(3), {}
            )
        error = info.value
        assert error.culprit_index == 1
        assert error.culprit_record == {"id": 1, "v": 1}
        assert f"record 1: RecordContractError: returned {returned};" in str(error)
        assert "a per-record step returns a record, a list of records, or None" in str(error)

    async def test_on_error_skip_does_not_hide_it(self):
        with pytest.raises(TransformationError, match="returned tuple"):
            await apply_transformations_iteratively_async(
                _Pipeline([_Returns((), on_error="skip")]), _records(3), {}
            )

    async def test_a_dict_or_none_passes(self):
        out, _ = await apply_transformations_iteratively_async(
            _Pipeline([_Returns(None)]), _records(3), {}
        )
        assert [r["id"] for r in out] == [0, 2]
        out, _ = await apply_transformations_iteratively_async(
            _Pipeline([_Returns({"data": {"order_id": 9}})]), _records(2), {}
        )
        assert out[1] == {"data": {"order_id": 9}}

    async def test_a_list_replaces_the_record_in_place(self):
        out, applied = await apply_transformations_iteratively_async(
            _Pipeline([_Returns([{"id": "1a"}, {"id": "1b"}])]), _records(3), {}
        )
        assert [r["id"] for r in out] == [0, "1a", "1b", 2]
        assert (applied[0].records_in, applied[0].records_out) == (3, 4)

    async def test_an_empty_list_drops_the_record(self):
        out, _ = await apply_transformations_iteratively_async(
            _Pipeline([_Returns([])]), _records(3), {}
        )
        assert [r["id"] for r in out] == [0, 2]

    async def test_the_decorated_form_explodes_in_order_with_concurrency(self):
        @record_transformation("ut_contract_decorated", concurrency=4)
        async def explode(record: Dict[str, Any], runtime_params: Any) -> List[Dict[str, Any]]:
            await asyncio.sleep(0.001 * (5 - record["id"]))  # finish out of order
            return [{**record, "copy": n} for n in range(record["id"])]

        out, _ = await apply_transformations_iteratively_async(
            _Pipeline([explode()]), _records(4), {}
        )
        assert [(r["id"], r["copy"]) for r in out] == [(1, 0), (2, 0), (2, 1), (3, 0), (3, 1), (3, 2)]

    async def test_the_decorated_form_is_checked_too(self):
        @record_transformation("ut_contract_decorated_bad")
        async def explode(record: Any, runtime_params: Any) -> Any:
            return (record, record)

        with pytest.raises(TransformationError, match="record 0: .*returned tuple"):
            await apply_transformations_iteratively_async(_Pipeline([explode()]), _records(1), {})


# ---------------------------------------------------------------------------
# #3 should_apply_transformation
# ---------------------------------------------------------------------------


@transformation("ut_decl_tag")
def tag(records: List[Any], runtime_params: Any, *, value: str = "t") -> List[Any]:
    return [{**r, "tag": value} for r in records]


@transformation("ut_decl_drop_first")
def drop_first(records: List[Any], runtime_params: Any) -> List[Any]:
    runtime_params["dropped"] = True
    return records[1:]


class _Deciding(_Pipeline):
    """Declines every step whose name is in ``declined``; records what it was asked."""

    def __init__(self, steps: Any, declined: Tuple[str, ...] = (), raises: Any = None):
        super().__init__(steps)
        self.declined = declined
        self.raises = raises
        self.asked: List[Tuple[str, int]] = []
        self.threads: List[int] = []

    def should_apply_transformation(
        self, transformation: Any, runtime_params: Any, records: List[Any]
    ) -> bool:
        self.threads.append(threading.get_ident())
        self.asked.append((transformation.name, len(records)))
        if self.raises is not None:
            raise self.raises
        return transformation.name not in self.declined


@pytest.fixture
def runner_logs() -> Iterator[List[logging.LogRecord]]:
    seen: List[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = seen.append  # type: ignore[method-assign]
    runner_logger = logging.getLogger("reflowfy.execution.transformation_runner")
    runner_logger.addHandler(handler)
    previous = runner_logger.level
    runner_logger.setLevel(logging.INFO)
    try:
        yield seen
    finally:
        runner_logger.removeHandler(handler)
        runner_logger.setLevel(previous)


class TestShouldApplyTransformation:
    def test_a_declined_step_is_reported_as_skipped(self, runner_logs):
        pipeline = _Deciding([drop_first(), tag()], declined=("ut_decl_tag",))
        out, applied = apply_transformations_iteratively(pipeline, _records(3), {})
        assert out == [{"id": 1, "v": 1}, {"id": 2, "v": 2}]
        assert [(s.name, s.mode) for s in applied] == [
            ("ut_decl_drop_first", "sync"),
            ("ut_decl_tag", SKIPPED_MODE),
        ]
        skipped = applied[1]
        assert skipped.records_in == skipped.records_out == 2
        assert skipped.delta == 0
        assert skipped.records_skipped == 0
        # Asked with the records as they are at that point.
        assert pipeline.asked == [("ut_decl_drop_first", 3), ("ut_decl_tag", 2)]
        [rec] = [r for r in runner_logs if getattr(r, "event", None) == "transform_declined"]
        assert getattr(rec, "transformation") == "ut_decl_tag"
        assert "should_apply_transformation declined it" in rec.getMessage()

    def test_a_declined_step_keeps_its_position(self):
        # Re-resolution grows the list once drop_first ran; the declined step at
        # position 0 must not be offered again, nor shift the ones after it.
        def steps(params: Dict[str, Any]) -> List[Any]:
            chain: List[Any] = [tag(value="first"), drop_first()]
            if params.get("dropped"):
                chain.append(tag(value="last"))
            return chain

        pipeline = _Deciding(steps, declined=("ut_decl_tag",))
        out, applied = apply_transformations_iteratively(pipeline, _records(2), {})
        assert [s.mode for s in applied] == [SKIPPED_MODE, "sync", SKIPPED_MODE]
        assert out == [{"id": 1, "v": 1}]
        assert [name for name, _ in pipeline.asked] == [
            "ut_decl_tag",
            "ut_decl_drop_first",
            "ut_decl_tag",
        ]

    def test_the_hook_runs_in_a_thread(self):
        pipeline = _Deciding([tag()])
        apply_transformations_iteratively(pipeline, _records(1), {})
        assert pipeline.threads and pipeline.threads[0] != threading.get_ident()

    def test_a_declined_per_record_step_does_not_run(self):
        calls: List[int] = []

        @record_transformation("ut_decl_per_record")
        async def count(record: Any, runtime_params: Any) -> Any:
            calls.append(record["id"])  # pragma: no cover
            return record  # pragma: no cover

        pipeline = _Deciding([count()], declined=("ut_decl_per_record",))
        out, applied = apply_transformations_iteratively(pipeline, _records(3), {})
        assert calls == [] and len(out) == 3
        assert applied[0].mode == SKIPPED_MODE and applied[0].concurrency is None

    def test_a_declined_step_is_not_validated(self):
        pipeline = _Deciding([typed_with_option()], declined=("ut_opt_typed",))
        pipeline.validate_steps = True
        out, applied = apply_transformations_iteratively(pipeline, _records(2), {})
        assert len(out) == 2 and applied[0].mode == SKIPPED_MODE

    def test_a_hook_that_raises_fails_the_job_naming_the_step(self):
        pipeline = _Deciding([tag()], raises=KeyError("feature_flag"))
        with pytest.raises(TransformationError) as info:
            apply_transformations_iteratively(pipeline, _records(2), {})
        error = info.value
        assert error.transformation_name == "ut_decl_tag"
        assert error.step_index == 0 and error.records_in == 2
        assert error.culprit_index is None
        assert "should_apply_transformation raised KeyError" in str(error)
        assert isinstance(error.original_error, KeyError)

    def test_skip_job_from_the_hook_drops_the_job(self):
        pipeline = _Deciding([tag()], raises=SkipJob("off today"))
        with pytest.raises(SkipJob):
            apply_transformations_iteratively(pipeline, _records(1), {})

    def test_a_falsy_answer_declines(self):
        class Falsy(_Deciding):
            def should_apply_transformation(self, *args: Any) -> bool:
                return 0  # type: ignore[return-value]

        _, applied = apply_transformations_iteratively(Falsy([tag()]), _records(1), {})
        assert applied[0].mode == SKIPPED_MODE

    def test_a_real_pipeline_through_the_job_runner(self):
        class DecliningPipeline(AbstractPipeline[RuntimeParams]):
            name = "ut_decl_real_pipeline"

            def define_source(self, runtime_params: RuntimeParams) -> Any:
                return StaticSource(_records(2))  # pragma: no cover

            def define_transformations(
                self, records: List[Any], runtime_params: RuntimeParams
            ) -> List[BaseTransformation]:
                return [tag(value="a"), multiply(factor=10)]

            def define_destination(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
                return None

            def should_apply_transformation(
                self,
                transformation: BaseTransformation,
                runtime_params: RuntimeParams,
                records: List[Any],
            ) -> bool:
                return transformation.name != "ut_opt_multiply"

        pipeline = pipeline_registry.get("ut_decl_real_pipeline")
        assert isinstance(pipeline, DecliningPipeline)
        _, transformed, applied, _ = run_job_records(StaticSource(_records(2)), pipeline, {})
        assert [r["v"] for r in transformed] == [0, 1]
        assert [r["tag"] for r in transformed] == ["a", "a"]
        assert [s.mode for s in applied] == ["sync", SKIPPED_MODE]

    def test_the_default_hook_is_not_called(self, monkeypatch: pytest.MonkeyPatch):
        class DefaultPipeline(AbstractPipeline[RuntimeParams]):
            name = "ut_decl_default_pipeline"

            def define_source(self, runtime_params: RuntimeParams) -> Any:
                return StaticSource(_records(1))  # pragma: no cover

            def define_transformations(
                self, records: List[Any], runtime_params: RuntimeParams
            ) -> List[BaseTransformation]:
                return [tag()]

            def define_destination(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
                return None  # pragma: no cover

        def never(*args: Any) -> bool:  # pragma: no cover
            raise AssertionError("the default hook says yes; it is not worth a thread hop")

        pipeline = pipeline_registry.get("ut_decl_default_pipeline")
        assert pipeline is not None
        monkeypatch.setattr(pipeline, "should_apply_transformation", never)
        _, applied = apply_transformations_iteratively(pipeline, _records(1), {})
        assert [s.mode for s in applied] == ["sync"]

    def test_applied_step_documents_the_mode(self):
        assert AppliedStep("x", 0.0, 1, 1, mode=SKIPPED_MODE).delta == 0
