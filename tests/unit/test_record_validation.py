"""Runtime record checks: record_type after fetch, validate_steps after each step.

The same TypedDict a pipeline annotates its hooks with is checked against the
real records with pydantic (strict, validate-only). See core/record_validation.py.
"""

from typing import Annotated, Any, Dict, List, Optional, Tuple, Union

import pytest
from typing_extensions import NotRequired, TypedDict

from reflowfy import (
    AbstractPipeline,
    BaseTransformation,
    RecordValidationError,
    Records,
    RuntimeParams,
    StepOutputError,
    TypedRecord,
    record_transformation,
    transformation,
)
from reflowfy.core.record_validation import (
    describe_invalid,
    find_invalid_records,
    step_output_type,
    type_label,
)
from reflowfy.execution.job_runner import RECORD_TYPE_STEP, run_job_records
from reflowfy.worker.executor import WorkerExecutor


class Order(TypedDict):
    order_id: str
    amount: float


class Item(TypedDict):
    price: float


class Basket(TypedDict):
    items: List[Item]


class TaxedOrder(TypedRecord[Order]):
    tax: NotRequired[float]


def _order(i: int, amount: Any = 1.0) -> Dict[str, Any]:
    return {"data": {"order_id": str(i), "amount": amount}}


# --- find_invalid_records / describe_invalid ------------------------------------


def test_matching_records_are_valid():
    assert find_invalid_records(TypedRecord[Order], [_order(1), _order(2)]) == {}


def test_a_missing_field_names_its_path():
    bad = {"data": {"order_id": "1"}}
    assert find_invalid_records(TypedRecord[Order], [_order(0), bad]) == {1: "data.amount: missing"}


def test_the_check_is_strict_but_an_int_is_a_float():
    assert find_invalid_records(TypedRecord[Order], [_order(0, amount=3)]) == {}
    assert find_invalid_records(TypedRecord[Order], [_order(0, amount="3.5")]) == {
        0: "data.amount: float_type"
    }


def test_one_reason_per_record_the_first_problem():
    bad = {"data": {"order_id": 1, "amount": "x"}}
    assert find_invalid_records(TypedRecord[Order], [bad]) == {0: "data.order_id: string_type"}


def test_undeclared_keys_do_not_fail_and_records_are_not_modified():
    record = {"data": {"order_id": "1", "amount": 1.0, "extra": True}, "seen": True}
    assert find_invalid_records(TypedRecord[Order], [record]) == {}
    assert record == {"data": {"order_id": "1", "amount": 1.0, "extra": True}, "seen": True}


def test_list_positions_inside_a_record_group_as_brackets():
    bad = {"data": {"items": [{"price": 1.0}, {"price": "x"}]}}
    assert find_invalid_records(TypedRecord[Basket], [bad]) == {0: "data.items[].price: float_type"}


def test_a_record_that_is_not_a_mapping_is_reported_whole():
    assert find_invalid_records(Order, ["raw text"]) == {0: "<record>: dict_type"}


def test_an_unhashable_type_is_still_checked():
    unhashable = Annotated[Order, {"unhashable": "metadata"}]
    assert find_invalid_records(unhashable, [{"order_id": "1"}]) == {0: "amount: missing"}


def test_describe_invalid_is_one_based_and_caps_how_many_it_names():
    invalid = {i: "data.amount: missing" for i in range(5)}
    message = describe_invalid(invalid, 50, TypedRecord[Order])
    assert message.startswith(
        "record 1 of 50 does not match TypedRecord[Order]: data.amount: missing; record 2 of 50"
    )
    assert message.endswith("(+2 more invalid)")
    assert "record 4" not in message


def test_type_label_drops_module_paths():
    assert type_label(Order) == "Order"
    assert type_label(TypedRecord[Order]) == "TypedRecord[Order]"
    assert type_label(Union[Order, Item]) == "Union[Order, Item]"


# --- step_output_type -----------------------------------------------------------


@transformation("rv_batch_typed")
def batch_typed(records: List[TaxedOrder], runtime_params: RuntimeParams) -> List[TaxedOrder]:
    return records


@transformation("rv_batch_tuple")
def batch_tuple(records: Any, runtime_params: RuntimeParams) -> Tuple[Order, ...]:
    return records


@transformation("rv_batch_records")
def batch_records(records: Records, runtime_params: RuntimeParams) -> Records:
    return records


@transformation("rv_batch_bare_list")
def batch_bare_list(records: Any, runtime_params: Any) -> list:  # type: ignore[type-arg]
    return records


@transformation("rv_batch_not_a_list")
def batch_not_a_list(records: Any, runtime_params: Any) -> Order:
    return records


@transformation("rv_batch_unannotated")
def batch_unannotated(records, runtime_params):  # type: ignore[no-untyped-def]
    return records


@transformation("rv_batch_unresolvable")
def batch_unresolvable(records: Any, runtime_params: Any) -> "List[DoesNotExist]":  # type: ignore[name-defined]  # noqa: F821
    return records


@record_transformation("rv_per_record_optional")
async def per_record_optional(record: TaxedOrder, params: RuntimeParams) -> Optional[TaxedOrder]:
    return record


@record_transformation("rv_per_record_union")
async def per_record_union(record: Any, params: RuntimeParams) -> "Order | Item | None":
    return record


@record_transformation("rv_per_record_plain")
async def per_record_plain(record: Any, params: RuntimeParams) -> Order:
    return record


class Explicit(BaseTransformation):
    name = "rv_explicit"
    output_type = Order

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        return records


class ExplicitDict(BaseTransformation):
    name = "rv_explicit_dict"
    output_type = Dict[str, Any]

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        return records


class Undeclared(BaseTransformation):
    name = "rv_undeclared"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        return records


@pytest.mark.parametrize(
    ("step", "expected"),
    [
        (batch_typed, TaxedOrder),
        (batch_tuple, Order),
        (per_record_optional, TaxedOrder),
        (per_record_union, Union[Order, Item]),
        (per_record_plain, Order),
        (Explicit, Order),
    ],
)
def test_the_promised_record_type_is_read_from_the_step(step: Any, expected: Any):
    assert step_output_type(step()) == expected
    assert step_output_type(step()) == expected  # cached per class


@pytest.mark.parametrize(
    "step",
    [
        batch_records,
        batch_bare_list,
        batch_not_a_list,
        batch_unannotated,
        batch_unresolvable,
        ExplicitDict,
        Undeclared,
    ],
)
def test_a_step_that_promises_no_record_type_is_unchecked(step: Any):
    assert step_output_type(step()) is None


# --- run_job_records ------------------------------------------------------------


class _Source:
    def __init__(self, records: List[Any]):
        self.records = records

    def fetch(self, runtime_params: Any, limit: Optional[int] = None) -> List[Any]:
        return [dict(r) for r in self.records]


class _Pipeline:
    name = "rv_pipeline"
    record_type: Any = TypedRecord[Order]
    on_invalid_record = "fail"
    validate_steps = False

    def __init__(self, steps: Optional[List[Any]] = None):
        self.steps = steps or []
        self.seen: List[Any] = []

    def define_transformations(self, records: List[Any], runtime_params: Any) -> List[Any]:
        self.seen = records
        return self.steps

    def define_destination(self, records: List[Any], runtime_params: Any) -> str:
        return "dest"


def test_valid_records_pass_and_the_check_is_reported_as_a_step():
    pipeline = _Pipeline()
    records, transformed, applied, destination = run_job_records(
        _Source([_order(1), _order(2)]), pipeline, {}
    )
    assert len(records) == len(transformed) == 2
    assert destination == "dest"
    (check,) = applied
    assert (check.name, check.mode, check.records_in, check.records_out) == (
        RECORD_TYPE_STEP,
        "validate",
        2,
        2,
    )
    assert check.skip_reasons is None


def test_an_invalid_record_fails_the_job_by_default():
    with pytest.raises(RecordValidationError) as info:
        run_job_records(_Source([_order(1), {"data": {"order_id": "2"}}]), _Pipeline(), {})
    assert str(info.value) == (
        "record 2 of 2 does not match TypedRecord[Order]: data.amount: missing"
    )
    assert info.value.invalid == {1: "data.amount: missing"}
    assert info.value.total == 2


def test_skip_drops_invalid_records_with_reasons_before_any_transformation():
    pipeline = _Pipeline([batch_typed()])
    pipeline.on_invalid_record = "skip"
    bad = [{"data": {"order_id": "2"}}, {"data": {"order_id": "3"}}]
    records, transformed, applied, _ = run_job_records(_Source([_order(1), *bad]), pipeline, {})
    assert len(records) == 3
    assert transformed == [_order(1)]
    assert pipeline.seen == [_order(1)]
    check, step = applied
    assert check.records_skipped == 2
    assert check.skip_reasons == {"invalid data.amount: missing": 2}
    assert (step.name, step.records_in) == ("rv_batch_typed", 1)


def test_no_record_type_means_no_check():
    pipeline = _Pipeline()
    pipeline.record_type = None
    _, transformed, applied, _ = run_job_records(_Source([{"anything": 1}]), pipeline, {})
    assert transformed == [{"anything": 1}]
    assert applied == []


@transformation("rv_breaks_its_promise")
def breaks_its_promise(
    records: List[TaxedOrder], runtime_params: RuntimeParams
) -> List[TaxedOrder]:
    for i, record in enumerate(records):
        record["tax"] = "high" if i == 1 else 0.2  # type: ignore[typeddict-item]
    return records


@record_transformation("rv_per_record_breaks")
async def per_record_breaks(record: Any, params: RuntimeParams) -> Optional[Order]:
    return {"order_id": record["data"]["order_id"]}  # type: ignore[typeddict-item]


def test_validate_steps_fails_the_step_that_broke_its_return_type():
    pipeline = _Pipeline([batch_typed(), breaks_its_promise()])
    pipeline.validate_steps = True
    with pytest.raises(StepOutputError) as info:
        run_job_records(_Source([_order(1), _order(2), _order(3)]), pipeline, {})
    err = info.value
    assert err.transformation_name == "rv_breaks_its_promise"
    assert "returned record 2 of 3 does not match TaxedOrder: tax: float_type" in str(err)
    assert (err.step_index, err.culprit_index) == (1, 1)
    assert [s.name for s in err.applied_steps] == ["rv_batch_typed"]


def test_validate_steps_checks_a_per_record_step_against_its_record_type():
    pipeline = _Pipeline([per_record_breaks()])
    pipeline.validate_steps = True
    with pytest.raises(
        StepOutputError, match="record 1 of 1 does not match Order: amount: missing"
    ):
        run_job_records(_Source([_order(1)]), pipeline, {})


def test_skip_does_not_cover_a_broken_step():
    pipeline = _Pipeline([breaks_its_promise()])
    pipeline.validate_steps = True
    pipeline.on_invalid_record = "skip"
    with pytest.raises(StepOutputError):
        run_job_records(_Source([_order(1), _order(2)]), pipeline, {})


def test_without_validate_steps_a_broken_step_is_not_checked():
    pipeline = _Pipeline([breaks_its_promise()])
    _, transformed, _, _ = run_job_records(_Source([_order(1), _order(2)]), pipeline, {})
    assert transformed[1]["tax"] == "high"


def test_validate_steps_passes_valid_output_and_skips_unchecked_or_empty_steps():
    @transformation("rv_drop_all")
    def drop_all(records: List[TaxedOrder], runtime_params: RuntimeParams) -> List[TaxedOrder]:
        return []

    pipeline = _Pipeline([batch_typed(), batch_records(), drop_all()])
    pipeline.validate_steps = True
    _, transformed, applied, _ = run_job_records(_Source([_order(1)]), pipeline, {})
    assert transformed == []
    assert [s.name for s in applied] == [
        RECORD_TYPE_STEP,
        "rv_batch_typed",
        "rv_batch_records",
        "rv_drop_all",
    ]


# --- AbstractPipeline -----------------------------------------------------------


class _Checked(AbstractPipeline[RuntimeParams]):
    record_type = TypedRecord[Order]
    on_invalid_record = "skip"
    validate_steps = True

    def define_source(self, runtime_params: RuntimeParams) -> Any:
        return []

    def define_transformations(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
        return []

    def define_destination(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
        return None


def test_the_pipeline_attributes_default_off():
    assert AbstractPipeline.record_type is None
    assert AbstractPipeline.on_invalid_record == "fail"
    assert AbstractPipeline.validate_steps is False


def test_a_pipeline_declares_the_checks_as_class_attributes():
    class Declared(_Checked):
        name = "rv_declared"

    assert Declared().on_invalid_record == "skip"


def test_an_unknown_on_invalid_record_raises():
    class Wrong(_Checked):
        name = "rv_wrong"
        on_invalid_record = "drop"  # type: ignore[assignment]

    with pytest.raises(ValueError, match='on_invalid_record must be "fail" or "skip"'):
        Wrong()


# --- worker ---------------------------------------------------------------------


class _Dest:
    def __init__(self) -> None:
        self.sent: List[Any] = []

    async def health_check(self) -> bool:
        return True

    async def send_with_retry(self, records: List[Any], params: Any) -> None:
        self.sent.extend(records)

    async def close(self) -> None:
        pass


def _payload(records: List[Any]) -> Dict[str, Any]:
    return {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "rv_pipeline",
        "source": {"type": "StaticSource", "config": {"records": records}},
        "metadata": {
            "batch_id": "b",
            "created_at": "t",
            "batch_number": 1,
            "total_batches": 1,
            "retry_count": 0,
            "is_retry": False,
            "runtime_params": {},
            "source_metadata": None,
        },
    }


def _worker(monkeypatch: pytest.MonkeyPatch, pipeline: Any) -> Tuple[WorkerExecutor, List[Any]]:
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipeline)
    ex = WorkerExecutor(database_url="postgresql://x/y")
    saved: List[Any] = []

    async def save(execution_id: str, job_id: str, stats: Any) -> None:
        saved.append(stats)

    monkeypatch.setattr(ex, "_update_job_in_db", save)
    return ex, saved


async def test_worker_counts_skipped_invalid_records(monkeypatch: pytest.MonkeyPatch):
    from reflowfy.observability import metrics

    dest = _Dest()
    pipeline = _Pipeline()
    pipeline.on_invalid_record = "skip"
    pipeline.define_destination = lambda records, params: dest  # type: ignore[method-assign]
    ex, saved = _worker(monkeypatch, pipeline)
    counter = metrics.records_skipped_total.labels(
        pipeline="rv_pipeline", transformation=RECORD_TYPE_STEP
    )
    before = counter._value.get()  # pyright: ignore[reportPrivateUsage]

    assert await ex.execute_job(_payload([_order(1), {"data": {}}])) is True

    assert dest.sent == [_order(1)]
    (stats,) = saved
    assert stats.records_input == 2
    assert stats.records_output == 1
    assert stats.records_skipped == 1
    assert stats.record_skip_reasons == {"invalid data.order_id: missing": 1}
    assert RECORD_TYPE_STEP in stats.transformation_times
    assert counter._value.get() == before + 1  # pyright: ignore[reportPrivateUsage]


async def test_worker_fails_the_job_naming_the_record(monkeypatch: pytest.MonkeyPatch):
    ex, saved = _worker(monkeypatch, _Pipeline())
    assert await ex.execute_job(_payload([{"data": {"order_id": "1"}}])) is False
    (stats,) = saved
    assert stats.error_type == "RecordValidationError"
    assert stats.error == "record 1 of 1 does not match TypedRecord[Order]: data.amount: missing"


async def test_worker_leaves_the_check_out_of_dedup_step_names(monkeypatch: pytest.MonkeyPatch):
    pipeline = _Pipeline([batch_typed()])
    ex, _ = _worker(monkeypatch, pipeline)
    seen: List[Any] = []

    async def claim(*args: Any) -> Any:
        seen.append(args[5])
        return args[4], []

    monkeypatch.setattr(ex, "_claim_records", claim)
    payload = _payload([_order(1)])
    payload["dedup_check"] = True
    pipeline.define_destination = lambda records, params: _Dest()  # type: ignore[method-assign]
    assert await ex.execute_job(payload) is True
    assert seen == [["rv_batch_typed"]]
