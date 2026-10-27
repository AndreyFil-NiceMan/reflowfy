"""Typed-records E2E pipelines: record_type checked after fetch, validate_steps after each step.

Each pipeline plans one job of three orders, the second with no ``amount``, and
delivers to the mock webhook tagged with a per-run ``marker``:

- ``e2e_typed_records_skip``: ``on_invalid_record = "skip"`` drops the bad order,
  the other two are delivered and the skip is on the job's stats.
- ``e2e_typed_records_fail``: the default ``"fail"`` fails the job, nothing is sent.
- ``e2e_typed_records_step``: valid orders, but ``e2e_break_tax`` returns a record
  that breaks its own ``-> List[TaxedOrder]``, so ``validate_steps`` fails the job.
"""

from typing import Any, List

from typing_extensions import NotRequired, TypedDict

from reflowfy import AbstractPipeline, RuntimeParams, Transformations, TypedRecord, transformation
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock


class Order(TypedDict):
    id: int
    marker: str
    amount: float


class TaxedOrder(TypedRecord[Order]):
    tax: NotRequired[float]


class TypedRecordsParams(RuntimeParams, total=False):
    marker: NotRequired[str]


@transformation("e2e_add_tax")
def e2e_add_tax(
    records: List[TypedRecord[Order]], runtime_params: TypedRecordsParams
) -> List[TaxedOrder]:
    return [{"data": r["data"], "tax": round(r["data"]["amount"] * 0.2, 2)} for r in records]


@transformation("e2e_break_tax")
def e2e_break_tax(
    records: List[TaxedOrder], runtime_params: TypedRecordsParams
) -> List[TaxedOrder]:
    broken: List[Any] = [{**r, "tax": "unknown"} for r in records]
    return broken


class _TypedRecordsBase(AbstractPipeline[TypedRecordsParams]):
    record_type = TypedRecord[Order]
    with_missing_amount = True

    def define_source(self, runtime_params: TypedRecordsParams) -> BaseSource:
        marker = runtime_params.get("marker", "none")
        orders: List[Any] = [
            {"id": 1, "marker": marker, "amount": 10.0},
            {"id": 2, "marker": marker},
            {"id": 3, "marker": marker, "amount": 5},
        ]
        if not self.with_missing_amount:
            orders[1]["amount"] = 1.0
        return e2e_mock(data=orders, batch_size=10)

    def define_transformations(
        self, records: List[TypedRecord[Order]], runtime_params: TypedRecordsParams
    ) -> Transformations:
        return [e2e_add_tax()]

    def define_destination(
        self, records: List[TaxedOrder], runtime_params: TypedRecordsParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})


class E2ETypedRecordsSkip(_TypedRecordsBase):
    name = "e2e_typed_records_skip"
    on_invalid_record = "skip"


class E2ETypedRecordsFail(_TypedRecordsBase):
    name = "e2e_typed_records_fail"


class E2ETypedRecordsStep(_TypedRecordsBase):
    name = "e2e_typed_records_step"
    validate_steps = True
    with_missing_amount = False

    def define_transformations(
        self, records: List[TypedRecord[Order]], runtime_params: TypedRecordsParams
    ) -> Transformations:
        return [e2e_add_tax(), e2e_break_tax()]
