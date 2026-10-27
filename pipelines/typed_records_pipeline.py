"""
Typed records: one TypedDict describes the records, for the type checker and at runtime.

- ``Order`` is what the source returns; ``TypedRecord[Order]`` is the record
  around it (``{"data": Order}``).
- ``TaxedOrder`` subclasses it to declare the key ``add_tax`` adds.
- ``record_type`` checks every fetched record right after ``fetch()``;
  ``on_invalid_record = "skip"`` drops the bad ones with a reason instead of
  failing the job.
- ``validate_steps`` checks each step's output against its return annotation.

The mock data has one order without an ``amount``: ``reflowfy test typed_records``
shows it skipped as ``invalid data.amount: missing``.
"""

from typing import List

from typing_extensions import NotRequired, TypedDict

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    RuntimeParams,
    Transformations,
    TypedRecord,
    record_transformation,
    transformation,
)
from reflowfy.destinations.console import console_destination
from reflowfy.sources.mock import MockSource, mock_source


class Order(TypedDict):
    order_id: str
    amount: float


class TaxedOrder(TypedRecord[Order]):
    tax: NotRequired[float]


@transformation("add_tax")
def add_tax(records: List[TypedRecord[Order]], runtime_params: RuntimeParams) -> List[TaxedOrder]:
    taxed: List[TaxedOrder] = []
    for record in records:
        taxed.append({"data": record["data"], "tax": round(record["data"]["amount"] * 0.2, 2)})
    return taxed


@record_transformation("drop_free_orders")
async def drop_free_orders(record: TaxedOrder, runtime_params: RuntimeParams) -> TaxedOrder | None:
    return record if record["data"]["amount"] > 0 else None


class TypedRecordsPipeline(AbstractPipeline[RuntimeParams]):
    name = "typed_records"
    record_type = TypedRecord[Order]
    on_invalid_record = "skip"
    validate_steps = True

    def define_source(self, runtime_params: RuntimeParams) -> MockSource:
        return mock_source(
            [
                {"order_id": "A-1", "amount": 120.0},
                {"order_id": "A-2", "amount": 0},
                {"order_id": "A-3"},  # no amount: skipped by record_type
                {"order_id": "A-4", "amount": 35.5},
            ]
        )

    def define_transformations(
        self, records: List[TypedRecord[Order]], runtime_params: RuntimeParams
    ) -> Transformations:
        return [add_tax(), drop_free_orders()]

    def define_destination(
        self, records: List[TaxedOrder], runtime_params: RuntimeParams
    ) -> BaseDestination:
        return console_destination()
