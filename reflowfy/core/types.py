"""Names for the values that flow through a pipeline.

A record is whatever a source yields, wrapped as ``{"data": <fetched item>}``.
Every built-in source (elastic, sql, api, mock, s3) wraps each item it fetches
this way, so a record is always a dict — the fetched value lives at
``record["data"]``, and enrichment a transformation adds lives alongside it as
sibling keys. These aliases exist so pipeline and transformation authors can
annotate the `records` argument in one token instead of writing
`List[Dict[str, Any]]` everywhere::

    from reflowfy import Records, RuntimeParams, transformation

    @transformation("stamp")
    def stamp(records: Records, runtime_params: RuntimeParams) -> Records:
        for record in records:
            record["seen"] = True  # sibling of "data", not nested inside it
        return records

``StaticSource`` is the one exception: it carries a list handed to it verbatim
(e.g. plain IDs from ``define_source``), so it does not wrap.
"""

from typing import Any, Dict, Generic, List, Sequence, TypeVar

# typing_extensions, not typing: a generic TypedDict needs it on Python 3.10.
from typing_extensions import TypedDict

from reflowfy.transformations.base import BaseTransformation

T = TypeVar("T")

Record = Dict[str, Any]
"""One record: a JSON-ish mapping."""

Records = List[Record]
"""A batch of records — what every hook receives and returns."""

Transformations = Sequence[BaseTransformation]
"""What ``define_transformations`` returns."""


class TypedRecord(TypedDict, Generic[T]):
    """A record whose fetched item has a known shape: ``{"data": T}``.

    Describe what the source returns as a TypedDict and annotate with
    ``List[TypedRecord[Order]]``; the type checker then knows every key of
    ``record["data"]``. Keys a transformation adds are declared by subclassing::

        class Order(TypedDict):
            order_id: str
            amount: float

        class TaxedOrder(TypedRecord[Order]):
            tax: NotRequired[float]

    The same type checks the data at runtime: ``record_type = TypedRecord[Order]``
    on the pipeline validates every fetched record, and ``validate_steps = True``
    validates each step's output against its return annotation.

    Use plain ``Record`` when records carry arbitrary keys: a TypedDict rejects
    every key it does not declare.
    """

    data: T


def wrap_as_record(item: Any) -> Record:
    """Wrap one fetched item as ``{"data": item}``.

    Used by built-in sources so every record has the same shape regardless of
    what the underlying system returns (a dict, a scalar, raw text/bytes).
    """
    return {"data": item}
