"""Check records against the TypedDict that describes them, at runtime.

A pipeline's ``record_type`` (e.g. ``TypedRecord[Order]``) and a transformation's
return annotation (``-> List[TaxedOrder]``) are what the type checker trusts. This
module checks the actual records against the same declarations with pydantic, so
one type gives both editor/type-checker help and a runtime check.

Validation only: records are never replaced by pydantic's output, which would
coerce values and silently drop keys the type does not declare. The check is
strict (``"3.5"`` is not a float, an ``int`` is), so what passes is exactly what
the annotated code will see.

Reasons name a field path and pydantic's error type (``data.amount: missing``),
never a value: they end up in logs, job stats and skip-reason counts.
"""

from typing import (
    Any,
    Dict,
    List,
    Sequence,
    cast,
    get_args,
    get_origin,
)

MAX_INVALID_SHOWN = 3
"""Invalid records named in one failure message; the rest are counted."""

_adapters: Dict[Any, Any] = {}


def _adapter(record_type: Any) -> Any:
    """A cached ``TypeAdapter(List[record_type])``; building one costs milliseconds."""
    try:
        return _adapters[record_type]
    except KeyError:
        pass
    except TypeError:  # unhashable type (e.g. Annotated with a dict): build each time
        return _build_adapter(record_type)
    adapter = _adapters[record_type] = _build_adapter(record_type)
    return adapter


def _build_adapter(record_type: Any) -> Any:
    # Imported here: pydantic is heavy, and only pipelines that opt in pay for it.
    from pydantic import TypeAdapter

    return TypeAdapter(List[record_type])


def type_label(record_type: Any) -> str:
    """``TypedRecord[Order]`` rather than ``reflowfy.core.types.TypedRecord[mod.Order]``."""
    origin, args = get_origin(record_type), get_args(record_type)
    if origin is None or not args:
        return str(getattr(record_type, "__name__", record_type))
    name = getattr(origin, "__name__", None) or str(origin).replace("typing.", "")
    return f"{name}[{', '.join(type_label(arg) for arg in args)}]"


def _describe(loc: Sequence[Any], error_type: str) -> str:
    # List positions inside a record become "[]" so reasons group: "data.items[].price".
    path = ""
    for part in loc:
        path += "[]" if isinstance(part, int) else (f".{part}" if path else str(part))
    return f"{path or '<record>'}: {error_type}"


def find_invalid_records(record_type: Any, records: List[Any]) -> Dict[int, str]:
    """Which ``records`` do not match ``record_type``: ``{index: "data.amount: missing"}``.

    The first problem found is reported per record. Empty means all records match.
    """
    from pydantic import ValidationError

    try:
        _adapter(record_type).validate_python(records, strict=True)
    except ValidationError as exc:
        invalid: Dict[int, str] = {}
        for err in exc.errors(include_url=False, include_input=False, include_context=False):
            # A list was validated, so every error starts at the record's index.
            index, *path = err["loc"]
            invalid.setdefault(cast(int, index), _describe(path, err["type"]))
        return invalid
    return {}


def describe_invalid(invalid: Dict[int, str], total: int, record_type: Any) -> str:
    """``record 3 of 50 does not match Order: data.amount: missing (+2 more invalid)``."""
    shown = sorted(invalid.items())[:MAX_INVALID_SHOWN]
    text = "; ".join(
        f"record {index + 1} of {total} does not match {type_label(record_type)}: {reason}"
        for index, reason in shown
    )
    more = len(invalid) - len(shown)
    return f"{text} (+{more} more invalid)" if more > 0 else text
