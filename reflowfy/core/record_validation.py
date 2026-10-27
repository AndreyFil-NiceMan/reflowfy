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

import types
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

MAX_INVALID_SHOWN = 3
"""Invalid records named in one failure message; the rest are counted."""

_adapters: Dict[Any, Any] = {}
_output_types: Dict[type, Any] = {}
_UNCHECKED = object()


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


def _is_checkable(record_type: Any) -> bool:
    """False for types that say nothing about a record: ``Any``, ``dict``, ``Dict[str, Any]``."""
    if record_type is Any or record_type is object or record_type is None:
        return False
    return (get_origin(record_type) or record_type) is not dict


def _strip_none(annotation: Any) -> Any:
    """``OrderRecord | None`` -> ``OrderRecord``: a per-record step may return None to drop."""
    if get_origin(annotation) not in (Union, types.UnionType):
        return annotation
    rest = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(rest) == 1:
        return rest[0]
    return Union[tuple(rest)]


def _record_type_of(transformation_class: type) -> Any:
    """The record type a decorated function's return annotation promises, or None."""
    func = getattr(transformation_class, "_transform_fn", None)
    if func is None:
        return None
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        # An annotation that cannot be resolved (a forward reference to a name that
        # does not exist at runtime) cannot be checked; the type checker still can.
        return None
    returned = hints.get("return")
    if returned is None:
        return None
    from reflowfy.transformations.base import RecordTransformation

    if issubclass(transformation_class, RecordTransformation):
        return _strip_none(returned)
    if (get_origin(returned) or returned) not in (list, List, Sequence, tuple):
        return None
    args = get_args(returned)
    return args[0] if args else None


def step_output_type(transformation: object) -> Optional[Any]:
    """The record type ``transformation`` promises to return, or None if it promises none.

    An explicit ``output_type`` class attribute wins (the way a ``BaseTransformation``
    subclass declares one); otherwise a ``@transformation``/``@record_transformation``
    function's return annotation is read. ``-> Records``, ``-> List[Any]`` and no
    annotation mean "no promise", so nothing is checked.
    """
    explicit = getattr(transformation, "output_type", None)
    if explicit is not None:
        return explicit if _is_checkable(explicit) else None
    cls = type(transformation)
    found = _output_types.get(cls, None)
    if found is None:
        resolved = _record_type_of(cls)
        found = _output_types[cls] = resolved if _is_checkable(resolved) else _UNCHECKED
    return None if found is _UNCHECKED else found
