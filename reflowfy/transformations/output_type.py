"""What record type a transformation step promises to return (``validate_steps``).

Read from an explicit ``output_type`` class attribute, or from a ``@transformation`` /
``@record_transformation`` function's return annotation. The runner checks each step's
output against it with :mod:`reflowfy.core.record_validation`.
"""

import types
from typing import Any, Dict, List, Optional, Sequence, Union, get_args, get_origin, get_type_hints

from reflowfy.transformations.base import RecordTransformation

_output_types: Dict[type, Any] = {}
_UNCHECKED = object()


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
