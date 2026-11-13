"""What record type a transformation step promises to return (``validate_steps``).

Read from an explicit ``output_type`` class attribute, or from a ``@transformation`` /
``@record_transformation`` function's return annotation. The runner checks each step's
output against it with :mod:`reflowfy.core.record_validation`.
"""

import collections.abc
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


_LIST_ORIGINS = (list, List, Sequence, collections.abc.Sequence)


def _per_record_type(annotation: Any) -> Any:
    """The record type a per-record step's return annotation promises.

    ``X | None`` -> ``X`` (None drops the record); ``List[X]`` -> ``X`` (a list of
    records replaces it); ``X | List[X] | None`` -> ``X``. A bare ``list`` promises
    nothing about its records, so it reads as ``Any``.
    """
    members = (
        get_args(annotation)
        if get_origin(annotation) in (Union, types.UnionType)
        else (annotation,)
    )
    record_types: List[Any] = []
    for member in members:
        if member is type(None):
            continue
        if (get_origin(member) or member) in _LIST_ORIGINS:
            args = get_args(member)
            member = args[0] if args else Any
        if member not in record_types:
            record_types.append(member)
    if not record_types:
        return None
    if len(record_types) == 1:
        return record_types[0]
    return Union[tuple(record_types)]


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
        return _per_record_type(returned)
    if (get_origin(returned) or returned) not in (*_LIST_ORIGINS, tuple):
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
