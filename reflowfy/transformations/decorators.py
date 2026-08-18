"""Decorator for defining reusable transformations.

The @transformation decorator provides a functional alternative to subclassing
BaseTransformation. It wraps a simple function into a BaseTransformation subclass
and auto-registers it.

Example:
    @transformation("uppercase_names")
    def uppercase_names(records: Records, runtime_params: RuntimeParams) -> Records:
        for r in records:
            if "name" in r:
                r["name"] = r["name"].upper()
        return records

    # Then in a pipeline:
    def define_transformations(self, records, runtime_params):
        return [uppercase_names()]
"""

from typing import TYPE_CHECKING, Any, Callable, cast

if TYPE_CHECKING:
    from reflowfy.transformations.base import BaseTransformation

TransformFn = Callable[[Any, Any], Any]
"""Shape of a decorated transformation: ``(records, runtime_params) -> records``.

Deliberately `Any` in every position rather than `(Records, RuntimeParams) ->
Records`: it checks the arity — the one mistake that reaches production as a
runtime TypeError — without rejecting an author who annotates `runtime_params`
with their own params TypedDict.
"""


def transformation(
    name: str,
) -> Callable[[TransformFn], "type[BaseTransformation]"]:
    """
    Decorator to create a reusable transformation from a function.

    The decorated function becomes a callable that returns a BaseTransformation
    instance when called. The function signature should be:
        def my_transform(records: Records, runtime_params: RuntimeParams) -> Records

    Args:
        name: Unique name for this transformation

    Returns:
        A callable class that can be instantiated as a transformation

    Example:
        @transformation("filter_active")
        def filter_active(records: Records, runtime_params: RuntimeParams) -> Records:
            return [r for r in records if r.get("active")]

        # Use in pipeline:
        def define_transformations(self, records, runtime_params):
            return [filter_active()]  # Instantiate to use
    """

    def decorator(func: TransformFn) -> "type[BaseTransformation]":
        from reflowfy.transformations.base import BaseTransformation

        def apply(self: "BaseTransformation", records: Any, runtime_params: Any) -> Any:
            return func(records, runtime_params)

        # Create a new BaseTransformation subclass dynamically
        cls = type(
            func.__name__,
            (BaseTransformation,),
            {
                "name": name,
                "apply": apply,
                "__doc__": func.__doc__ or f"Transformation: {name}",
                "__module__": func.__module__,
            },
        )

        return cast("type[BaseTransformation]", cls)

    return decorator
