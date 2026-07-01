"""Decorator for defining reusable transformations.

The @transformation decorator provides a functional alternative to subclassing
BaseTransformation. It wraps a simple function into a BaseTransformation subclass
and auto-registers it.

Example:
    @transformation("uppercase_names")
    def uppercase_names(records, runtime_params):
        for r in records:
            if "name" in r:
                r["name"] = r["name"].upper()
        return records

    # Then in a pipeline:
    def define_transformations(self, records, runtime_params):
        return [uppercase_names()]
"""

from typing import Any, Callable


def transformation(name: str):
    """
    Decorator to create a reusable transformation from a function.

    The decorated function becomes a callable that returns a BaseTransformation
    instance when called. The function signature should be:
        def my_transform(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]

    Args:
        name: Unique name for this transformation

    Returns:
        A callable class that can be instantiated as a transformation

    Example:
        @transformation("filter_active")
        def filter_active(records, runtime_params):
            return [r for r in records if r.get("active")]

        # Use in pipeline:
        def define_transformations(self, records, runtime_params):
            return [filter_active()]  # Instantiate to use
    """
    def decorator(func: Callable[..., Any]) -> type:
        from reflowfy.transformations.base import BaseTransformation

        # Create a new BaseTransformation subclass dynamically
        cls = type(
            func.__name__,
            (BaseTransformation,),
            {
                'name': name,
                'apply': lambda self, records, runtime_params: func(records, runtime_params),
                '__doc__': func.__doc__ or f"Transformation: {name}",
                '__module__': func.__module__,
            }
        )

        return cls
    return decorator
