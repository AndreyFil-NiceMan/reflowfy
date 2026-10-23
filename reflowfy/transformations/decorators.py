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

An ``async def`` works the same way and is awaited on the worker's event loop;
``@record_transformation`` runs an ``async def`` on every record concurrently.
"""

import inspect
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, cast

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
        from reflowfy.transformations.base import AsyncTransformation, BaseTransformation

        # Plain `type`: the bases are abstract until the methods below fill them in.
        base: type
        methods: Dict[str, Any]
        if inspect.iscoroutinefunction(func):

            async def apply_async(
                self: "BaseTransformation", records: Any, runtime_params: Any
            ) -> Any:
                return await func(records, runtime_params)

            base, methods = AsyncTransformation, {"apply_async": apply_async}
        else:

            def apply(self: "BaseTransformation", records: Any, runtime_params: Any) -> Any:
                return func(records, runtime_params)

            base, methods = BaseTransformation, {"apply": apply}

        return _make_class(func, name, base, methods)

    return decorator


def record_transformation(
    name: str,
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Callable[[TransformFn], "type[BaseTransformation]"]:
    """
    Decorator to create a transformation that awaits a coroutine once per record.

    The function signature should be:
        async def my_step(record: Record, runtime_params: RuntimeParams) -> Record | None

    Up to ``concurrency`` records are in flight at once, and the output keeps the
    input order. Returning ``None`` drops that record. The first record that raises
    fails the job, naming its index; ``SkipJob`` from any record drops the job.

    Args:
        name: Unique name for this transformation
        concurrency: Records in flight at once. Defaults to the
            ``TRANSFORM_CONCURRENCY`` env var (16).
        timeout: Seconds allowed per record; ``None`` means no limit.

    Example:
        @record_transformation("enrich_user", concurrency=32, timeout=10)
        async def enrich_user(record: Record, runtime_params: RuntimeParams) -> Record | None:
            async with httpx.AsyncClient() as client:
                resp = await client.get(f"http://users/{record['user_id']}")
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return {**record, "user": resp.json()}
    """
    if concurrency is not None and concurrency < 1:
        raise ValueError(f"record_transformation {name!r}: concurrency must be >= 1")
    if timeout is not None and timeout <= 0:
        raise ValueError(f"record_transformation {name!r}: timeout must be > 0")

    def decorator(func: TransformFn) -> "type[BaseTransformation]":
        from reflowfy.transformations.base import RecordTransformation

        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"record_transformation {name!r} needs an `async def` function: running "
                "records concurrently only helps code that awaits. Use @transformation "
                "for a synchronous function."
            )

        async def apply_record(self: "BaseTransformation", record: Any, runtime_params: Any) -> Any:
            return await func(record, runtime_params)

        methods: Dict[str, Any] = {
            "apply_record": apply_record,
            "concurrency": concurrency,
            "timeout": timeout,
        }
        return _make_class(func, name, RecordTransformation, methods)

    return decorator


def _make_class(
    func: TransformFn, name: str, base: type, methods: Dict[str, Any]
) -> "type[BaseTransformation]":
    """Create (and so auto-register) the transformation class for ``func``."""
    cls = type(
        func.__name__,
        (base,),
        {
            "name": name,
            **methods,
            "__doc__": func.__doc__ or f"Transformation: {name}",
            "__module__": func.__module__,
        },
    )
    return cast("type[BaseTransformation]", cls)
