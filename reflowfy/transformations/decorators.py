"""``@transformation``: turn a function into a pipeline step.

One decorator, two granularities::

    @transformation                                  # the whole batch
    def add_tax(records: Records, runtime_params: RuntimeParams) -> Records:
        return [{**r, "tax": r["amount"] * 0.2} for r in records]

    @transformation(per_record=True)                 # one record at a time
    def drop_free(record: Record, runtime_params: RuntimeParams) -> Record | None:
        return record if record["amount"] > 0 else None

    @transformation(per_record=True, concurrency=32, timeout=10, on_error="skip")
    async def enrich(record: Record, runtime_params: RuntimeParams, *, region: str) -> Record:
        ...

    def define_transformations(self, records, runtime_params):
        return [add_tax, drop_free, enrich(region="eu")]

The step's name (logs, metrics, ``reflowfy test``) defaults to the function's name;
``@transformation("tax_v2")`` or ``name="tax_v2"`` sets another. Either function may be
``def`` or ``async def``. A per-record ``def`` runs its records in order in one thread;
a per-record ``async def`` runs ``concurrency`` of them at once, each bounded by
``timeout``. ``concurrency``, ``timeout`` and ``on_error`` only exist per record.

Options: parameters after ``(records|record, runtime_params)`` are given as keyword
arguments when the step is built (``enrich(region="eu")``) and passed to every call.
They are bound against the function's signature right there, so a typo or a missing
option fails inside ``define_transformations``, not mid-job. A step without options
can be listed bare (``add_tax``) or called (``add_tax()``); both are the same.

``@record_transformation`` is the old spelling of ``@transformation(per_record=True)``
and is deprecated.
"""

import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

if TYPE_CHECKING:
    from reflowfy.transformations.base import DecoratedTransformation
    from typing import Awaitable, Mapping, Sequence

    from typing_extensions import Concatenate

    from reflowfy.transformations.base import OnError

    TransformFn = Callable[Concatenate[Any, Any, ...], Any]
    """Shape of a batch transformation: ``(records, runtime_params, **options) -> records``.

    Deliberately `Any` in the two leading positions rather than `(Records,
    RuntimeParams)`: it checks the arity — the one mistake that reaches production
    as a runtime TypeError — without rejecting an author who annotates
    `runtime_params` with their own params TypedDict. The `...` admits the
    keyword-only options that follow.
    """

    RecordResult = Union[Mapping[str, Any], Sequence[Mapping[str, Any]], None]
    """What a per-record step returns: a record, a list of records, or ``None``.

    `Mapping[str, Any]` rather than `Record` (`Dict[str, Any]`) so a function
    annotated with a TypedDict record (`-> TaxedOrder | None`) is accepted: a
    TypedDict is a Mapping but not a Dict. `Sequence` rather than `List` for the
    same reason: `List[TaxedOrder]` is not a `List[Mapping]` (invariance), but it
    is a `Sequence[Mapping]`. At runtime only a `list` is accepted.
    """

    RecordTransformFn = Callable[
        Concatenate[Any, Any, ...], Union[RecordResult, Awaitable[RecordResult]]
    ]
    """``(record, runtime_params, **options) -> Record | List[Record] | None``, ``def`` or ``async def``."""


@overload
def transformation(name: "TransformFn", /) -> "type[DecoratedTransformation]": ...


@overload
def transformation(
    name: Optional[str] = None, *, per_record: Literal[False] = False
) -> Callable[["TransformFn"], "type[DecoratedTransformation]"]: ...


@overload
def transformation(
    name: Optional[str] = None,
    *,
    per_record: Literal[True],
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
    on_error: "OnError" = "fail",
) -> Callable[["RecordTransformFn"], "type[DecoratedTransformation]"]: ...


def transformation(
    name: Any = None,
    *,
    per_record: bool = False,
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
    on_error: "OnError" = "fail",
) -> Any:
    """
    Turn a function into a transformation step.

    Batch (default): ``def f(records, runtime_params, **options) -> records``, run once
    per job. Per record (``per_record=True``): ``def f(record, runtime_params, **options)
    -> record | list[record] | None``, run for every record; ``None`` drops the record,
    a list replaces it in place (``[]`` drops it), ``raise SkipRecord("why")`` drops it
    with a reason, ``SkipJob`` drops the job. Either may be ``async def``.

    Usable bare (``@transformation``) or called (``@transformation("name", ...)``).

    Args:
        name: The step's name in logs, metrics and ``reflowfy test``; defaults to the
            function's name. Must be unique among transformations.
        per_record: Call the function once per record instead of once per batch.
        concurrency: Per-record ``async def`` only: records in flight at once.
            Defaults to the ``TRANSFORM_CONCURRENCY`` env var (16).
        timeout: Per-record ``async def`` only: seconds allowed per record; ``None``
            means no limit. A timeout counts as the call raising (``on_error``).
        on_error: Per record only. ``"fail"`` (default) fails the job when a record's
            call raises, naming its index; ``"skip"`` drops that record, its exception
            type as the skip reason. A wrong return type fails the job either way.

    Example:
        @transformation
        def filter_active(records: Records, runtime_params: RuntimeParams) -> Records:
            return [r for r in records if r.get("active")]

        @transformation(per_record=True, concurrency=32, timeout=10)
        async def enrich_user(record: Record, runtime_params: RuntimeParams) -> Record | None:
            ...

        def define_transformations(self, records, runtime_params):
            return [filter_active, enrich_user]
    """
    if callable(name) and not isinstance(name, str):
        # Bare `@transformation`: the function itself, every knob at its default.
        func = name
        return _decorate(func, func.__name__, per_record=False)
    if name is not None and not isinstance(name, str):
        raise TypeError(f"transformation: name must be a string, got {type(name).__name__}")
    owner = f"transformation {name!r}" if name else "transformation"
    if not per_record:
        given = [
            knob
            for knob, value, default in (
                ("concurrency", concurrency, None),
                ("timeout", timeout, None),
                ("on_error", on_error, "fail"),
            )
            if value != default
        ]
        if given:
            raise TypeError(
                f"{owner}: {', '.join(given)} only appl{'ies' if len(given) == 1 else 'y'} "
                "to a per-record step; add per_record=True"
            )
    else:
        _check_record_knobs(owner, concurrency, timeout, on_error)

    def decorator(func: Callable[..., Any]) -> "type[DecoratedTransformation]":
        return _decorate(
            func,
            name or func.__name__,
            per_record=per_record,
            concurrency=concurrency,
            timeout=timeout,
            on_error=on_error,
        )

    return decorator


def record_transformation(
    name: str,
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
    on_error: "OnError" = "fail",
) -> Callable[["RecordTransformFn"], "type[DecoratedTransformation]"]:
    """Deprecated: ``@transformation(name, per_record=True, ...)`` does the same.

    Kept so existing pipelines run unchanged, without a warning. It now also accepts
    a plain ``def``, as the new spelling does.
    """
    return transformation(
        name, per_record=True, concurrency=concurrency, timeout=timeout, on_error=on_error
    )


def _check_record_knobs(
    owner: str, concurrency: Optional[int], timeout: Optional[float], on_error: object
) -> None:
    from reflowfy.transformations.base import check_on_error

    if concurrency is not None and concurrency < 1:
        raise ValueError(f"{owner}: concurrency must be >= 1")
    if timeout is not None and timeout <= 0:
        raise ValueError(f"{owner}: timeout must be > 0")
    check_on_error(owner, on_error)


def _decorate(
    func: Callable[..., Any],
    name: str,
    *,
    per_record: bool,
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
    on_error: "OnError" = "fail",
) -> "type[DecoratedTransformation]":
    from reflowfy.transformations.base import (
        AsyncTransformation,
        BaseTransformation,
        RecordTransformation,
        SyncRecordTransformation,
    )

    is_async = inspect.iscoroutinefunction(func)
    # Plain `type`: the bases are abstract until the methods below fill them in.
    base: type
    methods: Dict[str, Any]
    if not per_record and is_async:

        async def apply_async(self: Any, records: Any, runtime_params: Any) -> Any:
            return await func(records, runtime_params, **self.options)

        base, methods = AsyncTransformation, {"apply_async": apply_async}
    elif not per_record:

        def apply(self: Any, records: Any, runtime_params: Any) -> Any:
            return func(records, runtime_params, **self.options)

        base, methods = BaseTransformation, {"apply": apply}
    elif is_async:

        async def apply_record(self: Any, record: Any, runtime_params: Any) -> Any:
            return await func(record, runtime_params, **self.options)

        base = RecordTransformation
        methods = {
            "apply_record": apply_record,
            "concurrency": concurrency,
            "timeout": timeout,
            "on_error": on_error,
        }
    else:
        for knob, value in (("concurrency", concurrency), ("timeout", timeout)):
            if value is not None:
                raise TypeError(
                    f"transformation {name!r}: {knob} needs an `async def` function. A "
                    "plain `def` runs its records one at a time in a thread, where "
                    f"{knob} cannot apply."
                )

        def apply_record_sync(self: Any, record: Any, runtime_params: Any) -> Any:
            return func(record, runtime_params, **self.options)

        base = SyncRecordTransformation
        methods = {"apply_record_sync": apply_record_sync, "on_error": on_error}
    return _make_class(func, name, base, methods)


def _make_class(
    func: Callable[..., Any], name: str, base: type, methods: Dict[str, Any]
) -> "type[DecoratedTransformation]":
    """Create (and so auto-register) the transformation class for ``func``."""
    from reflowfy.core.options import option_binder
    from reflowfy.transformations.base import DecoratedTransformation

    bind_options = option_binder(func, f"transformation {name!r}")

    cls = type(
        func.__name__,
        (DecoratedTransformation, base),
        {
            "name": name,
            **methods,
            "_bind_options": staticmethod(bind_options),
            # Read by validate_steps for the record type the return annotation promises.
            "_transform_fn": staticmethod(func),
            "__doc__": func.__doc__ or f"Transformation: {name}",
            "__module__": func.__module__,
        },
    )
    return cast("type[DecoratedTransformation]", cls)
