"""A destination chosen per record: ``@record_destination``.

``@destination`` builds one destination per job. ``@record_destination`` calls a
function once per record and sends each record to the destination it returned, so
one job can write to many topics, clusters or URLs::

    @record_destination("stt_engines", on_error="skip")
    def stt_engines(record, runtime_params):
        topic = Settings.KAFKA.topics[record["engine"]][runtime_params["env"]]
        return kafka_destination(bootstrap_servers=..., topic=topic.name, ...)

    def define_destination(self, records, runtime_params):
        return stt_engines()

How a job runs (``execution/job_runner.py`` calls :meth:`RecordDestination.route`
right after ``define_destination``):

1. The function is called for every record before anything is sent. Building a
   destination is cheap: none connects before its first send.
2. Records whose destinations have the same class and config share one destination.
   Anything kept out of ``config`` — a Kafka ``headers`` function, an ``ssl_context`` —
   is taken from the first record's destination of the group.
3. Connections are shared wider than groups, through one :class:`ConnectionPool` per
   job: one Kafka producer per cluster and credentials (a producer writes to any
   topic), one HTTP client per host. Health checks run once per cluster or host.
   Inside a worker that pool takes its connections from the worker's own pool
   (``destinations/pool.py``), so they outlive the job and serve the next ones.
4. ``send_with_retry`` sends the groups concurrently, each with its own retries, so a
   failed group does not re-send the groups already written. Order is kept within a
   group, not across groups. The first group that fails cancels the rest.
   ``concurrency`` (default ``TRANSFORM_CONCURRENCY``, 16, as for
   ``@record_transformation``) is the job's one limit: groups sent at once, and
   per-record requests in flight across *all* groups together. A Kafka group is one
   batch, which needs no limit.

``raise SkipRecord("why")`` drops one record with a reason; ``on_error="skip"`` does the
same for any exception, with its type name as the reason. ``SkipJob`` drops the job.
Skips are reported like a per-record transformation's, under the destination's name.

The function may also return a list of destinations (the record goes to each: a
multicast), or ``fan_out(d1, d2, optional=[d2])`` to mark some optional. A record then
joins several groups; each group is sent and retried on its own. A group that only
optional records chose may fail without failing the job: the failure is logged and
counted (``failed_optional``, ``destinations_failed_optional`` in the job's stats).
"""

import asyncio
import inspect
import json
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

from reflowfy.core.async_utils import for_each_concurrently
from reflowfy.core.exceptions import PipelineError, SkipJob, SkipRecord
from reflowfy.destinations.base import BaseDestination, ConnectionPool, DestinationError
from reflowfy.observability.logging import get_logger
from reflowfy.core.record_steps import count_skip_reason, default_record_concurrency

logger = get_logger("destinations.record")

ADVICE_MIN_RECORDS = 10
"""A job routing at least this many records, each to its own destination, gets advice."""

# Record destinations already advised: once per process, not once per job.
_advised: Set[str] = set()

OnError = Literal["fail", "skip"]
"""What a record whose function raises does: fail the job, or skip the record."""

RecordDestinationFn = Callable[..., Any]
"""``(record, runtime_params, **options) -> BaseDestination`` (sync or ``async def``)."""


class RecordDestinationError(PipelineError):
    """The function of a ``@record_destination`` failed on a record, or returned no destination.

    Names the destination, the record's position and the exception type — never the
    record's contents. ``original_error`` holds the exception the function raised.
    """


class RouteResult(NamedTuple):
    """What :meth:`RecordDestination.route` did with a job's records."""

    records: List[Any]
    """The records that got a destination, in input order."""
    skip_reasons: Dict[str, int]
    """Skip reason -> records skipped for it (bounded like a transformation's)."""


class _Skipped:
    """A record its destination function dropped, and why."""

    def __init__(self, reason: str):
        self.reason = reason


_Targets = List[Tuple[BaseDestination, bool]]
"""What one record goes to: (destination, optional) pairs."""


class RecordDestination(BaseDestination):
    """Sends each record to the destination :meth:`destination_for_record` returns for it.

    Usually built with ``@record_destination``. Subclass it and override
    :meth:`destination_for_record` (sync or ``async def``) for anything the decorator
    does not cover. One instance serves one job: ``define_destination`` builds a new one
    each time.
    """

    name: ClassVar[str] = "record_destination"
    on_error: ClassVar[OnError] = "fail"
    concurrency: ClassVar[Optional[int]] = None
    """The job's one limit: requests in flight across all destinations together (a
    per-record ``api_destination``; a Kafka group is one batch), destinations sent to,
    health-checked and closed at once, and records routed at once when the function is
    ``async def``. Default ``TRANSFORM_CONCURRENCY`` (16), as for
    ``@record_transformation``. A sync function routes its records one after another,
    in a thread."""
    timeout: ClassVar[Optional[float]] = None
    """Seconds allowed per record for an ``async def`` function; ``None`` means no
    limit. A timeout is an error like any other: it fails the job, or with
    ``on_error="skip"`` skips the record with the reason ``TimeoutError``."""

    def __init__(self, **options: Any):
        # The routed destinations are what records are written to; this one's config
        # only names it.
        super().__init__({"record_destination": self.name})
        self.options = options
        self._groups: List[BaseDestination] = []
        self._group_by_key: Dict[str, int] = {}
        # A group is required as soon as one record chose it without optional=.
        self._group_required: List[bool] = []
        self._groups_of_record: Dict[int, List[int]] = {}
        self.failed_optional: List[str] = []
        """Optional destinations that failed this job (type names), in failure order."""
        # Keeps routed records alive, so their id() is not reused within the job.
        self._routed: List[Any] = []
        # Connections shared by every routed destination, and the job's request budget.
        self._pool = ConnectionPool(request_limit=self.effective_concurrency())

    def destination_for_record(self, record: Any, runtime_params: Any) -> Any:
        """The destination ``record`` goes to. May be ``async def`` in a subclass."""
        raise NotImplementedError(f"{type(self).__name__} must implement destination_for_record")

    def effective_concurrency(self) -> int:
        """The cap this destination runs with."""

        return self.concurrency if self.concurrency is not None else default_record_concurrency()

    # ------------------------------------------------------------------ routing

    @property
    def destinations(self) -> List[BaseDestination]:
        """The distinct destinations the routed records go to, in first-seen order."""
        return list(self._groups)

    def destination_for(self, record: Any) -> BaseDestination:
        """The (first) destination a routed ``record`` (the same object) goes to."""
        return self.destinations_for(record)[0]

    def destinations_for(self, record: Any) -> List[BaseDestination]:
        """Every destination a routed ``record`` (the same object) goes to."""
        try:
            return [self._groups[index] for index in self._groups_of_record[id(record)]]
        except KeyError:
            raise KeyError(f"record_destination {self.name!r}: record was not routed") from None

    def keep_only(self, record: Any, destinations: Sequence[BaseDestination]) -> None:
        """Send ``record`` only to these of its destinations (content dedup: the others
        already have it)."""
        keep = {id(d) for d in destinations}
        self._groups_of_record[id(record)] = [
            index for index in self._groups_of_record[id(record)] if id(self._groups[index]) in keep
        ]

    async def route(self, records: List[Any], runtime_params: Any) -> RouteResult:
        """Choose a destination for every record. Nothing is sent.

        Every record is routed before the caller sends any, so a record that fails
        the job (``on_error="fail"``) fails it with nothing written.
        """

        total = len(records)
        results: List[Union[_Targets, _Skipped, None]] = [None] * total

        if inspect.iscoroutinefunction(self.destination_for_record):

            async def route_one(index: int, record: Any) -> None:
                try:
                    produced = await self._call_async(record, runtime_params)
                except Exception as exc:
                    results[index] = self._on_failure(exc, index, total)
                else:
                    results[index] = self._checked(produced, index, total)

            await for_each_concurrently(records, self.effective_concurrency(), route_one)
        else:

            def route_all() -> None:
                for index, record in enumerate(records):
                    try:
                        produced = self.destination_for_record(record, runtime_params)
                    except Exception as exc:
                        results[index] = self._on_failure(exc, index, total)
                    else:
                        results[index] = self._checked(produced, index, total)

            # User code: off the event loop, which the worker's Kafka heartbeat needs.
            await asyncio.to_thread(route_all)

        kept: List[Any] = []
        skip_reasons: Dict[str, int] = {}
        for record, result in zip(records, results):
            if isinstance(result, _Skipped):
                count_skip_reason(skip_reasons, result.reason)
                continue
            assert result is not None
            groups: List[int] = []
            for destination, optional in result:
                index = self._group_index(destination, optional)
                if index not in groups:
                    groups.append(index)
            self._groups_of_record[id(record)] = groups
            self._routed.append(record)
            kept.append(record)

        if kept:
            self._advise(len(kept))
            logger.info(
                "%s routed %d records to %d destinations",
                self.name,
                len(kept),
                len(self._groups),
                extra={
                    "event": "records_routed",
                    "record_destination": self.name,
                    "records_routed": len(kept),
                    "destinations": len(self._groups),
                },
            )
        elif total:
            # Every record skipped looks like an empty job otherwise; a broken config
            # (every lookup raising) should stand out.
            logger.warning(
                "All %d records skipped by %s — nothing to send",
                total,
                self.name,
                extra={"event": "records_all_skipped", "record_destination": self.name},
            )
        return RouteResult(kept, skip_reasons)

    async def _call_async(self, record: Any, runtime_params: Any) -> Any:
        if self.timeout is None:
            return await self.destination_for_record(record, runtime_params)
        try:
            return await asyncio.wait_for(
                self.destination_for_record(record, runtime_params), self.timeout
            )
        except asyncio.TimeoutError as exc:
            # The bare asyncio.TimeoutError says nothing on its own.
            raise TimeoutError(f"timed out after {self.timeout}s") from exc

    def _advise(self, routed: int) -> None:
        """Say once when every record got a destination of its own.

        Shared connections keep that fast, but it usually means a per-record value
        (an id in an f-string URL, a body) went into the destination's config, where
        it is not URL-encoded and makes every record a destination of its own.
        """
        if routed < ADVICE_MIN_RECORDS or len(self._groups) < routed:
            return
        if self.name in _advised:
            return
        _advised.add(self.name)
        logger.warning(
            "%s built a destination per record (%d/%d): put per-record values in "
            "templates — api_destination(url='.../{id}', per_record=True), "
            "headers={'X': '{field}'}, kafka_destination(key='{field}') — so records "
            "share a destination and values are encoded",
            self.name,
            len(self._groups),
            routed,
            extra={"event": "destination_per_record", "record_destination": self.name},
        )

    def _on_failure(self, exc: Exception, index: int, total: int) -> _Skipped:
        if isinstance(exc, SkipJob):
            raise exc
        if isinstance(exc, SkipRecord):
            return _Skipped(str(exc))
        if self.on_error == "skip":
            # The type, not the message: a message can carry record values, and
            # reasons are counted.
            return _Skipped(type(exc).__name__)
        raise RecordDestinationError(
            f"record_destination {self.name!r} failed on record {index + 1} of {total}: "
            f"{type(exc).__name__}",
            original_error=exc,
        ) from exc

    def _checked(self, produced: Any, index: int, total: int) -> _Targets:
        # A wrong return is a bug in the function, not bad data: it fails the job
        # whatever on_error says.
        from reflowfy.destinations.fan_out import FanOut

        kind = type(produced).__name__
        if isinstance(produced, FanOut):
            return produced.targets()
        if isinstance(produced, (list, tuple)) and produced:
            items = cast(Sequence[Any], produced)
            if all(is_routable(d) for d in items):
                return [(cast(BaseDestination, d), False) for d in items]
        elif is_routable(produced):
            return [(cast(BaseDestination, produced), False)]
        raise RecordDestinationError(
            f"record_destination {self.name!r} returned {kind} for "
            f"record {index + 1} of {total}; expected a destination such as "
            "kafka_destination(...), a non-empty list of them, or fan_out(...). "
            "Filter records in a transformation, not here."
        )

    def _group_index(self, destination: BaseDestination, optional: bool = False) -> int:
        key = _group_key(destination)
        index = self._group_by_key.get(key)
        if index is None:
            index = len(self._groups)
            destination.connection_pool = self._pool
            destination.send_concurrency = self.effective_concurrency()
            self._groups.append(destination)
            self._group_required.append(not optional)
            self._group_by_key[key] = index
        elif not optional:
            self._group_required[index] = True
        return index

    def is_optional(self, destination: BaseDestination) -> bool:
        """True when only ``optional=`` records chose this routed destination."""
        return not self._group_required[self._groups.index(destination)]

    def _buckets(self, records: List[Any]) -> List[Tuple[int, List[Any]]]:
        by_group: Dict[int, List[Any]] = {}
        for record in records:
            for index in self._groups_of_record[id(record)]:
                by_group.setdefault(index, []).append(record)
        return sorted(by_group.items())

    async def _send_groups(
        self, records: List[Any], metadata: Optional[Dict[str, Any]], retry: bool
    ) -> None:
        """Send each group its records, ``concurrency`` groups at a time.

        A required group's failure fails the job and cancels the sends in flight; an
        optional group's is logged and recorded in :attr:`failed_optional`.
        """

        async def send_one(_: int, bucket: Tuple[int, List[Any]]) -> None:
            index, batch = bucket
            destination = self._groups[index]
            try:
                if retry:
                    await destination.send_with_retry(batch, metadata)
                else:
                    await destination.send(batch, metadata)
            except Exception as exc:
                if self._group_required[index]:
                    raise
                self._optional_failed(destination, exc, len(batch))

        await for_each_concurrently(self._buckets(records), self.effective_concurrency(), send_one)

    def _optional_failed(self, destination: BaseDestination, exc: Exception, records: int) -> None:
        kind = type(destination).__name__
        self.failed_optional.append(kind)
        # A destination error names a position, never a record; anything else: its type.
        detail = str(exc) if isinstance(exc, DestinationError) else type(exc).__name__
        logger.warning(
            "%s: optional destination %s failed, %d records not written there: %s",
            self.name,
            kind,
            records,
            detail,
            extra={
                "event": "destination_optional_failed",
                "record_destination": self.name,
                "destination_type": kind,
                "error_type": type(exc).__name__,
                "records": records,
            },
        )

    async def _ensure_routed(self, records: List[Any], metadata: Any) -> List[Any]:
        """``records`` minus any skipped, routing those no caller routed yet.

        The job runner routes before the send; this covers a direct ``send`` call.
        """
        unrouted = [r for r in records if id(r) not in self._groups_of_record]
        if not unrouted:
            return records
        result = await self.route(unrouted, metadata if metadata is not None else {})
        skipped = len(unrouted) - len(result.records)
        if skipped:
            logger.warning(
                "%s skipped %d records at send time",
                self.name,
                skipped,
                extra={"event": "records_skipped", "record_destination": self.name},
            )
        return [r for r in records if id(r) in self._groups_of_record]

    # ------------------------------------------------------------------ destination API

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """Send each record to its destination, ``concurrency`` destinations at a time."""
        records = await self._ensure_routed(records, metadata)
        await self._send_groups(records, metadata, retry=False)

    async def send_with_retry(
        self, records: List[Any], metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Like :meth:`send`, each destination with its own retries.

        A destination that fails after its retries fails the job and cancels the sends
        still in flight; destinations already written are not written again by this call.
        """
        records = await self._ensure_routed(records, metadata)
        await self._send_groups(records, metadata, retry=True)

    async def health_check(self) -> bool:
        """Every routed destination is healthy (none routed: healthy).

        One check per :meth:`BaseDestination.health_key` — per cluster or host, not
        per routed destination — run concurrently. An unhealthy optional destination
        does not fail the job: it is logged, and its send will fail on its own.
        """
        checks: Dict[str, BaseDestination] = {}
        required: Dict[str, bool] = {}
        for index, destination in enumerate(self._groups):
            key = destination.health_key()
            key = key if key is not None else f"#{index}"
            checks.setdefault(key, destination)
            required[key] = required.get(key, False) or self._group_required[index]
        keys = list(checks)
        healthy: List[bool] = [True] * len(keys)

        async def check(index: int, key: str) -> None:
            healthy[index] = await checks[key].health_check()

        await for_each_concurrently(keys, self.effective_concurrency(), check)
        for key, ok in zip(keys, healthy):
            if not ok and not required[key]:
                logger.warning(
                    "%s: optional destination %s is unhealthy; sending to the others",
                    self.name,
                    type(checks[key]).__name__,
                    extra={
                        "event": "destination_optional_unhealthy",
                        "record_destination": self.name,
                    },
                )
        return all(ok or not required[key] for key, ok in zip(keys, healthy))

    async def close(self) -> None:
        """Close every routed destination; the first error is raised after all were tried."""
        errors: List[Optional[BaseException]] = [None] * len(self._groups)

        async def close_one(index: int, destination: BaseDestination) -> None:
            try:
                await destination.close()
            except Exception as exc:
                errors[index] = exc

        await for_each_concurrently(self._groups, self.effective_concurrency(), close_one)
        try:
            await self._pool.close()  # the shared producers and clients
        except Exception as exc:
            errors.append(exc)
        first_error = next((e for e in errors if e is not None), None)
        if first_error is not None:
            raise first_error

    def __repr__(self) -> str:
        # Not the routed destinations' configs: those hold credentials.
        types = sorted({type(d).__name__ for d in self._groups})
        routed = f", {len(self._groups)} destinations ({', '.join(types)})" if types else ""
        return f"{type(self).__name__}({self.name!r}{routed})"


def is_routable(destination: Any) -> bool:
    """A destination records can be routed to: not another record destination."""
    return isinstance(destination, BaseDestination) and not isinstance(
        destination, RecordDestination
    )


def _group_key(destination: BaseDestination) -> str:
    """Destinations with equal keys are the same place, reached the same way."""
    cls = type(destination)
    return json.dumps(
        {
            "type": f"{cls.__module__}.{cls.__qualname__}",
            "config": destination.config,
            "retry": repr(destination.retry_config),
            "dedup_identity": destination.dedup_identity,
        },
        sort_keys=True,
        default=repr,
    )


def make_record_destination(
    func: RecordDestinationFn,
    name: str,
    on_error: OnError,
    concurrency: Optional[int],
    timeout: Optional[float] = None,
) -> "type[RecordDestination]":
    """The :class:`RecordDestination` subclass ``@record_destination`` builds for ``func``.

    Instances take keyword options, passed on to every call:
    ``stt_engines(region="eu")`` calls ``func(record, runtime_params, region="eu")``.
    """
    if inspect.iscoroutinefunction(func):

        async def destination_for_record_async(
            self: RecordDestination, record: Any, runtime_params: Any
        ) -> Any:
            return await func(record, runtime_params, **self.options)

        method: Callable[..., Any] = destination_for_record_async
    else:

        def destination_for_record(
            self: RecordDestination, record: Any, runtime_params: Any
        ) -> Any:
            return func(record, runtime_params, **self.options)

        method = destination_for_record

    cls = type(
        func.__name__,
        (RecordDestination,),
        {
            "name": name,
            "on_error": on_error,
            "concurrency": concurrency,
            "timeout": timeout,
            "destination_for_record": method,
            "_destination_fn": staticmethod(func),
            "__doc__": func.__doc__ or f"Record destination: {name}",
            "__module__": func.__module__,
        },
    )
    return cls
