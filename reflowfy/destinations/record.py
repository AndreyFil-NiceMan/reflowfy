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
2. Records whose destinations have the same class and config share one destination
   (one producer per cluster/topic/credentials, not one per record). Anything kept
   out of ``config`` — a Kafka ``headers`` function, an ``ssl_context`` — is taken
   from the first record's destination of the group.
3. ``send_with_retry`` sends each group with that group's own retries, so a failed
   group does not re-send the groups already written.

``raise SkipRecord("why")`` drops one record with a reason; ``on_error="skip"`` does the
same for any exception, with its type name as the reason. ``SkipJob`` drops the job.
Skips are reported like a per-record transformation's, under the destination's name.
"""

import asyncio
import inspect
import json
from typing import Any, Callable, ClassVar, Dict, List, Literal, NamedTuple, Optional, Tuple, Union

from reflowfy.core.exceptions import PipelineError, SkipJob, SkipRecord
from reflowfy.destinations.base import BaseDestination
from reflowfy.observability.logging import get_logger

logger = get_logger("destinations.record")

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
    """Records routed at once when the function is ``async def`` (default
    ``TRANSFORM_CONCURRENCY``, 16). A sync function routes the records one after
    another, in a thread."""

    def __init__(self, **options: Any):
        # The routed destinations are what records are written to; this one's config
        # only names it (repr, and the legacy job-level dedup hash).
        super().__init__({"record_destination": self.name})
        self.options = options
        self._groups: List[BaseDestination] = []
        self._group_by_key: Dict[str, int] = {}
        self._group_of_record: Dict[int, int] = {}
        # Keeps routed records alive, so their id() is not reused within the job.
        self._routed: List[Any] = []

    def destination_for_record(self, record: Any, runtime_params: Any) -> Any:
        """The destination ``record`` goes to. May be ``async def`` in a subclass."""
        raise NotImplementedError(f"{type(self).__name__} must implement destination_for_record")

    # ------------------------------------------------------------------ routing

    @property
    def destinations(self) -> List[BaseDestination]:
        """The distinct destinations the routed records go to, in first-seen order."""
        return list(self._groups)

    def destination_for(self, record: Any) -> BaseDestination:
        """The destination a routed ``record`` (the same object) goes to."""
        try:
            return self._groups[self._group_of_record[id(record)]]
        except KeyError:
            raise KeyError(f"record_destination {self.name!r}: record was not routed") from None

    async def route(self, records: List[Any], runtime_params: Any) -> RouteResult:
        """Choose a destination for every record. Nothing is sent.

        Every record is routed before the caller sends any, so a record that fails
        the job (``on_error="fail"``) fails it with nothing written.
        """
        from reflowfy.transformations.base import count_skip_reason, default_record_concurrency

        total = len(records)
        results: List[Union[BaseDestination, _Skipped, None]] = [None] * total

        if inspect.iscoroutinefunction(self.destination_for_record):

            async def route_one(index: int, record: Any) -> None:
                try:
                    produced = await self.destination_for_record(record, runtime_params)
                except Exception as exc:
                    results[index] = self._on_failure(exc, index, total)
                else:
                    results[index] = self._checked(produced, index, total)

            from reflowfy.core.async_utils import for_each_concurrently

            concurrency = self.concurrency or default_record_concurrency()
            await for_each_concurrently(records, concurrency, route_one)
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
            self._group_of_record[id(record)] = self._group_index(result)
            self._routed.append(record)
            kept.append(record)

        if kept:
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

    def _checked(self, produced: Any, index: int, total: int) -> BaseDestination:
        # A wrong return is a bug in the function, not bad data: it fails the job
        # whatever on_error says.
        if not isinstance(produced, BaseDestination) or isinstance(produced, RecordDestination):
            raise RecordDestinationError(
                f"record_destination {self.name!r} returned {type(produced).__name__} for "
                f"record {index + 1} of {total}; expected a destination such as "
                "kafka_destination(...). Filter records in a transformation, not here."
            )
        return produced

    def _group_index(self, destination: BaseDestination) -> int:
        key = _group_key(destination)
        index = self._group_by_key.get(key)
        if index is None:
            index = len(self._groups)
            self._groups.append(destination)
            self._group_by_key[key] = index
        return index

    def _buckets(self, records: List[Any]) -> List[Tuple[BaseDestination, List[Any]]]:
        by_group: Dict[int, List[Any]] = {}
        for record in records:
            by_group.setdefault(self._group_of_record[id(record)], []).append(record)
        return [(self._groups[index], batch) for index, batch in sorted(by_group.items())]

    async def _ensure_routed(self, records: List[Any], metadata: Any) -> List[Any]:
        """``records`` minus any skipped, routing those no caller routed yet.

        The job runner routes before the send; this covers a direct ``send`` call.
        """
        unrouted = [r for r in records if id(r) not in self._group_of_record]
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
        return [r for r in records if id(r) in self._group_of_record]

    # ------------------------------------------------------------------ destination API

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """Send each record to its destination, one destination after another."""
        records = await self._ensure_routed(records, metadata)
        for destination, batch in self._buckets(records):
            await destination.send(batch, metadata)

    async def send_with_retry(
        self, records: List[Any], metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Like :meth:`send`, each destination with its own retries.

        A destination that fails after its retries fails the job; destinations
        already written are not written again by this call.
        """
        records = await self._ensure_routed(records, metadata)
        for destination, batch in self._buckets(records):
            await destination.send_with_retry(batch, metadata)

    async def health_check(self) -> bool:
        """Every routed destination is healthy (none routed: healthy)."""
        for destination in self._groups:
            if not await destination.health_check():
                return False
        return True

    async def close(self) -> None:
        """Close every routed destination; the first error is raised after all were tried."""
        first_error: Optional[BaseException] = None
        for destination in self._groups:
            try:
                await destination.close()
            except Exception as exc:
                first_error = first_error or exc
        if first_error is not None:
            raise first_error

    def __repr__(self) -> str:
        # Not the routed destinations' configs: those hold credentials.
        types = sorted({type(d).__name__ for d in self._groups})
        routed = f", {len(self._groups)} destinations ({', '.join(types)})" if types else ""
        return f"{type(self).__name__}({self.name!r}{routed})"


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
            "destination_for_record": method,
            "_destination_fn": staticmethod(func),
            "__doc__": func.__doc__ or f"Record destination: {name}",
            "__module__": func.__module__,
        },
    )
    return cls
