"""Base transformation class with automatic registration."""

import asyncio
import os
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from reflowfy.core.exceptions import SkipJob, SkipRecord
from reflowfy.core.query_loader import QueryLoaderMixin


class TransformationMeta(ABCMeta):
    """
    Metaclass for automatic transformation registration.

    When a class inherits from BaseTransformation and defines a 'name' attribute,
    it is automatically registered in the transformation registry.

    Inherits from ABCMeta to be compatible with ABC.
    """

    def __new__(mcs, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)

        # Only register concrete transformations (not the base class)
        if name != "BaseTransformation" and bases:
            # Check if this is a concrete transformation with a name
            if "name" in namespace and namespace["name"]:
                # Import here to avoid circular dependency
                from reflowfy.transformations.registry import transformation_registry

                transformation_registry.register(cls)

        return cls


class BaseTransformation(QueryLoaderMixin, metaclass=TransformationMeta):
    """
    Base class for all transformations.

    Users create custom transformations by inheriting from this class:

    Example:
        >>> class XmlToJson(BaseTransformation):
        ...     name = "xml_to_json"
        ...
        ...     def apply(self, records, runtime_params):
        ...         return [self.parse_xml(r) for r in records]
        ...
        ...     def parse_xml(self, record):
        ...         # Custom XML parsing logic
        ...         return {"parsed": record}

    The transformation is automatically registered and can be used in pipelines.
    """

    # Concrete transformations MUST set this
    name: str = ""

    # The record type apply() returns (a record, not a list: e.g. TaxedOrder), checked
    # when the pipeline sets validate_steps. None = unchecked. A @transformation or
    # @record_transformation reads it from the function's return annotation instead.
    output_type: Any = None

    @abstractmethod
    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """
        Apply transformation to a batch of records.

        Args:
            records: List of records to transform
            runtime_params: Flat dict of all pipeline params — both the user-supplied runtime
                parameters (e.g. env, multiplier) and execution-context keys (execution_id,
                batch_id, pipeline_name, created_at, current_ids for id-based pipelines).
                This is the same dict that define_source / define_destination receive.
                Mutations are visible to subsequent transformations and the destination.

        Returns:
            Transformed list of records

        Raises:
            TransformationError: If transformation fails
        """
        pass

    def validate_input(self, records: List[Any]) -> None:
        """
        Optional: Validate input records before transformation.

        Args:
            records: Records to validate

        Raises:
            ValueError: If validation fails
        """
        pass

    def validate_output(self, records: List[Any]) -> None:
        """
        Optional: Validate output records after transformation.

        Args:
            records: Records to validate

        Raises:
            ValueError: If validation fails
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"


class AsyncTransformation(BaseTransformation):
    """A transformation whose work is a coroutine: implement :meth:`apply_async`.

    The worker awaits it on its own event loop, so it can call HTTP APIs or
    databases without blocking (``@transformation`` on an ``async def`` builds one
    of these). Steps still run one after another; for many awaits per job, one
    per record, use :class:`RecordTransformation` / ``@record_transformation``.

    Example:
        >>> class Geocode(AsyncTransformation):
        ...     name = "geocode"
        ...
        ...     def __init__(self, provider: str):
        ...         self.provider = provider
        ...
        ...     async def apply_async(self, records, runtime_params):
        ...         async with httpx.AsyncClient() as client:
        ...             ...

    Open clients inside ``apply_async`` rather than at module level: ``reflowfy
    test`` and local mode run each job on a new event loop.
    """

    @abstractmethod
    async def apply_async(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """Transform a batch of records; same contract as :meth:`BaseTransformation.apply`."""

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """Synchronous entry point for callers that are not async; runs ``apply_async``."""
        from reflowfy.core.async_utils import run_coroutine_sync

        return run_coroutine_sync(self.apply_async(records, runtime_params))


DEFAULT_RECORD_CONCURRENCY = 16


def default_record_concurrency() -> int:
    """``TRANSFORM_CONCURRENCY`` (records in flight per per-record step), default 16."""
    raw = os.getenv("TRANSFORM_CONCURRENCY", "")
    try:
        value = int(raw) if raw.strip() else DEFAULT_RECORD_CONCURRENCY
    except ValueError:
        raise ValueError(f"TRANSFORM_CONCURRENCY must be an integer, got {raw!r}") from None
    if value < 1:
        raise ValueError(f"TRANSFORM_CONCURRENCY must be >= 1, got {value}")
    return value


class RecordFailure(Exception):
    """One record of a :class:`RecordTransformation` failed.

    Carries the record's position so the runner can report exactly which record
    broke the step, instead of guessing from the exception.
    """

    def __init__(self, index: int, record: Any, error: Exception):
        self.index = index
        self.record = record
        self.error = error
        super().__init__(f"record {index}: {type(error).__name__}: {error}")


MAX_SKIP_REASONS = 20
"""Distinct ``SkipRecord`` reasons kept per step; the rest are counted as ``OTHER_SKIP_REASON``."""
MAX_SKIP_REASON_LENGTH = 200
OTHER_SKIP_REASON = "(other reasons)"


def count_skip_reason(reasons: Dict[str, int], reason: str, times: int = 1) -> None:
    """Add ``times`` skips for ``reason`` to ``reasons``, keeping the dict bounded."""
    reason = reason[:MAX_SKIP_REASON_LENGTH] or "(no reason)"
    if reason not in reasons and len(reasons) >= MAX_SKIP_REASONS:
        reason = OTHER_SKIP_REASON
    reasons[reason] = reasons.get(reason, 0) + times


def top_skip_reasons(reasons: Dict[str, int], limit: int = 3) -> str:
    """``"user inactive" x3, "no email" x1`` — the most frequent reasons, for one log line."""
    top = sorted(reasons.items(), key=lambda item: -item[1])[:limit]
    more = len(reasons) - len(top)
    text = ", ".join(f"{reason!r} x{times}" for reason, times in top)
    return f"{text}, +{more} more" if more > 0 else text


class _Skipped:
    """Marks a record its transformation dropped with ``SkipRecord``."""

    def __init__(self, reason: str):
        self.reason = reason


class RecordResults(NamedTuple):
    """What a per-record step produced: the kept records, and why the others went."""

    records: List[Any]
    skip_reasons: Dict[str, int]
    """``SkipRecord`` reason -> records skipped for it (bounded, see ``MAX_SKIP_REASONS``)."""


class RecordTransformation(AsyncTransformation):
    """Awaits :meth:`apply_record` once per record, ``concurrency`` records at a time.

    Output keeps input order; a record for which ``apply_record`` returns ``None``
    is dropped. The first record that raises stops the step: the records still in
    flight are cancelled and the job fails naming that record's index.
    ``SkipRecord`` drops that record and records why; ``SkipJob`` from any record
    drops the whole job. ``timeout`` (seconds) bounds
    each record's call.

    The concurrent calls share one ``runtime_params``: read it, don't write it.
    Usually built with ``@record_transformation``; subclass it to share one client
    across the records of a step, opened in :meth:`setup` and closed in
    :meth:`teardown` (creating an ``httpx.AsyncClient`` costs tens of milliseconds,
    so one per record is slow)::

        class EnrichUser(RecordTransformation):
            name = "enrich_user"
            concurrency = 32

            async def setup(self, runtime_params):
                self.client = httpx.AsyncClient(base_url="http://users")

            async def teardown(self):
                await self.client.aclose()

            async def apply_record(self, record, runtime_params):
                resp = await self.client.get(f"/users/{record['user_id']}")
                return {**record, "user": resp.json()}
    """

    concurrency: Optional[int] = None
    """Records in flight at once; ``None`` means ``TRANSFORM_CONCURRENCY`` (16)."""
    timeout: Optional[float] = None
    """Seconds allowed per record; ``None`` means no limit."""

    @abstractmethod
    async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Any:
        """Transform one record; return the new record, or ``None`` to drop it."""

    async def setup(self, runtime_params: Dict[str, Any]) -> None:
        """Called once before the step's records run: open shared clients here."""

    async def teardown(self) -> None:
        """Called once after the step's records, whether they succeeded or not."""

    def effective_concurrency(self) -> int:
        """The cap this step runs with."""
        return self.concurrency if self.concurrency is not None else default_record_concurrency()

    async def _apply_one(self, index: int, record: Any, runtime_params: Dict[str, Any]) -> Any:
        try:
            if self.timeout is None:
                return await self.apply_record(record, runtime_params)
            try:
                return await asyncio.wait_for(
                    self.apply_record(record, runtime_params), self.timeout
                )
            except asyncio.TimeoutError as exc:
                # The bare asyncio.TimeoutError says nothing on its own.
                timed_out = TimeoutError(f"timed out after {self.timeout}s")
                raise RecordFailure(index, record, timed_out) from exc
        except (SkipJob, RecordFailure):
            raise
        except SkipRecord as exc:
            return _Skipped(str(exc))
        except Exception as exc:
            raise RecordFailure(index, record, exc) from exc

    async def apply_async(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        return (await self.apply_records(records, runtime_params)).records

    async def apply_records(
        self, records: List[Any], runtime_params: Dict[str, Any]
    ) -> RecordResults:
        """Run the step and also report the records it skipped (the runner uses this)."""
        if not records:
            return RecordResults([], {})
        await self.setup(runtime_params)
        try:
            results = await self._apply_all(records, runtime_params)
        finally:
            await self.teardown()
        kept: List[Any] = []
        skip_reasons: Dict[str, int] = {}
        for result in results:
            if isinstance(result, _Skipped):
                count_skip_reason(skip_reasons, result.reason)
            elif result is not None:
                kept.append(result)
        return RecordResults(kept, skip_reasons)

    async def _apply_all(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        results: List[Any] = [None] * len(records)
        # A fixed pool of workers pulling from one iterator, rather than a task per
        # record, keeps memory flat for a job of any size. next() never awaits, so
        # the workers cannot take the same record.
        pending = iter(enumerate(records))

        async def worker() -> None:
            for index, record in pending:
                results[index] = await self._apply_one(index, record, runtime_params)

        workers = [
            asyncio.ensure_future(worker())
            for _ in range(min(self.effective_concurrency(), len(records)))
        ]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        return results


class TransformationError(Exception):
    """Raised when a transformation fails.

    Carries enough context to say *where* in the chain the failure happened and
    *which* record caused it, so callers can report it without re-running any
    user code.

    Attributes:
        transformation_name: The transformation that raised.
        original_error: The underlying exception, if any.
        step_index: 0-based position in the applied chain, or None if unknown.
        records_in: How many records this step received, or None if unknown.
        culprit_index: Index of the implicated record, or None if not identified.
        culprit_record: The implicated record itself, or None.
        applied_steps: The steps that succeeded before this one, in order.
    """

    def __init__(
        self,
        transformation_name: str,
        message: str,
        original_error: Optional[Exception] = None,
        step_index: Optional[int] = None,
        records_in: Optional[int] = None,
        culprit_index: Optional[int] = None,
        culprit_record: Optional[Any] = None,
        applied_steps: Optional[List[Any]] = None,
    ):
        self.transformation_name = transformation_name
        self.original_error = original_error
        self.step_index = step_index
        self.records_in = records_in
        self.culprit_index = culprit_index
        self.culprit_record = culprit_record
        self.applied_steps: List[Any] = applied_steps or []
        super().__init__(f"Transformation '{transformation_name}' failed: {message}")


class StepOutputError(TransformationError):
    """A transformation returned records that do not match its own return type.

    Raised after the step when the pipeline sets ``validate_steps = True``. It is a
    bug in that step, not bad input, so it always fails the job — whatever
    ``on_invalid_record`` says — and is counted under its own ``error_type``.
    """
