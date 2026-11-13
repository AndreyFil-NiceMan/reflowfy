"""Base source interface for data fetching and job splitting."""

import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Tuple, Type, TypeVar, cast

from reflowfy.core.execution_context import parameter_resolver
from reflowfy.core.types import Records
from reflowfy.core.exceptions import SourceError as SourceError  # its old home

logger = logging.getLogger(__name__)

_S = TypeVar("_S", bound="BaseSource")


@dataclass
class SourceJob:
    """
    Represents a single job to be processed by a worker.

    Attributes:
        records: Data records for this job
        metadata: Job-specific metadata (e.g., scroll_id, offset, page_num)
    """

    records: Records
    metadata: Dict[str, Any]


class BaseSource(ABC):
    """
    Base class for all data sources.

    Sources are responsible for:
    1. Planning: ``split()`` yields one narrowed source per job (manager side)
    2. Fetching one job's records: ``fetch()`` (worker side)
    3. Resolving runtime parameters
    4. Health checks

    A subclass is registered with ``SourceFactory`` under its class name as soon as
    it is defined, so a custom source runs on workers like a built-in one. The
    worker rebuilds each job's source with :meth:`from_config` from the ``config``
    the manager serialized; that round trip is checked at planning.
    """

    # Config keys holding credentials: hidden in every API response that returns a
    # job payload, and a literal value in one is warned about when it is serialized
    # (use ``secret("ENV")``). Keys named like a credential (``password``, ``token``,
    # ``api_key``…) and passwords inside URLs are hidden whether listed or not.
    secret_fields: ClassVar[Tuple[str, ...]] = ()

    # Params the planner attached to this one job, merged into its
    # runtime_params before it runs. None = the job just gets the execution's
    # params. Set by the ID-based plan and by reflowfy.job(); never serialized
    # onto the wire, since it travels in the job message's metadata instead.
    job_params: Optional[Dict[str, Any]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if inspect.isabstract(cls):
            return
        register_source_type(cls.__name__, cls)

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize source with configuration.

        Args:
            config: Source-specific configuration
        """
        self.config = config
        self._resolved_config: Optional[Dict[str, Any]] = None

    @classmethod
    def from_config(cls: "type[_S]", config: Dict[str, Any]) -> _S:
        """Rebuild a source from its serialized ``config`` (on the worker).

        Default: ``cls(**config)`` — ``config`` holds exactly the constructor's
        keyword arguments, as every built-in source does — or ``cls(config)`` for a
        subclass that keeps ``BaseSource``'s ``__init__(self, config)`` signature.
        Override it when the constructor takes something else.
        """
        params = list(inspect.signature(cls.__init__).parameters.values())[1:]
        if [p.name for p in params] == ["config"]:
            return cls(config)  # __init__(self, config)
        return cls(**config)

    @property
    def registry_type(self) -> str:
        """Stable type name used to serialize/reconstruct this source."""
        return self.__class__.__name__

    def resolve_parameters(self, runtime_params: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Resolve Jinja2 templates in configuration with runtime parameters.

        Args:
            runtime_params: Runtime parameters provided by user

        Returns:
            Resolved configuration
        """
        self._resolved_config = parameter_resolver.resolve(self.config, runtime_params)
        return self._resolved_config

    def get_runtime_parameters(self) -> List[str]:
        """
        Extract runtime parameter names from configuration.

        Returns:
            List of parameter names used in templates
        """
        return list(parameter_resolver.extract_parameters(self.config))

    def split(self, runtime_params: Dict[str, Any]) -> "Iterator[BaseSource]":
        """Manager-side planning. Yield one narrowed source per job.

        Metadata-only and cheap — MUST NOT fetch bulk data. The default
        yields ``self`` (a single job; the worker fetches the whole source).
        Sources that can shard cheaply override this to yield N sub-sources,
        each with its slice baked into ``config``.
        """
        yield self

    @abstractmethod
    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Iterable[Any]:
        """
        Fetch this job's records (on the worker).

        Return a list of records, or any iterable of them: a generator that ``yield``-s
        rows from a cursor works too (it is consumed in full before the job's
        transformations run). For an ``async def``, subclass :class:`AsyncSource`.

        Args:
            runtime_params: Runtime parameters for template resolution
            limit: Optional limit for testing (e.g., first 100 records)

        Returns:
            The records
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if source is accessible and healthy.

        Returns:
            True if healthy, False otherwise
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


class AsyncSource(BaseSource):
    """A source whose fetch is a coroutine: implement :meth:`fetch_async`.

    The worker awaits it on its own event loop, so it can call HTTP APIs or databases
    without a thread. Planning (``split``) stays synchronous: it runs on the manager.

    Example:
        >>> class Users(AsyncSource):
        ...     def __init__(self, url: str):
        ...         super().__init__({"url": url})
        ...
        ...     async def fetch_async(self, runtime_params, limit=None):
        ...         async with httpx.AsyncClient() as client:
        ...             return (await client.get(self.config["url"])).json()
        ...
        ...     def health_check(self):
        ...         return True

    Open clients inside ``fetch_async`` rather than at module level: ``reflowfy test``
    and local mode run each job on a new event loop.
    """

    @abstractmethod
    async def fetch_async(
        self, runtime_params: Dict[str, Any], limit: Optional[int] = None
    ) -> Iterable[Any]:
        """Fetch this job's records; same contract as :meth:`BaseSource.fetch`."""

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """Synchronous entry point for callers that are not async; runs ``fetch_async``."""
        from reflowfy.core.async_utils import run_coroutine_sync

        async def fetch_all() -> List[Any]:
            return materialize_fetch(await self.fetch_async(runtime_params, limit), self)

        return run_coroutine_sync(fetch_all())


class SourceContractError(TypeError):
    """A source's fetch returned something that is not records.

    A ``TypeError``: a bug in the source, never retried by ``job_retry``.
    """


def materialize_fetch(result: Any, source: Any) -> List[Any]:
    """What ``fetch`` returned, as a list of records.

    A list is returned as is, a tuple or any other iterable (a generator) is consumed
    into a list, and ``None`` is an empty slice. Anything else raises
    :class:`SourceContractError` — it used to be ``str()``-ed and sent one character
    per record.
    """
    name = type(source).__name__
    if result is None:
        return []
    if isinstance(result, list):
        return cast(List[Any], result)
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()  # no "coroutine was never awaited" warning on top of the error
        raise SourceContractError(
            f"{name}.fetch returned a coroutine; an async source subclasses AsyncSource "
            "and implements `async def fetch_async`"
        )
    kind = type(cast(object, result)).__name__
    if isinstance(result, (str, bytes, bytearray, dict)) or not isinstance(result, Iterable):
        raise SourceContractError(
            f"{name}.fetch returned {kind}; return a list of records (or yield them)"
        )
    return list(cast(Iterable[Any], result))


source_types: Dict[str, Type[BaseSource]] = {}
"""Every concrete source class by name; how a worker rebuilds a job's ``source.type``."""


def register_source_type(type_name: str, source_class: Type[BaseSource]) -> None:
    """Register ``source_class`` under ``type_name``; a different class of that name replaces it."""
    previous = source_types.get(type_name)
    if previous is not None and previous is not source_class:
        same_place = (previous.__module__, previous.__qualname__) == (
            source_class.__module__,
            source_class.__qualname__,
        )
        if not same_place:  # a module re-imported redefines the same class
            logger.warning(
                "Source type %r registered twice (%s.%s replaces %s.%s): jobs of "
                "either rebuild as the later one; give one a different class name",
                type_name,
                source_class.__module__,
                source_class.__qualname__,
                previous.__module__,
                previous.__qualname__,
                extra={"event": "source_type_replaced", "source_type": type_name},
            )
    source_types[type_name] = source_class
