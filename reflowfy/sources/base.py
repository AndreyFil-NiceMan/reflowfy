"""Base source interface for data fetching and job splitting."""

import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Iterator, List, Optional, Tuple, TypeVar

from reflowfy.core.execution_context import parameter_resolver
from reflowfy.core.types import Records
from reflowfy.core.exceptions import SourceError as SourceError  # its old home

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
        from reflowfy.factories.source_factory import SourceFactory  # import cycle

        SourceFactory.register(cls.__name__, cls)

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
    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Records:
        """
        Fetch data from source (used in local mode).

        Args:
            runtime_params: Runtime parameters for template resolution
            limit: Optional limit for testing (e.g., first 100 records)

        Returns:
            List of records
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


