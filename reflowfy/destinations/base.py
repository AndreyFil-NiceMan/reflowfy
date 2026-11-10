"""Base destination interface with retry logic."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar
from tenacity import (
    RetryCallState,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from reflowfy.core.secrets import redact_config
from reflowfy.destinations.pool import ConnectionPool, loop_pool
from reflowfy.core.exceptions import (  # their old home
    DestinationError as DestinationError,
    PermanentDestinationError as PermanentDestinationError,
)

__all__ = [
    "BaseDestination",
    "ConnectionPool",
    "DestinationError",
    "PermanentDestinationError",
    "RetryConfig",
    "check_concurrency",
    "is_retryable",
]

T = TypeVar("T")


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    min_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    multiplier: float = 2.0


class BaseDestination(ABC):
    """
    Base class for all destinations.

    Destinations are responsible for:
    1. Sending transformed data
    2. Health checks
    3. Retry logic with exponential backoff
    """

    def __init__(self, config: Dict[str, Any], retry_config: Optional[RetryConfig] = None):
        """
        Initialize destination with configuration.

        Args:
            config: Destination-specific configuration
            retry_config: Optional retry configuration
        """
        self.config = config
        self.retry_config = retry_config or RetryConfig()
        # What identifies this destination for content dedup; None = the whole config.
        self.dedup_identity: Optional[Any] = None
        # Set by a @record_destination: connections shared with the job's other
        # destinations. None: this destination opens and closes its own.
        self.connection_pool: Optional[ConnectionPool] = None
        # Set by a @record_destination: requests in flight at once, for destinations
        # that send one request per record. None: the destination's default.
        self.send_concurrency: Optional[int] = None
        # The pool this destination took a connection from, released by close().
        self._used_pool: Optional[ConnectionPool] = None

    def _active_pool(self) -> Optional[ConnectionPool]:
        """Where this destination's connections come from: its job's pool, else the loop's.

        None outside any pool scope: the destination opens and closes its own.
        """
        return self.connection_pool if self.connection_pool is not None else loop_pool()

    async def _pooled(
        self,
        pool: ConnectionPool,
        key: str,
        create: Callable[[], Awaitable[T]],
        close: Callable[[T], Awaitable[None]],
    ) -> T:
        """The pooled connection for ``key``, held until :meth:`_release_pooled`."""
        self._used_pool = pool
        return await pool.get(key, create, close, holder=self)

    async def _release_pooled(self) -> None:
        """Release (never close) the pooled connections this destination used."""
        pool, self._used_pool = self._used_pool, None
        if pool is not None:
            await pool.release(self)

    async def _cached_health(self, probe: Callable[[], Awaitable[bool]]) -> bool:
        """``probe()``, reused for ``HEALTH_CACHE_SECONDS`` per ``health_key()`` when pooled."""
        pool = self._active_pool()
        key = self.health_key()
        if pool is None or key is None:
            return await probe()
        return await pool.cached_health(key, probe)

    @abstractmethod
    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to destination.

        Args:
            records: List of records to send
            metadata: Optional metadata (execution_id, batch_id, etc.)

        Raises:
            DestinationError: If send fails after retries
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if destination is healthy and accessible.

        Returns:
            True if healthy, False otherwise
        """
        pass

    def health_key(self) -> Optional[str]:
        """Destinations with the same key share one health check per job; None: its own.

        A ``@record_destination`` checks each distinct key once — one probe per host or
        cluster, not one per routed destination.
        """
        return None

    def identity_config(self) -> Dict[str, Any]:
        """The part of ``config`` that says where records are written, for content dedup.

        The whole config by default. A destination drops tuning knobs here (how
        fast, not where): they would make every record look like new work after a
        change, and adding one would change the hash of every existing destination.
        """
        return self.config

    async def close(self) -> None:
        """
        Release any resources held by the destination (connections, clients).

        Default is a no-op for stateless destinations. Connection-backed
        destinations (e.g. Kafka) override this to close their producer.
        Always safe and idempotent to call.
        """
        return None

    async def send_with_retry(
        self, records: List[Any], metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Send records with automatic retry and exponential backoff.

        A :class:`DestinationError` is retried, except a
        :class:`PermanentDestinationError` (retrying cannot fix it). A ``retry_after``
        on the error replaces the backoff, capped at ``max_wait_seconds``.

        Args:
            records: Records to send
            metadata: Optional metadata
        """
        from tenacity import AsyncRetrying

        config = self.retry_config
        backoff = wait_exponential(
            multiplier=config.multiplier,
            min=config.min_wait_seconds,
            max=config.max_wait_seconds,
        )

        def wait(state: RetryCallState) -> float:
            # A server that said when to come back (Retry-After) is believed, up to
            # max_wait_seconds; otherwise exponential backoff.
            error = state.outcome.exception() if state.outcome is not None else None
            retry_after = getattr(error, "retry_after", None)
            if isinstance(retry_after, (int, float)):
                return min(max(float(retry_after), 0.0), config.max_wait_seconds)
            return backoff(state)

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(config.max_attempts),
            wait=wait,
            retry=retry_if_exception(is_retryable),
            reraise=True,
        ):
            with attempt:
                await self.send(records, metadata)

    def __repr__(self) -> str:
        # Shown in logs and tracebacks: credentials (sasl_password, auth_token,
        # Authorization headers, URL passwords) are hidden.
        return f"{self.__class__.__name__}(config={redact_config(self.config)})"


def check_concurrency(destination_type: str, concurrency: object) -> int:
    """Validate a destination's ``concurrency`` (requests/messages in flight per job).

    Takes ``object``: it is a runtime check for what a type checker never saw (a
    value from config or an env var).
    """
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError(f"{destination_type}: concurrency must be an integer >= 1")
    return concurrency






def is_retryable(error: BaseException) -> bool:
    """What ``send_with_retry`` retries: a DestinationError that is not permanent."""
    return isinstance(error, DestinationError) and not isinstance(error, PermanentDestinationError)
