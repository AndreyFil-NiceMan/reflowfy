"""Base destination interface with retry logic."""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar, cast
from tenacity import (
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


T = TypeVar("T")


class ConnectionPool:
    """Connections shared by several destinations of one job.

    A ``@record_destination`` gives one to all its routed destinations, so those that
    reach the same cluster or host share a Kafka producer or an HTTP client instead
    of each opening its own (a client costs ~17 ms of CPU, a producer a connection
    and a metadata round trip). The pool, not the destination, closes them.
    """

    def __init__(self, request_limit: Optional[int] = None) -> None:
        self._items: Dict[str, Any] = {}
        self._closers: Dict[str, Callable[[], Awaitable[None]]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self.request_limit = request_limit
        self._requests: Optional[asyncio.Semaphore] = None

    @property
    def requests(self) -> Optional[asyncio.Semaphore]:
        """One budget of requests in flight for every destination of the job (or None).

        Created on first use, inside the loop that sends.
        """
        if self.request_limit is None:
            return None
        if self._requests is None:
            self._requests = asyncio.Semaphore(self.request_limit)
        return self._requests

    async def get(
        self,
        key: str,
        create: Callable[[], Awaitable[T]],
        close: Callable[[T], Awaitable[None]],
    ) -> T:
        """The connection for ``key``, created by ``create()`` the first time it is asked for."""
        if key in self._items:
            return cast(T, self._items[key])
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:  # destinations are sent concurrently: create it once
            if key not in self._items:
                item = await create()
                self._items[key] = item
                self._closers[key] = lambda: close(item)
        return cast(T, self._items[key])

    def __len__(self) -> int:
        return len(self._items)

    async def close(self) -> None:
        """Close every connection; the first error is raised after all were tried."""
        closers = list(self._closers.values())
        self._items.clear()
        self._closers.clear()
        self._locks.clear()
        errors = await asyncio.gather(*(c() for c in closers), return_exceptions=True)
        first = next((e for e in errors if isinstance(e, BaseException)), None)
        if first is not None:
            raise first


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
    4. Metrics reporting
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

        Args:
            records: Records to send
            metadata: Optional metadata
        """
        from tenacity import AsyncRetrying

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.retry_config.max_attempts),
            wait=wait_exponential(
                multiplier=self.retry_config.multiplier,
                min=self.retry_config.min_wait_seconds,
                max=self.retry_config.max_wait_seconds,
            ),
            retry=retry_if_exception_type(DestinationError),
            reraise=True,
        ):
            with attempt:
                await self.send(records, metadata)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(config={self.config})"


def check_concurrency(destination_type: str, concurrency: object) -> int:
    """Validate a destination's ``concurrency`` (requests/messages in flight per job).

    Takes ``object``: it is a runtime check for what a type checker never saw (a
    value from config or an env var).
    """
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError(f"{destination_type}: concurrency must be an integer >= 1")
    return concurrency


class DestinationError(Exception):
    """Raised when a destination operation fails."""

    def __init__(self, destination_type: str, message: str, original_error: Optional[Exception] = None):
        self.destination_type = destination_type
        self.original_error = original_error
        super().__init__(f"Destination '{destination_type}' error: {message}")
