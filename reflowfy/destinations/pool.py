"""Connections shared by destinations: one Kafka producer per cluster, one HTTP client per host.

Two scopes use the same :class:`ConnectionPool`:

- **The event loop's pool** (:func:`loop_pool`). A worker opens one for its whole life
  (:func:`loop_pool_scope` around the consume loop), so every job, execution and
  pipeline that writes to the same cluster with the same credentials reuses one
  producer, and every one that calls the same host with the same client settings one
  HTTP client. Different credentials or settings are a different ``_connection_key()``
  and never share. In local mode the manager's dispatcher opens one per execution (the
  loop that drives it), and ``LocalExecutor`` / ``reflowfy test`` one per job send: a
  Kafka producer belongs to the loop that started it, so a pool never outlives its loop.
- **A job's pool**, which a ``@record_destination`` gives its routed destinations. It
  carries the job's request budget and takes its connections from the loop's pool when
  one is open (else keeps its own, closed with the job).

A destination never closes a pooled connection: ``close()`` only releases it. The pool
closes connections idle for ``DESTINATION_POOL_IDLE_SECONDS`` (default 300), all of them
when its scope ends (the worker drains), and evicts one a destination saw fail
(:meth:`ConnectionPool.evict`), so the next send builds a fresh one. An evicted
connection still in use by another destination is closed once that one releases it.

Health checks are cached per ``health_key()`` for :data:`HEALTH_CACHE_SECONDS`: one probe
per host or cluster per worker, not one per job.
"""

import asyncio
import contextlib
import hashlib
import os
import time
import weakref
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    cast,
)

from reflowfy.observability.logging import get_logger

logger = get_logger("destinations.pool")

T = TypeVar("T")

DEFAULT_IDLE_SECONDS = 300.0
"""How long an unused pooled connection stays open (``DESTINATION_POOL_IDLE_SECONDS``)."""

HEALTH_CACHE_SECONDS = 30.0
"""How long a healthy check result is reused for the same ``health_key()``."""


def idle_seconds_from_env() -> float:
    """``DESTINATION_POOL_IDLE_SECONDS``, else :data:`DEFAULT_IDLE_SECONDS`."""
    raw = os.getenv("DESTINATION_POOL_IDLE_SECONDS")
    if not raw:
        return DEFAULT_IDLE_SECONDS
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "DESTINATION_POOL_IDLE_SECONDS=%r is not a number; using %.0f",
            raw,
            DEFAULT_IDLE_SECONDS,
            extra={"event": "pool_config_invalid"},
        )
        return DEFAULT_IDLE_SECONDS
    return max(value, 0.0)


def describe_key(key: str) -> str:
    """A loggable name for a connection key: its kind and a short hash.

    Never the key itself — a Kafka key holds the SASL password.
    """
    kind = key.split(":", 1)[0]
    return f"{kind}#{hashlib.sha256(key.encode('utf-8')).hexdigest()[:8]}"


class _Entry:
    """One pooled connection and who is using it."""

    __slots__ = ("key", "item", "closer", "holders", "last_used", "retired")

    def __init__(self, key: str, item: Any, closer: Callable[[], Awaitable[None]]) -> None:
        self.key = key
        self.item = item
        self.closer = closer
        self.holders: Set[int] = set()
        self.last_used = time.monotonic()
        # Evicted while in use: closed once its last holder releases it.
        self.retired = False


class ConnectionPool:
    """Connections keyed by a destination's ``_connection_key()``, shared by destinations.

    ``request_limit`` is a job's budget of requests in flight across all its
    destinations (``@record_destination``); a loop's pool has none.
    """

    def __init__(
        self,
        request_limit: Optional[int] = None,
        idle_seconds: Optional[float] = None,
        name: str = "job",
    ) -> None:
        self.request_limit = request_limit
        self.idle_seconds = idle_seconds
        self.name = name
        self._entries: Dict[str, _Entry] = {}
        self._held: Dict[int, List[_Entry]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._requests: Optional[asyncio.Semaphore] = None
        self._health: Dict[str, float] = {}
        self._health_locks: Dict[str, asyncio.Lock] = {}
        # Evicted while held: closed on their last release, or with the pool.
        self._retired: Set[_Entry] = set()
        # The loop's pool this (job) pool takes its connections from, once it asked.
        self._parent: Optional["ConnectionPool"] = None

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

    def _delegate(self) -> Optional["ConnectionPool"]:
        """The loop's pool, when this is a job's pool and the loop has one."""
        if self._parent is not None:
            return self._parent
        parent = loop_pool()
        if parent is not None and parent is not self:
            self._parent = parent
        return self._parent

    async def get(
        self,
        key: str,
        create: Callable[[], Awaitable[T]],
        close: Callable[[T], Awaitable[None]],
        holder: Optional[object] = None,
    ) -> T:
        """The connection for ``key``, created by ``create()`` the first time it is asked for.

        ``holder`` (a destination) marks it in use until :meth:`release`: idle eviction
        skips it, and an eviction closes it only once released.
        """
        parent = self._delegate()
        if parent is not None:
            # This job's destinations hold it through the job's pool, released with it.
            return await parent.get(key, create, close, holder=self)
        await self.close_idle()
        entry = self._entries.get(key)
        created = False
        if entry is None:
            lock = self._locks.setdefault(key, asyncio.Lock())
            async with lock:  # destinations are sent concurrently: create it once
                entry = self._entries.get(key)
                if entry is None:
                    item = await create()
                    entry = _Entry(key, item, lambda: close(item))
                    self._entries[key] = entry
                    created = True
                    if self.idle_seconds is not None:  # a long-lived pool: worth saying
                        logger.info(
                            "Opened pooled connection %s",
                            describe_key(key),
                            extra={"event": "pool_connection_created", "pool": self.name},
                        )
        entry.last_used = time.monotonic()
        if holder is not None and id(holder) not in entry.holders:
            if not created and self.idle_seconds is not None:
                # Once per destination (or job) that joins it, not once per request.
                logger.debug(
                    "Reusing pooled connection %s",
                    describe_key(key),
                    extra={"event": "pool_connection_reused", "pool": self.name},
                )
            entry.holders.add(id(holder))
            self._held.setdefault(id(holder), []).append(entry)
        return cast(T, entry.item)

    async def release(self, holder: object) -> None:
        """``holder`` no longer uses its connections. Closes any evicted meanwhile."""
        entries = self._held.pop(id(holder), [])
        now = time.monotonic()
        for entry in entries:
            entry.holders.discard(id(holder))
            entry.last_used = now
            if entry.retired and not entry.holders:
                self._retired.discard(entry)
                await self._close_entry(entry)

    async def evict(self, key: str, item: Any) -> None:
        """Drop ``item`` (it failed): the next ``get(key)`` builds a new connection.

        A no-op when ``key`` already maps to another connection (a concurrent send
        evicted it first). Closed now if nobody holds it, else on its last release.
        """
        parent = self._delegate()
        if parent is not None:
            await parent.evict(key, item)
            return
        entry = self._entries.get(key)
        if entry is None or entry.item is not item:
            return
        del self._entries[key]
        self._health.pop(key, None)
        logger.info(
            "Evicting pooled connection %s after an error",
            describe_key(key),
            extra={"event": "pool_connection_evicted", "pool": self.name, "reason": "error"},
        )
        if entry.holders:
            entry.retired = True
            self._retired.add(entry)
        else:
            await self._close_entry(entry)

    def _take_idle(self) -> List[_Entry]:
        """Unregister the connections unused for ``idle_seconds``; returns them, to close."""
        if self.idle_seconds is None:
            return []
        now = time.monotonic()
        cutoff = now - self.idle_seconds
        idle = [e for e in self._entries.values() if not e.holders and e.last_used <= cutoff]
        for entry in idle:
            del self._entries[entry.key]
            self._health.pop(entry.key, None)
            logger.info(
                "Closing pooled connection %s, idle for %.0fs",
                describe_key(entry.key),
                now - entry.last_used,
                extra={"event": "pool_connection_evicted", "pool": self.name, "reason": "idle"},
            )
        return idle

    async def close_idle(self) -> int:
        """Close every connection idle for ``idle_seconds``; returns how many."""
        idle = self._take_idle()
        for entry in idle:
            await self._close_entry(entry)
        return len(idle)

    async def _close_entry(self, entry: _Entry) -> None:
        try:
            await entry.closer()
        except Exception as exc:
            logger.warning(
                "Could not close pooled connection %s: %s",
                describe_key(entry.key),
                type(exc).__name__,
                extra={"event": "pool_connection_close_error", "pool": self.name},
            )

    async def cached_health(self, key: str, probe: Callable[[], Awaitable[bool]]) -> bool:
        """``probe()``, or its healthy result from the last :data:`HEALTH_CACHE_SECONDS`.

        Only a healthy result is reused: an unhealthy one is probed again by the next
        job, so a recovered host is noticed at once. Concurrent checks of one key wait
        for one probe.
        """
        parent = self._delegate()
        if parent is not None:
            return await parent.cached_health(key, probe)
        if self._fresh(key):
            return True
        async with self._health_locks.setdefault(key, asyncio.Lock()):
            if self._fresh(key):
                return True
            healthy = await probe()
            if healthy:
                self._health[key] = time.monotonic()
            else:
                self._health.pop(key, None)
            return healthy

    def _fresh(self, key: str) -> bool:
        checked = self._health.get(key)
        return checked is not None and time.monotonic() - checked < HEALTH_CACHE_SECONDS

    def __len__(self) -> int:
        return len(self._entries)

    async def close(self) -> None:
        """Close every connection this pool owns; the first error is raised after all were tried.

        A job's pool that took its connections from the loop's pool only releases them.
        """
        if self._parent is not None:
            await self._parent.release(self)
        entries = list(self._entries.values()) + list(self._retired)
        self._entries.clear()
        self._retired.clear()
        self._held.clear()
        self._locks.clear()
        self._health.clear()
        errors = await asyncio.gather(*(e.closer() for e in entries), return_exceptions=True)
        first = next((e for e in errors if isinstance(e, BaseException)), None)
        if first is not None:
            raise first


# ---------------------------------------------------------------- the loop's pool

_loop_pools: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Tuple[ConnectionPool, Optional[asyncio.Task[None]]]]" = (  # noqa: E501
    weakref.WeakKeyDictionary()
)


def loop_pool() -> Optional[ConnectionPool]:
    """The pool opened for the running event loop, or None (outside a loop, or none open)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    found = _loop_pools.get(loop)
    return found[0] if found is not None else None


def open_loop_pool(idle_seconds: Optional[float] = None) -> ConnectionPool:
    """The running loop's pool, opened now if it has none. Close it with :func:`close_loop_pool`."""
    loop = asyncio.get_running_loop()
    found = _loop_pools.get(loop)
    if found is not None:
        return found[0]
    idle = idle_seconds if idle_seconds is not None else idle_seconds_from_env()
    pool = ConnectionPool(idle_seconds=idle, name="loop")
    sweeper: Optional["asyncio.Task[None]"] = None
    if idle > 0:
        sweeper = loop.create_task(_sweep_forever(pool, idle))
    _loop_pools[loop] = (pool, sweeper)
    logger.debug(
        "Opened destination connection pool (idle %.0fs)",
        idle,
        extra={"event": "pool_opened"},
    )
    return pool


async def close_loop_pool() -> None:
    """Close the running loop's pool, if it has one: every pooled connection is closed."""
    loop = asyncio.get_running_loop()
    found = _loop_pools.pop(loop, None)
    if found is None:
        return
    pool, sweeper = found
    if sweeper is not None:
        sweeper.cancel()
        await asyncio.wait({sweeper})
    count = len(pool)
    try:
        await pool.close()
    finally:
        if count:
            logger.info(
                "Closed %d pooled destination connections",
                count,
                extra={"event": "pool_closed", "connections": count},
            )


@contextlib.asynccontextmanager
async def loop_pool_scope(
    idle_seconds: Optional[float] = None,
) -> AsyncGenerator[ConnectionPool, None]:
    """Destinations sent inside share the loop's pool; closed at exit if this opened it."""
    owned = loop_pool() is None
    pool = open_loop_pool(idle_seconds)
    try:
        yield pool
    finally:
        if owned:
            await close_loop_pool()


async def _sweep_forever(pool: ConnectionPool, idle_seconds: float) -> None:
    """Close idle connections even while no job asks for one (an idle worker)."""
    interval = min(max(idle_seconds / 2, 1.0), 30.0)
    while True:
        await asyncio.sleep(interval)
        await pool.close_idle()
