"""The event loop's connection pool: one producer per cluster and one HTTP client per host
for every job a worker (or a local execution) runs.

A destination releases, never closes, a pooled connection; the pool closes it when idle
for DESTINATION_POOL_IDLE_SECONDS, when a destination saw it fail (evict and rebuild),
and when its scope ends (the worker drains). Different credentials never share.
"""

import asyncio
from typing import Any, Dict, List

import httpx
import pytest

import reflowfy.destinations.pool as pool_mod
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.base import DestinationError
from reflowfy.destinations.decorators import record_destination
from reflowfy.destinations.kafka import KafkaDestination, kafka_destination
from reflowfy.destinations.pool import (
    DEFAULT_IDLE_SECONDS,
    ConnectionPool,
    close_loop_pool,
    describe_key,
    idle_seconds_from_env,
    loop_pool,
    loop_pool_scope,
    open_loop_pool,
)
from test_destination_pool import FakeProducer, producers  # type: ignore[import-not-found]  # noqa: F401
from test_worker_executor_paths import capture_logs  # type: ignore[import-not-found]

POOL_LOGGER = pool_mod.logger.name


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> List[float]:
    now = [1000.0]
    monkeypatch.setattr(pool_mod.time, "monotonic", lambda: now[0])
    return now


async def _make(value: Any) -> Any:
    return value


def _closer(closed: List[Any]) -> Any:
    async def close(item: Any) -> None:
        closed.append(item)

    return close


# ---------------------------------------------------------------- scope


class TestScope:
    def test_no_pool_outside_a_loop(self):
        assert loop_pool() is None

    async def test_no_pool_until_one_is_opened(self):
        assert loop_pool() is None
        async with loop_pool_scope() as pool:
            assert loop_pool() is pool
            async with loop_pool_scope() as inner:  # nested: the same one, left open
                assert inner is pool
            assert loop_pool() is pool
        assert loop_pool() is None

    async def test_open_is_idempotent_and_close_without_one_is_a_noop(self):
        await close_loop_pool()
        pool = open_loop_pool(idle_seconds=0)  # no sweeper for 0
        assert open_loop_pool() is pool
        await close_loop_pool()
        assert loop_pool() is None

    async def test_each_loop_has_its_own(self):
        async with loop_pool_scope() as here:

            def other() -> Any:
                async def run() -> Any:
                    async with loop_pool_scope() as there:
                        return there

                return asyncio.run(run())

            there = await asyncio.to_thread(other)
            assert there is not here

    def test_idle_seconds_from_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("DESTINATION_POOL_IDLE_SECONDS", raising=False)
        assert idle_seconds_from_env() == DEFAULT_IDLE_SECONDS
        monkeypatch.setenv("DESTINATION_POOL_IDLE_SECONDS", "60")
        assert idle_seconds_from_env() == 60.0
        monkeypatch.setenv("DESTINATION_POOL_IDLE_SECONDS", "-5")
        assert idle_seconds_from_env() == 0.0
        monkeypatch.setenv("DESTINATION_POOL_IDLE_SECONDS", "soon")
        with capture_logs(POOL_LOGGER) as logs:
            assert idle_seconds_from_env() == DEFAULT_IDLE_SECONDS
        assert any("is not a number" in m for m in logs)

    def test_a_key_is_logged_without_its_credentials(self):
        key = KafkaDestination(
            "k:9092", "t", sasl_username="u", sasl_password="hunter2"
        )._connection_key()
        assert "hunter2" in key
        described = describe_key(key)
        assert described.startswith("kafka#") and "hunter2" not in described


# ---------------------------------------------------------------- sharing across jobs


class TestSharing:
    async def test_jobs_on_one_cluster_share_one_producer(self, producers):  # noqa: F811
        async with loop_pool_scope():
            for job in range(3):  # three jobs, two topics, one cluster
                dest = kafka_destination(bootstrap_servers="k:9092", topic=f"t{job % 2}")
                assert await dest.health_check()
                await dest.send([{"job": job}])
                await dest.close()
                assert producers[0].stopped == 0  # released, not closed
            assert len(producers) == 1 and len(producers[0].sent) == 3
        assert producers[0].stopped == 1  # the scope closed it

    async def test_different_credentials_never_share(self, producers):  # noqa: F811
        async with loop_pool_scope() as pool:
            for user in ("alice", "bob", "alice"):
                dest = kafka_destination(
                    bootstrap_servers="k:9092", topic="t", sasl_username=user, sasl_password="p"
                )
                await dest.send([{"u": user}])
                await dest.close()
            assert len(producers) == 2 and len(pool) == 2

    async def test_without_a_pool_a_destination_owns_its_producer(self, producers):  # noqa: F811
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t")
        await dest.send([{"a": 1}])
        await dest.close()
        assert producers[0].stopped == 1

    async def test_api_destinations_share_one_client_per_host(self, monkeypatch):
        built: List[httpx.AsyncClient] = []
        real = httpx.AsyncClient
        transport = httpx.MockTransport(lambda r: httpx.Response(200))

        def client(**kw: Any) -> httpx.AsyncClient:
            built.append(real(transport=transport, **kw))
            return built[-1]

        monkeypatch.setattr(httpx, "AsyncClient", client)
        async with loop_pool_scope():
            for path in ("/a", "/b"):
                dest = api_destination(url=f"https://sink.test{path}", body={})
                await dest.send([])
                await dest.close()
                assert not built[0].is_closed
            other = api_destination(url="https://other.test/x", body={})
            await other.send([])
            await other.close()
            assert len(built) == 2
        assert all(c.is_closed for c in built)

    async def test_a_record_destination_takes_its_connections_from_the_loop(
        self, producers  # noqa: F811
    ):
        @record_destination("lp_by_topic")
        def by_topic(record: Any, params: Any) -> KafkaDestination:
            return kafka_destination(bootstrap_servers="k:9092", topic=record["t"])

        async with loop_pool_scope() as pool:
            for _ in range(2):  # two jobs
                dest = by_topic()
                routed = await dest.route([{"t": "a"}, {"t": "b"}], {})
                assert await dest.health_check()
                await dest.send_with_retry(routed.records, {})
                await dest.close()
                assert producers[0].stopped == 0
            assert len(producers) == 1 and len(pool) == 1
        assert producers[0].stopped == 1

    async def test_create_reuse_and_close_are_logged(self, producers):  # noqa: F811
        with capture_logs(POOL_LOGGER) as logs:
            async with loop_pool_scope():
                for _ in range(2):
                    dest = kafka_destination(bootstrap_servers="k:9092", topic="t")
                    await dest.send([{"a": 1}])
                    await dest.close()
        assert sum("Opened pooled connection kafka#" in m for m in logs) == 1
        assert sum("Reusing pooled connection kafka#" in m for m in logs) == 1
        assert "Closed 1 pooled destination connections" in logs


# ---------------------------------------------------------------- eviction


class TestEviction:
    async def test_idle_connections_are_closed(self, clock):
        pool = ConnectionPool(idle_seconds=60, name="loop")
        closed: List[Any] = []
        holder = object()
        await pool.get("a", lambda: _make("A"), _closer(closed), holder=holder)
        await pool.get("b", lambda: _make("B"), _closer(closed))
        clock[0] += 61
        assert await pool.close_idle() == 1  # "a" is still held
        assert closed == ["B"] and len(pool) == 1
        await pool.release(holder)
        assert await pool.close_idle() == 0  # released just now: not idle yet
        clock[0] += 61
        with capture_logs(POOL_LOGGER) as logs:
            # A get sweeps too: "a" goes, "c" is new.
            await pool.get("c", lambda: _make("C"), _closer(closed))
        assert closed == ["B", "A"]
        assert any("idle for 61s" in m for m in logs)

    async def test_a_job_pool_never_evicts_for_idleness(self, clock):
        pool = ConnectionPool()
        await pool.get("a", lambda: _make("A"), _closer([]))
        clock[0] += 10**6
        assert await pool.close_idle() == 0 and len(pool) == 1

    async def test_evict_rebuilds_and_closes_once_released(self):
        pool = ConnectionPool(idle_seconds=60)
        closed: List[Any] = []
        holder = object()
        first = await pool.get("k", lambda: _make(["v1"]), _closer(closed), holder=holder)
        await pool.evict("k", ["other"])  # not the current one: ignored
        await pool.evict("k", first)
        assert closed == []  # still held
        second = await pool.get("k", lambda: _make(["v2"]), _closer(closed))
        assert second == ["v2"]
        await pool.evict("k", first)  # already gone: ignored
        await pool.release(holder)
        assert closed == [["v1"]]

    async def test_an_unheld_evicted_connection_closes_at_once(self):
        pool = ConnectionPool()
        closed: List[Any] = []
        item = await pool.get("k", lambda: _make("v"), _closer(closed))
        await pool.evict("k", item)
        assert closed == ["v"] and len(pool) == 0

    async def test_a_job_pool_evicts_from_the_loop_pool(self):
        closed: List[Any] = []
        async with loop_pool_scope() as shared:
            job_pool = ConnectionPool(request_limit=2)
            item = await job_pool.get("k", lambda: _make("v"), _closer(closed))
            assert len(shared) == 1 and len(job_pool) == 0
            await job_pool.evict("k", item)
            assert len(shared) == 0 and closed == []  # the job still holds it
            await job_pool.close()
            assert closed == ["v"]

    async def test_closing_the_pool_closes_evicted_connections_still_held(self):
        pool = ConnectionPool()
        closed: List[Any] = []
        item = await pool.get("k", lambda: _make("v"), _closer(closed), holder=object())
        await pool.evict("k", item)
        await pool.close()
        assert closed == ["v"]

    async def test_a_close_error_is_logged_not_raised(self):
        pool = ConnectionPool()

        async def broken(item: Any) -> None:
            raise RuntimeError("nope")

        item = await pool.get("kafka:x", lambda: _make("v"), broken)
        with capture_logs(POOL_LOGGER) as logs:
            await pool.evict("kafka:x", item)
        assert any("Could not close pooled connection kafka#" in m for m in logs)

    async def test_a_kafka_send_error_evicts_the_producer(self, producers):  # noqa: F811
        from aiokafka.errors import KafkaConnectionError

        async with loop_pool_scope():
            dest = kafka_destination(bootstrap_servers="k:9092", topic="t")
            await dest.send([{"a": 1}])
            broken = producers[0]

            async def fail(*args: Any, **kwargs: Any) -> Any:
                raise KafkaConnectionError("gone")

            broken.send = fail  # type: ignore[method-assign]
            with pytest.raises(DestinationError):
                await dest.send([{"a": 2}])
            assert broken.stopped == 0  # this destination still holds it
            await dest.send([{"a": 3}])  # a fresh producer
            assert len(producers) == 2 and producers[1].sent[0]["value"] == {"a": 3}
            await dest.close()
            assert broken.stopped == 1

    async def test_a_failed_ack_evicts_the_producer(self, producers):  # noqa: F811
        async with loop_pool_scope():
            dest = kafka_destination(bootstrap_servers="k:9092", topic="t")
            await dest.send([{"a": 1}])

            async def nack(topic: str, value: bytes, key: Any = None, headers: Any = None):
                future: "asyncio.Future[None]" = asyncio.get_running_loop().create_future()
                future.set_exception(RuntimeError("no ack"))
                return future

            producers[0].send = nack  # type: ignore[method-assign]
            with pytest.raises(DestinationError, match="no ack"):
                await dest.send([{"a": 2}])
            await dest.send([{"a": 3}])
            assert len(producers) == 2
            await dest.close()

    async def test_an_api_transport_error_evicts_the_client(self, monkeypatch):
        built: List[httpx.AsyncClient] = []
        real = httpx.AsyncClient
        outcomes = iter(["fail", "ok"])

        def handler(request: httpx.Request) -> httpx.Response:
            if next(outcomes) == "fail":
                raise httpx.ConnectError("reset", request=request)
            return httpx.Response(200)

        def client(**kw: Any) -> httpx.AsyncClient:
            built.append(real(transport=httpx.MockTransport(handler), **kw))
            return built[-1]

        monkeypatch.setattr(httpx, "AsyncClient", client)
        async with loop_pool_scope():
            dest = api_destination(url="https://sink.test/in", body={})
            with pytest.raises(DestinationError, match="Request failed"):
                await dest.send([])
            await dest.send([])
            assert len(built) == 2
            await dest.close()
            assert built[0].is_closed

    async def test_the_sweeper_closes_idle_connections_while_nothing_asks(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        calls: List[float] = []
        pool = ConnectionPool(idle_seconds=4)

        async def close_idle() -> int:
            calls.append(1)
            raise asyncio.CancelledError  # stop after one round

        real_sleep = asyncio.sleep

        async def no_wait(seconds: float) -> None:
            calls.append(seconds)
            await real_sleep(0)

        monkeypatch.setattr(pool, "close_idle", close_idle)
        monkeypatch.setattr(pool_mod.asyncio, "sleep", no_wait)
        with pytest.raises(asyncio.CancelledError):
            await pool_mod._sweep_forever(pool, 4)  # pyright: ignore[reportPrivateUsage]
        assert calls == [2.0, 1]

    async def test_open_starts_and_close_stops_the_sweeper(self):
        open_loop_pool(idle_seconds=120)
        sweepers: List[Any] = [
            t for t in asyncio.all_tasks() if "_sweep_forever" in repr(t.get_coro())
        ]
        assert len(sweepers) == 1
        await close_loop_pool()
        assert sweepers[0].done()


# ---------------------------------------------------------------- health cache


class TestHealthCache:
    async def test_concurrent_checks_share_one_probe(self):
        pool = ConnectionPool()
        probes: List[int] = []

        async def probe() -> bool:
            probes.append(1)
            await asyncio.sleep(0.01)
            return True

        results = await asyncio.gather(*(pool.cached_health("k", probe) for _ in range(5)))
        assert results == [True] * 5 and probes == [1]

    async def test_kafka_health_is_cached_per_cluster(self, producers):  # noqa: F811
        probes: List[str] = []
        real = KafkaDestination._probe  # pyright: ignore[reportPrivateUsage]

        async def counting(self: KafkaDestination) -> bool:
            probes.append(self.config["topic"])
            return await real(self)

        KafkaDestination._probe = counting  # type: ignore[method-assign]
        try:
            async with loop_pool_scope():
                for topic in ("a", "b"):
                    assert await kafka_destination(
                        bootstrap_servers="k:9092", topic=topic
                    ).health_check()
        finally:
            KafkaDestination._probe = real  # type: ignore[method-assign]
        assert probes == ["a"]

    async def test_a_job_pool_delegates_to_the_loop_pool(self):
        probes: List[int] = []

        async def probe() -> bool:
            probes.append(1)
            return True

        async with loop_pool_scope():
            for _ in range(2):
                job_pool = ConnectionPool(request_limit=4)
                assert await job_pool.cached_health("k", probe)
                await job_pool.close()
        assert probes == [1]


# ---------------------------------------------------------------- wiring


class TestWiring:
    async def test_the_worker_closes_the_pool_when_it_drains(self, monkeypatch):
        import reflowfy.worker.consumer as consumer_mod

        events: List[str] = []
        monkeypatch.setattr(consumer_mod, "open_loop_pool", lambda: events.append("open"))

        async def close() -> None:
            events.append("close")

        monkeypatch.setattr(consumer_mod, "close_loop_pool", close)

        class FakeConsumer:
            def __init__(self, *a: Any, **k: Any) -> None: ...

            async def start(self) -> None:
                events.append("start")

            async def stop(self) -> None:
                events.append("stop")

        monkeypatch.setattr(consumer_mod, "AIOKafkaConsumer", FakeConsumer)
        worker = consumer_mod.KafkaJobConsumer("k:9092", "t", database_url="postgresql://x/y")

        async def consume() -> None:
            events.append("consume")

        async def executor_close() -> None:
            events.append("executor_close")

        monkeypatch.setattr(worker, "_consume_until_stopped", consume)
        monkeypatch.setattr(worker.executor, "close", executor_close)
        await worker.start()
        assert events == ["start", "open", "consume", "stop", "executor_close", "close"]

    async def test_the_local_dispatcher_pools_per_execution_loop(self, monkeypatch):
        from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher

        seen: List[Any] = []

        class Exec:
            async def execute_job(self, job: Dict[str, Any]) -> bool:
                seen.append(loop_pool())
                return True

            async def close(self) -> None: ...

        d = LocalDispatcher(rate_limiter=None)  # type: ignore[arg-type]
        monkeypatch.setattr(d, "_create_executor", Exec)
        await d.dispatch_jobs_batch([{"job_id": "a"}], "p")
        await d.dispatch_jobs_batch([{"job_id": "b"}], "p")
        assert seen[0] is not None and seen[0] is seen[1]  # one pool across batches
        await d.close()
        assert loop_pool() is None
