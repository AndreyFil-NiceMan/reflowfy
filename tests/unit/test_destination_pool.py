"""Shared connections for a job's destinations (ConnectionPool), as @record_destination
uses them: one Kafka producer per cluster, one HTTP client per host, one health check
per cluster or host, one request budget for the job — and per-record API headers.

Kafka is a fake producer; HTTP is a real httpx client on a MockTransport, so the
requests, headers and connections counted are the ones httpx would send.
"""

import asyncio
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List

import httpx
import pytest

import reflowfy.destinations.kafka as kafka_mod
import reflowfy.destinations.record as record_mod
from reflowfy import record_destination
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.base import ConnectionPool
from reflowfy.destinations.kafka import KafkaDestination, kafka_destination

# ---------------------------------------------------------------- fakes


class FakeProducer:
    instances: List["FakeProducer"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.started = 0
        self.stopped = 0
        self.sent: List[Dict[str, Any]] = []
        FakeProducer.instances.append(self)

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def send(self, topic: str, value: bytes, key: Any = None, headers: Any = None):
        self.sent.append({"topic": topic, "value": json.loads(value), "key": key})
        future: "asyncio.Future[None]" = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future


@pytest.fixture
def producers(monkeypatch):
    FakeProducer.instances = []
    monkeypatch.setattr(kafka_mod, "AIOKafkaProducer", FakeProducer)
    return FakeProducer.instances


class Http:
    """Every httpx.AsyncClient built is a real one on a MockTransport that records requests."""

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self.clients: List[httpx.AsyncClient] = []
        self.requests: List[httpx.Request] = []
        self.heads: List[str] = []
        self.now = 0
        self.peak = 0

    async def handle(self, request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            self.heads.append(str(request.url))
            return httpx.Response(200)
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        finally:
            self.now -= 1
        self.requests.append(request)
        return httpx.Response(200, json={})


@pytest.fixture
def http(monkeypatch):
    recorder = Http()
    real = httpx.AsyncClient

    def build(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        client = real(*args, transport=httpx.MockTransport(recorder.handle), **kwargs)
        recorder.clients.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", build)
    return recorder


@pytest.fixture(autouse=True)
def fresh_advice():
    record_mod._advised.clear()
    yield
    record_mod._advised.clear()


@contextmanager
def capture(name: str) -> Iterator[List[str]]:
    messages: List[str] = []

    class Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    target = logging.getLogger(name)
    handler = Handler(logging.DEBUG)
    old = target.level
    target.addHandler(handler)
    target.setLevel(logging.DEBUG)
    try:
        yield messages
    finally:
        target.removeHandler(handler)
        target.setLevel(old)


async def run_job(dest: Any, records: List[Any]) -> None:
    """What the worker does: route, health check, send, close."""
    routed = await dest.route(records, {})
    try:
        assert await dest.health_check()
        await dest.send_with_retry(routed.records, {})
    finally:
        await dest.close()


def _engines(n: int, *engines: str) -> List[Dict[str, Any]]:
    return [{"id": i, "engine": engines[i % len(engines)]} for i in range(n)]


# ---------------------------------------------------------------- kafka


class TestKafkaSharedProducer:
    async def test_topics_on_one_cluster_share_one_producer(self, producers):
        @record_destination("pool_topics")
        def route(record: Any, params: Any) -> KafkaDestination:
            return kafka_destination(bootstrap_servers="k:9092", topic=f"t.{record['engine']}")

        dest = route()
        await run_job(dest, _engines(30, "a", "b", "c"))

        assert len(dest.destinations) == 3  # three topics...
        assert len(producers) == 1  # ...one producer
        (producer,) = producers
        assert (producer.started, producer.stopped) == (1, 1)
        by_topic: Dict[str, List[int]] = {}
        for message in producer.sent:
            by_topic.setdefault(message["topic"], []).append(message["value"]["id"])
        assert by_topic == {
            "t.a": list(range(0, 30, 3)),
            "t.b": list(range(1, 30, 3)),
            "t.c": list(range(2, 30, 3)),
        }

    async def test_each_cluster_or_credential_gets_its_own(self, producers):
        @record_destination("pool_clusters")
        def route(record: Any, params: Any) -> KafkaDestination:
            cluster = {"a": "k1:9092", "b": "k2:9092", "c": "k2:9092"}[record["engine"]]
            user = "u2" if record["engine"] == "c" else "u1"
            return kafka_destination(
                bootstrap_servers=cluster, topic="t", sasl_username=user, sasl_password="p"
            )

        await run_job(route(), _engines(9, "a", "b", "c"))
        assert len(producers) == 3
        assert all(p.stopped == 1 for p in producers)

    async def test_one_health_check_per_cluster(self, producers, monkeypatch):
        checked: List[str] = []
        real = KafkaDestination.health_check

        async def counting(self: KafkaDestination) -> bool:
            checked.append(self.config["topic"])
            return await real(self)

        monkeypatch.setattr(KafkaDestination, "health_check", counting)

        @record_destination("pool_health_kafka")
        def route(record: Any, params: Any) -> KafkaDestination:
            return kafka_destination(bootstrap_servers="k:9092", topic=f"t.{record['engine']}")

        await run_job(route(), _engines(9, "a", "b", "c"))
        assert len(checked) == 1

    def test_health_key_is_per_topic_and_group_when_it_measures_lag(self):
        plain = [kafka_destination(bootstrap_servers="k:9092", topic=t) for t in ("a", "b")]
        assert plain[0].health_key() == plain[1].health_key()
        lag = [
            kafka_destination(
                bootstrap_servers="k:9092",
                topic=t,
                lag_health_check_enabled=True,
                consumer_group_id="g",
            )
            for t in ("a", "b")
        ]
        assert lag[0].health_key() != lag[1].health_key()

    def test_connection_key_names_an_ssl_context_by_identity(self):
        import ssl

        context = ssl.create_default_context()
        a = kafka_destination(
            bootstrap_servers="k:9093", topic="a", security_protocol="SSL", ssl_context=context
        )
        b = kafka_destination(
            bootstrap_servers="k:9093", topic="b", security_protocol="SSL", ssl_context=context
        )
        assert a._connection_key() == b._connection_key()
        assert f"SSLContext@{id(context)}" in a._connection_key()

    async def test_a_plain_destination_still_owns_its_producer(self, producers):
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t")
        await dest.send([{"a": 1}])
        await dest.close()
        assert len(producers) == 1 and producers[0].stopped == 1


# ---------------------------------------------------------------- http


class TestApiSharedClient:
    async def test_a_destination_per_record_still_shares_one_client_and_one_probe(self, http):
        @record_destination("pool_api_bad")
        def route(record: Any, params: Any) -> Any:
            # Per-record values in the config: a destination per record.
            return api_destination(url=f"https://api.example.com/u/{record['id']}", body=record)

        dest = route()
        await run_job(dest, _engines(30, "a"))

        assert len(dest.destinations) == 30
        assert len(http.clients) == 1
        assert len(http.requests) == 30
        assert len(http.heads) == 1
        assert http.clients[0].is_closed

    async def test_one_client_and_probe_per_host(self, http):
        @record_destination("pool_api_hosts")
        def route(record: Any, params: Any) -> Any:
            return api_destination(
                url=f"https://{record['engine']}.example.com/u/{{id}}", per_record=True
            )

        await run_job(route(), _engines(30, "eu", "us", "ap"))
        assert len(http.clients) == 3
        assert sorted(http.heads) == [
            "https://ap.example.com/u/",
            "https://eu.example.com/u/",
            "https://us.example.com/u/",
        ]
        assert sorted(str(r.url) for r in http.requests) == sorted(
            f"https://{e}.example.com/u/{i}" for i, e in enumerate(["eu", "us", "ap"] * 10)
        )
        assert all(c.is_closed for c in http.clients)

    async def test_different_credentials_on_one_host_do_not_share(self, http):
        @record_destination("pool_api_creds")
        def route(record: Any, params: Any) -> Any:
            return api_destination(
                url="https://api.example.com/u/{id}",
                per_record=True,
                auth_type="bearer",
                auth_token=f"token-{record['engine']}",
            )

        await run_job(route(), _engines(6, "a", "b"))
        assert len(http.clients) == 2
        tokens = {r.headers["Authorization"] for r in http.requests}
        assert tokens == {"Bearer token-a", "Bearer token-b"}

    async def test_the_budget_covers_every_destination_together(self, http):
        http.delay = 0.01

        @record_destination("pool_api_budget", concurrency=4)
        def route(record: Any, params: Any) -> Any:
            return api_destination(
                url=f"https://{record['engine']}.example.com/u/{{id}}", per_record=True
            )

        await run_job(route(), _engines(40, "a", "b", "c"))
        assert http.peak == 4  # not 4 per destination
        assert len(http.requests) == 40

    async def test_a_plain_destination_sends_16_at_once(self, http, monkeypatch):
        monkeypatch.delenv("TRANSFORM_CONCURRENCY", raising=False)
        http.delay = 0.01
        dest = api_destination(url="https://api.example.com/u/{id}", per_record=True)
        await dest.send([{"id": i} for i in range(40)])
        await dest.close()
        assert http.peak == 16


class TestPerRecordHeaders:
    async def test_template_headers_are_filled_per_request(self, http):
        dest = api_destination(
            url="https://api.example.com/u/{id}",
            per_record=True,
            headers={"X-Static": "s", "X-Tenant": "{tenant}", "X-Region": "{data.region}"},
        )
        await dest.send(
            [{"id": 1, "tenant": "acme", "data": {"region": "eu"}},
             {"id": 2, "tenant": "zeta", "data": {"region": "us"}}]
        )
        await dest.close()
        got = [(r.headers["X-Static"], r.headers["X-Tenant"], r.headers["X-Region"])
               for r in http.requests]
        assert got == [("s", "acme", "eu"), ("s", "zeta", "us")]
        # The static one rides on the client; the templated ones are per request.
        assert "X-Tenant" not in http.clients[0].headers
        assert http.clients[0].headers["X-Static"] == "s"

    async def test_a_function_sets_headers_and_wins_over_templates(self, http):
        dest = api_destination(
            url="https://api.example.com/u",
            per_record=True,
            headers=lambda r: {"X-Kind": r["kind"]} if r["kind"] != "none" else None,
        )
        await dest.send([{"kind": "a"}, {"kind": "none"}])
        await dest.close()
        assert http.requests[0].headers["X-Kind"] == "a"
        assert "X-Kind" not in http.requests[1].headers
        assert "headers" in dest.config and dest.config["headers"] == {}  # function off config

    async def test_missing_field_fails_before_any_request(self, http):
        dest = api_destination(
            url="https://api.example.com/u", per_record=True, headers={"X-T": "{tenant}"}
        )
        with pytest.raises(ValueError) as excinfo:
            await dest.send([{"tenant": "a"}, {"other": "secret-value"}])
        assert str(excinfo.value) == (
            "api_destination: header 'X-T' placeholder {tenant} has no matching field on "
            "record 2 of 2"
        )
        assert http.requests == []

    @pytest.mark.parametrize(
        "fn, match",
        [
            (lambda r: r["missing"], "headers function failed on record 1 of 1: KeyError"),
            (lambda r: [("a", "b")], "returned list for record 1 of 1"),
            (lambda r: {"X": 5}, "must map str to str"),
        ],
    )
    async def test_bad_functions_fail_before_any_request(self, http, fn, match):
        dest = api_destination(url="https://api.example.com/u", per_record=True, headers=fn)
        with pytest.raises(ValueError, match=match):
            await dest.send([{"a": 1}])
        assert http.requests == []

    def test_a_function_needs_per_record(self):
        with pytest.raises(ValueError, match="headers function needs per_record=True"):
            api_destination(url="https://api.example.com/u", headers=lambda r: {})

    async def test_without_per_record_a_template_header_is_sent_as_written(self, http):
        dest = api_destination(url="https://api.example.com/u", headers={"X-T": "{tenant}"})
        await dest.send([{"tenant": "a"}])
        await dest.close()
        assert http.requests[0].headers["X-T"] == "{tenant}"


# ---------------------------------------------------------------- advice


class TestDestinationPerRecordAdvice:
    async def test_warns_once_when_every_record_has_its_own_destination(self, http):
        @record_destination("advice_bad")
        def route(record: Any, params: Any) -> Any:
            return api_destination(url=f"https://api.example.com/u/{record['id']}", body=record)

        with capture("reflowfy.destinations.record") as logs:
            await route().route(_engines(12, "a"), {})
            await route().route(_engines(12, "a"), {})  # a second job: no repeat
        advice = [m for m in logs if "built a destination per record" in m]
        assert advice == [
            "advice_bad built a destination per record (12/12): put per-record values in "
            "templates — api_destination(url='.../{id}', per_record=True), "
            "headers={'X': '{field}'}, kafka_destination(key='{field}') — so records share "
            "a destination and values are encoded"
        ]

    async def test_quiet_for_small_jobs_and_grouped_records(self, http):
        @record_destination("advice_small")
        def small(record: Any, params: Any) -> Any:
            return api_destination(url=f"https://api.example.com/u/{record['id']}", body=record)

        @record_destination("advice_grouped")
        def grouped(record: Any, params: Any) -> Any:
            return api_destination(url="https://api.example.com/u/{id}", per_record=True)

        with capture("reflowfy.destinations.record") as logs:
            await small().route(_engines(5, "a"), {})
            await grouped().route(_engines(50, "a"), {})
        assert not [m for m in logs if "built a destination per record" in m]


# ---------------------------------------------------------------- the pool itself


class TestConnectionPool:
    async def test_concurrent_asks_create_one(self):
        pool = ConnectionPool()
        created: List[int] = []

        async def create() -> int:
            created.append(1)
            await asyncio.sleep(0.01)
            return 42

        async def close(item: int) -> None:
            return None

        got = await asyncio.gather(*(pool.get("k", create, close) for _ in range(10)))
        assert got == [42] * 10 and created == [1] and len(pool) == 1

    async def test_close_tries_all_and_raises_the_first_error(self):
        pool = ConnectionPool()
        closed: List[str] = []

        def closer(name: str, fail: bool) -> Any:
            async def close(item: Any) -> None:
                closed.append(name)
                if fail:
                    raise RuntimeError(f"close {name}")

            return close

        async def make() -> str:
            return "x"

        await pool.get("a", make, closer("a", True))
        await pool.get("b", make, closer("b", False))
        with pytest.raises(RuntimeError, match="close a"):
            await pool.close()
        assert sorted(closed) == ["a", "b"] and len(pool) == 0

    def test_no_budget_without_a_limit(self):
        assert ConnectionPool().requests is None
        pool = ConnectionPool(request_limit=3)
        assert pool.requests is pool.requests  # one semaphore for the job

    async def test_a_record_destination_reports_a_pool_close_error(self, producers, monkeypatch):
        @record_destination("pool_close_error")
        def route(record: Any, params: Any) -> KafkaDestination:
            return kafka_destination(bootstrap_servers="k:9092", topic="t")

        async def broken_stop(self: FakeProducer) -> None:
            raise RuntimeError("stop failed")

        monkeypatch.setattr(FakeProducer, "stop", broken_stop)
        dest = route()
        result = await dest.route([{"a": 1}], {})
        await dest.send_with_retry(result.records, {})
        with pytest.raises(RuntimeError, match="stop failed"):
            await dest.close()
