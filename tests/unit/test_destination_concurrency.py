"""concurrency= on kafka_destination and api_destination, and the pool behind it."""

import asyncio
from typing import Any, List
from unittest.mock import MagicMock

import httpx
import pytest
from aiokafka.errors import KafkaError

from reflowfy.core.async_utils import for_each_concurrently
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.base import DestinationError
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.schemas import ApiDestinationConfig
from reflowfy.execution.content_dedup import destination_identity
from reflowfy.factories.destination_factory import DestinationFactory


class InFlight:
    """Counts awaits in progress; each one yields so others can start."""

    def __init__(self) -> None:
        self.now = 0
        self.peak = 0
        self.started: List[Any] = []

    async def run(self, item: Any, delay: float = 0.01) -> None:
        self.started.append(item)
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            await asyncio.sleep(delay)
        finally:
            self.now -= 1


# ---------------------------------------------------------------- for_each_concurrently


class TestForEachConcurrently:
    async def test_concurrency_one_is_sequential_and_ordered(self):
        seen = InFlight()
        await for_each_concurrently(list(range(5)), 1, lambda i, x: seen.run(x))
        assert seen.peak == 1
        assert seen.started == [0, 1, 2, 3, 4]

    async def test_bounded_by_concurrency_and_every_item_once(self):
        seen = InFlight()
        await for_each_concurrently(list(range(50)), 8, lambda i, x: seen.run(x))
        assert seen.peak == 8
        assert sorted(seen.started) == list(range(50))
        assert seen.started[:8] == list(range(8))  # started in order

    async def test_fewer_items_than_workers(self):
        seen = InFlight()
        await for_each_concurrently([1, 2], 16, lambda i, x: seen.run(x))
        assert seen.peak == 2

    async def test_empty(self):
        await for_each_concurrently([], 4, lambda i, x: InFlight().run(x))

    async def test_first_failure_cancels_the_rest_and_is_raised(self):
        cancelled: List[int] = []

        async def fn(index: int, item: int) -> None:
            if item == 0:
                await asyncio.sleep(0)
                raise RuntimeError("boom")
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.append(item)
                raise

        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(for_each_concurrently(list(range(10)), 4, fn), 2)
        assert sorted(cancelled) == [1, 2, 3]  # the three in flight; the rest never started

    async def test_passes_the_index(self):
        got: List[Any] = []

        async def fn(index: int, item: str) -> None:
            got.append((index, item))

        await for_each_concurrently(["a", "b", "c"], 2, fn)
        assert sorted(got) == [(0, "a"), (1, "b"), (2, "c")]


# ---------------------------------------------------------------- validation


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, "4"])
def test_invalid_concurrency_rejected(bad):
    with pytest.raises(ValueError, match="concurrency"):
        kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=bad)
    with pytest.raises(ValueError, match="concurrency"):
        api_destination(url="https://x.com", per_record=True, concurrency=bad)


def test_api_concurrency_requires_per_record():
    with pytest.raises(ValueError, match="per_record"):
        api_destination(url="https://x.com", concurrency=4)


def test_api_schema_accepts_concurrency():
    assert (
        ApiDestinationConfig(url="https://x.com", per_record=True, concurrency=8).concurrency == 8
    )
    with pytest.raises(Exception):
        ApiDestinationConfig(url="https://x.com", concurrency=0)


def test_defaults_are_one():
    assert kafka_destination(bootstrap_servers="k:9092", topic="t").config["concurrency"] == 1
    assert api_destination(url="https://x.com").config["concurrency"] == 1


def test_config_round_trips_through_factory():
    kafka = kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=32)
    api = api_destination(url="https://x.com/{id}", per_record=True, concurrency=8)
    assert DestinationFactory.create("kafka", kafka.config).config == kafka.config
    assert DestinationFactory.create("api", api.config).config == api.config


def test_kafka_concurrency_is_not_passed_to_the_producer():
    dest = kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=8)
    assert "concurrency" not in dest._build_connection_kwargs()


# ---------------------------------------------------------------- kafka send


class FakeProducer:
    def __init__(self, fail_value: Any = None) -> None:
        self.seen = InFlight()
        self.sent: List[bytes] = []
        self.fail_value = fail_value

    async def send_and_wait(self, topic: str, value: bytes, headers: Any = None) -> None:
        await self.seen.run(value)
        if value == self.fail_value:
            raise KafkaError("broker gone")
        self.sent.append(value)


def _kafka(producer: FakeProducer, **kwargs: Any):
    dest = kafka_destination(bootstrap_servers="k:9092", topic="t", **kwargs)

    async def get_producer() -> FakeProducer:
        return producer

    dest._get_producer = get_producer  # type: ignore[method-assign]
    return dest


class TestKafkaConcurrency:
    async def test_default_sends_one_at_a_time_in_order(self):
        producer = FakeProducer()
        await _kafka(producer).send([{"i": i} for i in range(5)])
        assert producer.seen.peak == 1
        assert producer.sent == [f'{{"i": {i}}}'.encode() for i in range(5)]

    async def test_concurrency_keeps_n_messages_in_flight(self):
        producer = FakeProducer()
        await _kafka(producer, concurrency=10).send([{"i": i} for i in range(40)])
        assert producer.seen.peak == 10
        assert sorted(producer.sent) == sorted(f'{{"i": {i}}}'.encode() for i in range(40))

    async def test_key_field_and_headers_still_apply(self):
        producer = FakeProducer()
        headers: List[Any] = []

        async def send_and_wait(topic: str, value: bytes, headers_: Any = None, **kw: Any):
            headers.append(kw.get("headers", headers_))
            producer.sent.append(value)

        producer.send_and_wait = send_and_wait  # type: ignore[method-assign]
        dest = _kafka(producer, key_field="p", concurrency=4)
        await dest.send([{"p": 1}, {"p": 2}], {"execution_id": "e1", "n": 3})
        assert sorted(producer.sent) == [b"1", b"2"]
        assert headers == [[("execution_id", b"e1")]] * 2

    async def test_a_failed_message_raises_destination_error(self):
        producer = FakeProducer(fail_value=b'{"i": 3}')
        with pytest.raises(DestinationError, match="Failed to send to topic 't'"):
            await _kafka(producer, concurrency=4).send([{"i": i} for i in range(20)])
        assert len(producer.sent) < 20


# ---------------------------------------------------------------- api send


def _api(seen: InFlight, fail_url: Any = None, **kwargs: Any):
    dest = api_destination(url="https://api.example.com/users/{id}", per_record=True, **kwargs)
    urls: List[str] = []

    async def request(method: str, url: str, **_: Any) -> Any:
        urls.append(url)
        await seen.run(url)
        resp = MagicMock()
        if url == fail_url:
            bad = MagicMock(status_code=503, text="Unavailable")
            resp.raise_for_status = MagicMock(
                side_effect=httpx.HTTPStatusError("503", request=MagicMock(), response=bad)
            )
        return resp

    dest._client = MagicMock()
    dest._client.request = request
    return dest, urls


class TestApiConcurrency:
    async def test_default_one_request_at_a_time_in_order(self):
        seen = InFlight()
        dest, urls = _api(seen)
        await dest.send([{"id": i} for i in range(4)])
        assert seen.peak == 1
        assert urls == [f"https://api.example.com/users/{i}" for i in range(4)]

    async def test_concurrency_keeps_n_requests_in_flight(self):
        seen = InFlight()
        dest, urls = _api(seen, concurrency=16)
        await dest.send([{"id": i} for i in range(100)])
        assert seen.peak == 16
        assert sorted(urls) == sorted(f"https://api.example.com/users/{i}" for i in range(100))

    async def test_failure_names_the_record_and_cancels_the_rest(self):
        seen = InFlight()
        dest, urls = _api(seen, fail_url="https://api.example.com/users/5", concurrency=4)
        with pytest.raises(DestinationError) as exc_info:
            await dest.send([{"id": i} for i in range(100)])
        assert str(exc_info.value) == (
            "Destination 'api' error: record 6 of 100: HTTP 503: Unavailable"
        )
        assert len(urls) < 100

    async def test_missing_placeholder_fails_the_job(self):
        dest, _ = _api(InFlight(), concurrency=4)
        with pytest.raises(ValueError, match=r"\{id\}"):
            await dest.send([{"id": 1}, {"name": "x"}, {"id": 3}])


# ---------------------------------------------------------------- dedup identity


class TestDedupIdentity:
    def test_concurrency_does_not_change_kafka_identity(self):
        one = kafka_destination(bootstrap_servers="k:9092", topic="t")
        many = kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=64)
        assert destination_identity(one) == destination_identity(many)
        assert "concurrency" not in destination_identity(one)["config"]

    def test_concurrency_does_not_change_api_identity(self):
        one = api_destination(url="https://x.com/{id}", per_record=True)
        many = api_destination(url="https://x.com/{id}", per_record=True, concurrency=16)
        assert destination_identity(one) == destination_identity(many)

    def test_per_record_still_changes_api_identity(self):
        batched = api_destination(url="https://x.com")
        per_record = api_destination(url="https://x.com", per_record=True)
        assert destination_identity(batched) != destination_identity(per_record)

    def test_plain_api_identity_is_unchanged_by_the_new_options(self):
        # The config keys an api_destination had before per_record/key_field/concurrency:
        # its records must keep the hashes they were claimed under.
        dest = api_destination(url="https://x.com/in", body={"a": 1}, auth_token="secret")

        class ApiDestination:  # stand-in with the old config shape, same type name
            config = {
                "url": "https://x.com/in",
                "method": "POST",
                "headers": {},
                "auth_type": None,
                "auth_token": "secret",
                "timeout": 30.0,
                "params": None,
                "body": {"a": 1},
                "health_check_enabled": True,
            }

        assert destination_identity(dest) == destination_identity(ApiDestination())

    def test_destination_without_identity_config_uses_its_config(self):
        class Plain:
            config = {"topic": "t"}

        assert destination_identity(Plain()) == {"type": "Plain", "config": {"topic": "t"}}
