"""Requests in flight per job: the pool behind them, the API destination's default of 16,
and that removing concurrency= from the destinations (1.1.4) changed no dedup hash."""

import asyncio
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from reflowfy.core.async_utils import for_each_concurrently
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.base import DestinationError
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.base import ConnectionPool
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


# ---------------------------------------------------------------- removed knobs


def test_kafka_concurrency_was_removed():
    with pytest.raises(TypeError, match="concurrency= was removed"):
        kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=4)


def test_api_concurrency_was_removed():
    with pytest.raises(TypeError, match="concurrency"):
        api_destination(url="https://x.com", per_record=True, concurrency=4)  # type: ignore[call-arg]


def test_api_schema_has_no_concurrency():
    assert "concurrency" not in ApiDestinationConfig.model_fields


def test_config_round_trips_through_factory():
    kafka = kafka_destination(bootstrap_servers="k:9092", topic="t", value_field="d", key="{k}")
    api = api_destination(url="https://x.com/{id}", per_record=True, headers={"X": "{x}"})
    assert DestinationFactory.create("kafka", kafka.config).config == kafka.config
    assert DestinationFactory.create("api", api.config).config == api.config


# ---------------------------------------------------------------- api per-record


def _api(seen: InFlight, fail_url: Any = None, url: str = "https://api.example.com/users/{id}"):
    dest = api_destination(url=url, per_record=True)
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

    client = MagicMock()
    client.request = request
    dest._client = client
    dest._get_client = AsyncMock(return_value=client)  # pooled or not: this fake
    return dest, urls


class TestApiRequestsInFlight:
    async def test_default_is_16(self, monkeypatch):
        monkeypatch.delenv("TRANSFORM_CONCURRENCY", raising=False)
        seen = InFlight()
        dest, urls = _api(seen)
        await dest.send([{"id": i} for i in range(100)])
        assert seen.peak == 16
        assert sorted(urls) == sorted(f"https://api.example.com/users/{i}" for i in range(100))

    async def test_env_sets_the_default(self, monkeypatch):
        monkeypatch.setenv("TRANSFORM_CONCURRENCY", "1")
        seen = InFlight()
        dest, urls = _api(seen)
        await dest.send([{"id": i} for i in range(4)])
        assert seen.peak == 1
        assert urls == [f"https://api.example.com/users/{i}" for i in range(4)]  # in order

    async def test_a_record_destination_sets_it(self):
        seen = InFlight()
        dest, _ = _api(seen)
        dest.send_concurrency = 3
        await dest.send([{"id": i} for i in range(20)])
        assert seen.peak == 3

    async def test_a_pool_budget_caps_several_destinations_together(self):
        """Two destinations of one record destination share one limit, not one each."""
        seen = InFlight()
        pool = ConnectionPool(request_limit=4)
        dests = []
        for host in ("a", "b"):
            dest, _ = _api(seen, url=f"https://{host}.example.com/{{id}}")
            dest.connection_pool = pool
            dest.send_concurrency = 4
            dests.append(dest)
        records = [{"id": i} for i in range(20)]
        await asyncio.gather(*(d.send(records) for d in dests))
        assert seen.peak == 4  # not 8

    async def test_failure_names_the_record_and_cancels_the_rest(self, monkeypatch):
        monkeypatch.setenv("TRANSFORM_CONCURRENCY", "4")
        seen = InFlight()
        dest, urls = _api(seen, fail_url="https://api.example.com/users/5")
        with pytest.raises(DestinationError) as exc_info:
            await dest.send([{"id": i} for i in range(100)])
        assert str(exc_info.value) == (
            "Destination 'api' error: record 6 of 100: HTTP 503: Unavailable"
        )
        assert len(urls) < 100

    async def test_missing_placeholder_fails_the_job_before_any_request(self):
        dest, urls = _api(InFlight())
        with pytest.raises(ValueError, match=r"\{id\}.*record 2 of 3"):
            await dest.send([{"id": 1}, {"name": "x"}, {"id": 3}])
        assert urls == []


# ---------------------------------------------------------------- dedup identity


class TestDedupIdentity:
    def test_kafka_identity_has_the_shape_it_had_before_1_1_4(self):
        """concurrency was never hashed, and value_field is hashed as key_field."""
        dest = kafka_destination(bootstrap_servers="k:9092", topic="t", value_field="data")
        config = destination_identity(dest)["config"]
        assert "concurrency" not in config and "value_field" not in config
        assert config["key_field"] == "data"

    def test_api_identity_has_no_concurrency(self):
        dest = api_destination(url="https://x.com/{id}", per_record=True)
        assert "concurrency" not in destination_identity(dest)["config"]

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
