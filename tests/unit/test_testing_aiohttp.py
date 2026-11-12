"""reflowfy.testing with aiohttp: mocking, recording against a real localhost
server, replaying with the server gone, and a whole pipeline round trip."""

import asyncio
import json
import re
from typing import Any, Dict, Iterator, List

import aiohttp
import pytest

from reflowfy import (
    AbstractPipeline,
    RecordTransformation,
    RuntimeParams,
    SkipRecord,
    kafka_destination,
)
from reflowfy.sources.mock import MockSource
from reflowfy.testing import UnmockedCallError, mock_network, record_fixture, run_pipeline
from reflowfy.testing.aiohttp_layer import ReplayedResponse
from reflowfy.testing.cassette import RawResponse, Session, activate
from reflowfy.testing.fixtures import diff_fixture, replay
from testing_support import LocalServer, isolated_registry, local_server


@pytest.fixture
def server() -> Iterator[LocalServer]:
    with local_server() as running:
        yield running


def recording() -> Session:
    session = Session("record")
    session.in_job = True
    return session


class TestMocked:
    async def test_await_and_async_with_and_every_body_kind(self, mock_http):
        mock_http.get("https://a.test/j", json={"a": 1})
        mock_http.post("https://a.test/t", text="hello", status=201)
        mock_http.get(re.compile(r"https://a\.test/items/\d+$"), body=b"\x00\x01")
        async with aiohttp.ClientSession() as client:
            response = await client.get("https://a.test/j")
            assert (response.status, await response.json()) == (200, {"a": 1})
            assert response.ok and response.content_type == "application/json"
            async with client.post("https://a.test/t", json={"n": 1}) as created:
                assert (created.status, created.reason) == (201, "Created")
                assert await created.text() == "hello" and created.charset == "utf-8"
            assert created.closed
            assert await (await client.get("https://a.test/items/7")).read() == b"\x00\x01"
            with pytest.raises(UnmockedCallError, match="GET https://a.test/other"):
                await client.get("https://a.test/other")
        assert mock_http.calls[1].json() == {"n": 1}

    async def test_params_base_url_form_and_raw_bodies(self, mock_http):
        mock_http.get("https://a.test/search?q=x&page=2", json=[1])
        mock_http.post("https://a.test/form", json="form")
        async with aiohttp.ClientSession(base_url="https://a.test") as client:
            assert await (await client.get("/search", params={"q": "x", "page": 2})).json() == [1]
            for data in ({"k": "v"}, "k=v", b"k=v"):
                assert await (await client.post("/form", data=data)).json() == "form"
        assert {call.body for call in mock_http.calls[1:]} == {b"k=v"}

    async def test_raise_for_status_on_the_call_or_the_session(self, mock_http):
        mock_http.get("https://a.test/gone", status=404)
        async with aiohttp.ClientSession() as client:
            response = await client.get("https://a.test/gone")
            assert not response.ok
            with pytest.raises(aiohttp.ClientResponseError) as excinfo:
                response.raise_for_status()
            assert excinfo.value.status == 404
            with pytest.raises(aiohttp.ClientResponseError):
                await client.get("https://a.test/gone", raise_for_status=True)
            seen: List[int] = []

            async def check(resp: Any) -> None:
                seen.append(resp.status)

            await client.get("https://a.test/gone", raise_for_status=check)
            assert seen == [404]
        async with aiohttp.ClientSession(raise_for_status=True) as strict:
            with pytest.raises(aiohttp.ClientResponseError):
                await strict.get("https://a.test/gone")

    async def test_module_level_request(self, mock_http):
        mock_http.get("https://a.test/x", json={"ok": True})
        async with aiohttp.request("GET", "https://a.test/x") as response:
            assert await response.json() == {"ok": True}

    def test_nothing_is_patched_once_the_session_ends(self):
        original = aiohttp.ClientSession._request
        with mock_network():
            assert aiohttp.ClientSession._request is not original
        assert aiohttp.ClientSession._request is original


class TestReplayedResponse:
    def make(self, body: bytes, content_type: str = "text/plain") -> ReplayedResponse:
        headers = {"content-type": content_type} if content_type else {}
        return ReplayedResponse("GET", "https://a.test/x", RawResponse(200, headers, body))

    async def test_streaming_the_content(self):
        response = self.make(b"line 1\nline 2\nend")
        assert [line async for line in response.content] == [b"line 1\n", b"line 2\n", b"end"]
        chunked = self.make(b"abcdefg")
        assert [c async for c in chunked.content.iter_chunked(3)] == [b"abc", b"def", b"g"]
        whole = self.make(b"abc")
        assert [c async for c in whole.content.iter_any()] == [b"abc"]
        assert [c async for c in whole.content.iter_any()] == []
        assert await self.make(b"abc").content.readany() == b"abc"

    async def test_json_rules_follow_aiohttp(self):
        assert await self.make(b"", "application/json").json() is None
        assert await self.make(b'{"a": 1}', "text/plain").json(content_type=None) == {"a": 1}
        with pytest.raises(aiohttp.ContentTypeError):
            await self.make(b'{"a": 1}', "text/plain").json()

    async def test_raise_for_status_passes_when_ok(self):
        self.make(b"x").raise_for_status()

    async def test_with_no_session_the_real_request_runs(self, monkeypatch):
        from reflowfy.testing import aiohttp_layer

        async def real(client: Any, method: str, url: Any, **kwargs: Any) -> str:
            return "real request"

        monkeypatch.setattr(aiohttp_layer, "_original_request", real)
        async with aiohttp.ClientSession() as client:
            assert await aiohttp_layer._request(client, "GET", "https://a.test") == "real request"

    async def test_the_rest_of_the_surface(self):
        response = self.make(b"x", "text/plain; charset=latin-1")
        assert response.charset == "latin-1" and await response.text() == "x"
        assert self.make(b"x", "").content_type == "application/octet-stream"
        assert self.make(b"x", "text/plain; format=flowed").charset is None
        response.close()
        assert response.closed
        await response.wait_for_close()
        assert "200 OK" in repr(response)
        odd = ReplayedResponse("GET", "https://a.test", RawResponse(799, {}, b""))
        assert odd.reason == "" and not odd.ok


class TestRecorded:
    def test_record_then_replay_with_the_server_gone(self, server):
        async def call() -> List[Any]:
            async with aiohttp.ClientSession() as client:
                async with client.get(f"{server.url}/json/a", params={"q": "1"}) as first:
                    payload = await first.json()
                    again = await first.read()  # still readable after being recorded
                second = await client.post(f"{server.url}/json/b", json={"n": 1})
                texts = await asyncio.gather(
                    *(client.get(f"{server.url}/text?i={i}") for i in range(4))
                )
                return [payload, json.loads(again), await second.json(), [await t.text() for t in texts]]

        recorded = recording()
        with activate(recorded):
            first = asyncio.run(call())
        exchanges = recorded.recorded_http
        assert [e["request"]["url"] for e in exchanges][:2] == [
            f"{server.url}/json/a?q=1", f"{server.url}/json/b",
        ]
        assert exchanges[1]["request"]["body_sha256"] is not None
        assert "set-cookie" not in exchanges[0]["response"]["headers"]

        server.close()
        with activate(Session("replay", http=exchanges)):
            second = asyncio.run(call())
        assert first == second
        assert first[0]["path"] == "/json/a" and first[3] == ["plain words"] * 4

    def test_outside_a_job_nothing_is_recorded_and_passthrough_works(self, server):
        async def call() -> int:
            async with aiohttp.ClientSession() as client:
                return (await client.get(f"{server.url}/json/x")).status

        session = Session("record")
        with activate(session):
            assert asyncio.run(call()) == 200
        assert session.recorded_http == []
        with mock_network(allow_network=True):
            assert asyncio.run(call()) == 200

    def test_a_pipeline_using_aiohttp_records_and_replays(self, server):
        class Geo(RecordTransformation):
            name = "aio_geo"
            concurrency = 4

            async def apply_record(self, rec: Any, params: Dict[str, Any]) -> Any:
                async with aiohttp.ClientSession() as client:
                    path = "missing" if rec["data"]["id"] == 3 else f"json/{rec['data']['id']}"
                    async with client.get(f"{server.url}/{path}") as response:
                        if response.status == 404:
                            raise SkipRecord("no geo")
                        rec["geo"] = (await response.json())["path"]
                return rec

        with isolated_registry():

            class Users(AbstractPipeline[RuntimeParams]):
                name = "aio_users"

                def define_source(self, runtime_params):
                    return MockSource(data=[{"id": 1}, {"id": 2}, {"id": 3}])

                def define_transformations(self, records, runtime_params):
                    return [Geo()]

                def define_destination(self, records, runtime_params):
                    return kafka_destination(topic="t", bootstrap_servers="kafka:9092")

            fixture = record_fixture("aio_users")
            asked = len(server.hits)
            server.close()
            assert diff_fixture(replay(fixture), fixture) == []
            assert len(server.hits) == asked
            assert [r["geo"] for r in fixture.expected["sent"][0]] == ["/json/1", "/json/2"]
            assert fixture.expected["skip_reasons"] == {"no geo": 1}

            with pytest.raises(UnmockedCallError, match="step 'aio_geo' made GET"):
                run_pipeline("aio_users", records=[{"id": 1}])
