"""reflowfy.testing.cassette: mocking, recording and replaying HTTP and source calls.

Recording is tested against a real server on localhost, through every client the
module claims to cover: httpx (sync and async), requests, bare urllib3, the
Elasticsearch client and boto3.
"""

import asyncio
import re
from typing import Any, Dict, Iterator, List

import boto3
import httpx
import pytest
import requests
import urllib3
from elasticsearch import Elasticsearch

from reflowfy.sources.mock import MockSource
from reflowfy.testing import UnmockedCallError, mock_network
from reflowfy.testing.cassette import (
    FixtureError,
    RawResponse,
    Session,
    activate,
    current_session,
    decode_body,
    encode_body,
    patch_sources,
    source_key,
)
from testing_support import LocalServer, local_server


@pytest.fixture
def server() -> Iterator[LocalServer]:
    with local_server() as running:
        yield running


def recording() -> Session:
    session = Session("record")
    session.in_job = True
    return session


def replaying(recorded: Session, **kwargs: Any) -> Session:
    return Session(
        "replay",
        http=recorded.recorded_http,
        source_calls=recorded.recorded_source_calls,
        **kwargs,
    )


class TestMockHttp:
    def test_every_verb_and_body_kind(self, mock_http):
        mock_http.get("https://a.test/j", json={"a": 1})
        mock_http.post("https://a.test/t", text="hello", status=201)
        mock_http.put("https://a.test/b", body=b"\x00\x01", headers={"X-Kind": "raw"})
        mock_http.patch("https://a.test/e")
        mock_http.delete(re.compile(r"https://a\.test/items/\d+$"), status=204)

        assert httpx.get("https://a.test/j").json() == {"a": 1}
        created = httpx.post("https://a.test/t", json={"n": 1})
        assert (created.status_code, created.text) == (201, "hello")
        assert created.headers["content-type"].startswith("text/plain")
        raw = httpx.put("https://a.test/b")
        assert raw.content == b"\x00\x01" and raw.headers["x-kind"] == "raw"
        assert httpx.patch("https://a.test/e").content == b""
        assert httpx.delete("https://a.test/items/42").status_code == 204

        assert [c.method for c in mock_http.calls] == ["GET", "POST", "PUT", "PATCH", "DELETE"]
        assert mock_http.calls[1].json() == {"n": 1}
        assert mock_http.calls[0].json() is None and mock_http.calls[0].step is None

    def test_the_latest_registration_wins_and_the_method_must_match(self, mock_http):
        mock_http.get("https://a.test/x", json=1)
        mock_http.get("https://a.test/x", json=2)
        assert httpx.get("https://a.test/x").json() == 2
        with pytest.raises(UnmockedCallError, match="the pipeline made POST https://a.test/x"):
            httpx.post("https://a.test/x")
        with pytest.raises(UnmockedCallError):
            httpx.get("https://a.test/x?page=2")  # the query string is part of the URL

    async def test_async_client(self, mock_http):
        mock_http.get("https://a.test/x", json={"ok": True})
        async with httpx.AsyncClient() as client:
            assert (await client.get("https://a.test/x")).json() == {"ok": True}
            with pytest.raises(UnmockedCallError):
                await client.get("https://a.test/other")

    def test_requests_and_urllib3_are_answered_too(self, mock_http):
        mock_http.get("https://a.test/x", json={"ok": True})
        mock_http.post("http://b.test:8080/y", text="made")
        assert requests.get("https://a.test/x").json() == {"ok": True}
        assert requests.post("http://b.test:8080/y", data="payload").text == "made"
        assert mock_http.calls[1].body == b"payload"
        pool = urllib3.PoolManager()
        assert pool.request("GET", "https://a.test/x").json() == {"ok": True}
        assert pool.request("POST", "http://b.test:8080/y", body=b"raw").data == b"made"
        with pytest.raises(UnmockedCallError, match="GET https://a.test/nope"):
            requests.get("https://a.test/nope")

    def test_nothing_is_patched_once_the_session_ends(self):
        original = httpx.HTTPTransport.handle_request
        with mock_network():
            assert httpx.HTTPTransport.handle_request is not original
            assert current_session() is not None
        assert httpx.HTTPTransport.handle_request is original
        assert current_session() is None

    def test_sessions_do_not_nest(self):
        with mock_network():
            with pytest.raises(RuntimeError, match="already active"):
                with mock_network():
                    pass

    def test_unknown_mode(self):
        with pytest.raises(ValueError, match="unknown mode"):
            Session("sometimes")

    def test_passthrough_serves_mocks_and_lets_the_rest_out(self, server):
        with mock_network(allow_network=True) as net:
            net.http.get("https://a.test/x", json={"mocked": True})
            assert httpx.get("https://a.test/x").json() == {"mocked": True}
            assert httpx.get(f"{server.url}/json/1").json()["path"] == "/json/1"
            assert requests.get(f"{server.url}/json/2").json()["path"] == "/json/2"

            async def fetch() -> Any:
                async with httpx.AsyncClient() as client:
                    return (await client.get(f"{server.url}/json/3")).json()

            assert asyncio.run(fetch())["path"] == "/json/3"
        assert len(server.hits) == 3


class TestRecordAndReplay:
    def _round_trip(self, server: LocalServer, call: Any) -> List[Any]:
        """Run ``call`` while recording, then again from the recording, server off."""
        recorded = recording()
        with activate(recorded):
            first = call()
        hits = len(server.hits)
        with activate(replaying(recorded)):
            second = call()
        assert len(server.hits) == hits, "the replay reached the server"
        return [first, second, recorded.recorded_http]

    def test_httpx_sync(self, server):
        first, second, stored = self._round_trip(
            server, lambda: httpx.post(f"{server.url}/json/a", json={"q": 1}).json()
        )
        assert first == second == {"path": "/json/a", "email": "a@b.c", "method": "POST"}
        request, response = stored[0]["request"], stored[0]["response"]
        assert request["method"] == "POST" and request["url"] == f"{server.url}/json/a"
        assert len(request["body_sha256"]) == 64 and "headers" not in request
        assert response["status"] == 200 and response["body"]["path"] == "/json/a"
        assert response["headers"]["content-type"] == "application/json"
        assert "set-cookie" not in response["headers"] and "date" not in response["headers"]

    def test_httpx_async_with_concurrent_requests(self, server):
        async def fan_out() -> List[Any]:
            async with httpx.AsyncClient() as client:
                responses = await asyncio.gather(
                    *(client.get(f"{server.url}/json/{n}") for n in range(8))
                )
            return [r.json()["path"] for r in responses]

        first, second, stored = self._round_trip(server, lambda: asyncio.run(fan_out()))
        assert first == second == [f"/json/{n}" for n in range(8)]
        assert len(stored) == 8

    def test_requests(self, server):
        def call() -> Any:
            response = requests.get(f"{server.url}/json/r", headers={"Authorization": "Bearer t"})
            return response.status_code, response.json()["path"]

        first, second, stored = self._round_trip(server, call)
        assert first == second == (200, "/json/r")
        assert "Bearer" not in str(stored)

    def test_requests_gets_its_cookie_while_recording(self, server):
        with activate(recording()):
            assert requests.get(f"{server.url}/json/c").headers["set-cookie"] == "session=secret"

    def test_text_binary_empty_gzip_and_404(self, server):
        def call() -> Any:
            return [
                httpx.get(f"{server.url}/text").text,
                httpx.get(f"{server.url}/binary").content,
                httpx.get(f"{server.url}/empty").content,
                httpx.get(f"{server.url}/gzip").json(),
                requests.get(f"{server.url}/gzip").json(),
                httpx.get(f"{server.url}/badjson").text,
                httpx.get(f"{server.url}/null").json(),
                httpx.get(f"{server.url}/missing").status_code,
            ]

        first, second, stored = self._round_trip(server, call)
        assert first == second == [
            "plain words", b"\xff\xfe\x00\x01", b"", {"zipped": True}, {"zipped": True},
            "{not json", None, 404,
        ]
        encodings = [e["response"].get("body_encoding", "json") for e in stored]
        assert encodings == ["text", "base64", "json", "json", "json", "text", "text", "json"]
        assert "content-encoding" not in stored[3]["response"]["headers"]

    def test_redirects(self, server):
        # requests and PoolManager follow a redirect themselves: two exchanges.
        first, second, stored = self._round_trip(
            server, lambda: requests.get(f"{server.url}/redirect").json()["path"]
        )
        assert first == second == "/json/redirected" and len(stored) == 2

        # A connection pool follows it inside urlopen, by calling itself: one exchange.
        def through_the_pool() -> Any:
            host, port = server.url.removeprefix("http://").split(":")
            pool = urllib3.HTTPConnectionPool(host, int(port))
            return pool.urlopen("GET", "/redirect").json()["path"]

        first, second, stored = self._round_trip(server, through_the_pool)
        assert first == second == "/json/redirected" and len(stored) == 1

    def test_urllib3_string_body_and_streamed_read(self, server):
        def call() -> Any:
            pool = urllib3.PoolManager()
            streamed = pool.request(
                "POST", f"{server.url}/json/s", body="text body", preload_content=False
            )
            return streamed.read()

        first, second, _ = self._round_trip(server, call)
        assert first == second and b"/json/s" in first
        assert server.bodies[0] == b"text body"

    def test_a_repeated_request_replays_in_order_then_keeps_the_last_answer(self):
        exchange = lambda n: {  # noqa: E731
            "request": {"method": "GET", "url": "https://a.test/x", "body_sha256": None},
            "response": {"status": 200, "headers": {}, "body": {"n": n}},
        }
        with activate(Session("replay", http=[exchange(1), exchange(2)])):
            assert [httpx.get("https://a.test/x").json()["n"] for _ in range(4)] == [1, 2, 2, 2]

    def test_a_different_body_is_a_different_request(self, server):
        recorded = recording()
        with activate(recorded):
            httpx.post(f"{server.url}/json/q", json={"q": 1})
        with activate(replaying(recorded)):
            with pytest.raises(UnmockedCallError):
                httpx.post(f"{server.url}/json/q", json={"q": 2})

    def test_only_what_happens_inside_a_job_is_recorded(self, server):
        session = Session("record")
        with activate(session):
            httpx.get(f"{server.url}/json/planning")
            requests.get(f"{server.url}/json/planning2")

            async def fetch() -> None:
                async with httpx.AsyncClient() as client:
                    await client.get(f"{server.url}/json/planning3")

            asyncio.run(fetch())
        assert session.recorded_http == [] and len(server.hits) == 3

    def test_a_response_over_the_size_limit_fails_the_recording(self, server):
        session = Session("record", max_body_bytes=1024)
        session.in_job = True
        with activate(session):
            with pytest.raises(FixtureError, match="4 KB, over the 1 KB.*--freeze-step"):
                httpx.get(f"{server.url}/big")

    def test_elasticsearch_client(self, mock_http):
        mock_http.post(
            "http://es.test:9200/orders/_search",
            json={"hits": {"hits": [{"_source": {"id": 1}}]}},
            headers={"X-Elastic-Product": "Elasticsearch"},
        )
        found = Elasticsearch("http://es.test:9200").search(index="orders", query={"match_all": {}})
        assert found["hits"]["hits"][0]["_source"] == {"id": 1}

    def test_boto3_s3(self, server):
        def call() -> Any:
            s3 = boto3.client(
                "s3",
                endpoint_url=server.url,
                aws_access_key_id="k",
                aws_secret_access_key="s",
                region_name="us-east-1",
            )
            return s3.get_object(Bucket="text", Key="report.txt")["Body"].read()

        first, second, stored = self._round_trip(server, call)
        assert first == second == b"plain words"
        assert "Signature" not in str(stored) and "AWS4" not in str(stored)


class Lookup(MockSource):
    """A source a step would fetch from; counts real fetches."""

    fetched = 0

    def fetch(self, runtime_params: Dict[str, Any], limit: Any = None) -> Any:
        type(self).fetched += 1
        return super().fetch(runtime_params, limit)


class TestSources:
    @pytest.fixture(autouse=True)
    def reset(self) -> None:
        Lookup.fetched = 0

    def test_a_stub_answers_by_class_name_or_predicate(self, mock_source):
        mock_source(Lookup, records=[{"id": 1}])
        mock_source("Lookup", records=[{"raw": True}], where=lambda c: c["batch_size"] == 3, wrap=False)
        assert Lookup(data=[{"real": 1}]).fetch({}) == [{"data": {"id": 1}}]
        assert Lookup(data=[], batch_size=3).fetch({}) == [{"raw": True}]
        assert MockSource(data=[{"real": 1}]).fetch({}) == [{"data": {"real": 1}}]
        mock_source("SomethingElse", records=[])
        assert Lookup(data=[]).fetch({}) == [{"data": {"id": 1}}]
        assert Lookup.fetched == 0
        assert [c.source_type for c in mock_source.calls][:2] == ["Lookup", "Lookup"]

    def test_record_then_replay_without_fetching(self):
        recorded = recording()
        with activate(recorded):
            first = Lookup(data=[{"id": 1}]).fetch({})
            Lookup(data=[{"id": 2}]).fetch({})
        assert Lookup.fetched == 2
        assert [c["source"]["type"] for c in recorded.recorded_source_calls] == ["Lookup", "Lookup"]
        replay = replaying(recorded, strict_sources=True)
        replay.in_job = True
        with activate(replay):
            assert Lookup(data=[{"id": 1}]).fetch({}) == first
            with pytest.raises(UnmockedCallError, match="a fetch on Lookup.*mock_source"):
                Lookup(data=[{"id": 999}]).fetch({})
        assert Lookup.fetched == 2

    def test_the_http_of_a_recorded_source_call_is_not_stored_twice(self, server):
        class HttpLookup(MockSource):
            def fetch(self, runtime_params, limit=None):
                return [httpx.get(f"{server.url}/json/src").json(),
                        requests.get(f"{server.url}/json/src2").json()]

        recorded = recording()
        with activate(recorded):
            HttpLookup(data=[]).fetch({})
        assert len(recorded.recorded_source_calls) == 1 and recorded.recorded_http == []

    def test_outside_a_job_and_in_a_plain_test_the_real_fetch_runs(self):
        with activate(Session("record")):
            Lookup(data=[{"id": 1}]).fetch({})
        with mock_network():
            Lookup(data=[{"id": 1}]).fetch({})
        assert Lookup.fetched == 2

    def test_the_jobs_own_source_runs_and_its_records_are_kept(self):
        session = recording()
        with activate(session):
            main = Lookup(data=[{"id": 1}])
            session.main_sources.add(id(main))
            assert main.fetch({}) == [{"data": {"id": 1}}]
        assert session.main_fetches[id(main)] == [{"data": {"id": 1}}]
        assert session.recorded_source_calls == []

    def test_a_fetch_inside_a_fetch_is_one_call(self, mock_source):
        class Outer(MockSource):
            def fetch(self, runtime_params, limit=None):
                return Lookup(data=[{"inner": True}]).fetch(runtime_params)

        patch_sources()
        assert Outer(data=[]).fetch({}) == [{"data": {"inner": True}}]
        assert [c.source_type for c in mock_source.calls] == ["Outer"]

    def test_a_source_class_defined_after_the_session_started(self):
        with mock_network() as net:
            class Late(MockSource):
                def fetch(self, runtime_params, limit=None):
                    return ["real"]

            patch_sources()
            patch_sources()  # idempotent
            net.sources(Late, records=["stubbed"], wrap=False)
            assert Late(data=[]).fetch({}) == ["stubbed"]
        assert Late(data=[]).fetch({}) == ["real"]
        patch_sources()  # no session: nothing to do
        assert Late(data=[]).fetch({}) == ["real"]

    def test_the_key_ignores_credentials(self):
        class WithSecret(MockSource):
            pass

        a, b = WithSecret(data=[1]), WithSecret(data=[1])
        a.config["password"], b.config["password"] = "one", "two"
        assert source_key(a) == source_key(b)
        b.config["index"] = "other"
        assert source_key(a) != source_key(b)


async def test_a_request_in_flight_when_the_session_ends_goes_out_normally(monkeypatch):
    """The patched methods fall back to the real ones when no session is active."""
    from reflowfy.testing import cassette

    monkeypatch.setattr(cassette, "_httpx_sync", lambda transport, request: httpx.Response(201))

    async def real_async(transport: Any, request: Any) -> httpx.Response:
        return httpx.Response(202)

    monkeypatch.setattr(cassette, "_httpx_async", real_async)
    monkeypatch.setattr(cassette, "_urllib3_urlopen", lambda pool, *a, **k: "real urlopen")
    request = httpx.Request("GET", "https://a.test/x")
    assert cassette._sync_handle(httpx.HTTPTransport(), request).status_code == 201
    assert (await cassette._async_handle(httpx.AsyncHTTPTransport(), request)).status_code == 202
    assert cassette._urlopen(urllib3.HTTPConnectionPool("a.test"), "GET", "/x") == "real urlopen"


def test_urllib3_with_an_absolute_url_and_a_default_port(mock_http):
    mock_http.get("http://a.test/abs", json=1)
    mock_http.get("https://b.test/tls", json=2)
    assert urllib3.HTTPConnectionPool("ignored.test").urlopen("GET", "http://a.test/abs").json() == 1
    assert urllib3.HTTPSConnectionPool("b.test", 443).urlopen("GET", "/tls").json() == 2


def test_bodies_round_trip():
    for body, content_type in [
        (b'{"a": 1}', "application/json"),
        (b"words", "text/plain"),
        (b"\xff\x00", "application/octet-stream"),
        (b"", "application/json"),
    ]:
        stored = encode_body(body, content_type)
        if content_type == "application/json" and body:
            assert stored == {"body": {"a": 1}}
        else:
            assert decode_body(stored) == body
    assert RawResponse(200, {}, b"x").status == 200
