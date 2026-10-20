"""IDBasedAPISource over httpx.MockTransport: real request building, no network.

``httpx.Client`` is swapped for one bound to a MockTransport, so the source's own
``_get_client`` (base_url, auth headers, timeout) is what builds every request.
"""

import base64
import json
from typing import Callable, List

import httpx
import pytest

from reflowfy.sources.api import IDBasedAPISource, id_based_api_source
from reflowfy.sources.base import SourceError

Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> Callable[[Handler], List[httpx.Request]]:
    """Route every httpx.Client the source creates through ``handler``; return the log."""
    real_client = httpx.Client

    def install(handler: Handler) -> List[httpx.Request]:
        seen: List[httpx.Request] = []

        def recording(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return handler(request)

        mock = httpx.MockTransport(recording)
        monkeypatch.setattr(
            httpx, "Client", lambda **kw: real_client(transport=mock, **kw)  # type: ignore[misc]
        )
        return seen

    return install


def _users(request: httpx.Request) -> httpx.Response:
    uid = request.url.path.rsplit("/", 1)[-1]
    if uid == "404":
        return httpx.Response(404, json={"error": "not found"})
    return httpx.Response(200, json={"id": int(uid), "name": f"user{uid}"})


# -- per-ID mode ---------------------------------------------------------------


def test_fetch_per_id_one_request_per_id_and_404_is_skipped(transport) -> None:
    seen = transport(_users)
    src = id_based_api_source("https://api.test", "/users/{id}", ids=[1, 404, 3])

    records = src.fetch({})

    assert [r["data"]["id"] for r in records] == [1, 3]
    assert [str(r.url) for r in seen] == [
        "https://api.test/users/1",
        "https://api.test/users/404",
        "https://api.test/users/3",
    ]


def test_fetch_per_id_ids_from_runtime_params_and_limit(transport) -> None:
    seen = transport(_users)
    src = id_based_api_source("https://api.test", "/users/{id}")

    records = src.fetch({"input_ids": [7, 8, 9]}, limit=2)

    assert [r["data"]["id"] for r in records] == [7, 8]
    assert len(seen) == 2


def test_per_id_post_substitutes_id_into_dict_body_and_sends_params(transport) -> None:
    seen = transport(lambda req: httpx.Response(200, json={"ok": True}))
    src = id_based_api_source(
        "https://api.test",
        "/items/{id}",
        ids=["a"],
        method="post",
        body={"item": "{id}", "n": 1},
        params={"v": "2"},
    )

    src.fetch({})

    assert seen[0].method == "POST"
    assert json.loads(seen[0].content) == {"item": "a", "n": 1}
    assert seen[0].url.params["v"] == "2"


def test_per_id_get_never_sends_a_body(transport) -> None:
    seen = transport(lambda req: httpx.Response(200, json={}))
    id_based_api_source("https://api.test", "/x/{id}", ids=[1], body={"a": 1}).fetch({})
    assert seen[0].content == b""


def test_per_id_raw_string_body_sent_as_is(transport) -> None:
    seen = transport(lambda req: httpx.Response(200, json={"ok": 1}))
    id_based_api_source(
        "https://api.test",
        "/x/{id}",
        ids=[1],
        method="PUT",
        body="<x>{id}</x>",
        headers={"Content-Type": "application/xml"},
    ).fetch({})
    assert seen[0].content == b"<x>{id}</x>"  # raw bodies get no {id} substitution
    assert seen[0].headers["content-type"] == "application/xml"


@pytest.mark.parametrize("status", [429, 500, 503, 401])
def test_per_id_non_404_error_fails_the_job_instead_of_dropping_the_id(transport, status) -> None:
    """A 429 (or any non-404) is not retried by the source: it raises so the job
    fails and can be re-run, rather than completing with that ID missing."""
    seen = transport(lambda req: httpx.Response(status, text="slow down"))
    src = id_based_api_source("https://api.test", "/users/{id}", ids=[1, 2])

    with pytest.raises(SourceError, match=f"HTTP {status} for id 1"):
        src.fetch({})
    assert len(seen) == 1  # stops at the first failure


def test_per_id_connection_error_is_a_source_error(transport) -> None:
    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=req)

    transport(boom)
    with pytest.raises(SourceError, match="Request failed for id 5"):
        id_based_api_source("https://api.test", "/u/{id}", ids=[5]).fetch({})


# -- auth headers ----------------------------------------------------------------


@pytest.mark.parametrize(
    "auth_type,token,header,expected",
    [
        ("bearer", "tok", "authorization", "Bearer tok"),
        ("apikey", "k123", "x-api-key", "k123"),
        ("basic", "u:p", "authorization", "Basic " + base64.b64encode(b"u:p").decode()),
    ],
)
def test_auth_headers_are_sent(transport, auth_type, token, header, expected) -> None:
    seen = transport(lambda req: httpx.Response(200, json={}))
    id_based_api_source(
        "https://api.test",
        "/u/{id}",
        ids=[1],
        auth_type=auth_type,
        auth_token=token,
        headers={"X-Tenant": "t1"},
    ).fetch({})
    assert seen[0].headers[header] == expected
    assert seen[0].headers["x-tenant"] == "t1"


# -- batch mode ------------------------------------------------------------------


def _batch_handler(n: int) -> Handler:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"users": [{"id": i} for i in range(n)]}})

    return handler


def test_batch_mode_one_request_with_verbatim_body_and_response_key(transport) -> None:
    seen = transport(_batch_handler(3))
    src = id_based_api_source(
        "https://api.test",
        "/users/batch",
        method="POST",
        body={"ids": [1, 2, 3]},
        response_key="data.users",
    )

    records = src.fetch({})

    assert [r["data"]["id"] for r in records] == [0, 1, 2]
    assert len(seen) == 1
    assert json.loads(seen[0].content) == {"ids": [1, 2, 3]}


def test_batch_mode_raw_list_body(transport) -> None:
    seen = transport(lambda req: httpx.Response(200, json=[{"id": 1}]))
    src = id_based_api_source("https://api.test", "/b", method="POST", body="[1,2]")
    assert src.fetch({}) == [{"data": {"id": 1}}]
    assert seen[0].content == b"[1,2]"


def test_batch_mode_http_error_and_connection_error(transport) -> None:
    transport(lambda req: httpx.Response(429, text="rate limited"))
    with pytest.raises(SourceError, match="HTTP 429: rate limited"):
        id_based_api_source("https://api.test", "/b").fetch({})

    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=req)

    transport(boom)
    with pytest.raises(SourceError, match="Request failed"):
        id_based_api_source("https://api.test", "/b").fetch({})


@pytest.mark.parametrize(
    "response_key,payload,expected",
    [
        (None, {"not": "a list"}, []),
        ("a.b", {"a": [1]}, []),  # walks into a list → nothing
        ("a", {"a": {"single": 1}}, [{"single": 1}]),  # a lone object becomes one record
        ("missing", {"a": 1}, []),
    ],
)
def test_extract_records_shapes(response_key, payload, expected) -> None:
    src = IDBasedAPISource("https://api.test", "/b", response_key=response_key)
    assert src._extract_records(payload) == expected


# -- split_jobs ------------------------------------------------------------------


def test_split_jobs_batch_mode_chunks_the_response(transport) -> None:
    transport(_batch_handler(5))
    src = id_based_api_source(
        "https://api.test", "/batch", ids=[1, 2], batch_size=2, response_key="data.users"
    )

    jobs = list(src.split_jobs({}))

    assert [j.metadata["record_count"] for j in jobs] == [2, 2, 1]
    assert [j.metadata["batch_num"] for j in jobs] == [0, 1, 2]
    assert all(j.metadata["ids_count"] == 2 for j in jobs)


def test_split_jobs_per_id_groups_ids_and_drops_empty_batches(transport) -> None:
    seen = transport(_users)
    src = id_based_api_source("https://api.test", "/users/{id}", ids=[1, 2, 404], batch_size=2)

    jobs = list(src.split_jobs({}))

    assert len(jobs) == 1  # second batch [404] had no records
    assert jobs[0].metadata == {"batch_num": 0, "id_count": 2, "record_count": 2, "ids": [1, 2]}
    assert len(seen) == 3


def test_split_jobs_invalid_config_is_a_source_error() -> None:
    src = id_based_api_source("https://api.test", "/u/{id}", ids=[1], batch_size=0)
    with pytest.raises(SourceError, match="Invalid configuration"):
        list(src.split_jobs({}))


# -- split (planning) ------------------------------------------------------------


def test_split_per_id_carries_connection_config_without_requests(transport) -> None:
    seen = transport(_users)
    src = id_based_api_source(
        "https://api.test",
        "/users/{id}",
        batch_size=2,
        auth_type="bearer",
        auth_token="t",
        params={"v": "1"},
    )

    subs = list(src.split({"input_ids": [1, 2, 3]}))

    assert [s.config["ids"] for s in subs] == [[1, 2], [3]]
    assert all(s.config["auth_token"] == "t" and s.config["params"] == {"v": "1"} for s in subs)
    assert seen == []  # planning is metadata only
    assert [r["data"]["id"] for r in subs[1].fetch({})] == [3]


def test_split_batch_mode_is_one_job() -> None:
    src = id_based_api_source("https://api.test", "/batch")
    assert list(src.split({"ids": [1, 2, 3]})) == [src]


# -- health_check ----------------------------------------------------------------


@pytest.mark.parametrize("status,healthy", [(200, True), (404, True), (500, False)])
def test_health_check_status(transport, status, healthy) -> None:
    seen = transport(lambda req: httpx.Response(status))
    assert id_based_api_source("https://api.test", "/u/{id}").health_check() is healthy
    assert seen[0].method == "HEAD"


def test_health_check_connection_error_is_unhealthy(transport) -> None:
    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=req)

    transport(boom)
    assert id_based_api_source("https://api.test", "/u/{id}").health_check() is False


def test_health_check_disabled_makes_no_request(transport) -> None:
    seen = transport(lambda req: httpx.Response(500))
    src = id_based_api_source("https://api.test", "/u/{id}", health_check_enabled=False)
    assert src.health_check() is True
    assert seen == []


def test_client_is_reused_across_requests(transport) -> None:
    transport(_users)
    src = id_based_api_source("https://api.test", "/users/{id}", ids=[1, 2])
    src.fetch({})
    client = src._client
    src.fetch({})
    assert client is not None and src._client is client
