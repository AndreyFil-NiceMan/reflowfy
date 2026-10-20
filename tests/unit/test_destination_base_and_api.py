"""BaseDestination.send_with_retry, ApiDestination over httpx.MockTransport, and
destination_identity() for the built-in destinations."""

from typing import Any, Callable, Dict, List, Optional

import httpx
import pytest

from reflowfy.destinations.api import ApiDestination, api_destination
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig
from reflowfy.destinations.console import console_destination
from reflowfy.execution.content_dedup import destination_identity

_FAST = RetryConfig(max_attempts=3, min_wait_seconds=0, max_wait_seconds=0, multiplier=0)


class _Flaky(BaseDestination):
    """Fails ``failures`` times with ``exc`` before succeeding."""

    def __init__(self, failures: int, exc: Exception, retry: RetryConfig = _FAST):
        super().__init__({"name": "flaky"}, retry)
        self.failures = failures
        self.exc = exc
        self.calls = 0

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exc

    async def health_check(self) -> bool:
        return await super().health_check()  # type: ignore[safe-super]


# -- BaseDestination ---------------------------------------------------------------


async def test_send_with_retry_recovers_from_transient_destination_errors() -> None:
    dest = _Flaky(failures=2, exc=DestinationError("x", "blip"))
    await dest.send_with_retry([{"id": 1}])
    assert dest.calls == 3


async def test_send_with_retry_exhaustion_reraises_the_last_error() -> None:
    dest = _Flaky(failures=99, exc=DestinationError("x", "down"))
    with pytest.raises(DestinationError, match="Destination 'x' error: down"):
        await dest.send_with_retry([{"id": 1}])
    assert dest.calls == 3  # max_attempts, then gives up


async def test_send_with_retry_does_not_retry_other_exceptions() -> None:
    dest = _Flaky(failures=99, exc=ValueError("bug in my code"))
    with pytest.raises(ValueError):
        await dest.send_with_retry([])
    assert dest.calls == 1


async def test_defaults_on_the_base_class() -> None:
    dest = _Flaky(failures=0, exc=ValueError())
    assert await dest.close() is None
    assert await dest.health_check() is None  # abstract body; subclasses must override
    assert dest.dedup_identity is None
    assert repr(dest) == "_Flaky(config={'name': 'flaky'})"
    assert BaseDestination.__abstractmethods__ == frozenset({"send", "health_check"})


def test_default_retry_config() -> None:
    assert RetryConfig() == RetryConfig(3, 1.0, 60.0, 2.0)


def test_destination_error_keeps_type_and_cause() -> None:
    cause = RuntimeError("root")
    err = DestinationError("kafka", "boom", cause)
    assert err.destination_type == "kafka" and err.original_error is cause
    assert str(err) == "Destination 'kafka' error: boom"


# -- ApiDestination over MockTransport ------------------------------------------------


Handler = Callable[[httpx.Request], httpx.Response]


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> Callable[[Handler], List[httpx.Request]]:
    real = httpx.AsyncClient

    def install(handler: Handler) -> List[httpx.Request]:
        seen: List[httpx.Request] = []

        def recording(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return handler(request)

        mock = httpx.MockTransport(recording)
        monkeypatch.setattr(
            httpx, "AsyncClient", lambda **kw: real(transport=mock, **kw)  # type: ignore[misc]
        )
        return seen

    return install


async def test_api_retry_exhaustion_on_5xx(transport) -> None:
    seen = transport(lambda r: httpx.Response(503, text="unavailable"))
    dest = api_destination(url="https://sink.test/in", body={"a": 1}, retry_config=_FAST)

    with pytest.raises(DestinationError, match="HTTP 503: unavailable"):
        await dest.send_with_retry([{"a": 1}])
    assert len(seen) == 3


async def test_api_retry_succeeds_after_a_transient_failure(transport) -> None:
    statuses = iter([500, 200])
    seen = transport(lambda r: httpx.Response(next(statuses)))
    dest = api_destination(url="https://sink.test/in", body=[1, 2], retry_config=_FAST)

    await dest.send_with_retry([])

    assert len(seen) == 2
    assert seen[0].content == seen[1].content == b"[1,2]"


async def test_api_unserializable_body_is_a_destination_error(transport) -> None:
    seen = transport(lambda r: httpx.Response(200))
    dest = api_destination(url="https://sink.test/in", body={"tags": {"a", "b"}})
    with pytest.raises(DestinationError, match="Unexpected error"):
        await dest.send([])
    assert seen == []


async def test_api_query_params_and_method(transport) -> None:
    seen = transport(lambda r: httpx.Response(204))
    await api_destination(url="https://sink.test/in", method="put", params={"t": "1"}).send([])
    assert seen[0].method == "PUT" and seen[0].url.params["t"] == "1"


async def test_api_health_head_ok_and_5xx(transport) -> None:
    transport(lambda r: httpx.Response(405 if r.method == "HEAD" else 200))
    assert await api_destination(url="https://sink.test/in").health_check() is True
    transport(lambda r: httpx.Response(502))
    assert await api_destination(url="https://sink.test/in").health_check() is False


async def test_api_health_falls_back_to_options_when_head_errors(transport) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        if r.method == "HEAD":
            raise httpx.ConnectError("no HEAD", request=r)
        return httpx.Response(200)

    seen = transport(handler)
    assert await api_destination(url="https://sink.test/in").health_check() is True
    assert [r.method for r in seen] == ["HEAD", "OPTIONS"]


async def test_api_health_down_when_both_probes_fail(transport) -> None:
    def handler(r: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=r)

    seen = transport(handler)
    assert await api_destination(url="https://sink.test/in").health_check() is False
    assert [r.method for r in seen] == ["HEAD", "OPTIONS"]


async def test_api_close_is_idempotent(transport) -> None:
    transport(lambda r: httpx.Response(200))
    dest = ApiDestination(url="https://sink.test/in")
    await dest.send([])
    await dest.close()
    await dest.close()
    assert dest._client is None


# -- destination_identity ----------------------------------------------------------


def test_identity_of_console_is_its_whole_config() -> None:
    ident = destination_identity(console_destination(max_records_display=5))
    assert ident == {
        "type": "ConsoleDestination",
        "config": {"pretty_print": True, "max_records_display": 5},
    }


def test_identity_strips_nested_credentials_and_keeps_the_target() -> None:
    dest = api_destination(
        url="https://sink.test/in",
        auth_type="basic",
        auth_token="u:p",
        body={"tenant": "t1", "client_secret": "s", "nested": {"password": "p", "k": 1}},
    )
    config = destination_identity(dest)["config"]
    assert config["url"] == "https://sink.test/in"
    assert "auth_token" not in config and "auth_type" not in config
    assert config["body"] == {"tenant": "t1", "nested": {"k": 1}}


def test_explicit_dedup_identity_wins_and_is_stripped_too() -> None:
    dest = api_destination(
        url="https://sink.test/in", dedup_identity={"url": "u", "api_key": "secret"}
    )
    assert destination_identity(dest) == {"type": "ApiDestination", "identity": {"url": "u"}}


def test_identity_ignores_a_rotated_api_key_header() -> None:
    old = api_destination(url="https://sink.test/in", headers={"X-API-Key": "old"})
    new = api_destination(url="https://sink.test/in", headers={"X-API-Key": "new"})
    assert destination_identity(old) == destination_identity(new)


def test_previous_identity_keeps_a_hyphenated_key_header() -> None:
    # Content claimed by the previous release was hashed with the X-API-Key header in
    # its identity; the worker honors that hash so an upgrade does not rewrite it.
    from reflowfy.execution.content_dedup import previous_destination_identity

    dest = api_destination(url="https://sink.test/in", headers={"X-API-Key": "k"})
    assert previous_destination_identity(dest) != destination_identity(dest)
    plain = api_destination(url="https://sink.test/in", headers={"api_key": "k"})
    assert previous_destination_identity(plain) == destination_identity(plain)
