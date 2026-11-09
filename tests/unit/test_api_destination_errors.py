"""api_destination errors: what is retried, how long it waits, what the message says.

- 4xx other than 408/425/429 → PermanentDestinationError, sent once (not retried).
- 429/503 honour Retry-After (seconds or HTTP date), capped at max_wait_seconds.
- Messages carry status, type and size — never the body; the body is a debug log,
  truncated to 200 characters. The id_based_api source formats its errors the same way.
- Health check: one retry before failing; a healthy result is cached for 30 s per host
  when the destination is pooled.
"""

import email.utils
import logging
import time
from typing import Any, Callable, List

import httpx
import pytest

import reflowfy.destinations.pool as pool_mod
from reflowfy.core.http_errors import (
    describe_response,
    is_permanent,
    log_response_body,
    retry_after_seconds,
)
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.base import (
    BaseDestination,
    DestinationError,
    PermanentDestinationError,
    RetryConfig,
    is_retryable,
)
from reflowfy.destinations.pool import loop_pool_scope

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


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> List[float]:
    """Every asyncio.sleep the retry loop asked for, without waiting."""
    waited: List[float] = []

    async def fake_sleep(seconds: float, *args: Any, **kwargs: Any) -> None:
        waited.append(seconds)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    return waited


def _resp(status: int, **kw: Any) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("POST", "https://sink.test/in"), **kw)


RETRY = RetryConfig(max_attempts=3, min_wait_seconds=1, max_wait_seconds=10, multiplier=1)


# ---------------------------------------------------------------- the helpers


class TestDescribe:
    def test_status_type_and_size(self):
        resp = _resp(404, json={"secret": "x" * 790})
        assert describe_response(resp) == (
            f"HTTP 404 Not Found (application/json, {len(resp.content)} bytes)"
        )

    def test_charset_is_dropped_and_an_empty_body_still_has_a_size(self):
        assert (
            describe_response(_resp(500, headers={"content-type": "text/html; charset=utf-8"}))
            == "HTTP 500 Internal Server Error (text/html, 0 bytes)"
        )
        assert describe_response(_resp(500)) == "HTTP 500 Internal Server Error (0 bytes)"

    def test_unknown_status_and_unread_streams(self):
        stream = httpx.Response(
            599,
            headers={"content-length": "42"},
            stream=httpx.ByteStream(b"x" * 42),
            request=httpx.Request("GET", "https://x"),
        )
        assert describe_response(stream) == "HTTP 599 (42 bytes)"

    @pytest.mark.parametrize(
        "status,permanent",
        [
            (400, True),
            (401, True),
            (404, True),
            (422, True),
            (408, False),
            (425, False),
            (429, False),
            (500, False),
            (503, False),
            (302, False),
        ],
    )
    def test_is_permanent(self, status: int, permanent: bool):
        assert is_permanent(status) is permanent

    def test_retry_after_seconds(self):
        assert retry_after_seconds(_resp(429, headers={"retry-after": "7"})) == 7.0
        assert retry_after_seconds(_resp(503, headers={"retry-after": " 2.5 "})) == 2.5
        assert retry_after_seconds(_resp(503, headers={"retry-after": "-3"})) == 0.0
        assert retry_after_seconds(_resp(503, headers={"retry-after": "nan"})) is None
        assert retry_after_seconds(_resp(503, headers={"retry-after": "soon"})) is None
        assert retry_after_seconds(_resp(503)) is None
        assert retry_after_seconds(_resp(500, headers={"retry-after": "7"})) is None  # not 429/503

    def test_retry_after_http_date(self):
        future = email.utils.formatdate(time.time() + 120, usegmt=True)
        waited = retry_after_seconds(_resp(429, headers={"retry-after": future}))
        assert waited is not None and 100 < waited <= 120
        past = email.utils.formatdate(time.time() - 120, usegmt=True)
        assert retry_after_seconds(_resp(429, headers={"retry-after": past})) == 0.0
        naive = "Mon, 01 Jan 2001 00:00:00 -0000"  # no zone: read as UTC
        assert retry_after_seconds(_resp(429, headers={"retry-after": naive})) == 0.0

    def test_body_is_logged_at_debug_only_and_truncated(self, caplog: pytest.LogCaptureFixture):
        logger = logging.getLogger("test.http_errors")
        resp = _resp(400, text="y" * 500)
        logger.setLevel(logging.INFO)
        with caplog.at_level(logging.INFO, logger="test.http_errors"):
            log_response_body(logger, resp, "owner")
        assert caplog.records == []
        logger.setLevel(logging.DEBUG)
        with caplog.at_level(logging.DEBUG, logger="test.http_errors"):
            log_response_body(logger, resp, "owner")
        (record,) = caplog.records
        assert record.getMessage().endswith(": " + "y" * 200)

    def test_an_unreadable_body_is_not_logged(self, caplog: pytest.LogCaptureFixture):
        logger = logging.getLogger("test.http_errors.unread")
        logger.setLevel(logging.DEBUG)
        resp = httpx.Response(
            400, stream=httpx.ByteStream(b"x"), request=httpx.Request("GET", "https://x")
        )
        with caplog.at_level(logging.DEBUG, logger="test.http_errors.unread"):
            log_response_body(logger, resp, "owner")
        assert caplog.records == []


# ---------------------------------------------------------------- retries


class TestRetries:
    async def test_a_4xx_is_permanent_and_sent_once(self, transport, sleeps):
        seen = transport(lambda r: httpx.Response(404, json={"error": "no such thing"}))
        dest = api_destination(url="https://sink.test/in", body={"a": 1}, retry_config=RETRY)
        with pytest.raises(PermanentDestinationError) as exc:
            await dest.send_with_retry([])
        assert len(seen) == 1 and sleeps == []
        message = str(exc.value)
        assert "HTTP 404 Not Found (application/json, 25 bytes)" in message
        assert "not retried" in message and "no such thing" not in message

    @pytest.mark.parametrize("status", [408, 425, 429, 500, 502])
    async def test_retryable_statuses_are_retried(self, transport, sleeps, status: int):
        seen = transport(lambda r: httpx.Response(status))
        dest = api_destination(url="https://sink.test/in", body={}, retry_config=RETRY)
        with pytest.raises(DestinationError) as exc:
            await dest.send_with_retry([])
        assert not isinstance(exc.value, PermanentDestinationError)
        assert len(seen) == 3

    async def test_retry_after_is_honoured(self, transport, sleeps):
        statuses = iter([_r(429, "3"), _r(503, "4"), httpx.Response(200)])
        seen = transport(lambda r: next(statuses))
        dest = api_destination(url="https://sink.test/in", body={}, retry_config=RETRY)
        await dest.send_with_retry([])
        assert len(seen) == 3 and sleeps == [3.0, 4.0]

    async def test_retry_after_is_capped_at_max_wait(self, transport, sleeps):
        statuses = iter([_r(429, "3600"), httpx.Response(200)])
        transport(lambda r: next(statuses))
        dest = api_destination(url="https://sink.test/in", body={}, retry_config=RETRY)
        await dest.send_with_retry([])
        assert sleeps == [10.0]

    async def test_without_retry_after_the_backoff_applies(self, transport, sleeps):
        statuses = iter([httpx.Response(503), httpx.Response(200)])
        transport(lambda r: next(statuses))
        dest = api_destination(url="https://sink.test/in", body={}, retry_config=RETRY)
        await dest.send_with_retry([])
        assert sleeps == [1.0]

    async def test_per_record_keeps_the_class_and_retry_after(self, transport, sleeps):
        statuses = iter([_r(429, "2"), httpx.Response(200), httpx.Response(200)])
        seen = transport(lambda r: next(statuses))
        dest = api_destination(url="https://sink.test/{id}", per_record=True, retry_config=RETRY)
        await dest.send_with_retry([{"id": 1}])
        assert sleeps == [2.0] and len(seen) == 2

        transport(lambda r: httpx.Response(403))
        dest = api_destination(url="https://sink.test/{id}", per_record=True, retry_config=RETRY)
        with pytest.raises(PermanentDestinationError, match=r"record 1 of 1: HTTP 403 Forbidden"):
            await dest.send_with_retry([{"id": 1}])

    def test_is_retryable(self):
        assert is_retryable(DestinationError("x", "y"))
        assert not is_retryable(PermanentDestinationError("x", "y"))
        assert not is_retryable(ValueError("x"))

    def test_errors_keep_their_reason_and_retry_after(self):
        err = DestinationError("api", "HTTP 503", retry_after=5)
        assert (err.reason, err.retry_after) == ("HTTP 503", 5)
        assert isinstance(PermanentDestinationError("api", "x"), DestinationError)


def _r(status: int, retry_after: str) -> httpx.Response:
    return httpx.Response(status, headers={"retry-after": retry_after})


# ---------------------------------------------------------------- health check


class TestHealth:
    async def test_one_retry_recovers(self, transport, sleeps):
        statuses = iter([503, 200])
        seen = transport(lambda r: httpx.Response(next(statuses)))
        assert await api_destination(url="https://sink.test/in").health_check()
        assert [r.method for r in seen] == ["HEAD", "HEAD"]
        assert sleeps == [1.0]

    async def test_cached_per_host_for_30s_in_a_pool(self, transport, monkeypatch):
        seen = transport(lambda r: httpx.Response(200))
        now = [1000.0]
        monkeypatch.setattr(pool_mod.time, "monotonic", lambda: now[0])
        async with loop_pool_scope():
            for path in ("/a", "/b", "/c"):  # three destinations, one host
                dest = api_destination(url=f"https://sink.test{path}")
                assert await dest.health_check()
                await dest.close()
            assert len(seen) == 1
            now[0] += 31
            assert await api_destination(url="https://sink.test/d").health_check()
            assert len(seen) == 2

    async def test_an_unhealthy_result_is_not_cached(self, transport, sleeps):
        statuses = iter([503, 503, 200])
        seen = transport(lambda r: httpx.Response(next(statuses)))
        async with loop_pool_scope():
            dest = api_destination(url="https://sink.test/in")
            assert not await dest.health_check()
            assert await dest.health_check()  # probed again: the host came back
            assert await dest.health_check()  # now cached
        assert len(seen) == 3

    async def test_different_hosts_are_probed_separately(self, transport):
        seen = transport(lambda r: httpx.Response(200))
        async with loop_pool_scope():
            assert await api_destination(url="https://a.test/x").health_check()
            assert await api_destination(url="https://b.test/x").health_check()
        assert sorted(r.url.host for r in seen) == ["a.test", "b.test"]

    async def test_no_key_means_no_cache(self):
        class NoKey(BaseDestination):
            probes = 0

            async def send(self, records: Any, metadata: Any = None) -> None: ...

            async def health_check(self) -> bool:
                async def probe() -> bool:
                    NoKey.probes += 1
                    return True

                return await self._cached_health(probe)

        async with loop_pool_scope():
            dest = NoKey({})
            await dest.health_check()
            await dest.health_check()
        assert NoKey.probes == 2


# ---------------------------------------------------------------- the id_based_api source


def test_source_errors_carry_no_body(monkeypatch: pytest.MonkeyPatch):
    from reflowfy.sources import id_based_api_source
    from reflowfy.sources.api import logger as source_logger
    from reflowfy.sources.base import SourceError
    from test_worker_executor_paths import capture_logs  # type: ignore[import-not-found]

    real = httpx.Client
    mock = httpx.MockTransport(
        lambda r: httpx.Response(500, text="stack trace with password=hunter2")
    )
    monkeypatch.setattr(
        httpx, "Client", lambda **kw: real(transport=mock, **kw)  # type: ignore[misc]
    )
    with capture_logs(source_logger.name) as logs:
        with pytest.raises(SourceError) as exc:
            id_based_api_source("https://api.test", "/b").fetch({})
    assert str(exc.value).endswith("HTTP 500 Internal Server Error (text/plain, 33 bytes)")
    assert "hunter2" not in str(exc.value)
    assert any("hunter2" in m for m in logs)  # at debug level only
