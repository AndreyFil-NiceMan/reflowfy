"""aiohttp support for :mod:`reflowfy.testing.cassette` (used only if aiohttp is installed).

aiohttp does not send through httpx or urllib3, so it gets a patch of its own, on
``ClientSession._request``: every ``session.get/post/...``, ``session.request`` and
the module-level ``aiohttp.request`` go through it.

A replayed or mocked answer is a :class:`ReplayedResponse`, not an
``aiohttp.ClientResponse``: building a real one needs a live connection and
private constructor arguments that change between aiohttp releases. It supports
what pipeline code uses: ``status``, ``reason``, ``ok``, ``headers``, ``url``,
``content_type``, ``read()``, ``text()``, ``json()``, ``raise_for_status()``,
``async with``, and ``content.read()`` / ``iter_chunked()`` / ``async for``.
``isinstance(response, aiohttp.ClientResponse)`` is false.
"""

import json as _json
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, cast
from urllib.parse import urlencode

import aiohttp
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

if TYPE_CHECKING:
    from reflowfy.testing.cassette import RawResponse, Session

_original_request = aiohttp.ClientSession._request  # pyright: ignore[reportPrivateUsage]


class _Content:
    """The body as a stream, for code that reads ``response.content``."""

    def __init__(self, body: bytes) -> None:
        self._body = body
        self._position = 0

    async def read(self, n: int = -1) -> bytes:
        end = len(self._body) if n < 0 else self._position + n
        chunk = self._body[self._position : end]
        self._position += len(chunk)
        return chunk

    async def readany(self) -> bytes:
        return await self.read()

    async def readline(self) -> bytes:
        newline = self._body.find(b"\n", self._position)
        end = len(self._body) if newline < 0 else newline + 1
        return await self.read(end - self._position)

    def at_eof(self) -> bool:
        return self._position >= len(self._body)

    async def iter_chunked(self, n: int) -> AsyncIterator[bytes]:
        while not self.at_eof():
            yield await self.read(n)

    async def iter_any(self) -> AsyncIterator[bytes]:
        if not self.at_eof():
            yield await self.read()

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._lines()

    async def _lines(self) -> AsyncIterator[bytes]:
        while not self.at_eof():
            yield await self.readline()


class ReplayedResponse:
    """A recorded or mocked answer, shaped like ``aiohttp.ClientResponse``."""

    def __init__(self, method: str, url: str, raw: "RawResponse") -> None:
        self.method = method
        self.url = URL(url)
        self.status = raw.status
        self.reason = _reason(raw.status)
        self.headers = CIMultiDictProxy(CIMultiDict(raw.headers))
        self.content = _Content(raw.body)
        self.closed = False
        self._body = raw.body

    @property
    def ok(self) -> bool:
        return self.status < 400

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "application/octet-stream").split(";")[0].strip()

    @property
    def charset(self) -> Optional[str]:
        for part in self.headers.get("content-type", "").split(";")[1:]:
            key, _, value = part.strip().partition("=")
            if key.lower() == "charset":
                return value.strip('"')
        return None

    async def read(self) -> bytes:
        return self._body

    async def text(self, encoding: Optional[str] = None, errors: str = "strict") -> str:
        return self._body.decode(encoding or self.charset or "utf-8", errors)

    async def json(
        self,
        *,
        encoding: Optional[str] = None,
        loads: Callable[[str], Any] = _json.loads,
        content_type: Optional[str] = "application/json",
    ) -> Any:
        if not self._body.strip():
            return None
        if content_type is not None and content_type not in self.content_type:
            raise aiohttp.ContentTypeError(
                self._request_info(),
                (),
                status=self.status,
                message=f"Attempt to decode JSON with unexpected mimetype: {self.content_type}",
                headers=self.headers,
            )
        return loads(await self.text(encoding))

    def raise_for_status(self) -> None:
        if not self.ok:
            raise aiohttp.ClientResponseError(
                self._request_info(),
                (),
                status=self.status,
                message=self.reason,
                headers=self.headers,
            )

    def _request_info(self) -> aiohttp.RequestInfo:
        empty: "CIMultiDict[str]" = CIMultiDict()
        return aiohttp.RequestInfo(self.url, self.method, CIMultiDictProxy(empty), self.url)

    def release(self) -> None:
        self.closed = True

    def close(self) -> None:
        self.closed = True

    async def wait_for_close(self) -> None:
        self.closed = True

    async def __aenter__(self) -> "ReplayedResponse":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.release()

    def __repr__(self) -> str:
        return f"<ReplayedResponse({self.url}) [{self.status} {self.reason}]>"


def _reason(status: int) -> str:
    from http import HTTPStatus

    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return ""


def _request_body(session: aiohttp.ClientSession, kwargs: Dict[str, Any]) -> Optional[bytes]:
    """The bytes aiohttp will send, as far as they can be known before sending.

    A stream or ``FormData`` body cannot be read without consuming it, so it does
    not count: requests that differ only in such a body match each other.
    """
    if kwargs.get("json") is not None:
        serialize = getattr(session, "_json_serialize", _json.dumps)
        return str(serialize(kwargs["json"])).encode("utf-8")
    data = kwargs.get("data")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, dict):
        fields = cast(Dict[str, Any], data)
        return urlencode(fields, doseq=True).encode("utf-8")
    return None


def _request_url(session: aiohttp.ClientSession, str_or_url: Any, params: Any) -> str:
    url: URL = session._build_url(str_or_url)  # pyright: ignore[reportPrivateUsage]
    if params:
        url = url.extend_query(params)
    return str(url)


async def _request(
    self: aiohttp.ClientSession, method: str, str_or_url: Any, **kwargs: Any
) -> Any:
    from reflowfy.testing.cassette import RawResponse, current_session

    session: Optional["Session"] = current_session()
    if session is None:
        return await _original_request(self, method, str_or_url, **kwargs)
    verb = method.upper()
    url = _request_url(self, str_or_url, kwargs.get("params"))
    body = _request_body(self, kwargs)
    found = session.http_lookup(verb, url, body)
    if found is not None:
        response = ReplayedResponse(verb, url, found)
        await _raise_if_asked(self, kwargs, response)
        return response
    real = await _original_request(self, method, str_or_url, **kwargs)
    if session.records_http():
        data = await real.read()  # kept on the response: the caller can still read it
        headers = {k: v for k, v in real.headers.items()}
        session.http_record(verb, url, body, RawResponse(real.status, headers, data))
    return real


async def _raise_if_asked(
    client: aiohttp.ClientSession, kwargs: Dict[str, Any], response: ReplayedResponse
) -> None:
    check = kwargs.get("raise_for_status")
    if check is None:
        check = getattr(client, "_raise_for_status", False)
    if check is True:
        response.raise_for_status()
    elif callable(check):
        hook = cast(Callable[[Any], Awaitable[None]], check)
        await hook(response)


def install() -> None:
    setattr(aiohttp.ClientSession, "_request", _request)


def uninstall() -> None:
    setattr(aiohttp.ClientSession, "_request", _original_request)


__all__: List[str] = ["ReplayedResponse", "install", "uninstall"]
