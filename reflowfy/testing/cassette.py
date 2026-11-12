"""Freezing what the outside world answers while a pipeline runs under test.

A transformation that calls an HTTP API, or fetches from a source of its own, makes
a test depend on a live service. A :class:`Session` stands between the pipeline and
that service for the length of a run:

- **replay** (the default in tests): a request is answered from a mock or from a
  recorded fixture, and one that is neither raises :class:`UnmockedCallError`. A
  test therefore never reaches the network by accident.
- **record** (``reflowfy test --save-fixture``): requests go out for real and the
  answers are kept, to be written into the fixture.
- **passthrough** (``allow_network=True``): mocks are served, everything else is
  real.

Two layers are covered. HTTP is intercepted at ``httpx``'s transports and at
``urllib3``'s connection pool, which is what ``requests``, ``elasticsearch`` and
``boto3`` send through. ``fetch()`` on every :class:`BaseSource` subclass is
intercepted too, so a source used inside a step is answered without building its
client (and covers sources that are not HTTP at all, like SQL).

Not covered: ``aiohttp``, raw sockets and database drivers used directly. Freeze
such a step instead (``--freeze-step``).
"""

import base64
import copy
import hashlib
import io
import json as _json
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Generator,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Pattern,
    Set,
    Tuple,
    Union,
)

import httpx
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.response import HTTPResponse

from reflowfy.core.serialization import to_json_safe
from reflowfy.core.types import wrap_as_record
from reflowfy.execution.content_dedup import without_secrets
from reflowfy.sources.base import BaseSource


class UnmockedCallError(Exception):
    """The pipeline reached for something outside the test that nothing answers for."""


class FixtureError(Exception):
    """A fixture could not be recorded, read or replayed."""


class RawResponse(NamedTuple):
    """An HTTP response reduced to what both client libraries can be rebuilt from."""

    status: int
    headers: Dict[str, str]
    body: bytes


RequestKey = Tuple[str, str, Optional[str]]

# The body is stored decoded and whole, so these no longer describe it.
_FRAMING_HEADERS = {"content-encoding", "content-length", "transfer-encoding", "connection"}
# Never written to a fixture: session state, or a checksum of the body as it was.
_UNSTORED_HEADERS = {"set-cookie", "date", "www-authenticate", "proxy-authenticate", "content-md5"}
_UNSTORED_PREFIXES = ("x-amz-checksum-",)


def body_hash(body: Optional[bytes]) -> Optional[str]:
    """SHA-256 of a request body, or ``None`` for no body."""
    if not body:
        return None
    return hashlib.sha256(body).hexdigest()


def encode_body(body: bytes, content_type: str) -> Dict[str, Any]:
    """A response body in the most readable form JSON can hold it.

    Parsed JSON where it is JSON (so a person can read, edit and redact it), text
    where it is text, base64 otherwise.
    """
    if not body:
        return {"body": None}
    if "json" in content_type.lower():
        try:
            parsed = _json.loads(body)
        except ValueError:
            parsed = None
        if parsed is not None:
            return {"body": parsed}
    try:
        return {"body": body.decode("utf-8"), "body_encoding": "text"}
    except UnicodeDecodeError:
        return {"body": base64.b64encode(body).decode("ascii"), "body_encoding": "base64"}


def decode_body(response: Dict[str, Any]) -> bytes:
    """The bytes a stored response body stands for."""
    body = response.get("body")
    if body is None:
        return b""
    encoding = response.get("body_encoding", "json")
    if encoding == "base64":
        return base64.b64decode(body)
    if encoding == "text":
        return str(body).encode("utf-8")
    return _json.dumps(body).encode("utf-8")


def source_key(source: Any) -> Tuple[str, str]:
    """What identifies a source call: its type, and a hash of its config.

    Credentials are left out of the hash, so rotating a password does not
    invalidate a fixture. A lookup whose query depends on the record gets a key
    of its own, because the query is part of the config.
    """
    config = without_secrets(to_json_safe(getattr(source, "config", None)))
    canonical = _json.dumps(config, sort_keys=True, default=str)
    return type(source).__name__, hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


@dataclass
class HttpCall:
    """One request the pipeline made, kept for assertions."""

    method: str
    url: str
    body: Optional[bytes]
    step: Optional[str]

    def json(self) -> Any:
        """The request body parsed as JSON (``None`` when there was no body)."""
        return _json.loads(self.body) if self.body else None


@dataclass
class _Route:
    method: str
    url: Union[str, Pattern[str]]
    response: RawResponse

    def matches(self, method: str, url: str) -> bool:
        if method != self.method:
            return False
        if isinstance(self.url, str):
            return url == self.url
        return self.url.search(url) is not None


class MockHttp:
    """Canned HTTP answers for a test.

    Example:
        >>> def test_enrich(mock_http):
        ...     mock_http.get("https://crm.internal/customers/7", json={"tier": "gold"})
        ...     result = run_pipeline("orders", records=[{"customer_id": 7}])
        ...     assert mock_http.calls[0].url.endswith("/customers/7")

    A URL is matched exactly (query string included), or by a compiled regular
    expression. The same answer is served every time the request is made.
    """

    def __init__(self) -> None:
        self._routes: List[_Route] = []
        self.calls: List[HttpCall] = []

    def add(
        self,
        method: str,
        url: Union[str, Pattern[str]],
        *,
        status: int = 200,
        json: Any = None,
        text: Optional[str] = None,
        body: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Answer ``method url`` with the given status and body."""
        response_headers = {k.lower(): v for k, v in (headers or {}).items()}
        if body is not None:
            payload = body
        elif text is not None:
            payload = text.encode("utf-8")
            response_headers.setdefault("content-type", "text/plain; charset=utf-8")
        elif json is not None:
            payload = _json.dumps(json).encode("utf-8")
            response_headers.setdefault("content-type", "application/json")
        else:
            payload = b""
        self._routes.append(
            _Route(method.upper(), url, RawResponse(status, response_headers, payload))
        )

    def get(self, url: Union[str, Pattern[str]], **response: Any) -> None:
        """Answer ``GET url``. Keywords as for :meth:`add`."""
        self.add("GET", url, **response)

    def post(self, url: Union[str, Pattern[str]], **response: Any) -> None:
        """Answer ``POST url``. Keywords as for :meth:`add`."""
        self.add("POST", url, **response)

    def put(self, url: Union[str, Pattern[str]], **response: Any) -> None:
        """Answer ``PUT url``. Keywords as for :meth:`add`."""
        self.add("PUT", url, **response)

    def patch(self, url: Union[str, Pattern[str]], **response: Any) -> None:
        """Answer ``PATCH url``. Keywords as for :meth:`add`."""
        self.add("PATCH", url, **response)

    def delete(self, url: Union[str, Pattern[str]], **response: Any) -> None:
        """Answer ``DELETE url``. Keywords as for :meth:`add`."""
        self.add("DELETE", url, **response)

    def find(self, method: str, url: str) -> Optional[RawResponse]:
        """The answer registered for this request; the latest registration wins."""
        for route in reversed(self._routes):
            if route.matches(method, url):
                return route.response
        return None


@dataclass
class SourceCall:
    """One ``fetch()`` a step made on a source of its own."""

    source_type: str
    config: Any
    step: Optional[str]


@dataclass
class _SourceStub:
    source_type: Union[type, str]
    records: List[Any]
    where: Optional[Callable[[Dict[str, Any]], bool]]

    def matches(self, source: Any) -> bool:
        if isinstance(self.source_type, str):
            names = {type(source).__name__, str(getattr(source, "registry_type", ""))}
            if self.source_type not in names:
                return False
        elif not isinstance(source, self.source_type):
            return False
        return self.where is None or bool(self.where(getattr(source, "config", {})))


class MockSources:
    """Canned records for sources a transformation fetches from itself.

    Example:
        >>> def test_invoices(mock_source):
        ...     mock_source(ElasticSource, records=[{"invoice_id": "A1"}])
        ...     result = run_pipeline("orders", records=[{"id": 1}])

    ``where`` narrows a stub to some configs (``where=lambda c: c["index"] ==
    "invoices"``). Records are wrapped as ``{"data": record}``, the shape built-in
    sources return; pass ``wrap=False`` to return them as they are.
    """

    def __init__(self) -> None:
        self._stubs: List[_SourceStub] = []
        self.calls: List[SourceCall] = []

    def __call__(
        self,
        source_type: Union[type, str],
        records: List[Any],
        *,
        where: Optional[Callable[[Dict[str, Any]], bool]] = None,
        wrap: bool = True,
    ) -> None:
        kept = [wrap_as_record(r) for r in records] if wrap else list(records)
        self._stubs.append(_SourceStub(source_type, kept, where))

    def find(self, source: Any) -> Optional[List[Any]]:
        """The records stubbed for this source; the latest matching stub wins."""
        for stub in reversed(self._stubs):
            if stub.matches(source):
                return copy.deepcopy(stub.records)
        return None


class Session:
    """What answers for the outside world during one run (see the module docstring)."""

    def __init__(
        self,
        mode: str = "replay",
        *,
        http: Optional[List[Dict[str, Any]]] = None,
        source_calls: Optional[List[Dict[str, Any]]] = None,
        strict_sources: bool = False,
        max_body_bytes: Optional[int] = None,
    ) -> None:
        if mode not in ("replay", "record", "passthrough"):
            raise ValueError(f"unknown mode {mode!r}")
        self.mode = mode
        self.http = MockHttp()
        self.sources = MockSources()
        self.strict_sources = strict_sources
        self.max_body_bytes = max_body_bytes
        self.recorded_http: List[Dict[str, Any]] = []
        self.recorded_source_calls: List[Dict[str, Any]] = []
        # Set by the runner. ``in_job`` is true from the moment a job's records are
        # fetched until the job ends: only what happens there is recorded, so the
        # main fetch and the planning never end up in a fixture twice.
        self.current_step: Optional[str] = None
        self.in_job = False
        # True while a frozen step runs during recording: its output is what the
        # fixture keeps, so its calls are neither needed nor wanted in the file.
        self.paused = False
        self.main_sources: Set[int] = set()
        self.main_fetches: Dict[int, List[Any]] = {}
        self._lock = threading.Lock()
        self._http_queue: Dict[RequestKey, Deque[Dict[str, Any]]] = {}
        self._source_queue: Dict[Tuple[str, str], Deque[List[Any]]] = {}
        for exchange in http or []:
            request = exchange["request"]
            key = (str(request["method"]).upper(), str(request["url"]), request.get("body_sha256"))
            self._http_queue.setdefault(key, deque()).append(exchange["response"])
        for call in source_calls or []:
            source = call["source"]
            self._source_queue.setdefault(
                (str(source["type"]), str(source["config_hash"])), deque()
            ).append(call["records"])

    # -- shared --------------------------------------------------------------

    def _unmocked(self, what: str, fix: str) -> UnmockedCallError:
        who = f"step '{self.current_step}'" if self.current_step else "the pipeline"
        return UnmockedCallError(f"{who} made {what}, which nothing in this test answers. {fix}")

    @staticmethod
    def _next(queue: Deque[Any]) -> Any:
        # The last recorded answer is kept, so a request repeated more often than
        # it was recorded (a retry) still gets the answer it got then.
        return queue[0] if len(queue) == 1 else queue.popleft()

    # -- HTTP ----------------------------------------------------------------

    def http_lookup(self, method: str, url: str, body: Optional[bytes]) -> Optional[RawResponse]:
        """The canned answer for a request, or ``None`` to send it for real."""
        self.http.calls.append(HttpCall(method, url, body, self.current_step))
        mocked = self.http.find(method, url)
        if mocked is not None:
            return mocked
        with self._lock:
            queue = self._http_queue.get((method, url, body_hash(body)))
            stored = self._next(queue) if queue else None
        if stored is not None:
            return RawResponse(
                int(stored.get("status", 200)), dict(stored.get("headers") or {}), decode_body(stored)
            )
        if self.mode == "replay":
            raise self._unmocked(
                f"{method} {url}",
                "Mock it (mock_http), re-record the fixture, or pass allow_network=True.",
            )
        return None

    def records_http(self) -> bool:
        """Whether a response that came back just now belongs in the fixture."""
        return self._recording() and not getattr(_local, "in_source_call", False)

    def _recording(self) -> bool:
        return self.mode == "record" and self.in_job and not self.paused

    def http_record(
        self, method: str, url: str, body: Optional[bytes], response: RawResponse
    ) -> RawResponse:
        """Keep one real exchange; returns the response to hand back to the caller."""
        if self.max_body_bytes is not None and len(response.body) > self.max_body_bytes:
            raise FixtureError(
                f"the response to {method} {url} is {len(response.body) // 1024} KB, over the "
                f"{self.max_body_bytes // 1024} KB a fixture stores per response. Freeze the "
                "step that makes this call (--freeze-step), or raise --max-body-kb."
            )
        headers = {k.lower(): v for k, v in response.headers.items()}
        kept = {k: v for k, v in headers.items() if k not in _FRAMING_HEADERS}
        stored_headers = {
            k: v
            for k, v in kept.items()
            if k not in _UNSTORED_HEADERS and not k.startswith(_UNSTORED_PREFIXES)
        }
        stored: Dict[str, Any] = {"status": response.status, "headers": stored_headers}
        stored.update(encode_body(response.body, headers.get("content-type", "")))
        with self._lock:
            self.recorded_http.append(
                {
                    "request": {"method": method, "url": url, "body_sha256": body_hash(body)},
                    "response": stored,
                }
            )
        return RawResponse(response.status, kept, response.body)

    # -- sources -------------------------------------------------------------

    def source_fetch(self, source: Any, fetch: Callable[[], Any]) -> Any:
        """Answer a ``fetch()`` made from inside a step, or let it through."""
        self.sources.calls.append(
            SourceCall(type(source).__name__, getattr(source, "config", None), self.current_step)
        )
        stubbed = self.sources.find(source)
        if stubbed is not None:
            return stubbed
        key = source_key(source)
        with self._lock:
            queue = self._source_queue.get(key)
            stored = copy.deepcopy(self._next(queue)) if queue else None
        if stored is not None:
            return stored
        if self.mode == "replay" and self.strict_sources and self.in_job:
            raise self._unmocked(
                f"a fetch on {key[0]}",
                "Re-record the fixture, or stub it (mock_source).",
            )
        if not self._recording():
            return fetch()
        # Its HTTP is part of this call: storing both would only double the file.
        _local.in_source_call = True
        try:
            records = to_json_safe(fetch())
        finally:
            _local.in_source_call = False
        with self._lock:
            self.recorded_source_calls.append(
                {
                    "step": self.current_step,
                    "source": {"type": key[0], "config_hash": key[1]},
                    "records": records,
                }
            )
        return copy.deepcopy(records)


# ---------------------------------------------------------------------------
# Patching
# ---------------------------------------------------------------------------

_local = threading.local()
_active: Optional[Session] = None
_install_lock = threading.Lock()
_patched_sources: Dict[type, Any] = {}

_httpx_sync = httpx.HTTPTransport.handle_request
_httpx_async = httpx.AsyncHTTPTransport.handle_async_request
_urllib3_urlopen = HTTPConnectionPool.urlopen


def current_session() -> Optional[Session]:
    """The session answering for the outside world right now, if any."""
    return _active


def _to_httpx(raw: RawResponse) -> httpx.Response:
    return httpx.Response(raw.status, headers=raw.headers, content=raw.body)


def _sync_handle(self: httpx.HTTPTransport, request: httpx.Request) -> httpx.Response:
    session = _active
    if session is None:
        return _httpx_sync(self, request)
    method, url, body = request.method, str(request.url), request.read()
    found = session.http_lookup(method, url, body)
    if found is not None:
        return _to_httpx(found)
    response = _httpx_sync(self, request)
    if not session.records_http():
        return response
    try:
        response.read()
    finally:
        response.close()
    raw = RawResponse(response.status_code, dict(response.headers), response.content)
    return _to_httpx(session.http_record(method, url, body, raw))


async def _async_handle(self: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
    session = _active
    if session is None:
        return await _httpx_async(self, request)
    method, url, body = request.method, str(request.url), await request.aread()
    found = session.http_lookup(method, url, body)
    if found is not None:
        return _to_httpx(found)
    response = await _httpx_async(self, request)
    if not session.records_http():
        return response
    try:
        await response.aread()
    finally:
        await response.aclose()
    raw = RawResponse(response.status_code, dict(response.headers), response.content)
    return _to_httpx(session.http_record(method, url, body, raw))


def _pool_url(pool: HTTPConnectionPool, url: str) -> str:
    if url.startswith(("http://", "https://")):
        return url
    default_port = 443 if pool.scheme == "https" else 80
    port = "" if pool.port in (None, default_port) else f":{pool.port}"
    return f"{pool.scheme}://{pool.host}{port}{url}"


def _to_urllib3(raw: RawResponse, method: str, preload: bool) -> HTTPResponse:
    headers = dict(raw.headers)
    headers["content-length"] = str(len(raw.body))
    return HTTPResponse(
        body=io.BytesIO(raw.body),
        headers=headers,
        status=raw.status,
        preload_content=preload,
        decode_content=False,
        request_method=method,
    )


def _urlopen(
    self: HTTPConnectionPool,
    method: str,
    url: str,
    body: Any = None,
    headers: Any = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    session = _active
    # urlopen calls itself to retry and to follow a redirect: only the outer call counts.
    if session is None or getattr(_local, "in_urlopen", False):
        return _urllib3_urlopen(self, method, url, body, headers, *args, **kwargs)
    verb = method.upper()
    full_url = _pool_url(self, url)
    given: Any = body  # narrowed below; `body` itself goes on to urllib3 untouched
    if isinstance(given, str):
        sent: Optional[bytes] = given.encode("utf-8")
    else:
        sent = bytes(given) if isinstance(given, (bytes, bytearray)) else None
    preload = bool(kwargs.get("preload_content", True))
    found = session.http_lookup(verb, full_url, sent)
    if found is not None:
        return _to_urllib3(found, verb, preload)
    if not session.records_http():
        return _urllib3_urlopen(self, method, url, body, headers, *args, **kwargs)
    _local.in_urlopen = True
    try:
        response = _urllib3_urlopen(
            self, method, url, body, headers, *args, **{**kwargs, "preload_content": False}
        )
        try:
            data = response.read(decode_content=True)
        finally:
            response.release_conn()
    finally:
        _local.in_urlopen = False
    raw = RawResponse(response.status, {k: v for k, v in response.headers.items()}, data)
    return _to_urllib3(session.http_record(verb, full_url, sent, raw), verb, preload)


def _wrap_fetch(original: Callable[..., Any]) -> Callable[..., Any]:
    def fetch(self: Any, *args: Any, **kwargs: Any) -> Any:
        session = _active
        if session is None or getattr(_local, "in_fetch", False):
            return original(self, *args, **kwargs)
        _local.in_fetch = True
        try:
            if id(self) in session.main_sources:
                # The job's own source: let it run, and keep what it returned.
                records = original(self, *args, **kwargs)
                session.main_fetches[id(self)] = records
                return records
            return session.source_fetch(self, lambda: original(self, *args, **kwargs))
        finally:
            _local.in_fetch = False

    return fetch


def _all_subclasses(cls: type) -> Iterator[type]:
    for sub in cls.__subclasses__():
        yield sub
        yield from _all_subclasses(sub)


def patch_sources() -> None:
    """Intercept ``fetch`` on every source class defined so far.

    Called when a session starts and again before each job, so a source class
    created in between (a ``@source`` factory, a class defined in the test) is
    covered too.
    """
    with _install_lock:
        if _active is None:
            return
        for cls in _all_subclasses(BaseSource):
            if cls in _patched_sources or "fetch" not in cls.__dict__:
                continue
            original = cls.__dict__["fetch"]
            _patched_sources[cls] = original
            setattr(cls, "fetch", _wrap_fetch(original))


@contextmanager
def activate(session: Session) -> Generator[Session, None, None]:
    """Put ``session`` between the pipeline and the outside world.

    One at a time: a run started inside an active session (``run_pipeline`` under
    the ``mock_http`` fixture) uses that session instead of starting its own.
    """
    global _active
    with _install_lock:
        if _active is not None:
            raise RuntimeError("a reflowfy.testing session is already active")
        _active = session
        setattr(httpx.HTTPTransport, "handle_request", _sync_handle)
        setattr(httpx.AsyncHTTPTransport, "handle_async_request", _async_handle)
        setattr(HTTPConnectionPool, "urlopen", _urlopen)
    patch_sources()
    try:
        yield session
    finally:
        with _install_lock:
            _active = None
            setattr(httpx.HTTPTransport, "handle_request", _httpx_sync)
            setattr(httpx.AsyncHTTPTransport, "handle_async_request", _httpx_async)
            setattr(HTTPConnectionPool, "urlopen", _urllib3_urlopen)
            for cls, original in _patched_sources.items():
                setattr(cls, "fetch", original)
            _patched_sources.clear()


@contextmanager
def mock_network(allow_network: bool = False) -> Generator[Session, None, None]:
    """A session for a hand-written test, outside pytest.

    Example:
        >>> with mock_network() as net:
        ...     net.http.get("https://geo.internal/ip/1.2.3.4", json={"country": "IL"})
        ...     result = run_pipeline("users", records=[{"ip": "1.2.3.4"}])
    """
    with activate(Session("passthrough" if allow_network else "replay")) as session:
        yield session
