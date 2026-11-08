"""API destination for webhooks and REST endpoints."""

import json
from typing import Any, Callable, Dict, List, Mapping, NamedTuple, Optional, Union, cast
from urllib.parse import quote

import httpx

from reflowfy.core.async_utils import for_each_concurrently
from reflowfy.core.record_templates import fill, fill_value, has_placeholders
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig
from reflowfy.http_auth import build_auth_headers
from reflowfy.observability.logging import get_logger

logger = get_logger(__name__)

ApiHeadersFn = Callable[[Any], Optional[Mapping[str, str]]]
"""Per-record request headers: called with each record, returns that request's headers."""

ApiHeaders = Union[Mapping[str, str], ApiHeadersFn]
"""What ``api_destination(headers=...)`` takes: a mapping (``{field}`` templates allowed
in per-record mode) or, in per-record mode, a function of the record."""


class _Request(NamedTuple):
    """One per-record request, fully built before anything is sent."""

    url: str
    payload: Any
    params: Optional[Dict[str, Any]]
    headers: Optional[Dict[str, str]]


class ApiDestination(BaseDestination):
    """
    API destination for sending data to webhooks and REST endpoints.

    The request ``body`` is sent verbatim — build it yourself in
    ``define_destination(records, runtime_params)``. A ``dict``/``list`` is sent
    as JSON (``Content-Type: application/json``); a ``str`` or ``bytes`` is sent
    as a raw body (set your own ``Content-Type`` via ``headers``). ``body=None``
    sends a request with no body. Each ``send()`` issues exactly one request.

    With ``per_record=True`` it works like the Kafka destination instead: one
    request per record, the record itself as the body (or only
    ``record[body_field]`` when ``body_field`` is set and present on the record),
    and ``{field}`` placeholders in ``url`` filled from each record
    (URL-encoded), e.g. ``url="https://api.example.com/users/{user_id}"``.
    Query parameters are per record too: ``{field}`` placeholders in ``params``
    values are filled the same way, and ``params_field="query"`` merges the dict
    in ``record["query"]`` over ``params`` (and leaves that field out of the
    body), and so are headers: ``{field}`` in a ``headers`` value, or a function
    of the record. Up to 16 requests are in flight at once (``TRANSFORM_CONCURRENCY``,
    or a ``@record_destination``'s ``concurrency``).

    Supports:
    - Configurable authentication (Bearer, API key, Basic)
    - URL query parameters
    - Timeout configuration
    - Custom headers
    """

    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: Optional[ApiHeaders] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        timeout: float = 30.0,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
        retry_config: Optional[RetryConfig] = None,
        health_check_enabled: bool = True,
        per_record: bool = False,
        params_field: Optional[str] = None,
        body_field: Optional[str] = None,
    ):
        """
        Initialize API destination.

        Args:
            url: Target URL.
            method: HTTP method (POST, PUT, PATCH).
            headers: Custom headers. In per-record mode a value may hold ``{field}``
                placeholders filled from each record (``{"X-Tenant": "{tenant}"}``),
                or ``headers`` may be a function called with each record that returns
                that request's headers (or None). A missing field, or a function that
                raises, fails the job before its first request.
            auth_type: ``"bearer"``, ``"apikey"``, or ``"basic"``.
            auth_token: Credential. For ``"basic"`` pass ``"user:password"``.
            timeout: Request timeout in seconds.
            params: URL query parameters appended to every request. In
                per-record mode a value may hold ``{field}`` placeholders filled
                from each record: ``"{field}"`` alone passes the record's value
                as is (a list repeats the parameter), anything else is
                formatted as text, e.g. ``{"v": "{version}", "q": "id:{id}"}``.
                A missing field fails the job. Values are URL-encoded by httpx.
            body: Request body sent verbatim. ``dict``/``list`` → JSON;
                ``str``/``bytes`` → raw body (set ``Content-Type`` via
                ``headers``). ``None`` sends no body.
            retry_config: Optional retry configuration.
            health_check_enabled: Enable/disable destination health check.
            per_record: Send one request per record, with the record as the
                body and ``{field}`` placeholders in ``url`` filled from it.
                Cannot be combined with ``body``.
            params_field: Per-record mode only. Name of a record field holding a
                dict of query parameters for that record's request, merged over
                ``params`` (the record wins on a shared key; a ``None`` value
                removes the parameter). The field is not sent in the body. A
                record without the field uses ``params`` alone; a field that is
                not a dict fails the job.
            body_field: Per-record mode only. When set and present on a dict
                record, only that field's value is sent as the body (instead
                of the whole record). Records missing the field, or non-dict
                records, are sent unchanged (minus ``params_field``).
        """
        headers_fn: Optional[ApiHeadersFn] = None
        static_headers: Dict[str, str] = {}
        if callable(headers):
            if not per_record:
                raise ValueError(
                    "api_destination: a headers function needs per_record=True "
                    "(without it there is no single record to call it with)"
                )
            headers_fn = headers
        elif headers is not None:
            static_headers = dict(headers)
        if per_record and body is not None:
            raise ValueError(
                "api_destination: per_record=True sends each record as the body; "
                "do not pass body= as well"
            )
        if body_field is not None and not per_record:
            raise ValueError("api_destination: body_field= requires per_record=True")
        if params_field is not None and not per_record:
            raise ValueError("api_destination: params_field= requires per_record=True")
        if params_field is not None and params_field == body_field:
            raise ValueError("api_destination: params_field= and body_field= must differ")
        config = {
            "url": url,
            "method": method.upper(),
            "headers": static_headers,
            "auth_type": auth_type,
            "auth_token": auth_token,
            "timeout": timeout,
            "params": params,
            "body": body,
            "health_check_enabled": health_check_enabled,
            "per_record": per_record,
            "body_field": body_field,
            "params_field": params_field,
        }
        super().__init__(config, retry_config)
        # A function is not config: its repr is an address (like Kafka's headers=).
        self._headers_fn = headers_fn
        self._client: Optional[httpx.AsyncClient] = None

    def _client_headers(self) -> Dict[str, str]:
        """Headers every request carries: the static ones, plus auth.

        In per-record mode a header with a ``{field}`` placeholder is per request
        instead (see :meth:`_build_requests`).
        """
        static: Dict[str, str] = self.config["headers"]
        if self.config.get("per_record"):
            static = {k: v for k, v in static.items() if not has_placeholders(v)}
        return build_auth_headers(
            static, self.config.get("auth_type"), self.config.get("auth_token")
        )

    def _connection_key(self) -> str:
        """Same key, same client: the host, plus everything the client is built with."""
        url = httpx.URL(self.config["url"].split("{", 1)[0])
        return "http:" + json.dumps(
            [
                f"{url.scheme}://{url.netloc.decode('ascii')}",
                self._client_headers(),
                self.config["timeout"],
            ],
            sort_keys=True,
        )

    def _concurrency(self) -> int:
        """Requests in flight per job: the record destination's limit, else the default."""
        from reflowfy.transformations.base import default_record_concurrency

        return self.send_concurrency or default_record_concurrency()

    def _new_client(self) -> httpx.AsyncClient:
        # Enough kept-alive connections for every request in flight: httpx keeps 20
        # by default, and above that each request opens (and drops) a connection.
        size = max(self._concurrency(), 20)
        return httpx.AsyncClient(
            headers=self._client_headers(),
            timeout=self.config["timeout"],
            limits=httpx.Limits(max_connections=max(size, 100), max_keepalive_connections=size),
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client (the job's shared one for this host, when pooled)."""
        if self.connection_pool is not None:

            async def create() -> httpx.AsyncClient:
                return self._new_client()

            async def close(client: httpx.AsyncClient) -> None:
                await client.aclose()

            return await self.connection_pool.get(self._connection_key(), create, close)
        if self._client is None:
            self._client = self._new_client()
        return self._client

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send to the API endpoint.

        By default the body is ``config["body"]`` verbatim in a single request;
        ``records``/``metadata`` are accepted for interface compatibility but
        the body is authored by the user in ``define_destination``. With
        ``per_record`` each record is sent in its own request instead.

        Raises:
            DestinationError: If a request fails.
            ValueError: If a per-record URL names a field a record lacks.
        """
        if self.config.get("per_record"):
            await self._send_per_record(records)
            return
        await self._request(self.config["url"], self.config.get("body"), self.config.get("params"))

    async def _send_per_record(self, records: List[Any]) -> None:
        """One request per record, many in flight; the first failure stops the rest.

        Every request is built first, so a record whose URL, params or headers cannot
        be filled fails the job (a ValueError, not retried) before anything is sent.
        """
        requests = self._build_requests(records)
        total = len(requests)
        concurrency = self._concurrency()

        budget = self.connection_pool.requests if self.connection_pool is not None else None

        async def send_one(index: int, request: _Request) -> None:
            try:
                if budget is None:
                    await self._request(
                        request.url, request.payload, request.params, request.headers
                    )
                else:
                    # A record destination's limit covers all its destinations together.
                    async with budget:
                        await self._request(
                            request.url, request.payload, request.params, request.headers
                        )
            except DestinationError as e:
                # Say which record, not what it held (URL and body are record data).
                reason = str(e).removeprefix("Destination 'api' error: ")
                raise DestinationError(
                    "api", f"record {index + 1} of {total}: {reason}", e.original_error
                ) from e

        await for_each_concurrently(requests, concurrency, send_one)
        logger.debug(
            "Sent %d per-record requests (concurrency %d)",
            total,
            concurrency,
            extra={
                "event": "api_per_record_sent",
                "requests": total,
                "concurrency": concurrency,
            },
        )

    def _build_requests(self, records: List[Any]) -> List[_Request]:
        body_field = self.config.get("body_field")
        params_field = self.config.get("params_field")
        total = len(records)
        requests: List[_Request] = []
        for index, record in enumerate(records):
            payload: Any = record
            if isinstance(record, dict):
                fields = cast(Dict[str, Any], record)
                if body_field and body_field in fields:
                    payload = fields[body_field]
                elif params_field and params_field in fields:
                    # The field is request metadata, not part of the record sent.
                    payload = {k: v for k, v in fields.items() if k != params_field}
            requests.append(
                _Request(
                    url=fill(
                        self.config["url"],
                        record,
                        owner="api_destination",
                        what="url",
                        index=index,
                        total=total,
                        encode=lambda text: quote(text, safe=""),
                    ),
                    payload=payload,
                    params=self._record_params(record, index, total),
                    headers=self._record_headers(record, index, total),
                )
            )
        return requests

    def _record_headers(self, record: Any, index: int, total: int) -> Optional[Dict[str, str]]:
        """This request's own headers: filled templates, then the function's (which win)."""
        own: Dict[str, str] = {
            name: fill(
                template,
                record,
                owner="api_destination",
                what=f"header {name!r}",
                index=index,
                total=total,
            )
            for name, template in cast(Dict[str, str], self.config["headers"]).items()
            if has_placeholders(template)
        }
        if self._headers_fn is not None:
            try:
                produced: Any = self._headers_fn(record)
            except Exception as e:
                raise ValueError(
                    f"api_destination: headers function failed on record {index + 1} of "
                    f"{total}: {type(e).__name__}"
                ) from e
            if produced is not None:
                if not isinstance(produced, Mapping):
                    raise ValueError(
                        f"api_destination: headers function returned "
                        f"{type(produced).__name__} for record {index + 1} of {total}; "
                        "expected a mapping or None"
                    )
                for name, value in cast(Mapping[Any, Any], produced).items():
                    if not isinstance(name, str) or not isinstance(value, str):
                        raise ValueError(
                            f"api_destination: headers on record {index + 1} of {total} "
                            "must map str to str"
                        )
                    own[name] = value
        return own or None

    def _record_params(self, record: Any, index: int, total: int) -> Optional[Dict[str, Any]]:
        """``config["params"]`` with placeholders filled, then ``record[params_field]`` over it."""
        static: Optional[Dict[str, Any]] = self.config.get("params")
        fields = cast(Dict[str, Any], record) if isinstance(record, dict) else {}
        params: Optional[Dict[str, Any]] = None
        if static is not None:
            params = {
                name: fill_value(
                    value,
                    record,
                    owner="api_destination",
                    what=f"params[{name!r}]",
                    index=index,
                    total=total,
                )
                for name, value in static.items()
            }

        params_field: Optional[str] = self.config.get("params_field")
        if params_field and params_field in fields:
            own = fields[params_field]
            if own is None:
                return params
            if not isinstance(own, dict):
                raise ValueError(
                    f"api_destination: params_field {params_field!r} on record "
                    f"{index + 1} of {total} must be a dict of query parameters, got "
                    f"{type(own).__name__}"
                )
            merged = dict(params or {})
            for name, value in cast(Dict[str, Any], own).items():
                if value is None:
                    merged.pop(str(name), None)
                else:
                    merged[str(name)] = value
            params = merged
        return params

    async def _request(
        self,
        url: str,
        body: Any,
        params: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Issue one request; ``body`` follows the dict/list → JSON, str/bytes → raw rule."""
        client = await self._get_client()
        method = self.config["method"]

        extra: Dict[str, Any] = {"params": params}
        if headers:
            extra["headers"] = headers  # only this request's own; the client has the rest
        try:
            if body is None:
                response = await client.request(method, url, **extra)
            elif isinstance(body, (str, bytes)):
                response = await client.request(method, url, content=body, **extra)
            else:
                response = await client.request(method, url, json=body, **extra)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise DestinationError(
                "api",
                f"HTTP {e.response.status_code}: {e.response.text}",
                e,
            )
        except httpx.RequestError as e:
            raise DestinationError("api", f"Request failed: {e}", e)
        except Exception as e:
            raise DestinationError("api", f"Unexpected error: {e}", e)

    async def health_check(self) -> bool:
        """Check if the API endpoint is accessible."""
        if not self.config.get("health_check_enabled", True):
            return True
        try:
            client = await self._get_client()
            # A per-record URL template is probed up to its first placeholder.
            url = self.config["url"].split("{", 1)[0]
            try:
                response = await client.head(url, timeout=5.0)
                return response.status_code < 500
            except Exception:
                response = await client.request("OPTIONS", url, timeout=5.0)
                return response.status_code < 500
        except Exception:
            return False

    def health_key(self) -> Optional[str]:
        """One probe per host (and client settings) per job."""
        return f"{self._connection_key()}|{self.config.get('health_check_enabled', True)}"

    async def close(self) -> None:
        """Close HTTP client (a pooled one is closed by its pool)."""
        if self._client:
            await self._client.aclose()
            self._client = None


def api_destination(
    url: str,
    method: str = "POST",
    headers: Optional[ApiHeaders] = None,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    timeout: float = 30.0,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Any] = None,
    retry_config: Optional[RetryConfig] = None,
    health_check_enabled: bool = True,
    dedup_identity: Optional[Any] = None,
    per_record: bool = False,
    params_field: Optional[str] = None,
    body_field: Optional[str] = None,
) -> ApiDestination:
    """
    Factory function for API destination.

    Build the body in ``define_destination(records, runtime_params)``::

        def define_destination(self, records, runtime_params):
            body = {"events": records, "tenant": runtime_params["tenant"]}
            return api_destination(url="https://api.example.com/ingest", body=body,
                                   auth_type="basic", auth_token="user:pass")

    With ``enable_duplicate_jobs=False`` the destination's whole config, body included,
    is part of every record's dedup hash ("where this record is written"). If the body
    carries the records themselves, or something that changes on every run (a timestamp,
    a request id), pass ``dedup_identity`` — any JSON-able value naming the real target,
    e.g. ``{"url": url, "tenant": tenant}`` — so dedup can match.

    Or send each record on its own, like ``kafka_destination``::

        def define_destination(self, records, runtime_params):
            return api_destination(
                url="https://api.example.com/users/{user_id}",  # filled per record
                method="PUT",
                per_record=True,
                body_field="payload",  # optional: send only record["payload"]
                headers={"X-Tenant": "{tenant}"},  # optional: per-record headers
            )

    Query parameters per record, two ways (they combine)::

        api_destination(
            url="https://api.example.com/users/{user_id}",
            per_record=True,
            params={"tenant": runtime_params["tenant"],  # same for every record
                    "version": "{version}"},             # from record["version"]
            params_field="query",  # record["query"] = {"dry_run": "true"} → merged in,
        )                          # and "query" is left out of the body

    Per-record mode needs no ``dedup_identity``: the records are not part of the
    config. Up to 16 requests are in flight at once (``TRANSFORM_CONCURRENCY``; inside a
    ``@record_destination`` its ``concurrency``), so they may land out of order, and a
    retry (``retry_config``) re-sends the whole job, records already accepted included —
    make the endpoint idempotent (PUT/upsert) when that matters.
    """
    destination = ApiDestination(
        url=url,
        method=method,
        headers=headers,
        auth_type=auth_type,
        auth_token=auth_token,
        timeout=timeout,
        params=params,
        body=body,
        retry_config=retry_config,
        health_check_enabled=health_check_enabled,
        per_record=per_record,
        params_field=params_field,
        body_field=body_field,
    )
    destination.dedup_identity = dedup_identity
    return destination
