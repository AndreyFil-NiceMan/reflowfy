"""API destination for webhooks and REST endpoints."""

import re
from typing import Any, Dict, List, Optional, cast
from urllib.parse import quote

import httpx

from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig
from reflowfy.http_auth import build_auth_headers
from reflowfy.observability.logging import get_logger

logger = get_logger(__name__)

# ``{field}`` in a per-record URL. Deliberately not str.format: that would also
# evaluate ``{a.b}`` / ``{a[0]}`` / format specs against record data.
_URL_PLACEHOLDER = re.compile(r"\{(\w+)\}")


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
    ``record[key_field]`` when ``key_field`` is set and present on the record),
    and ``{field}`` placeholders in ``url`` filled from each record
    (URL-encoded), e.g. ``url="https://api.example.com/users/{user_id}"``.

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
        headers: Optional[Dict[str, str]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        timeout: float = 30.0,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        retry_config: Optional[RetryConfig] = None,
        health_check_enabled: bool = True,
        per_record: bool = False,
        key_field: Optional[str] = None,
    ):
        """
        Initialize API destination.

        Args:
            url: Target URL.
            method: HTTP method (POST, PUT, PATCH).
            headers: Custom headers.
            auth_type: ``"bearer"``, ``"apikey"``, or ``"basic"``.
            auth_token: Credential. For ``"basic"`` pass ``"user:password"``.
            timeout: Request timeout in seconds.
            params: URL query parameters appended to every request.
            body: Request body sent verbatim. ``dict``/``list`` → JSON;
                ``str``/``bytes`` → raw body (set ``Content-Type`` via
                ``headers``). ``None`` sends no body.
            retry_config: Optional retry configuration.
            health_check_enabled: Enable/disable destination health check.
            per_record: Send one request per record, with the record as the
                body and ``{field}`` placeholders in ``url`` filled from it.
                Cannot be combined with ``body``.
            key_field: Per-record mode only. When set and present on a dict
                record, only that field's value is sent as the body (instead
                of the whole record), as ``kafka_destination(key_field=...)``
                does. Records missing the field, or non-dict records, are sent
                unchanged.
        """
        if per_record and body is not None:
            raise ValueError(
                "api_destination: per_record=True sends each record as the body; "
                "do not pass body= as well"
            )
        if key_field is not None and not per_record:
            raise ValueError("api_destination: key_field= requires per_record=True")
        config = {
            "url": url,
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "timeout": timeout,
            "params": params,
            "body": body,
            "health_check_enabled": health_check_enabled,
            "per_record": per_record,
            "key_field": key_field,
        }
        super().__init__(config, retry_config)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None:
            headers = build_auth_headers(
                self.config["headers"],
                self.config.get("auth_type"),
                self.config.get("auth_token"),
            )
            self._client = httpx.AsyncClient(
                headers=headers,
                timeout=self.config["timeout"],
            )
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
        await self._request(self.config["url"], self.config.get("body"))

    async def _send_per_record(self, records: List[Any]) -> None:
        """One request per record, in order; the first failure stops the rest."""
        key_field = self.config.get("key_field")
        total = len(records)
        for index, record in enumerate(records):
            # Built outside _request so a bad template fails the job at once
            # instead of being retried as if the endpoint were down.
            url = self._record_url(record, index)
            payload: Any = record
            if key_field and isinstance(record, dict):
                fields = cast(Dict[str, Any], record)
                if key_field in fields:
                    payload = fields[key_field]
            try:
                await self._request(url, payload)
            except DestinationError as e:
                # Say which record, not what it held (URL and body are record data).
                reason = str(e).removeprefix("Destination 'api' error: ")
                raise DestinationError(
                    "api", f"record {index + 1} of {total}: {reason}", e.original_error
                ) from e
        logger.debug(
            "Sent %d per-record requests",
            total,
            extra={"event": "api_per_record_sent", "requests": total},
        )

    def _record_url(self, record: Any, index: int) -> str:
        """``config["url"]`` with each ``{field}`` replaced by the record's value."""
        template: str = self.config["url"]
        if "{" not in template:
            return template

        def substitute(match: "re.Match[str]") -> str:
            field = match.group(1)
            fields = cast(Dict[str, Any], record) if isinstance(record, dict) else {}
            if field not in fields:
                raise ValueError(
                    f"api_destination: url placeholder {{{field}}} has no matching "
                    f"field on record {index + 1}"
                )
            return quote(str(fields[field]), safe="")

        return _URL_PLACEHOLDER.sub(substitute, template)

    async def _request(self, url: str, body: Any) -> None:
        """Issue one request; ``body`` follows the dict/list → JSON, str/bytes → raw rule."""
        client = await self._get_client()
        method = self.config["method"]
        params = self.config.get("params")

        try:
            if body is None:
                response = await client.request(method, url, params=params)
            elif isinstance(body, (str, bytes)):
                response = await client.request(method, url, content=body, params=params)
            else:
                response = await client.request(method, url, json=body, params=params)
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

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


def api_destination(
    url: str,
    method: str = "POST",
    headers: Optional[Dict[str, str]] = None,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    timeout: float = 30.0,
    params: Optional[Dict[str, str]] = None,
    body: Optional[Any] = None,
    retry_config: Optional[RetryConfig] = None,
    health_check_enabled: bool = True,
    dedup_identity: Optional[Any] = None,
    per_record: bool = False,
    key_field: Optional[str] = None,
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
                key_field="payload",  # optional: send only record["payload"]
            )

    Per-record mode needs no ``dedup_identity``: the records are not part of the
    config. Requests go one at a time, and a retry (``retry_config``) re-sends the
    whole job, records already accepted included — make the endpoint idempotent
    (PUT/upsert) when that matters.
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
        key_field=key_field,
    )
    destination.dedup_identity = dedup_identity
    return destination
