"""REST API sources."""

from typing import Any, Dict, Iterator, List, Optional, Union

import httpx

from reflowfy.http_auth import build_auth_headers
from reflowfy.sources.base import BaseSource, SourceError, SourceJob
from reflowfy.sources.schemas import IDBasedAPISourceConfig


class IDBasedAPISource(BaseSource):
    """
    ID-based REST API source with full HTTP method and body control.

    Behaviour is auto-detected from the endpoint template:
    - ``{id}`` in ``endpoint_template`` → **per-ID mode**: one request per ID,
      ID substituted into the URL (and into string values of ``body``).
    - No ``{id}`` in template → **batch mode**: one request whose body is the
      ``body`` you built (e.g. ``{"ids": params["current_ids"]}``).

    The request ``body`` is sent verbatim. Build it yourself in
    ``define_source(runtime_params)`` from the IDs you already have, e.g.
    ``body={"ids": params["current_ids"]}`` or ``body=params["current_ids"]``
    for a raw list. A ``dict``/``list`` is sent as JSON; a ``str`` or ``bytes`` is
    sent as a raw body (set your own ``Content-Type`` via ``headers``).
    ``body=None`` sends no request body.
    """

    def __init__(
        self,
        base_url: str,
        endpoint_template: str,
        ids: Optional[List[Union[str, int]]] = None,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        batch_size: int = 50,
        timeout: float = 30.0,
        response_key: Optional[str] = None,
        body: Optional[Any] = None,
        params: Optional[Dict[str, Any]] = None,
        health_check_enabled: bool = True,
    ):
        """
        Initialize ID-based API source.

        Note: there is intentionally **no** ``**kwargs`` — unknown keyword
        arguments (including the removed ``batch_id_key`` / ``request_body`` /
        ``query_params`` / ``data_key`` / ``ids_source`` / ``ids_field``) raise
        ``TypeError`` so typos and stale call sites fail loudly.

        Args:
            base_url: Base API URL (e.g. ``"https://api.example.com"``).
            endpoint_template: Endpoint path. Include ``{id}`` for per-ID mode
                (e.g. ``"/users/{id}"``); omit it for batch mode.
            ids: Static list of IDs. May also arrive via ``runtime_params["ids"]``.
            method: HTTP method.
            headers: Custom request headers.
            auth_type: ``"bearer"``, ``"apikey"``, or ``"basic"``.
            auth_token: Credential. For ``"basic"`` pass ``"user:password"``.
            batch_size: Per-ID mode: IDs grouped per SourceJob. Batch mode:
                response records grouped per SourceJob.
            timeout: HTTP request timeout in seconds.
            response_key: Dotted key to extract the records list from the
                response JSON (e.g. ``"data.users"``). ``None`` means the
                response itself is the list.
            body: Request body sent verbatim. ``dict``/``list`` → JSON;
                ``str``/``bytes`` → raw body (set ``Content-Type`` via
                ``headers``, sent as-is with no ``{id}`` substitution). In per-ID
                mode, string values of a *dict* body support ``{id}``
                substitution. ``None`` sends no body.
            params: Extra query-string parameters appended to every request.
            health_check_enabled: Enable/disable source health check.
        """
        config = {
            "base_url": base_url,
            "endpoint_template": endpoint_template,
            "ids": ids or [],
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "batch_size": batch_size,
            "timeout": timeout,
            "response_key": response_key,
            "body": body,
            "params": params or {},
            "health_check_enabled": health_check_enabled,
        }
        super().__init__(config)
        self._client: Optional[httpx.Client] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_per_id_mode(self) -> bool:
        """True when the endpoint template contains ``{id}``."""
        return "{id}" in self.config["endpoint_template"]

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            headers = build_auth_headers(
                self.config["headers"],
                self.config.get("auth_type"),
                self.config.get("auth_token"),
            )
            self._client = httpx.Client(
                base_url=self.config["base_url"],
                headers=headers,
                timeout=self.config["timeout"],
            )
        return self._client

    def _get_all_ids(self, runtime_params: Dict[str, Any]) -> List[Union[str, int]]:
        """Get all IDs from config or runtime params."""
        if self.config["ids"]:
            return self.config["ids"]
        return runtime_params.get("ids", [])

    def _extract_records(self, data: Any) -> List[Any]:
        """Extract records list from a response using ``response_key``."""
        response_key = self.config.get("response_key")
        if not response_key:
            return data if isinstance(data, list) else []
        keys = response_key.split(".")
        result = data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []
        return result if isinstance(result, list) else [result]

    def _fetch_by_id(self, id_value: Union[str, int]) -> Optional[Any]:
        """Fetch a single resource by ID (per-ID mode)."""
        client = self._get_client()
        endpoint = self.config["endpoint_template"].format(id=id_value)
        method = self.config["method"]

        body = self.config.get("body")
        if method in ("GET", "HEAD"):
            body = None
        elif isinstance(body, dict):
            body = {k: v.format(id=id_value) if isinstance(v, str) else v for k, v in body.items()}

        query = self.config["params"] or None

        try:
            if body is None:
                response = client.request(method, endpoint, params=query)
            elif isinstance(body, (str, bytes)):
                response = client.request(method, endpoint, content=body, params=query)
            else:
                response = client.request(method, endpoint, json=body, params=query)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            return None

    def _fetch_batch(self, ids_batch: List[Union[str, int]]) -> List[Any]:
        """Send the batch request and return the extracted records.

        The request body is ``config["body"]`` verbatim — the caller is
        expected to have placed any IDs into it. ``ids_batch`` is retained for
        the public signature/metadata only.
        """
        client = self._get_client()
        method = self.config["method"]
        endpoint = self.config["endpoint_template"]
        body = self.config.get("body")
        query = self.config["params"] or None

        try:
            if body is None:
                response = client.request(method, endpoint, params=query)
            elif isinstance(body, (str, bytes)):
                response = client.request(method, endpoint, content=body, params=query)
            else:
                response = client.request(method, endpoint, json=body, params=query)
            response.raise_for_status()
            return self._extract_records(response.json())
        except httpx.HTTPStatusError as e:
            raise SourceError(
                "id_based_api", f"HTTP {e.response.status_code}: {e.response.text}", e
            )
        except httpx.RequestError as e:
            raise SourceError("id_based_api", f"Request failed: {e}", e)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """Fetch resources by ID (local/preview mode)."""
        self.resolve_parameters(runtime_params)
        ids = self._get_all_ids(runtime_params)
        if limit:
            ids = ids[:limit]

        if not self._is_per_id_mode():
            return self._fetch_batch(ids)

        records = []
        for id_value in ids:
            record = self._fetch_by_id(id_value)
            if record:
                records.append(record)
        return records

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 50
    ) -> Iterator[SourceJob]:
        """Split IDs into SourceJobs (per-ID or batch mode)."""
        _raw = self.resolve_parameters(runtime_params)
        if _raw is None:
            raise SourceError("id_based_api", "No valid configuration resolved", None)
        try:
            resolved_config = IDBasedAPISourceConfig(**_raw)
        except Exception as exc:
            raise SourceError("id_based_api", f"Invalid configuration: {exc}", exc)

        ids = self._get_all_ids(runtime_params)
        batch_size = resolved_config.batch_size
        batch_num = 0

        if not self._is_per_id_mode():
            records = self._fetch_batch(ids)
            for i in range(0, len(records), batch_size):
                chunk = records[i : i + batch_size]
                if chunk:
                    yield SourceJob(
                        records=chunk,
                        metadata={
                            "batch_num": batch_num,
                            "record_count": len(chunk),
                            "ids_count": len(ids),
                        },
                    )
                batch_num += 1
            return

        for i in range(0, len(ids), batch_size):
            id_batch = ids[i : i + batch_size]
            records = []
            for id_value in id_batch:
                record = self._fetch_by_id(id_value)
                if record:
                    records.append(record)
            if records:
                yield SourceJob(
                    records=records,
                    metadata={
                        "batch_num": batch_num,
                        "id_count": len(id_batch),
                        "record_count": len(records),
                        "ids": id_batch,
                    },
                )
            batch_num += 1

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["IDBasedAPISource"]:
        """Per-ID mode: group ids into batch_size chunks, one source each.

        Batch mode (no ``{id}`` in the template) is a single request, so it
        yields one job (self).
        """
        if not self._is_per_id_mode():
            yield self
            return

        ids = self._get_all_ids(runtime_params)
        size = self.config["batch_size"]
        c = self.config
        for i in range(0, len(ids), size):
            yield IDBasedAPISource(
                base_url=c["base_url"],
                endpoint_template=c["endpoint_template"],
                ids=ids[i : i + size],
                method=c["method"],
                headers=c["headers"],
                auth_type=c["auth_type"],
                auth_token=c["auth_token"],
                batch_size=size,
                timeout=c["timeout"],
                response_key=c["response_key"],
                body=c["body"],
                params=c["params"],
                health_check_enabled=c["health_check_enabled"],
            )

    def health_check(self) -> bool:
        """Check if the API is accessible."""
        if not self.config.get("health_check_enabled", True):
            return True
        try:
            client = self._get_client()
            response = client.request("HEAD", "/", timeout=5.0)
            return response.status_code < 500
        except Exception:
            return False


# Factory functions
def id_based_api_source(
    base_url: str,
    endpoint_template: str,
    ids: Optional[List[Union[str, int]]] = None,
    batch_size: int = 50,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    response_key: Optional[str] = None,
    body: Optional[Any] = None,
    params: Optional[Dict[str, Any]] = None,
    health_check_enabled: bool = True,
) -> IDBasedAPISource:
    """
    Factory for the ID-based API source.

    Mode is auto-detected from ``endpoint_template``:
    - ``{id}`` present → per-ID mode (one request per ID)
    - no ``{id}``      → batch mode (one request; you build the body)

    Build the request body yourself from the IDs you have::

        # batch object body
        id_based_api_source(base_url="...", endpoint_template="/users/batch",
            method="POST", body={"ids": params["current_ids"]}, response_key="users")

        # batch raw-list body
        id_based_api_source(base_url="...", endpoint_template="/users/batch",
            method="POST", body=params["current_ids"])

        # per-ID GET (no body)
        id_based_api_source(base_url="...", endpoint_template="/users/{id}",
            ids=[1, 2, 3])
    """
    return IDBasedAPISource(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids,
        method=method,
        headers=headers,
        batch_size=batch_size,
        auth_type=auth_type,
        auth_token=auth_token,
        response_key=response_key,
        body=body,
        params=params,
        health_check_enabled=health_check_enabled,
    )
