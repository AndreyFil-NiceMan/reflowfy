"""REST API sources with pagination support."""

from typing import Any, Dict, Iterator, List, Optional, Union

import httpx

from reflowfy.sources.base import BaseSource, SourceError, SourceJob
from reflowfy.sources.schemas import IDBasedAPISourceConfig, PaginatedAPISourceConfig


class PaginatedAPISource(BaseSource):
    """
    Paginated REST API source.

    Supports multiple pagination styles:
    - offset: Uses offset/limit parameters
    - page: Uses page/per_page parameters
    - cursor: Uses cursor token in response
    - link: Uses Link header (RFC 5988)
    """

    def __init__(
        self,
        base_url: str,
        endpoint: str,
        method: str = "GET",
        headers: Dict[str, str] | None = None,
        auth_type: str | None = None,
        auth_token: str | None = None,
        pagination_type: str = "offset",
        page_size: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        page_param: str = "page",
        per_page_param: str = "per_page",
        cursor_param: str = "cursor",
        cursor_response_key: str = "next_cursor",
        data_key: str = "data",
        total_key: str | None = "total",
        timeout: float = 30.0,
        health_check_enabled: bool = True,
        **kwargs,
    ):
        """
        Initialize Paginated API source.

        Args:
            base_url: Base API URL (e.g., "https://api.example.com")
            endpoint: API endpoint (e.g., "/users")
            method: HTTP method
            headers: Custom headers
            auth_type: Authentication type (bearer, apikey, basic)
            auth_token: Authentication token
            pagination_type: Pagination style (offset, page, cursor, link)
            page_size: Records per page
            offset_param: Query param for offset
            limit_param: Query param for limit
            page_param: Query param for page number
            per_page_param: Query param for page size
            cursor_param: Query param for cursor
            cursor_response_key: Response key containing next cursor
            data_key: Response key containing records
            total_key: Response key containing total count
            timeout: Request timeout
            health_check_enabled: Enable/disable source health check
        """
        config = {
            "base_url": base_url,
            "endpoint": endpoint,
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "pagination_type": pagination_type,
            "page_size": page_size,
            "offset_param": offset_param,
            "limit_param": limit_param,
            "page_param": page_param,
            "per_page_param": per_page_param,
            "cursor_param": cursor_param,
            "cursor_response_key": cursor_response_key,
            "data_key": data_key,
            "total_key": total_key,
            "timeout": timeout,
            "health_check_enabled": health_check_enabled,
            **kwargs,
        }
        super().__init__(config)
        self._client: Optional[httpx.Client] = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            headers = dict(self.config["headers"])

            # Add authentication
            auth_type = self.config.get("auth_type")
            auth_token = self.config.get("auth_token")

            if auth_type == "bearer" and auth_token:
                headers["Authorization"] = f"Bearer {auth_token}"
            elif auth_type == "apikey" and auth_token:
                headers["X-API-Key"] = auth_token

            self._client = httpx.Client(
                base_url=self.config["base_url"],
                headers=headers,
                timeout=self.config["timeout"],
            )

        return self._client

    def _extract_data(self, response_data: Any) -> List[Any]:
        """Extract records from response using data_key."""
        data_key = self.config["data_key"]

        if not data_key:
            # Response is the array itself
            return response_data if isinstance(response_data, list) else [response_data]

        keys = data_key.split(".")
        result = response_data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []

        return result if isinstance(result, list) else [result]

    def _get_next_cursor(self, response_data: Any) -> Optional[str]:
        """Extract next cursor from response."""
        cursor_key = self.config["cursor_response_key"]
        keys = cursor_key.split(".")
        result = response_data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key)
            else:
                return None
        return result

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch data from API (local mode).

        Args:
            runtime_params: Runtime parameters
            limit: Optional limit for testing

        Returns:
            List of records
        """
        resolved_config = self.resolve_parameters(runtime_params)
        if resolved_config is None:
            raise SourceError("api", "No valid configuration resolved", None)

        client = self._get_client()

        endpoint = resolved_config["endpoint"]
        page_size = min(limit or resolved_config["page_size"], resolved_config["page_size"])
        pagination_type = resolved_config["pagination_type"]

        records = []

        try:
            if pagination_type == "offset":
                params = {
                    resolved_config["offset_param"]: 0,
                    resolved_config["limit_param"]: page_size,
                }
            elif pagination_type == "page":
                params = {
                    resolved_config["page_param"]: 1,
                    resolved_config["per_page_param"]: page_size,
                }
            else:
                params = {}

            response = client.request(resolved_config["method"], endpoint, params=params)
            response.raise_for_status()

            data = response.json()
            records = self._extract_data(data)

            if limit:
                records = records[:limit]

            return records

        except httpx.HTTPStatusError as e:
            raise SourceError("api", f"HTTP {e.response.status_code}: {e.response.text}", e)
        except httpx.RequestError as e:
            raise SourceError("api", f"Request failed: {e}", e)

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split API data into jobs using pagination.

        Args:
            runtime_params: Runtime parameters
            batch_size: Records per job (uses config page_size)

        Yields:
            SourceJob instances
        """
        _raw = self.resolve_parameters(runtime_params)
        if _raw is None:
            raise SourceError("api", "No valid configuration resolved", None)
        try:
            resolved_config = PaginatedAPISourceConfig(**_raw)
        except Exception as exc:
            raise SourceError("api", f"Invalid configuration: {exc}", exc)

        client = self._get_client()

        endpoint = resolved_config.endpoint
        page_size = resolved_config.page_size
        pagination_type = resolved_config.pagination_type

        page_num = 0
        offset = 0
        cursor = None

        try:
            while True:
                # Build params based on pagination type
                if pagination_type == "offset":
                    params = {
                        resolved_config.offset_param: offset,
                        resolved_config.limit_param: page_size,
                    }
                elif pagination_type == "page":
                    params = {
                        resolved_config.page_param: page_num + 1,
                        resolved_config.per_page_param: page_size,
                    }
                elif pagination_type == "cursor":
                    params: Dict[str, Any] = {resolved_config.limit_param: page_size}
                    if cursor:
                        params[resolved_config.cursor_param] = cursor
                else:
                    params = {}

                response = client.request(resolved_config.method, endpoint, params=params)
                response.raise_for_status()

                data = response.json()
                records = self._extract_data(data)

                if not records:
                    break

                yield SourceJob(
                    records=records,
                    metadata={
                        "page_num": page_num,
                        "offset": offset,
                        "cursor": cursor,
                        "record_count": len(records),
                        "endpoint": endpoint,
                    },
                )

                page_num += 1
                offset += len(records)

                # Check for next page
                if pagination_type == "cursor":
                    cursor = self._get_next_cursor(data)
                    if not cursor:
                        break
                elif len(records) < page_size:
                    # Last page for offset/page pagination
                    break

        except httpx.HTTPStatusError as e:
            raise SourceError("api", f"HTTP {e.response.status_code}: {e.response.text}", e)
        except httpx.RequestError as e:
            raise SourceError("api", f"Request failed: {e}", e)

    def health_check(self) -> bool:
        """Check if API is accessible."""
        if not self.config.get("health_check_enabled", True):
            return True

        try:
            client = self._get_client()
            response = client.request("HEAD", self.config["endpoint"], timeout=5.0)
            return response.status_code < 500
        except Exception:
            # Try GET if HEAD fails
            try:
                client = self._get_client()
                response = client.request("GET", self.config["endpoint"], timeout=5.0)
                return response.status_code < 500
            except Exception:
                return False


class IDBasedAPISource(BaseSource):
    """
    ID-based REST API source with full HTTP method and body control.

    Behaviour is auto-detected from the endpoint template:
    - ``{id}`` in ``endpoint_template`` → **per-ID mode**: one request per ID,
      ID substituted into the URL (and optionally into the body).
    - No ``{id}`` in template → **batch mode**: one request for the whole ID list,
      IDs placed in the request body.

    Body shape in batch mode is controlled by ``batch_id_key``:
    - ``batch_id_key="ids"`` (default) → ``{"ids": ["id1","id2",...]}``
    - ``batch_id_key=None``            → ``["id1","id2",...]``  (raw list body)
    - Any key + ``request_body``       → merged: ``{"ids": [...], "extra": True}``
    """

    def __init__(
        self,
        base_url: str,
        endpoint_template: str,
        ids: Optional[List[Union[str, int]]] = None,
        ids_source: Optional[BaseSource] = None,
        ids_field: str = "id",
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        batch_size: int = 50,
        timeout: float = 30.0,
        batch_id_key: Optional[str] = "ids",
        data_key: Optional[str] = None,
        request_body: Optional[Dict[str, Any]] = None,
        query_params: Optional[Dict[str, Any]] = None,
        health_check_enabled: bool = True,
        **kwargs,
    ):
        """
        Initialize ID-based API source.

        Args:
            base_url: Base API URL (e.g. ``"https://api.example.com"``)
            endpoint_template: Endpoint path. Include ``{id}`` for per-ID mode
                (e.g. ``"/users/{id}"``); omit it for batch mode
                (e.g. ``"/users/batch"``).
            ids: Static list of IDs to fetch.
            ids_source: Another source whose records supply the IDs.
            ids_field: Field name to extract IDs from ``ids_source`` records.
            method: HTTP method — GET, POST, PATCH, PUT, DELETE, etc.
                In per-ID mode this applies to every single request.
                In batch mode this applies to the single batch request.
            headers: Custom request headers.
            auth_type: Authentication scheme (``"bearer"`` or ``"apikey"``).
            auth_token: Credential for the chosen auth scheme.
            batch_size: Per-ID mode: IDs grouped per SourceJob.
                Batch mode: response records grouped per SourceJob.
            timeout: HTTP request timeout in seconds.
            batch_id_key: Body key under which the IDs list is placed in batch
                mode. Set to ``None`` to send the IDs as a **raw JSON array**
                (no wrapping object). Default ``"ids"``.
            data_key: Dotted response key used to extract the records list from
                the response JSON. ``None`` means the response is the list.
            request_body: Extra body fields merged into every request.
                In per-ID mode, string values support ``{id}`` substitution.
                In batch mode, merged alongside the IDs (unless ``batch_id_key``
                is already present in this dict).
            query_params: Extra query-string parameters appended to every request.
            health_check_enabled: Enable/disable source health check.
        """
        config = {
            "base_url": base_url,
            "endpoint_template": endpoint_template,
            "ids": ids or [],
            "ids_field": ids_field,
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "batch_size": batch_size,
            "timeout": timeout,
            "batch_id_key": batch_id_key,
            "data_key": data_key,
            "request_body": request_body or {},
            "query_params": query_params or {},
            "health_check_enabled": health_check_enabled,
            **kwargs,
        }
        super().__init__(config)
        self._ids_source = ids_source
        self._client: Optional[httpx.Client] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_per_id_mode(self) -> bool:
        """True when the endpoint template contains ``{id}`` → one request per ID."""
        return "{id}" in self.config["endpoint_template"]

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            headers = dict(self.config["headers"])
            auth_type = self.config.get("auth_type")
            auth_token = self.config.get("auth_token")
            if auth_type == "bearer" and auth_token:
                headers["Authorization"] = f"Bearer {auth_token}"
            elif auth_type == "apikey" and auth_token:
                headers["X-API-Key"] = auth_token
            self._client = httpx.Client(
                base_url=self.config["base_url"],
                headers=headers,
                timeout=self.config["timeout"],
            )
        return self._client

    def _get_all_ids(self, runtime_params: Dict[str, Any]) -> List[Union[str, int]]:
        """Get all IDs from config, ids_source, or runtime params."""
        if self.config["ids"]:
            return self.config["ids"]
        if self._ids_source:
            records = self._ids_source.fetch(runtime_params)
            ids_field = self.config["ids_field"]
            return [r.get(ids_field) for r in records if r.get(ids_field)]
        return runtime_params.get("ids", [])

    def _extract_records(self, data: Any) -> List[Any]:
        """Extract records list from a response using ``data_key``."""
        data_key = self.config.get("data_key")
        if not data_key:
            return data if isinstance(data, list) else []
        keys = data_key.split(".")
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

        # Build body for non-GET methods; substitute {id} in string values
        body: Optional[Dict] = None
        if method not in ("GET", "HEAD"):
            body = {
                k: v.format(id=id_value) if isinstance(v, str) else v
                for k, v in self.config["request_body"].items()
            }

        query = self.config["query_params"] or None

        try:
            response = client.request(method, endpoint, json=body, params=query)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            return None

    def _fetch_batch(self, ids_batch: List[Union[str, int]]) -> List[Any]:
        """Send a batch request with the IDs list and return the records."""
        client = self._get_client()
        method = self.config["method"]
        endpoint = self.config["endpoint_template"]
        batch_id_key = self.config.get("batch_id_key")

        if batch_id_key:
            # Object body: {"ids": [...]} merged with any extra request_body fields
            body: Any = dict(self.config["request_body"])
            if batch_id_key not in body:
                body[batch_id_key] = ids_batch
        else:
            # Raw list body: ["id1", "id2", "id3"]
            body = ids_batch

        query = self.config["query_params"] or None

        try:
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
        """
        Fetch resources by ID (local/preview mode).

        Args:
            runtime_params: Runtime parameters.
            limit: Optional cap on the number of records returned.

        Returns:
            List of fetched records.
        """
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
        """
        Split IDs into SourceJobs.

        Per-ID mode: groups IDs by ``batch_size``, fetches each individually.
        Batch mode: sends one request for all IDs, then splits the response
        records into jobs of ``batch_size``.

        Args:
            runtime_params: Runtime parameters.
            batch_size: IDs per job (per-ID mode) or records per job (batch mode).

        Yields:
            SourceJob instances.
        """
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
            # Batch mode: one request → split response records into jobs
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

        # Per-ID mode: group IDs into batches, fetch each individually
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
def paginated_api_source(
    base_url: str,
    endpoint: str,
    pagination_type: str = "offset",
    page_size: int = 100,
    data_key: str = "data",
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    health_check_enabled: bool = True,
    **kwargs,
) -> PaginatedAPISource:
    """
    Factory function for paginated API source.

    Example:
        >>> source = paginated_api_source(
        ...     base_url="https://api.example.com",
        ...     endpoint="/users",
        ...     pagination_type="offset",
        ...     page_size=100,
        ...     data_key="data",
        ...     auth_type="bearer",
        ...     auth_token="secret-token"
        ... )
    """
    return PaginatedAPISource(
        base_url=base_url,
        endpoint=endpoint,
        pagination_type=pagination_type,
        page_size=page_size,
        data_key=data_key,
        auth_type=auth_type,
        auth_token=auth_token,
        health_check_enabled=health_check_enabled,
        **kwargs,
    )


def id_based_api_source(
    base_url: str,
    endpoint_template: str,
    ids: Optional[List[Union[str, int]]] = None,
    batch_size: int = 50,
    method: str = "GET",
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    batch_id_key: Optional[str] = "ids",
    data_key: Optional[str] = None,
    request_body: Optional[Dict[str, Any]] = None,
    query_params: Optional[Dict[str, Any]] = None,
    health_check_enabled: bool = True,
    **kwargs,
) -> IDBasedAPISource:
    """
    Factory function for ID-based API source.

    Mode is auto-detected from the endpoint template:
    - ``{id}`` present → per-ID mode (one request per ID)
    - No ``{id}``      → batch mode (one request, IDs in body)

    Body shape (batch mode):
    - ``batch_id_key="ids"`` → ``{"ids": ["id1","id2","id3"]}``
    - ``batch_id_key=None``  → ``["id1","id2","id3"]``  (raw list)
    - ``request_body``       → merged into the object body alongside IDs

    Examples::

        # Per-ID GET (default)
        id_based_api_source(base_url="...", endpoint_template="/users/{id}", ids=[1,2,3])

        # Batch POST — object body: {"ids": [1,2,3]}
        id_based_api_source(base_url="...", endpoint_template="/users/batch",
            method="POST", ids=[1,2,3], batch_id_key="ids", data_key="users")

        # Batch POST — raw list body: [1,2,3]
        id_based_api_source(base_url="...", endpoint_template="/users/batch",
            method="POST", ids=[1,2,3], batch_id_key=None, data_key="users")

        # Batch PATCH — merged body: {"ids": [...], "status": "active"}
        id_based_api_source(base_url="...", endpoint_template="/users/bulk",
            method="PATCH", ids=[1,2,3], request_body={"status": "active"})

        # Per-ID POST with dynamic body: POST /users/1  body: {"ref": "1"}
        id_based_api_source(base_url="...", endpoint_template="/users/{id}",
            method="POST", ids=[1,2,3], request_body={"ref": "{id}"})
    """
    return IDBasedAPISource(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids,
        method=method,
        batch_size=batch_size,
        auth_type=auth_type,
        auth_token=auth_token,
        batch_id_key=batch_id_key,
        data_key=data_key,
        request_body=request_body,
        query_params=query_params,
        health_check_enabled=health_check_enabled,
        **kwargs,
    )
