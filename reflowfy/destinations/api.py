"""API destination for webhooks and REST endpoints."""

from typing import Any, Dict, List, Optional

import httpx

from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig


class ApiDestination(BaseDestination):
    """
    API destination for sending data to webhooks and REST endpoints.

    Supports:
    - Configurable authentication (Bearer, API key, Basic)
    - URL query parameters
    - Custom static body fields merged into every request
    - runtime_params metadata merged into every request body under 'runtime_params'
    - Request batching
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
        batch_requests: bool = False,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
        retry_config: Optional[RetryConfig] = None,
        health_check_enabled: bool = True,
    ):
        """
        Initialize API destination.

        Args:
            url: Target URL
            method: HTTP method (POST, PUT, PATCH)
            headers: Custom headers
            auth_type: Authentication type (bearer, apikey, basic)
            auth_token: Authentication token/credentials
            timeout: Request timeout in seconds
            batch_requests: Whether to send all records in one request
            params: URL query parameters appended to every request
            body: Static fields merged into every request body alongside records
            runtime_params (metadata): Added to every request body as runtime_params
            retry_config: Optional retry configuration
            health_check_enabled: Enable/disable destination health check
        """
        config = {
            "url": url,
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "timeout": timeout,
            "batch_requests": batch_requests,
            "params": params,
            "body": body,
            "health_check_enabled": health_check_enabled,
        }
        super().__init__(config, retry_config)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None:
            headers = dict(self.config["headers"])

            auth_type = self.config.get("auth_type")
            auth_token = self.config.get("auth_token")

            if auth_type == "bearer" and auth_token:
                headers["Authorization"] = f"Bearer {auth_token}"
            elif auth_type == "apikey" and auth_token:
                headers["X-API-Key"] = auth_token

            self._client = httpx.AsyncClient(
                headers=headers,
                timeout=self.config["timeout"],
            )

        return self._client

    def _serialize_metadata(self, metadata: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not metadata:
            return None
        safe: Dict[str, Any] = {}
        for key, value in metadata.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                safe[key] = value
            elif isinstance(value, (list, dict)):
                safe[key] = value
            else:
                safe[key] = str(value)
        return safe

    def _build_payload(
        self, data: Any, metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Merge static body fields with record data and runtime metadata."""
        payload = dict(self.config.get("body") or {})
        runtime_params = self._serialize_metadata(metadata)
        if runtime_params:
            payload["runtime_params"] = runtime_params
        return payload

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to the API endpoint.

        Args:
            records: List of records to send
            metadata: Optional metadata (unused in payload, available for subclasses)

        Raises:
            DestinationError: If the request fails
        """
        client = await self._get_client()
        url = self.config["url"]
        method = self.config["method"]
        params = self.config.get("params")

        try:
            if self.config["batch_requests"]:
                payload = self._build_payload(records, metadata)
                payload["records"] = records

                response = await client.request(method, url, json=payload, params=params)
                response.raise_for_status()

            else:
                for record in records:
                    payload = self._build_payload(record, metadata)
                    payload["record"] = record

                    response = await client.request(method, url, json=payload, params=params)
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
            try:
                response = await client.head(self.config["url"], timeout=5.0)
                return response.status_code < 500
            except Exception:
                response = await client.request("OPTIONS", self.config["url"], timeout=5.0)
                return response.status_code < 500
        except Exception:
            return False

    async def close(self):
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
    batch_requests: bool = False,
    params: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    retry_config: Optional[RetryConfig] = None,
    health_check_enabled: bool = True,
) -> ApiDestination:
    """
    Factory function for API destination.

    Example:
        >>> destination = api_destination(
        ...     url="https://api.example.com/ingest",
        ...     auth_type="bearer",
        ...     auth_token="secret-token",
        ...     params={"tenant_id": "acme", "env": "prod"},
        ...     body={"source": "reflowfy", "version": "2"},
        ...     batch_requests=True,
        ... )
    """
    return ApiDestination(
        url=url,
        method=method,
        headers=headers,
        auth_type=auth_type,
        auth_token=auth_token,
        timeout=timeout,
        batch_requests=batch_requests,
        params=params,
        body=body,
        retry_config=retry_config,
        health_check_enabled=health_check_enabled,
    )
