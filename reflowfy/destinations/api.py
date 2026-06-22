"""API destination for webhooks and REST endpoints."""

from typing import Any, Dict, List, Optional

import httpx

from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig
from reflowfy.http_auth import build_auth_headers


class ApiDestination(BaseDestination):
    """
    API destination for sending data to webhooks and REST endpoints.

    The request ``body`` is sent verbatim — build it yourself in
    ``define_destination(records, runtime_params)``. A ``dict``/``list`` is sent
    as JSON (``Content-Type: application/json``); a ``str`` or ``bytes`` is sent
    as a raw body (set your own ``Content-Type`` via ``headers``). ``body=None``
    sends a request with no body. Each ``send()`` issues exactly one request.

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
        """
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
        Send the configured body to the API endpoint in a single request.

        The body is ``config["body"]`` verbatim; ``records``/``metadata`` are
        accepted for interface compatibility but the body is authored by the
        user in ``define_destination``.

        Raises:
            DestinationError: If the request fails.
        """
        client = await self._get_client()
        url = self.config["url"]
        method = self.config["method"]
        params = self.config.get("params")
        body = self.config.get("body")

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
            try:
                response = await client.head(self.config["url"], timeout=5.0)
                return response.status_code < 500
            except Exception:
                response = await client.request("OPTIONS", self.config["url"], timeout=5.0)
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
) -> ApiDestination:
    """
    Factory function for API destination.

    Build the body in ``define_destination(records, runtime_params)``::

        def define_destination(self, records, runtime_params):
            body = {"events": records, "tenant": runtime_params["tenant"]}
            return api_destination(url="https://api.example.com/ingest", body=body,
                                   auth_type="basic", auth_token="user:pass")
    """
    return ApiDestination(
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
    )
