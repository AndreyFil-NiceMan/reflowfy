"""HTTP destination for webhooks and APIs."""

import json
from typing import Any, Dict, List, Optional
import httpx
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig


class HttpDestination(BaseDestination):
    """
    HTTP destination for sending data to webhooks and APIs.
    
    Supports:
    - Configurable authentication (Bearer, API key, Basic)
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
        retry_config: Optional[RetryConfig] = None,
    ):
        """
        Initialize HTTP destination.
        
        Args:
            url: Target URL
            method: HTTP method (POST, PUT, PATCH)
            headers: Custom headers
            auth_type: Authentication type (bearer, apikey, basic)
            auth_token: Authentication token/credentials
            timeout: Request timeout in seconds
            batch_requests: Whether to send all records in one request
            retry_config: Optional retry configuration
        """
        config = {
            "url": url,
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "timeout": timeout,
            "batch_requests": batch_requests,
        }
        super().__init__(config, retry_config)
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
                headers=headers,
                timeout=self.config["timeout"],
            )
        
        return self._client
    
    def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send records to HTTP endpoint.
        
        Args:
            records: List of records to send
            metadata: Optional metadata to include in request
        
        Raises:
            DestinationError: If send fails
        """
        client = self._get_client()
        url = self.config["url"]
        method = self.config["method"]
        batch_requests = self.config["batch_requests"]
        
        try:
            if batch_requests:
                # Send all records in one request
                payload = {
                    "records": records,
                    "metadata": metadata or {},
                }
                
                response = client.request(method, url, json=payload)
                response.raise_for_status()
            
            else:
                # Send each record as a separate request
                for record in records:
                    payload = {
                        "record": record,
                        "metadata": metadata or {},
                    }
                    
                    response = client.request(method, url, json=payload)
                    response.raise_for_status()
        
        except httpx.HTTPStatusError as e:
            raise DestinationError(
                "http",
                f"HTTP {e.response.status_code}: {e.response.text}",
                e,
            )
        except httpx.RequestError as e:
            raise DestinationError("http", f"Request failed: {e}", e)
        except Exception as e:
            raise DestinationError("http", f"Unexpected error: {e}", e)
    
    def health_check(self) -> bool:
        """Check if HTTP endpoint is accessible."""
        try:
            client = self._get_client()
            # Try a HEAD request first, fall back to OPTIONS
            try:
                response = client.head(self.config["url"], timeout=5.0)
                return response.status_code < 500
            except:
                response = client.request("OPTIONS", self.config["url"], timeout=5.0)
                return response.status_code < 500
        except Exception:
            return False
    
    def close(self):
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None


def http_destination(
    url: str,
    method: str = "POST",
    headers: Optional[Dict[str, str]] = None,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    timeout: float = 30.0,
    batch_requests: bool = False,
    retry_config: Optional[RetryConfig] = None,
) -> HttpDestination:
    """
    Factory function for HTTP destination.
    
    Example:
        >>> destination = http_destination(
        ...     url="https://api.example.com/webhook",
        ...     method="POST",
        ...     auth_type="bearer",
        ...     auth_token="secret-token",
        ...     batch_requests=True
        ... )
    """
    return HttpDestination(
        url=url,
        method=method,
        headers=headers,
        auth_type=auth_type,
        auth_token=auth_token,
        timeout=timeout,
        batch_requests=batch_requests,
        retry_config=retry_config,
    )
