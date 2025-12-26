"""API source connector for HTTP APIs."""

from typing import Any, Dict, Iterator, List, Optional
import httpx
from reflowfy.sources.base import BaseSource, SourceJob, SourceError


class ApiSource(BaseSource):
    """
    HTTP API source connector.
    
    Supports:
    - Pagination (offset/limit, cursor-based)
    - Authentication (Bearer, API key)
    - Rate limiting
    - Runtime parameters in URL/body/headers
    """
    
    def __init__(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        pagination_strategy: str = "offset",  # offset, cursor, none
        pagination_config: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ):
        """
        Initialize API source.
        
        Args:
            url: API endpoint URL (supports Jinja2 templates)
            method: HTTP method (GET, POST)
            headers: Request headers (supports Jinja2)
            body: Request body for POST (supports Jinja2)
            auth_type: Authentication type (bearer, apikey)
            auth_token: Authentication token
            pagination_strategy: How to paginate (offset, cursor, none)
            pagination_config: Strategy-specific config
            timeout: Request timeout
        """
        config = {
            "url": url,
            "method": method.upper(),
            "headers": headers or {},
            "body": body,
            "auth_type": auth_type,
            "auth_token": auth_token,
            "pagination_strategy": pagination_strategy,
            "pagination_config": pagination_config or {},
            "timeout": timeout,
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
                headers=headers,
                timeout=self.config["timeout"],
            )
        
        return self._client
    
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
        client = self._get_client()
        
        try:
            # Make single request
            response = client.request(
                method=resolved_config["method"],
                url=resolved_config["url"],
                json=resolved_config.get("body"),
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Extract records from response
            # Assume response is either a list or has a "data" field
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and "data" in data:
                records = data["data"]
            else:
                records = [data]
            
            if limit:
                records = records[:limit]
            
            return records
        
        except httpx.HTTPError as e:
            raise SourceError("api", f"HTTP request failed: {e}", e)
    
    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split API data into jobs using pagination.
        
        Args:
            runtime_params: Runtime parameters
            batch_size: Records per job
        
        Yields:
            SourceJob instances
        """
        resolved_config = self.resolve_parameters(runtime_params)
        strategy = resolved_config["pagination_strategy"]
        
        if strategy == "offset":
            yield from self._paginate_offset(resolved_config, batch_size)
        elif strategy == "cursor":
            yield from self._paginate_cursor(resolved_config, batch_size)
        else:
            # No pagination - single request
            yield from self._fetch_all(resolved_config)
    
    def _paginate_offset(
        self, config: Dict[str, Any], batch_size: int
    ) -> Iterator[SourceJob]:
        """Paginate using offset/limit."""
        client = self._get_client()
        pagination_config = config["pagination_config"]
        
        offset_param = pagination_config.get("offset_param", "offset")
        limit_param = pagination_config.get("limit_param", "limit")
        
        offset = 0
        page_num = 0
        
        while True:
            # Build URL with pagination params
            url = f"{config['url']}?{offset_param}={offset}&{limit_param}={batch_size}"
            
            try:
                response = client.request(method=config["method"], url=url)
                response.raise_for_status()
                
                data = response.json()
                records = data if isinstance(data, list) else data.get("data", [])
                
                if not records:
                    break
                
                yield SourceJob(
                    records=records,
                    metadata={"offset": offset, "page_num": page_num, "count": len(records)},
                )
                
                if len(records) < batch_size:
                    break
                
                offset += batch_size
                page_num += 1
            
            except httpx.HTTPError as e:
                raise SourceError("api", f"Pagination failed: {e}", e)
    
    def _paginate_cursor(
        self, config: Dict[str, Any], batch_size: int
    ) -> Iterator[SourceJob]:
        """Paginate using cursor."""
        client = self._get_client()
        pagination_config = config["pagination_config"]
        
        cursor_param = pagination_config.get("cursor_param", "cursor")
        cursor_field = pagination_config.get("cursor_field", "next_cursor")
        
        cursor = None
        page_num = 0
        
        while True:
            # Build URL with cursor
            url = config["url"]
            if cursor:
                url = f"{url}?{cursor_param}={cursor}"
            
            try:
                response = client.request(method=config["method"], url=url)
                response.raise_for_status()
                
                data = response.json()
                records = data.get("data", [])
                
                if not records:
                    break
                
                yield SourceJob(
                    records=records,
                    metadata={"cursor": cursor, "page_num": page_num, "count": len(records)},
                )
                
                # Get next cursor
                cursor = data.get(cursor_field)
                if not cursor:
                    break
                
                page_num += 1
            
            except httpx.HTTPError as e:
                raise SourceError("api", f"Cursor pagination failed: {e}", e)
    
    def _fetch_all(self, config: Dict[str, Any]) -> Iterator[SourceJob]:
        """Fetch all data in a single request."""
        client = self._get_client()
        
        try:
            response = client.request(
                method=config["method"],
                url=config["url"],
                json=config.get("body"),
            )
            response.raise_for_status()
            
            data = response.json()
            records = data if isinstance(data, list) else data.get("data", [])
            
            yield SourceJob(
                records=records,
                metadata={"page_num": 0, "count": len(records)},
            )
        
        except httpx.HTTPError as e:
            raise SourceError("api", f"Failed to fetch data: {e}", e)
    
    def health_check(self) -> bool:
        """Check if API is accessible."""
        try:
            client = self._get_client()
            # Resolve with empty params for health check
            url = self.config["url"].split("?")[0]  # Remove query params
            response = client.head(url, timeout=5.0)
            return response.status_code < 500
        except Exception:
            return False


def api_source(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Dict[str, Any]] = None,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    pagination_strategy: str = "offset",
    pagination_config: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> ApiSource:
    """
    Factory function for API source.
    
    Example:
        >>> source = api_source(
        ...     url="https://api.example.com/events?start={{ start_time }}",
        ...     method="GET",
        ...     auth_type="bearer",
        ...     auth_token="secret-token",
        ...     pagination_strategy="offset",
        ...     pagination_config={"offset_param": "page", "limit_param": "per_page"}
        ... )
    """
    return ApiSource(
        url=url,
        method=method,
        headers=headers,
        body=body,
        auth_type=auth_type,
        auth_token=auth_token,
        pagination_strategy=pagination_strategy,
        pagination_config=pagination_config,
        timeout=timeout,
    )
