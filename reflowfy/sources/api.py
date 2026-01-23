"""REST API sources with pagination support."""

from abc import abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Union
import httpx
from reflowfy.sources.base import BaseSource, SourceJob, SourceError


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
        headers: Optional[Dict[str, str]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        pagination_type: str = "offset",
        page_size: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        page_param: str = "page",
        per_page_param: str = "per_page",
        cursor_param: str = "cursor",
        cursor_response_key: str = "next_cursor",
        data_key: str = "data",
        total_key: Optional[str] = "total",
        timeout: float = 30.0,
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
        resolved_config = self.resolve_parameters(runtime_params)
        client = self._get_client()
        
        endpoint = resolved_config["endpoint"]
        page_size = resolved_config["page_size"]
        pagination_type = resolved_config["pagination_type"]
        
        page_num = 0
        offset = 0
        cursor = None
        
        try:
            while True:
                # Build params based on pagination type
                if pagination_type == "offset":
                    params = {
                        resolved_config["offset_param"]: offset,
                        resolved_config["limit_param"]: page_size,
                    }
                elif pagination_type == "page":
                    params = {
                        resolved_config["page_param"]: page_num + 1,
                        resolved_config["per_page_param"]: page_size,
                    }
                elif pagination_type == "cursor":
                    params = {resolved_config["limit_param"]: page_size}
                    if cursor:
                        params[resolved_config["cursor_param"]] = cursor
                else:
                    params = {}
                
                response = client.request(resolved_config["method"], endpoint, params=params)
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
        try:
            client = self._get_client()
            response = client.request("HEAD", self.config["endpoint"], timeout=5.0)
            return response.status_code < 500
        except Exception:
            # Try GET if HEAD fails
            try:
                response = client.request("GET", self.config["endpoint"], timeout=5.0)
                return response.status_code < 500
            except Exception:
                return False


class IDBasedAPISource(BaseSource):
    """
    ID-based REST API source for endpoints like /api/resource/{id}.
    
    Fetches resources by ID from a list of IDs.
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
        **kwargs,
    ):
        """
        Initialize ID-based API source.
        
        Args:
            base_url: Base API URL
            endpoint_template: Endpoint template with {id} placeholder
            ids: Static list of IDs to fetch
            ids_source: Another source that provides IDs
            ids_field: Field name containing ID in ids_source records
            method: HTTP method
            headers: Custom headers
            auth_type: Authentication type (bearer, apikey)
            auth_token: Authentication token
            batch_size: Number of IDs per job batch
            timeout: Request timeout
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
            **kwargs,
        }
        super().__init__(config)
        self._ids_source = ids_source
        self._client: Optional[httpx.Client] = None
    
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
        """Get all IDs from config or ids_source."""
        if self.config["ids"]:
            return self.config["ids"]
        
        if self._ids_source:
            # Fetch IDs from source
            records = self._ids_source.fetch(runtime_params)
            ids_field = self.config["ids_field"]
            return [r.get(ids_field) for r in records if r.get(ids_field)]
        
        # Check runtime params for IDs
        return runtime_params.get("ids", [])
    
    def _fetch_by_id(self, id_value: Union[str, int]) -> Optional[Any]:
        """Fetch a single resource by ID."""
        client = self._get_client()
        endpoint = self.config["endpoint_template"].format(id=id_value)
        
        try:
            response = client.request(self.config["method"], endpoint)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            return None
    
    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch resources by ID (local mode).
        
        Args:
            runtime_params: Runtime parameters
            limit: Optional limit for testing
        
        Returns:
            List of fetched resources
        """
        resolved_config = self.resolve_parameters(runtime_params)
        ids = self._get_all_ids(runtime_params)
        
        if limit:
            ids = ids[:limit]
        
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
        Split IDs into batched jobs.
        
        Args:
            runtime_params: Runtime parameters
            batch_size: IDs per job
        
        Yields:
            SourceJob instances
        """
        resolved_config = self.resolve_parameters(runtime_params)
        ids = self._get_all_ids(runtime_params)
        batch_size = resolved_config.get("batch_size", batch_size)
        
        # Batch the IDs
        batch_num = 0
        for i in range(0, len(ids), batch_size):
            id_batch = ids[i:i + batch_size]
            
            # Fetch records for this batch
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
        """Check if API is accessible."""
        try:
            client = self._get_client()
            # Try hitting the base URL
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
        **kwargs,
    )


def id_based_api_source(
    base_url: str,
    endpoint_template: str,
    ids: Optional[List[Union[str, int]]] = None,
    batch_size: int = 50,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    **kwargs,
) -> IDBasedAPISource:
    """
    Factory function for ID-based API source.
    
    Example:
        >>> source = id_based_api_source(
        ...     base_url="https://api.example.com",
        ...     endpoint_template="/users/{id}",
        ...     ids=[1, 2, 3, 4, 5],
        ...     batch_size=10
        ... )
    """
    return IDBasedAPISource(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids,
        batch_size=batch_size,
        auth_type=auth_type,
        auth_token=auth_token,
        **kwargs,
    )
