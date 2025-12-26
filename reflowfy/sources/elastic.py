"""Elasticsearch source with scroll-based pagination."""

from typing import Any, Dict, Iterator, List, Optional
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from reflowfy.sources.base import BaseSource, SourceJob, SourceError


class ElasticSource(BaseSource):
    """
    Elasticsearch source connector.
    
    Supports:
    - Runtime parameter resolution in queries (Jinja2)
    - Scroll API for pagination
    - Job splitting per scroll page
    """
    
    def __init__(
        self,
        url: str,
        index: str,
        base_query: Dict[str, Any],
        scroll: str = "2m",
        size: int = 1000,
        auth: Optional[tuple] = None,
        verify_certs: bool = True,
        **kwargs,
    ):
        """
        Initialize Elasticsearch source.
        
        Args:
            url: Elasticsearch URL
            index: Index pattern to query
            base_query: Query DSL (supports Jinja2 templates)
            scroll: Scroll window duration
            size: Documents per scroll page
            auth: Optional (username, password) tuple
            verify_certs: Whether to verify SSL certificates
            **kwargs: Additional Elasticsearch client params
        """
        config = {
            "url": url,
            "index": index,
            "base_query": base_query,
            "scroll": scroll,
            "size": size,
            "auth": auth,
            "verify_certs": verify_certs,
            **kwargs,
        }
        super().__init__(config)
        self._client: Optional[Elasticsearch] = None
    
    def _get_client(self) -> Elasticsearch:
        """Get or create Elasticsearch client."""
        if self._client is None:
            self._client = Elasticsearch(
                hosts=[self.config["url"]],
                basic_auth=self.config.get("auth"),
                verify_certs=self.config["verify_certs"],
            )
        return self._client
    
    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch data from Elasticsearch (local mode).
        
        Args:
            runtime_params: Runtime parameters for query template
            limit: Optional limit for testing
        
        Returns:
            List of documents
        """
        resolved_config = self.resolve_parameters(runtime_params)
        client = self._get_client()
        
        try:
            # Use search API with limit for local mode
            search_size = min(limit, resolved_config["size"]) if limit else resolved_config["size"]
            
            response = client.search(
                index=resolved_config["index"],
                body=resolved_config["base_query"],
                size=search_size,
            )
            
            hits = response["hits"]["hits"]
            return [hit["_source"] for hit in hits]
        
        except ApiError as e:
            raise SourceError("elasticsearch", f"Failed to fetch data: {e}", e)
    
    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split Elasticsearch data into jobs using scroll API.
        
        Each scroll page becomes one job.
        
        Args:
            runtime_params: Runtime parameters for query template
            batch_size: Documents per job (uses config size if not specified)
        
        Yields:
            SourceJob instances
        """
        resolved_config = self.resolve_parameters(runtime_params)
        client = self._get_client()
        
        size = resolved_config.get("size", batch_size)
        scroll = resolved_config["scroll"]
        
        try:
            # Initialize scroll
            response = client.search(
                index=resolved_config["index"],
                body=resolved_config["base_query"],
                scroll=scroll,
                size=size,
            )
            
            # Convert response to dict if it's not already (Elasticsearch 8.x compatibility)
            if hasattr(response, 'body'):
                response = response.body
            elif not isinstance(response, dict):
                response = dict(response)
            
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]
            
            page_num = 0
            
            while hits:
                # Extract source documents
                records = [hit["_source"] for hit in hits]
                
                yield SourceJob(
                    records=records,
                    metadata={
                        "scroll_id": str(scroll_id),  # Convert to string to ensure JSON serializable
                        "page_num": page_num,
                        "count": len(records),
                    },
                )
                
                page_num += 1
                
                # Get next page
                response = client.scroll(scroll_id=scroll_id, scroll=scroll)
                
                # Convert response to dict if needed
                if hasattr(response, 'body'):
                    response = response.body
                elif not isinstance(response, dict):
                    response = dict(response)
                
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]
            
            # Clear scroll
            client.clear_scroll(scroll_id=scroll_id)
        
        except ApiError as e:
            raise SourceError("elasticsearch", f"Failed to split jobs: {e}", e)
    
    def health_check(self) -> bool:
        """Check Elasticsearch cluster health."""
        try:
            client = self._get_client()
            health = client.cluster.health()
            return health["status"] in ["green", "yellow"]
        except Exception:
            return False


def elastic_source(
    url: str,
    index: str,
    base_query: Dict[str, Any],
    scroll: str = "2m",
    size: int = 1000,
    auth: Optional[tuple] = None,
    verify_certs: bool = True,
    **kwargs,
) -> ElasticSource:
    """
    Factory function for Elasticsearch source.
    
    Example:
        >>> source = elastic_source(
        ...     url="https://elastic:9200",
        ...     index="logs-*",
        ...     base_query={
        ...         "query": {
        ...             "range": {
        ...                 "@timestamp": {
        ...                     "gte": "{{ start_time }}",
        ...                     "lte": "{{ end_time }}"
        ...                 }
        ...             }
        ...         }
        ...     },
        ...     scroll="2m",
        ...     size=1000
        ... )
    """
    return ElasticSource(
        url=url,
        index=index,
        base_query=base_query,
        scroll=scroll,
        size=size,
        auth=auth,
        verify_certs=verify_certs,
        **kwargs,
    )
