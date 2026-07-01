"""Elasticsearch source with scroll-based pagination."""

from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError

from reflowfy.sources.base import BaseSource, SourceError, SourceJob


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
        auth: Optional[Tuple[str, str]] = None,
        verify_certs: bool = True,
        **kwargs: Any,
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
            auth = self.config.get("auth")
            if isinstance(auth, list):
                auth = tuple(auth)
            self._client = Elasticsearch(
                hosts=[self.config["url"]],
                basic_auth=auth,
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

        if resolved_config is None:
            raise SourceError("elasticsearch", "No valid configuration resolved", None)

        client = self._get_client()

        pit_id = resolved_config.get("pit_id")
        slice_spec = resolved_config.get("slice")
        if pit_id and slice_spec is not None:
            try:
                body = dict(resolved_config["base_query"])
                body["slice"] = slice_spec
                body["pit"] = {"id": pit_id, "keep_alive": resolved_config["scroll"]}
                records: List[Any] = []
                search_after = None
                while True:
                    page_body = dict(body)
                    if search_after is not None:
                        page_body["search_after"] = search_after
                    page_body.setdefault("sort", ["_shard_doc"])
                    resp = client.search(body=page_body, size=resolved_config["size"])
                    resp = resp.body if hasattr(resp, "body") else resp
                    hits = resp["hits"]["hits"]
                    if not hits:
                        break
                    records.extend(h["_source"] for h in hits)
                    search_after = hits[-1]["sort"]
                    if limit and len(records) >= limit:
                        return records[:limit]
                return records
            except ApiError as e:
                raise SourceError("elasticsearch", f"Failed to fetch data: {e}", e)

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

    def _count_documents(self, client: Any, resolved: Dict[str, Any]) -> int:
        """Return how many documents the base query matches (metadata only)."""
        base_query = resolved.get("base_query") or {}
        query = base_query.get("query") if isinstance(base_query, dict) else None
        body = {"query": query} if query is not None else None
        resp = client.count(index=resolved["index"], body=body)
        resp = resp.body if hasattr(resp, "body") else resp
        return int(resp.get("count", 0))

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["ElasticSource"]:
        """Open a PIT and yield one source per sliced-scroll slice.

        ``num_slices`` (config, default 1) controls parallelism. With 1 slice
        this is a single job. No documents are fetched here — but the query is
        counted first, so a query matching no documents yields no jobs.
        """
        resolved = self.resolve_parameters(runtime_params) or self.config
        client = self._get_client()
        if self._count_documents(client, resolved) == 0:
            return

        num_slices = int(resolved.get("num_slices", 1))
        if num_slices <= 1:
            yield self
            return

        pit = client.open_point_in_time(index=resolved["index"], keep_alive=resolved["scroll"])
        pit_id = pit["id"]
        for i in range(num_slices):
            sub = ElasticSource(
                url=resolved["url"],
                index=resolved["index"],
                base_query=resolved["base_query"],
                scroll=resolved["scroll"],
                size=resolved["size"],
                auth=resolved.get("auth"),
                verify_certs=resolved["verify_certs"],
            )
            sub.config["pit_id"] = pit_id
            sub.config["slice"] = {"id": i, "max": num_slices}
            yield sub

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
        if resolved_config is None:
            raise SourceError("elasticsearch", "No valid configuration resolved", None)
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
            if hasattr(response, "body"):
                response = response.body
            elif not isinstance(response, dict):
                response = dict(response)  # pyright: ignore[reportCallIssue, reportArgumentType]

            response = cast(Dict[str, Any], response)
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

            page_num = 0

            while hits:
                # Extract source documents
                records = [hit["_source"] for hit in hits]

                yield SourceJob(
                    records=records,
                    metadata={
                        "scroll_id": str(
                            scroll_id
                        ),  # Convert to string to ensure JSON serializable
                        "page_num": page_num,
                        "count": len(records),
                    },
                )

                page_num += 1

                # Get next page
                response = client.scroll(scroll_id=scroll_id, scroll=scroll)

                # Convert response to dict if needed
                if hasattr(response, "body"):
                    response = response.body
                elif not isinstance(response, dict):
                    response = dict(response)  # pyright: ignore[reportCallIssue, reportArgumentType]

                response = cast(Dict[str, Any], response)
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
    auth: Optional[Tuple[str, str]] = None,
    verify_certs: bool = True,
    **kwargs: Any,
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
