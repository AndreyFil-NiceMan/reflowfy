"""Elasticsearch source with scroll-based pagination."""

from math import ceil
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
        window = resolved_config.get("window")
        if pit_id and window is not None:
            # Deterministic positional window (docs_per_job path): resume from this
            # job's ``search_after`` cursor and pull exactly ``size`` docs. Adjacent
            # windows share boundary cursors, so there is no overlap or gap.
            try:
                body = dict(resolved_config["base_query"])
                body["pit"] = {"id": pit_id, "keep_alive": resolved_config["scroll"]}
                body.setdefault("sort", ["_shard_doc"])
                target = int(window["size"])
                search_after = window.get("search_after")
                out: List[Any] = []
                while len(out) < target:
                    page_body = dict(body)
                    if search_after is not None:
                        page_body["search_after"] = search_after
                    remaining = target - len(out)
                    raw = client.search(
                        body=page_body, size=min(resolved_config["size"], remaining)
                    )
                    page = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
                    hits = page["hits"]["hits"]
                    if not hits:
                        break
                    out.extend(h["_source"] for h in hits)
                    search_after = hits[-1]["sort"]
                    if limit and len(out) >= limit:
                        return out[:limit]
                return out[:target]
            except ApiError as e:
                raise SourceError("elasticsearch", f"Failed to fetch data: {e}", e)

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
            # Scroll through the whole job. ``size`` is the per-page batch
            # size, not a cap on the job — a single job fetches every matching
            # document. ``limit`` (test/preview only) caps the sample early.
            page_size = min(limit, resolved_config["size"]) if limit else resolved_config["size"]
            scroll = resolved_config["scroll"]

            raw = client.search(
                index=resolved_config["index"],
                body=resolved_config["base_query"],
                scroll=scroll,
                size=page_size,
            )
            page = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
            scroll_id = page["_scroll_id"]
            hits = page["hits"]["hits"]

            records = []
            while hits:
                records.extend(hit["_source"] for hit in hits)
                if limit and len(records) >= limit:
                    records = records[:limit]
                    break
                raw = client.scroll(scroll_id=scroll_id, scroll=scroll)
                page = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
                scroll_id = page["_scroll_id"]
                hits = page["hits"]["hits"]

            client.clear_scroll(scroll_id=scroll_id)
            return records

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
        """Open a PIT and yield one narrowed source per job.

        Two planning strategies:

        - ``docs_per_job`` set → **deterministic positional windows**. Job count
          is ``num_windows = min(ceil(count / docs_per_job), max_slices)`` and
          each job holds ~``ceil(count / num_windows)`` *consecutive* docs (so
          ``docs_per_job=1`` gives exactly one doc per job, no empty jobs). The
          windows are cut by one ``search_after`` pre-scan of the sort keys, so
          this scales past ``index.max_result_window`` unlike ``from``/``size``.
        - ``docs_per_job`` unset → legacy **sliced scroll** with ``num_slices``
          (config, default 1). Elastic hash-partitions docs across slices, so
          slice sizes are uneven — fine for parallelism, not for exact sizing.

        No documents are fetched here — the query is counted first, so a query
        matching no documents yields no jobs. A single job yields ``self``.
        """
        resolved = self.resolve_parameters(runtime_params) or self.config
        client = self._get_client()
        count = self._count_documents(client, resolved)
        if count == 0:
            return

        docs_per_job = resolved.get("docs_per_job")
        if docs_per_job:
            max_slices = int(resolved.get("max_slices", 1024))
            num_windows = min(ceil(count / int(docs_per_job)), max_slices)
            if num_windows <= 1:
                yield self
                return
            window_size = ceil(count / num_windows)
            pit = client.open_point_in_time(index=resolved["index"], keep_alive=resolved["scroll"])
            pit_id = pit["id"]
            for start in self._scan_window_cursors(
                client, resolved, pit_id, window_size, num_windows
            ):
                sub = self._sub_source(resolved)
                sub.config["pit_id"] = pit_id
                sub.config["window"] = {"search_after": start, "size": window_size}
                yield sub
            return

        num_slices = int(resolved.get("num_slices", 1))
        if num_slices <= 1:
            yield self
            return

        pit = client.open_point_in_time(index=resolved["index"], keep_alive=resolved["scroll"])
        pit_id = pit["id"]
        for i in range(num_slices):
            sub = self._sub_source(resolved)
            sub.config["pit_id"] = pit_id
            sub.config["slice"] = {"id": i, "max": num_slices}
            yield sub

    def _sub_source(self, resolved: Dict[str, Any]) -> "ElasticSource":
        """Build a narrowed child source carrying the parent's connection config."""
        return ElasticSource(
            url=resolved["url"],
            index=resolved["index"],
            base_query=resolved["base_query"],
            scroll=resolved["scroll"],
            size=resolved["size"],
            auth=resolved.get("auth"),
            verify_certs=resolved["verify_certs"],
        )

    def _scan_window_cursors(
        self,
        client: Any,
        resolved: Dict[str, Any],
        pit_id: str,
        window_size: int,
        num_windows: int,
    ) -> List[Any]:
        """Return the ``search_after`` start cursor for each of ``num_windows`` windows.

        One O(count) pass over the sort keys (``_source: false``), recording the
        cursor at every ``window_size``-th doc. Window 0 starts at the beginning
        (``None``); window k starts after the last doc of window k-1.

        ponytail: single pre-scan is O(count); the alternative (each job doing
        ``from = k*window_size``) hits ``index.max_result_window`` past ~10k docs.
        """
        base = dict(resolved["base_query"])
        base["pit"] = {"id": pit_id, "keep_alive": resolved["scroll"]}
        base.setdefault("sort", ["_shard_doc"])
        base["_source"] = False
        page_size = int(resolved["size"])
        cursors: List[Any] = [None]  # window 0 starts at the beginning
        seen = 0
        search_after = None
        while len(cursors) < num_windows:
            page = dict(base)
            if search_after is not None:
                page["search_after"] = search_after
            raw = client.search(body=page, size=page_size)
            page_resp = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
            hits = page_resp["hits"]["hits"]
            if not hits:
                break
            for h in hits:
                seen += 1
                search_after = h["sort"]
                if seen % window_size == 0 and len(cursors) < num_windows:
                    cursors.append(h["sort"])
        return cursors

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
                    response = dict(
                        response
                    )  # pyright: ignore[reportCallIssue, reportArgumentType]

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
    docs_per_job: Optional[int] = None,
    max_slices: int = 1024,
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

        To split the query across the worker pool, pass ``docs_per_job`` — the
        manager counts matches and dispatches ``ceil(count / docs_per_job)``
        jobs (capped by ``max_slices``, default 1024). Each job holds a
        *deterministic, consecutive* window of that many docs, so
        ``docs_per_job=1`` gives exactly one doc per job with no empty jobs.
        Windows are cut by a single ``search_after`` pre-scan of the sort keys,
        so this scales past ``index.max_result_window``:

        >>> source = elastic_source(
        ...     url="https://elastic:9200",
        ...     index="logs-*",
        ...     base_query={"query": {"match_all": {}}},
        ...     docs_per_job=1000,
        ... )

    Note:
        ``docs_per_job=1`` produces one job per matched document. On large
        result sets that is a very large number of jobs (each its own Kafka
        message, DB row, and ES query) — use a larger ``docs_per_job`` unless
        per-document isolation is truly required.
    """
    return ElasticSource(
        url=url,
        index=index,
        base_query=base_query,
        scroll=scroll,
        size=size,
        auth=auth,
        verify_certs=verify_certs,
        docs_per_job=docs_per_job,
        max_slices=max_slices,
        **kwargs,
    )
