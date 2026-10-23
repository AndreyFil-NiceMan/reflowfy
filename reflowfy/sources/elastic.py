"""Elasticsearch source with scroll-based pagination."""

from copy import deepcopy
from math import ceil
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError
from elastic_transport import TransportError

from reflowfy.core.types import Record, Records, wrap_as_record
from reflowfy.observability.logging import get_logger
from reflowfy.sources.base import BaseSource, SourceError, SourceJob

# The manager opens a PIT while planning; workers search it later, as the
# rate-limited dispatch loop sends their jobs. Each search extends the
# keep_alive, so this only has to cover the gap between two consecutive fetches
# (about 60 / rate_limit seconds, longer if workers stall) — not the whole
# execution. It is deliberately NOT the ``scroll`` value: that is a per-page
# scroll timeout (default 2m), and reusing it here expired the PIT before later
# jobs ran, surfacing as ApiError(503, 'search_phase_execution_exception') in the
# worker. Workers stopped for longer than this (or an Elasticsearch restart) lose
# the PIT: every remaining job fails with "No search context found".
DEFAULT_PIT_KEEP_ALIVE = "30m"

# Lifetime of the PIT a range-mode job opens for itself. It only has to span the
# paging of one job (seconds to minutes), and is refreshed by every page.
RANGE_JOB_PIT_KEEP_ALIVE = "5m"

WINDOW_SNAPSHOT = "snapshot"
WINDOW_RANGE = "range"

logger = get_logger(__name__)


def _pit_keep_alive(config: Dict[str, Any]) -> str:
    """Read the PIT keep_alive, tolerating job payloads serialized before it existed."""
    return str(config.get("pit_keep_alive") or DEFAULT_PIT_KEEP_ALIVE)


def _first_sort_key(base_query: Dict[str, Any]) -> Tuple[str, str]:
    """``(field, "asc"|"desc")`` of the query's first sort key, for range windows.

    Range windows cut the result set by value of this key, so it must be a real field.
    ``_shard_doc`` / ``_doc`` / ``_score`` (and no sort at all) order documents in
    a way a later query cannot reproduce, so they cannot define a window.
    """
    sort: Any = base_query.get("sort")
    first: Any = sort
    if isinstance(sort, list) and sort:
        first = cast(List[Any], sort)[0]
    field: Optional[str] = None
    order = "asc"
    if isinstance(first, str):
        field = first
    elif isinstance(first, dict) and len(first) == 1:  # pyright: ignore[reportUnknownArgumentType]
        spec = cast(Dict[str, Any], first)
        field, opts = next(iter(spec.items()))
        if isinstance(opts, str):
            order = opts
        elif isinstance(opts, dict):
            order = str(cast(Dict[str, Any], opts).get("order", "asc"))
    if not field or field in ("_shard_doc", "_doc", "_score") or order not in ("asc", "desc"):
        raise SourceError(
            "elasticsearch",
            'window_mode="range" needs base_query["sort"] to start with a field, e.g. '
            '[{"@timestamp": "asc"}] — the windows are ranges of that field',
            None,
        )
    return field, order


def _range_clause(field: str, order: str, after: Any, upto: Any) -> Dict[str, Any]:
    """The ES ``range`` filter for the window ``(after, upto]`` in sort order."""
    bounds: Dict[str, Any] = {}
    if after is not None:
        bounds["gt" if order == "asc" else "lt"] = after
    if upto is not None:
        bounds["lte" if order == "asc" else "gte"] = upto
    return {"range": {field: bounds}}


def _hit_record(hit: Dict[str, Any]) -> Record:
    """One search hit as a record: ``{"data": _source}``, plus ``fields`` if present.

    ``script_fields`` / ``fields`` / ``stored_fields`` come back in ``hit["fields"]``,
    and with ``_source: false`` (or ``script_fields`` alone) there is no ``_source``.
    """
    record = wrap_as_record(hit.get("_source", {}))
    if "fields" in hit:
        record["fields"] = hit["fields"]
    return record


def _sub_aggs(spec: Any) -> Dict[str, Any]:
    """The ``aggs``/``aggregations`` children of one aggregation (or query) body."""
    if not isinstance(spec, dict):
        return {}
    d = cast(Dict[str, Any], spec)
    return cast(Dict[str, Any], d.get("aggs") or d.get("aggregations") or {})


def _find_composites(aggs: Dict[str, Any], prefix: Tuple[str, ...] = ()) -> List[Tuple[str, ...]]:
    """Name paths to every composite aggregation, at any nesting depth."""
    found: List[Tuple[str, ...]] = []
    for name, spec in aggs.items():
        path = (*prefix, str(name))
        if isinstance(spec, dict) and "composite" in spec:
            found.append(path)
        else:
            found.extend(_find_composites(_sub_aggs(spec), path))
    return found


def _composite_path(base_query: Any) -> Optional[Tuple[str, ...]]:
    """Path to the query's composite aggregation, ``None`` if it has none.

    Raises ``SourceError`` for several: each needs its own ``after_key`` paging
    and they would otherwise be silently dropped after the first.
    """
    found = _find_composites(_sub_aggs(base_query))
    if len(found) > 1:
        names = ", ".join("/".join(p) for p in found)
        raise SourceError(
            "elasticsearch",
            f"Query has {len(found)} composite aggregations ({names}); "
            "only one per source is supported — use one source per composite.",
            None,
        )
    return found[0] if found else None


def _prune_to_path(aggs: Dict[str, Any], path: Tuple[str, ...]) -> List[str]:
    """Drop every aggregation not on ``path`` (in place); return the dropped names.

    Siblings are dead weight — their results are never read — and cost the
    cluster work on every page.
    """
    dropped: List[str] = []
    for name in [n for n in aggs if n != path[0]]:
        dropped.append(name)
        del aggs[name]
    if len(path) > 1:
        dropped.extend(_prune_to_path(_sub_aggs(aggs[path[0]]), path[1:]))
    return dropped


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
        pit_keep_alive: str = DEFAULT_PIT_KEEP_ALIVE,
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
            pit_keep_alive: Lifetime of the point-in-time opened by ``split()``.
                Must outlive the gap between two consecutive worker fetches
                against it, not the whole execution — every search on a PIT
                extends its keep_alive. See :data:`DEFAULT_PIT_KEEP_ALIVE`.
            **kwargs: Additional Elasticsearch client params
        """
        config = {
            "url": url,
            "index": index,
            "base_query": base_query,
            "scroll": scroll,
            "size": size,
            "pit_keep_alive": pit_keep_alive,
            "auth": auth,
            "verify_certs": verify_certs,
            **kwargs,
        }
        super().__init__(config)
        self._client: Optional[Elasticsearch] = None

    def _get_client(self) -> Elasticsearch:
        """Get or create Elasticsearch client."""
        if self._client is None:
            auth: Any = self.config.get("auth")
            if isinstance(auth, list):
                auth = tuple(cast(List[Any], auth))
            url = self.config["url"]
            verify_certs = self.config["verify_certs"]
            kwargs: Dict[str, Any] = {
                "hosts": [url],
                "basic_auth": auth,
                "verify_certs": verify_certs,
            }
            # Silence elastic_transport's "verify_certs=False is insecure"
            # SecurityWarning when cert verification is deliberately disabled on
            # an https cluster (ssl_show_warn is only valid on https URLs).
            if url.lower().startswith("https://") and not verify_certs:
                kwargs["ssl_show_warn"] = False
            self._client = Elasticsearch(**kwargs)
        return self._client

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Records:
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

        path = _composite_path(resolved_config["base_query"])
        if path:
            return self._fetch_composite(client, resolved_config, path, limit)

        if "search_after" in resolved_config["base_query"] and not resolved_config.get("pit_id"):
            return self._fetch_search_after(client, resolved_config, limit)

        window = resolved_config.get("window")
        if window is not None and window.get("mode") == WINDOW_RANGE:
            return self._fetch_range_window(client, resolved_config, window, limit)

        pit_id = resolved_config.get("pit_id")
        if pit_id and window is not None:
            # Deterministic positional window (docs_per_job path): resume from this
            # job's ``search_after`` cursor and pull exactly ``size`` docs. Adjacent
            # windows share boundary cursors, so there is no overlap or gap.
            try:
                body = dict(resolved_config["base_query"])
                body["pit"] = {"id": pit_id, "keep_alive": _pit_keep_alive(resolved_config)}
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
                    out.extend(_hit_record(h) for h in hits)
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
                body["pit"] = {"id": pit_id, "keep_alive": _pit_keep_alive(resolved_config)}
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
                    records.extend(_hit_record(h) for h in hits)
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
                records.extend(_hit_record(hit) for hit in hits)
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

    def _fetch_search_after(
        self, client: Any, resolved: Dict[str, Any], limit: Optional[int]
    ) -> Records:
        """Page a query that brings its own ``search_after`` cursor.

        Elasticsearch rejects ``search_after`` inside a scroll, so the default
        scroll path can't serve it. Resume from the given cursor and keep going
        with the last hit's ``sort`` until a page comes back empty.

        ponytail: no PIT, so the caller's ``sort`` must end in a unique
        tiebreaker (e.g. ``_doc`` or an id) or ties can repeat/skip across pages.
        """
        body = deepcopy(resolved["base_query"])
        if not body.get("sort"):
            raise SourceError(
                "elasticsearch",
                "'search_after' needs a 'sort' in the query; it is the cursor's key.",
                None,
            )
        body["size"] = min(limit, resolved["size"]) if limit else resolved["size"]
        records: Records = []
        try:
            while True:
                raw = client.search(index=resolved["index"], body=body)
                page = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
                hits = page["hits"]["hits"]
                if not hits:
                    return records
                records.extend(_hit_record(h) for h in hits)
                if limit and len(records) >= limit:
                    return records[:limit]
                body["search_after"] = hits[-1]["sort"]
        except ApiError as e:
            raise SourceError("elasticsearch", f"Failed to fetch data: {e}", e)

    def _composite_request(
        self, resolved: Dict[str, Any], path: Tuple[str, ...], size: Optional[int] = None
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Build the composite search body; return ``(body, composite_spec)``.

        Other aggregations are pruned from the request (their results are never
        read). The composite's page size is ``size``, else its own, else the
        source's ``size`` (Elasticsearch would default to 10).
        """
        body = deepcopy(resolved["base_query"])
        # In the body, not as ``size=``: the 8.x client copies kwargs into the
        # body dict we reuse across pages, so page 2 would send ``size`` twice.
        body["size"] = 0
        aggs = _sub_aggs(body)
        dropped = _prune_to_path(aggs, path)
        if dropped:
            logger.info(
                "elastic composite: ignoring non-composite aggregations %s",
                dropped,
                extra={"event": "source_aggs_ignored"},
            )
        node: Dict[str, Any] = aggs
        for name in path[:-1]:
            node = _sub_aggs(node[name])
        composite = node[path[-1]]["composite"]
        if size:
            composite["size"] = size
        else:
            composite.setdefault("size", resolved["size"])
        return body, composite

    @staticmethod
    def _composite_page(
        client: Any, resolved: Dict[str, Any], body: Dict[str, Any], path: Tuple[str, ...]
    ) -> Tuple[List[Any], Any]:
        """Run one composite request; return ``(buckets, after_key)``."""
        try:
            raw = client.search(index=resolved["index"], body=body)
        except ApiError as e:
            raise SourceError("elasticsearch", f"Failed to fetch data: {e}", e)
        resp = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
        agg: Any = resp.get("aggregations", {})
        for name in path:
            if not isinstance(agg, dict) or name not in agg:
                raise SourceError(
                    "elasticsearch",
                    f"Composite aggregation '{'/'.join(path)}' not found in the "
                    f"response at '{name}'. Only single-bucket parents "
                    "(filter, nested, global, ...) are supported, not terms/range. "
                    "Make the parent a source of the composite instead.",
                    None,
                )
            agg = cast(Dict[str, Any], agg)[name]
        found = cast(Dict[str, Any], agg)
        return found.get("buckets", []), found.get("after_key")

    def _fetch_composite(
        self, client: Any, resolved: Dict[str, Any], path: Tuple[str, ...], limit: Optional[int]
    ) -> Records:
        """Read a composite aggregation; one record per bucket.

        Buckets live in ``aggregations``, not ``hits``, so the hit-based paths
        would return nothing (or raw docs) for such a query. The composite may sit
        under single-bucket parents (``filter``, ``nested``, ...). The top-level
        hit ``size`` is forced to 0.

        A sub-source planned by ``split()`` carries ``composite_page`` and reads
        exactly that one page. Otherwise every page is walked via ``after_key``
        and held in memory — use ``docs_per_job`` to bound that.
        """
        page_spec = resolved.get("composite_page")
        body, composite = self._composite_request(
            resolved, path, page_spec["size"] if page_spec else None
        )
        if page_spec:
            if page_spec["after"] is not None:
                composite["after"] = page_spec["after"]
            buckets, _ = self._composite_page(client, resolved, body, path)
            paged: Records = [wrap_as_record(b) for b in buckets]
            return paged[:limit] if limit else paged

        records: Records = []
        pages = 0
        while True:
            buckets, after_key = self._composite_page(client, resolved, body, path)
            pages += 1
            records.extend(wrap_as_record(b) for b in buckets)
            logger.debug(
                "elastic composite page %d: %d buckets",
                pages,
                len(buckets),
                extra={"event": "source_page"},
            )
            if limit and len(records) >= limit:
                return records[:limit]
            if not buckets or not after_key:
                logger.info(
                    "elastic composite '%s': %d buckets in %d pages",
                    "/".join(path),
                    len(records),
                    pages,
                    extra={"event": "source_planned", "source_type": "elastic", "pages": pages},
                )
                return records
            composite["after"] = after_key

    def _split_composite(
        self, client: Any, resolved: Dict[str, Any], path: Tuple[str, ...], page_size: int
    ) -> Iterator["ElasticSource"]:
        """Yield one sub-source per composite page of ``page_size`` buckets.

        The scan keeps only each page's ``after_key`` — sub-aggregations are
        stripped so it stays cheap — and workers re-read their own page. Jobs
        are yielded as the scan advances, so neither side holds all buckets.
        """
        body, composite = self._composite_request(resolved, path, page_size)
        node: Dict[str, Any] = _sub_aggs(body)
        for name in path[:-1]:
            node = _sub_aggs(node[name])
        node[path[-1]].pop("aggs", None)
        node[path[-1]].pop("aggregations", None)

        cursor = composite.get("after")
        pages = 0
        while True:
            buckets, after_key = self._composite_page(client, resolved, body, path)
            if not buckets:
                break
            sub = self._sub_source(resolved)
            sub.config["composite_page"] = {"after": cursor, "size": page_size}
            pages += 1
            yield sub
            if len(buckets) < page_size or not after_key:
                break
            cursor = composite["after"] = after_key
        logger.info(
            "elastic composite '%s': planned %d jobs",
            "/".join(path),
            pages,
            extra={"event": "source_planned", "source_type": "elastic", "jobs_planned": pages},
        )

    def _count_documents(self, client: Any, resolved: Dict[str, Any]) -> int:
        """Return how many documents the base query matches (metadata only)."""
        base_query: Any = resolved.get("base_query") or {}
        query = (
            cast(Dict[str, Any], base_query).get("query") if isinstance(base_query, dict) else None
        )
        body = {"query": query} if query is not None else None
        resp = client.count(index=resolved["index"], body=body)
        resp = resp.body if hasattr(resp, "body") else resp
        return int(resp.get("count", 0))

    def _fetch_range_window(
        self,
        client: Elasticsearch,
        resolved: Dict[str, Any],
        window: Dict[str, Any],
        limit: Optional[int],
    ) -> Records:
        """Fetch one range window with a PIT opened just for this job.

        Nothing is shared with the plan or with other jobs: an Elasticsearch restart
        or a long worker stall costs only the jobs in flight, and re-running a failed
        job just works. The price is that jobs no longer see one snapshot — documents
        indexed or deleted while the execution runs show up or not depending on when
        their window is read.
        """
        field, order = window["field"], window["order"]
        base = dict(resolved["base_query"])
        clause = _range_clause(field, order, window.get("after"), window.get("upto"))
        user_query: Dict[str, Any] = base.get("query") or {"match_all": {}}
        base["query"] = {"bool": {"filter": [user_query, clause]}}
        base.pop("search_after", None)
        page_size = int(resolved["size"])
        pit_id: Optional[str] = None
        try:
            pit_id = client.open_point_in_time(
                index=resolved["index"], keep_alive=RANGE_JOB_PIT_KEEP_ALIVE
            )["id"]
            base["pit"] = {"id": pit_id, "keep_alive": RANGE_JOB_PIT_KEEP_ALIVE}
            out: List[Any] = []
            search_after: Any = None
            while True:
                page_body = dict(base)
                if search_after is not None:
                    page_body["search_after"] = search_after
                raw = client.search(body=page_body, size=page_size)
                page = cast(Dict[str, Any], raw.body if hasattr(raw, "body") else raw)
                hits = page["hits"]["hits"]
                if not hits:
                    return out
                out.extend(_hit_record(h) for h in hits)
                search_after = hits[-1]["sort"]
                if limit and len(out) >= limit:
                    return out[:limit]
        except (ApiError, TransportError) as e:
            raise SourceError("elasticsearch", f"Failed to fetch data: {e}", e)
        finally:
            if pit_id:
                try:
                    client.close_point_in_time(id=pit_id)
                except Exception:  # best effort: it expires on its own
                    logger.debug(
                        "Could not close PIT for a range window",
                        exc_info=True,
                        extra={"event": "pit_close_error"},
                    )

    def _split_range_windows(
        self, client: Elasticsearch, resolved: Dict[str, Any], count: int
    ) -> Iterator["ElasticSource"]:
        """Plan ``window_mode="range"`` jobs: value ranges of the first sort key.

        One pre-scan under a short PIT finds the key's value at every
        ``window_size``-th document; window k is ``(value[k-1], value[k]]``. A range
        takes every document with a value in it, so documents sharing a boundary value
        all land in one window (windows are therefore approximately, not exactly,
        ``docs_per_job`` big) and none is skipped or read twice. The PIT is closed
        before the first job is yielded.
        """
        field, order = _first_sort_key(resolved["base_query"])
        docs_per_job = int(resolved["docs_per_job"])
        num_windows = min(ceil(count / docs_per_job), int(resolved.get("max_slices", 1024)))
        if num_windows <= 1:
            yield self
            return
        window_size = ceil(count / num_windows)
        pit_id = client.open_point_in_time(
            index=resolved["index"], keep_alive=_pit_keep_alive(resolved)
        )["id"]
        try:
            cursors = self._scan_window_cursors(client, resolved, pit_id, window_size, num_windows)
        finally:
            try:
                client.close_point_in_time(id=pit_id)
            except Exception:
                logger.debug(
                    "Could not close the planning PIT",
                    exc_info=True,
                    extra={"event": "pit_close_error"},
                )
        boundaries: List[Any] = []
        for cursor in cursors[1:]:  # cursors[0] is None: window 0 starts at the beginning
            value = cursor[0]
            if not boundaries or value != boundaries[-1]:
                boundaries.append(value)
        edges: List[Any] = [None, *boundaries, None]
        for after, upto in zip(edges[:-1], edges[1:]):
            sub = self._sub_source(resolved)
            sub.config["window"] = {
                "mode": WINDOW_RANGE,
                "field": field,
                "order": order,
                "after": after,
                "upto": upto,
            }
            yield sub

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
        path = _composite_path(resolved["base_query"])
        if path:
            if int(resolved.get("num_slices") or 1) > 1:
                logger.warning(
                    "elastic source: num_slices is ignored for composite aggregations",
                    extra={"event": "source_option_ignored"},
                )
            docs_per_job = int(resolved.get("docs_per_job") or 0)
            if docs_per_job:
                yield from self._split_composite(self._get_client(), resolved, path, docs_per_job)
            else:
                yield self  # one job pages the whole aggregation into memory
            return
        client = self._get_client()
        count = self._count_documents(client, resolved)
        if count == 0:
            return

        docs_per_job = resolved.get("docs_per_job")
        if docs_per_job and resolved.get("window_mode") == WINDOW_RANGE:
            yield from self._split_range_windows(client, resolved, count)
            return
        if docs_per_job:
            max_slices = int(resolved.get("max_slices", 1024))
            num_windows = min(ceil(count / int(docs_per_job)), max_slices)
            if num_windows <= 1:
                yield self
                return
            window_size = ceil(count / num_windows)
            pit = client.open_point_in_time(
                index=resolved["index"], keep_alive=_pit_keep_alive(resolved)
            )
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

        pit = client.open_point_in_time(
            index=resolved["index"], keep_alive=_pit_keep_alive(resolved)
        )
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
            pit_keep_alive=_pit_keep_alive(resolved),
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
        base["pit"] = {"id": pit_id, "keep_alive": _pit_keep_alive(resolved)}
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
                records = [_hit_record(hit) for hit in hits]

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
                    # elasticsearch's ObjectApiResponse iterates keys, not pairs
                    response = dict(cast(Dict[str, Any], response))

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
    pit_keep_alive: str = DEFAULT_PIT_KEEP_ALIVE,
    window_mode: str = WINDOW_SNAPSHOT,
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

    Window modes (``docs_per_job`` only):

        ``window_mode="snapshot"`` (default) reads every job from one point-in-time
        opened at planning, so the whole execution sees the index as it was when it
        started. The price: that PIT is shared state. An Elasticsearch restart
        or a worker stall longer than ``pit_keep_alive`` destroys it and every
        remaining job fails with "No search context found".

        ``window_mode="range"`` cuts the result set into value ranges of the first
        ``sort`` key and each job opens its own short PIT. An Elasticsearch restart
        fails only the jobs in flight (re-run them with
        ``POST /executions/{id}/retry-failed``), and there is no long-lived PIT to
        expire — use it for long runs. Trade-offs: documents indexed or deleted
        while the run is in progress may or may not appear, and documents sharing a
        boundary value stay together, so job sizes are only approximately
        ``docs_per_job``. Requires ``base_query["sort"]`` to start with a field:

        >>> source = elastic_source(
        ...     url="https://elastic:9200",
        ...     index="logs-*",
        ...     base_query={"query": {"match_all": {}}, "sort": [{"@timestamp": "asc"}]},
        ...     docs_per_job=1000,
        ...     window_mode="range",
        ... )

    Note:
        The PIT that ``split()`` opens lives for ``pit_keep_alive`` (default
        ``"30m"``), independent of ``scroll``. Workers search that PIT as their
        jobs are dispatched, so a value shorter than the gap between two
        consecutive fetches (a very low rate_limit, or workers stopped for a
        while) expires it mid-execution and the remaining jobs fail with
        ``ApiError(503, 'search_phase_execution_exception')`` / "No search
        context found". Raise it for such executions; every search on the PIT
        extends the window, so it need not span the whole run.

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
        pit_keep_alive=pit_keep_alive,
        window_mode=window_mode,
        **kwargs,
    )
