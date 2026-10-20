"""ElasticSource paths the other elastic suites don't reach: client construction,
Elasticsearch errors on every read path, PIT clean-up failures, composite edge
cases, a pre-scan that runs out of documents, and response-object shapes."""

import logging
from typing import Any, Dict, Iterator, List, Mapping
from unittest.mock import MagicMock

import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
from elastic_transport import ConnectionError as TransportConnectionError
from elasticsearch import ApiError

import reflowfy.sources.elastic as elastic_mod
from reflowfy.sources.base import SourceError
from reflowfy.sources.elastic import ElasticSource, _composite_path, _first_sort_key

LOGGER = elastic_mod.logger.name  # get_logger prefixes "reflowfy."


def _api_error(status: int = 503) -> ApiError:
    meta = ApiResponseMeta(
        status=status,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig("http", "es", 9200),
    )
    return ApiError("search_phase_execution_exception", meta=meta, body={})


def _hit(v: int) -> Dict[str, Any]:
    return {"_source": {"v": v}, "sort": [v]}


def _src(base_query: Dict[str, Any], **kw: Any) -> ElasticSource:
    src = ElasticSource(url="http://es:9200", index="i", base_query=base_query, **kw)
    src._client = MagicMock()
    return src


@pytest.fixture
def logs() -> Iterator[List[str]]:
    messages: List[str] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    target = logging.getLogger(LOGGER)
    handler = _Handler(logging.DEBUG)
    old = target.level
    target.addHandler(handler)
    target.setLevel(logging.DEBUG)
    try:
        yield messages
    finally:
        target.removeHandler(handler)
        target.setLevel(old)


# ----- small helpers ------------------------------------------------------------------


def test_an_unrecognised_sort_option_defaults_to_ascending() -> None:
    assert _first_sort_key({"sort": [{"ts": 1}]}) == ("ts", "asc")


def test_a_malformed_aggregation_is_not_mistaken_for_a_composite() -> None:
    assert _composite_path({"aggs": {"weird": 5, "max_v": {"max": {"field": "v"}}}}) is None


# ----- client construction --------------------------------------------------------------


def test_client_is_built_once_with_tuple_auth_and_quiet_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(elastic_mod, "Elasticsearch", lambda **kw: built.append(kw) or object())
    # auth arrives as a list once a job payload has been through JSON
    src = ElasticSource(
        url="HTTPS://es:9200", index="i", base_query={}, auth=["u", "p"], verify_certs=False
    )

    client = src._get_client()

    assert src._get_client() is client
    assert built == [
        {
            "hosts": ["HTTPS://es:9200"],
            "basic_auth": ("u", "p"),
            "verify_certs": False,
            "ssl_show_warn": False,
        }
    ]


def test_plain_http_or_verified_tls_keeps_the_warning_setting_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built: List[Dict[str, Any]] = []
    monkeypatch.setattr(elastic_mod, "Elasticsearch", lambda **kw: built.append(kw) or object())

    ElasticSource(url="http://es:9200", index="i", base_query={}, verify_certs=False)._get_client()
    ElasticSource(url="https://es:9200", index="i", base_query={})._get_client()

    assert all("ssl_show_warn" not in kw for kw in built)
    assert built[0]["basic_auth"] is None


# ----- Elasticsearch errors become SourceErrors ----------------------------------------


def test_a_scroll_fetch_error_is_a_source_error() -> None:
    src = _src({"query": {"match_all": {}}})
    src._client.search.side_effect = _api_error()

    with pytest.raises(SourceError, match="Failed to fetch data"):
        src.fetch({})


def test_a_search_after_fetch_error_is_a_source_error() -> None:
    src = _src({"sort": [{"v": "asc"}], "search_after": [0]})
    src._client.search.side_effect = _api_error()

    with pytest.raises(SourceError, match="Failed to fetch data"):
        src.fetch({})


COMPOSITE: Dict[str, Any] = {
    "aggs": {"by_v": {"composite": {"sources": [{"v": {"terms": {"field": "v"}}}]}}}
}


def test_a_composite_fetch_error_is_a_source_error() -> None:
    src = _src(COMPOSITE)
    src._client.search.side_effect = _api_error()

    with pytest.raises(SourceError, match="Failed to fetch data"):
        src.fetch({})


# ----- composite edge cases ---------------------------------------------------------------


def _page(buckets: List[Any], after: Any = None) -> Dict[str, Any]:
    agg: Dict[str, Any] = {"buckets": buckets}
    if after is not None:
        agg["after_key"] = after
    return {"aggregations": {"by_v": agg}}


def test_the_first_composite_page_is_read_without_an_after_key() -> None:
    job = _src(COMPOSITE)
    job.config["composite_page"] = {"after": None, "size": 2}
    job._client.search.return_value = _page([{"key": {"v": 1}}, {"key": {"v": 2}}], {"v": 2})

    records = job.fetch({}, limit=1)

    assert [r["data"]["key"] for r in records] == [{"v": 1}]
    sent = job._client.search.call_args.kwargs["body"]["aggs"]["by_v"]["composite"]
    assert "after" not in sent and sent["size"] == 2


def test_a_composite_preview_stops_paging_at_the_limit() -> None:
    src = _src(COMPOSITE)
    src._client.search.side_effect = [
        _page([{"key": {"v": 1}}, {"key": {"v": 2}}], {"v": 2}),
        AssertionError("no second page once the limit is reached"),
    ]

    assert len(src.fetch({}, limit=2)) == 2


def test_composite_split_under_a_parent_aggregation_strips_nested_sub_aggs() -> None:
    query = {
        "aggs": {
            "recent": {
                "filter": {"term": {"x": 1}},
                "aggs": {
                    "by_v": {
                        "composite": {"sources": [{"v": {"terms": {"field": "v"}}}]},
                        "aggregations": {"total": {"sum": {"field": "n"}}},
                    }
                },
            }
        }
    }
    src = _src(query, docs_per_job=2)
    src._client.search.side_effect = [
        {"aggregations": {"recent": {"by_v": {"buckets": [{"key": 1}], "after_key": {"v": 1}}}}}
    ]

    jobs = list(src.split({}))

    assert [j.config["composite_page"] for j in jobs] == [{"after": None, "size": 2}]
    scan = src._client.search.call_args.kwargs["body"]
    assert "aggregations" not in scan["aggs"]["recent"]["aggs"]["by_v"]


def test_a_composite_with_no_buckets_plans_no_jobs() -> None:
    src = _src(COMPOSITE, docs_per_job=5)
    src._client.search.return_value = _page([])

    assert list(src.split({})) == []


def test_num_slices_is_ignored_for_composites(logs: List[str]) -> None:
    src = _src(COMPOSITE, num_slices=4)

    assert list(src.split({})) == [src]
    assert "elastic source: num_slices is ignored for composite aggregations" in logs


# ----- range windows ------------------------------------------------------------------------


RANGE_QUERY: Dict[str, Any] = {"query": {"match_all": {}}, "sort": [{"v": "asc"}]}
WINDOW = {"mode": "range", "field": "v", "order": "asc", "after": None, "upto": None}


def _range_job(**kw: Any) -> ElasticSource:
    job = _src(RANGE_QUERY, size=2, **kw)
    job.config["window"] = dict(WINDOW)
    job._client.open_point_in_time.return_value = {"id": "pit-1"}
    return job


def test_a_range_window_preview_stops_at_the_limit_and_closes_its_pit() -> None:
    job = _range_job()
    job._client.search.side_effect = [{"hits": {"hits": [_hit(1), _hit(2)]}}]

    records = job.fetch({}, limit=1)

    assert [r["data"]["v"] for r in records] == [1]
    job._client.close_point_in_time.assert_called_once_with(id="pit-1")


@pytest.mark.parametrize("error", [_api_error(), TransportConnectionError("es down")])
def test_a_range_window_error_is_a_source_error(error: Exception) -> None:
    job = _range_job()
    job._client.search.side_effect = error

    with pytest.raises(SourceError, match="Failed to fetch data"):
        job.fetch({})
    job._client.close_point_in_time.assert_called_once_with(id="pit-1")


def test_a_pit_that_cannot_be_closed_does_not_fail_the_job(logs: List[str]) -> None:
    job = _range_job()
    job._client.search.side_effect = [{"hits": {"hits": [_hit(1)]}}, {"hits": {"hits": []}}]
    job._client.close_point_in_time.side_effect = _api_error(404)

    assert [r["data"]["v"] for r in job.fetch({})] == [1]
    assert "Could not close PIT for a range window" in logs


def test_a_planning_pit_that_cannot_be_closed_does_not_fail_planning(logs: List[str]) -> None:
    src = _src(RANGE_QUERY, size=10, docs_per_job=1, window_mode="range")
    client = src._client
    client.count.return_value = {"count": 2}
    client.open_point_in_time.return_value = {"id": "plan-pit"}
    client.search.side_effect = [{"hits": {"hits": [_hit(1), _hit(2)]}}]
    client.close_point_in_time.side_effect = TransportConnectionError("gone")

    windows = [j.config["window"] for j in src.split({})]

    assert [(w["after"], w["upto"]) for w in windows] == [(None, 1), (1, None)]
    assert "Could not close the planning PIT" in logs


# ----- snapshot pre-scan ----------------------------------------------------------------------


def test_a_pre_scan_that_runs_out_of_documents_plans_fewer_windows() -> None:
    """Documents deleted between the count and the scan: no empty windows are planned."""
    src = _src({"query": {"match_all": {}}}, size=10, docs_per_job=1)
    client = src._client
    client.count.return_value = {"count": 4}
    client.open_point_in_time.return_value = {"id": "pit"}
    client.search.side_effect = [{"hits": {"hits": [_hit(1), _hit(2)]}}, {"hits": {"hits": []}}]

    windows = [j.config["window"] for j in src.split({})]

    assert [w["search_after"] for w in windows] == [None, [1], [2]]
    assert {w["size"] for w in windows} == {1}


# ----- split_jobs response shapes -------------------------------------------------------------


class _Body:
    """An elasticsearch 8.x ObjectApiResponse-like object exposing ``.body``."""

    def __init__(self, body: Dict[str, Any]) -> None:
        self.body = body


class _MappingResponse(Mapping[str, Any]):
    """A response that is a Mapping but not a dict (iterating it yields keys)."""

    def __init__(self, body: Dict[str, Any]) -> None:
        self._body = body

    def __getitem__(self, key: str) -> Any:
        return self._body[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._body)

    def __len__(self) -> int:
        return len(self._body)


@pytest.mark.parametrize("wrap", [_Body, _MappingResponse], ids=["body", "mapping"])
def test_split_jobs_accepts_response_objects(wrap: Any) -> None:
    src = _src({"query": {"match_all": {}}}, size=1)
    src._client.search.return_value = wrap({"_scroll_id": "s1", "hits": {"hits": [_hit(1)]}})
    src._client.scroll.side_effect = [
        wrap({"_scroll_id": "s2", "hits": {"hits": [_hit(2)]}}),
        wrap({"_scroll_id": "s3", "hits": {"hits": []}}),
    ]

    jobs = list(src.split_jobs({}))

    assert [[r["data"]["v"] for r in j.records] for j in jobs] == [[1], [2]]
    assert [j.metadata["page_num"] for j in jobs] == [0, 1]
    src._client.clear_scroll.assert_called_once_with(scroll_id="s3")
