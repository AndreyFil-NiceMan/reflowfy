"""Composite-aggregation queries (``after_key`` paging) through ElasticSource."""

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from reflowfy.sources.base import SourceError
from reflowfy.sources.elastic import ElasticSource

QUERY: Dict[str, Any] = {
    "size": 0,
    "query": {"match_all": {}},
    "aggs": {
        "by_key": {
            "composite": {
                "size": 2,
                "sources": [
                    {"k": {"terms": {"script": {"source": "doc['a'].value + doc['b'].value"}}}}
                ],
            }
        }
    },
}

PAGES: List[Dict[str, Any]] = [
    {
        "hits": {"hits": []},
        "aggregations": {
            "by_key": {
                "after_key": {"k": "b"},
                "buckets": [
                    {"key": {"k": "a"}, "doc_count": 1},
                    {"key": {"k": "b"}, "doc_count": 2},
                ],
            }
        },
    },
    {
        "hits": {"hits": []},
        "aggregations": {"by_key": {"buckets": [{"key": {"k": "c"}, "doc_count": 3}]}},
    },
    {"hits": {"hits": []}, "aggregations": {"by_key": {"buckets": []}}},
]


def _source() -> ElasticSource:
    src = ElasticSource(url="http://x", index="i", base_query=QUERY)
    client = MagicMock()
    client.search.side_effect = PAGES
    client.count.return_value = {"count": 10}
    src._client = client
    return src


def test_composite_buckets_become_records() -> None:
    records = _source().fetch({})
    assert [r["data"]["key"] for r in records] == [{"k": "a"}, {"k": "b"}, {"k": "c"}]


def test_composite_pages_with_after_key() -> None:
    src = _source()
    src.fetch({})
    second = src._client.search.call_args_list[1].kwargs["body"]  # type: ignore[union-attr]
    assert second["aggs"]["by_key"]["composite"]["after"] == {"k": "b"}


def test_composite_split_is_single_job() -> None:
    src = ElasticSource(url="http://x", index="i", base_query=QUERY)
    src._client = MagicMock()
    src._client.count.return_value = {"count": 10}
    assert list(src.split({})) == [src]


COMP: Dict[str, Any] = {"composite": {"sources": [{"k": {"terms": {"field": "a"}}}]}}


def _with(aggs: Dict[str, Any], *pages: Dict[str, Any]) -> ElasticSource:
    src = ElasticSource(url="http://x", index="i", base_query={"aggs": aggs}, size=7)
    src._client = MagicMock()
    src._client.search.side_effect = list(pages)
    return src


def test_nested_under_filter() -> None:
    aggs = {"recent": {"filter": {"term": {"a": 1}}, "aggs": {"by_key": COMP}}}
    page = {"aggregations": {"recent": {"doc_count": 1, "by_key": {"buckets": [{"key": 1}]}}}}
    assert [r["data"] for r in _with(aggs, page).fetch({})] == [{"key": 1}]


def test_other_aggs_pruned_and_default_size_applied() -> None:
    aggs = {"avg": {"avg": {"field": "x"}}, "by_key": COMP}
    src = _with(aggs, {"aggregations": {"by_key": {"buckets": []}}})
    src.fetch({})
    body = src._client.search.call_args.kwargs["body"]  # type: ignore[union-attr]
    assert list(body["aggs"]) == ["by_key"]
    assert body["aggs"]["by_key"]["composite"]["size"] == 7


def test_multiple_composites_raise() -> None:
    with pytest.raises(SourceError, match="2 composite"):
        _with({"c1": COMP, "c2": COMP}).fetch({})


def test_multi_bucket_parent_gives_clear_error() -> None:
    aggs = {"t": {"terms": {"field": "a"}, "aggs": {"by_key": COMP}}}
    page = {"aggregations": {"t": {"buckets": [{"by_key": {}}]}}}
    with pytest.raises(SourceError, match="single-bucket"):
        _with(aggs, page).fetch({})


def _bucket(k: str) -> Dict[str, Any]:
    return {"key": {"k": k}, "doc_count": 1}


def test_docs_per_job_splits_one_job_per_page() -> None:
    aggs = {"by_key": {**COMP, "aggs": {"total": {"sum": {"field": "x"}}}}}
    src = ElasticSource(url="http://x", index="i", base_query={"aggs": aggs}, docs_per_job=2)
    src._client = MagicMock()
    src._client.search.side_effect = [
        {
            "aggregations": {
                "by_key": {"after_key": {"k": "b"}, "buckets": [_bucket("a"), _bucket("b")]}
            }
        },
        {"aggregations": {"by_key": {"after_key": {"k": "c"}, "buckets": [_bucket("c")]}}},
    ]
    jobs = list(src.split({}))

    assert [j.config["composite_page"] for j in jobs] == [
        {"after": None, "size": 2},
        {"after": {"k": "b"}, "size": 2},
    ]
    scan = src._client.search.call_args_list[0].kwargs["body"]
    assert "aggs" not in scan["aggs"]["by_key"]  # scan drops sub-aggs


def test_paged_job_fetches_only_its_page() -> None:
    job = ElasticSource(url="http://x", index="i", base_query=QUERY)
    job.config["composite_page"] = {"after": {"k": "b"}, "size": 2}
    job._client = MagicMock()
    job._client.search.return_value = {
        "aggregations": {"by_key": {"after_key": {"k": "c"}, "buckets": [_bucket("c")]}}
    }
    records = job.fetch({})

    assert [r["data"]["key"] for r in records] == [{"k": "c"}]
    assert job._client.search.call_count == 1  # no after_key chasing
    sent = job._client.search.call_args.kwargs["body"]["aggs"]["by_key"]["composite"]
    assert sent["after"] == {"k": "b"} and sent["size"] == 2


def test_size_goes_in_body_not_kwarg() -> None:
    # The 8.x client copies a ``size=`` kwarg into the body dict we reuse across
    # pages, so page 2 raised "multiple values for 'size'" against real ES.
    src = _source()
    src.fetch({})
    for call in src._client.search.call_args_list:  # type: ignore[union-attr]
        assert "size" not in call.kwargs
        assert call.kwargs["body"]["size"] == 0
