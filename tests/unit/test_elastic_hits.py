"""Plain-hits queries: script_fields / fields (no _source) and user search_after."""

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from reflowfy.sources.base import SourceError
from reflowfy.sources.elastic import ElasticSource


def _src(query: Dict[str, Any], *pages: Dict[str, Any], **kw: Any) -> ElasticSource:
    src = ElasticSource(url="http://x", index="i", base_query=query, size=2, **kw)
    src._client = MagicMock()  # type: ignore[assignment]
    src._client.search.side_effect = list(pages)
    return src


def _hits(*hits: Dict[str, Any]) -> Dict[str, Any]:
    return {"_scroll_id": "s", "hits": {"hits": list(hits)}}


def test_hit_without_source_keeps_fields() -> None:
    hit = {"fields": {"double": [4]}}  # script_fields only: no _source at all
    src = _src({"script_fields": {}}, _hits(hit), _hits())
    src._client.scroll.return_value = _hits()  # type: ignore[union-attr]
    assert src.fetch({}) == [{"data": {}, "fields": {"double": [4]}}]


def test_hit_with_source_and_fields() -> None:
    hit = {"_source": {"a": 1}, "fields": {"b": [2]}}
    src = _src({}, _hits(hit))
    src._client.scroll.return_value = _hits()  # type: ignore[union-attr]
    assert src.fetch({}) == [{"data": {"a": 1}, "fields": {"b": [2]}}]


def test_plain_hit_record_shape_unchanged() -> None:
    src = _src({}, _hits({"_source": {"a": 1}}))
    src._client.scroll.return_value = _hits()  # type: ignore[union-attr]
    assert src.fetch({}) == [{"data": {"a": 1}}]  # no "fields" key added


def test_user_search_after_pages_from_cursor() -> None:
    q = {"sort": [{"id": "asc"}], "search_after": [10]}
    pages = [
        _hits({"_source": {"id": 11}, "sort": [11]}, {"_source": {"id": 12}, "sort": [12]}),
        _hits({"_source": {"id": 13}, "sort": [13]}),
        _hits(),
    ]
    src = _src(q, *pages)
    cursors: List[Any] = []
    replies = iter(pages)

    def fake_search(**kw: Any) -> Dict[str, Any]:
        cursors.append(list(kw["body"]["search_after"]))  # snapshot: the body is reused
        return next(replies)

    src._client.search.side_effect = fake_search  # type: ignore[union-attr]
    records: List[Dict[str, Any]] = src.fetch({})

    assert [r["data"]["id"] for r in records] == [11, 12, 13]
    assert cursors == [[10], [12], [13]]
    assert not src._client.scroll.called  # type: ignore[union-attr]  # never scrolls


def test_user_search_after_honours_limit() -> None:
    q = {"sort": [{"id": "asc"}], "search_after": [0]}
    page = _hits({"_source": {"id": 1}, "sort": [1]}, {"_source": {"id": 2}, "sort": [2]})
    assert len(_src(q, page).fetch({}, limit=1)) == 1


def test_search_after_without_sort_is_a_clear_error() -> None:
    with pytest.raises(SourceError, match="needs a 'sort'"):
        _src({"search_after": [1]}).fetch({})


def test_num_slices_one_does_not_warn(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    comp = {"composite": {"sources": [{"k": {"terms": {"field": "a"}}}]}}
    src = ElasticSource(url="http://x", index="i", base_query={"aggs": {"c": comp}}, num_slices=1)
    with caplog.at_level(logging.WARNING):
        assert list(src.split({})) == [src]
    assert "num_slices" not in caplog.text
