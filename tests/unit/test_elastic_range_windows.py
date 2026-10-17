"""window_mode="range": value-range windows, each job with its own short PIT.

A small fake Elasticsearch (sorted docs, PIT, range + search_after) lets the tests
check the property that matters — every document is read exactly once, including
documents that share a window-boundary value — without a cluster.
"""

from typing import Any, Dict, List, Optional

import pytest

from reflowfy.sources.base import SourceError
from reflowfy.sources.elastic import ElasticSource, _first_sort_key, _range_clause


class _FakeEs:
    """Enough of the client for split() and fetch(): count, PIT, sorted search."""

    def __init__(self, values: List[int], order: str = "asc"):
        # one doc per entry, "id" is the unique tiebreaker (stands in for _shard_doc)
        self.docs = [{"id": i, "v": v} for i, v in enumerate(values)]
        self.order = order
        self.pits: Dict[str, bool] = {}
        self.closed: List[str] = []
        self.opened = 0
        self.fail_open = False

    # -- API used by the source
    def count(self, index=None, body=None):
        return {"count": len(self.docs)}

    def open_point_in_time(self, index=None, keep_alive=None):
        if self.fail_open:
            raise ConnectionError("elasticsearch is down")
        self.opened += 1
        pit = f"pit{self.opened}"
        self.pits[pit] = True
        return {"id": pit}

    def close_point_in_time(self, id=None):
        self.closed.append(id)
        self.pits[id] = False

    def search(self, body=None, size=10):
        assert self.pits[body["pit"]["id"]], "search on a closed PIT"
        docs = [d for d in self.docs if self._matches(body["query"], d)]
        rev = self.order == "desc"
        docs.sort(key=lambda d: (d["v"], d["id"]), reverse=rev)
        after = body.get("search_after")
        if after is not None:
            docs = [d for d in docs if self._after(d, after, rev)]
        hits = [{"_source": {"v": d["v"], "id": d["id"]}, "sort": [d["v"], d["id"]]}
                for d in docs[:size]]
        return {"hits": {"hits": hits}}

    # -- helpers
    @staticmethod
    def _after(doc, after, rev):
        key, ref = (doc["v"], doc["id"]), (after[0], after[1])
        return key < ref if rev else key > ref

    def _matches(self, query: Dict[str, Any], doc) -> bool:
        if "match_all" in query:
            return True
        for clause in query["bool"]["filter"]:
            if "range" in clause:
                bounds = clause["range"]["v"]
                v = doc["v"]
                if "gt" in bounds and not v > bounds["gt"]:
                    return False
                if "lt" in bounds and not v < bounds["lt"]:
                    return False
                if "lte" in bounds and not v <= bounds["lte"]:
                    return False
                if "gte" in bounds and not v >= bounds["gte"]:
                    return False
        return True


def _source(es: _FakeEs, monkeypatch, docs_per_job: int, order: str = "asc", **extra):
    src = ElasticSource(
        url="http://es",
        index="i",
        base_query={"query": {"match_all": {}}, "sort": [{"v": order}]},
        size=4,
        docs_per_job=docs_per_job,
        window_mode="range",
        **extra,
    )
    monkeypatch.setattr(ElasticSource, "_get_client", lambda self: es)
    return src


def _read_all(src: ElasticSource) -> List[int]:
    return [r["data"]["id"] for sub in src.split({}) for r in sub.fetch({})]


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_every_document_read_exactly_once_with_ties_on_boundaries(monkeypatch, order):
    # 7 distinct values, runs of equal values that straddle every window boundary
    values = [1] * 5 + [2] * 6 + [3] * 3 + [4] * 7 + [5] * 2 + [6] * 4 + [7] * 3
    es = _FakeEs(values, order)

    ids = _read_all(_source(es, monkeypatch, docs_per_job=6, order=order))

    assert sorted(ids) == list(range(len(values)))  # none missing, none twice


def test_windows_are_value_ranges_and_cover_the_whole_axis(monkeypatch):
    es = _FakeEs(list(range(1, 41)))
    subs = list(_source(es, monkeypatch, docs_per_job=10).split({}))

    windows = [s.config["window"] for s in subs]
    assert len(windows) == 4
    assert windows[0]["after"] is None and windows[-1]["upto"] is None  # open-ended ends
    for left, right in zip(windows, windows[1:]):
        assert left["upto"] == right["after"]  # adjacent, no gap, no overlap
    assert all(w["mode"] == "range" and w["field"] == "v" for w in windows)
    assert all("pit_id" not in s.config for s in subs)  # nothing shared between jobs


def test_planning_pit_is_closed_and_each_job_closes_its_own(monkeypatch):
    es = _FakeEs(list(range(1, 31)))
    src = _source(es, monkeypatch, docs_per_job=10)

    subs = list(src.split({}))
    assert es.opened == 1 and es.closed == ["pit1"]  # the plan's PIT is gone

    subs[1].fetch({})
    assert es.opened == 2 and es.closed[-1] == "pit2"


def test_a_job_survives_the_planning_pit_being_gone(monkeypatch):
    """The point of the mode: nothing the plan opened is needed to fetch."""
    es = _FakeEs(list(range(1, 31)))
    src = _source(es, monkeypatch, docs_per_job=10)
    subs = list(src.split({}))
    es.pits = {k: False for k in es.pits}  # an ES restart: every PIT destroyed

    assert len(subs[0].fetch({})) > 0  # opens a fresh PIT and reads its window


def test_all_equal_values_collapse_to_few_windows_without_losing_docs(monkeypatch):
    es = _FakeEs([5] * 30)
    ids = _read_all(_source(es, monkeypatch, docs_per_job=10))
    assert sorted(ids) == list(range(30))


def test_fewer_docs_than_docs_per_job_is_a_single_job(monkeypatch):
    es = _FakeEs([1, 2, 3])
    subs = list(_source(es, monkeypatch, docs_per_job=10).split({}))
    assert len(subs) == 1 and es.opened == 0


def test_a_down_cluster_is_a_source_error_not_a_crash(monkeypatch):
    es = _FakeEs(list(range(1, 31)))
    src = _source(es, monkeypatch, docs_per_job=10)
    sub = list(src.split({}))[0]
    es.fail_open = True

    with pytest.raises(Exception) as err:
        sub.fetch({})
    assert "down" in str(err.value) or isinstance(err.value, SourceError)


@pytest.mark.parametrize(
    "sort",
    [None, [], ["_shard_doc"], [{"_doc": "asc"}], ["_score"], [{"a": "asc", "b": "asc"}]],
)
def test_range_mode_needs_a_real_first_sort_field(sort):
    base: Dict[str, Any] = {"query": {"match_all": {}}}
    if sort is not None:
        base["sort"] = sort
    with pytest.raises(SourceError, match="window_mode"):
        _first_sort_key(base)


@pytest.mark.parametrize(
    "sort,expected",
    [
        (["ts"], ("ts", "asc")),
        ([{"ts": "desc"}], ("ts", "desc")),
        ([{"ts": {"order": "desc", "missing": "_last"}}, {"id": "asc"}], ("ts", "desc")),
        ("ts", ("ts", "asc")),
    ],
)
def test_first_sort_key_parsing(sort, expected):
    assert _first_sort_key({"sort": sort}) == expected


def test_range_clause_directions():
    assert _range_clause("v", "asc", 3, 9) == {"range": {"v": {"gt": 3, "lte": 9}}}
    assert _range_clause("v", "desc", 9, 3) == {"range": {"v": {"lt": 9, "gte": 3}}}
    assert _range_clause("v", "asc", None, 9) == {"range": {"v": {"lte": 9}}}
    assert _range_clause("v", "asc", 3, None) == {"range": {"v": {"gt": 3}}}


def test_snapshot_mode_is_unchanged_by_default(monkeypatch):
    es = _FakeEs(list(range(1, 31)))
    src = ElasticSource(
        url="http://es", index="i", base_query={"query": {"match_all": {}}}, docs_per_job=10
    )
    assert src.config.get("window_mode") in (None, "snapshot")
    monkeypatch.setattr(ElasticSource, "_get_client", lambda self: es)
    subs = list(src.split({}))
    assert all("pit_id" in s.config and "mode" not in s.config["window"] for s in subs)
