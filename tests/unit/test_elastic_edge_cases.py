"""ElasticSource edge cases: a lost PIT, max_slices capping, an empty index,
sliced-scroll planning, the scroll path and health_check.

Builds on the fake Elasticsearch in test_elastic_range_windows.py, extended with
scroll, sliced PIT search, a PIT that can be destroyed, and cluster health.
"""

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig
from elasticsearch import NotFoundError

from reflowfy.sources.base import SourceError
from reflowfy.sources.elastic import ElasticSource, elastic_source
from test_elastic_range_windows import _FakeEs  # type: ignore[import-not-found]


def _not_found(reason: str) -> NotFoundError:
    meta = ApiResponseMeta(
        status=404,
        http_version="1.1",
        headers=HttpHeaders(),
        duration=0.0,
        node=NodeConfig("http", "es", 9200),
    )
    body = {
        "error": {"root_cause": [{"type": "search_context_missing_exception", "reason": reason}]}
    }
    return NotFoundError("search_phase_execution_exception", meta=meta, body=body)


class _Es(_FakeEs):
    """Adds what the snapshot/scroll/sliced paths need on top of the range-mode fake."""

    def __init__(self, values: List[int], health: str = "green"):
        super().__init__(values)
        self.scrolls: Dict[str, List[Dict[str, Any]]] = {}
        self.cleared: List[str] = []
        self.searches = 0
        self.cluster = SimpleNamespace(health=lambda: {"status": health})

    def search(  # type: ignore[override]
        self,
        index: Optional[str] = None,
        body: Optional[Dict[str, Any]] = None,
        size: int = 10,
        scroll: Optional[str] = None,
    ) -> Dict[str, Any]:
        self.searches += 1
        body = body or {}
        if scroll is not None:  # classic scroll: no PIT
            hits = [
                {"_source": {"v": d["v"], "id": d["id"]}, "sort": [d["v"], d["id"]]}
                for d in sorted(self.docs, key=lambda d: (d["v"], d["id"]))
            ]
            sid = f"scroll{len(self.scrolls) + 1}"
            self.scrolls[sid] = hits[size:]
            self._scroll_size = size
            return {"_scroll_id": sid, "hits": {"hits": hits[:size]}}
        pit = body["pit"]["id"]
        if not self.pits.get(pit):
            raise _not_found(f"No search context found for id [{pit}]")
        if "slice" in body:
            spec = body["slice"]
            saved = self.docs
            self.docs = [d for d in saved if d["id"] % spec["max"] == spec["id"]]
            try:
                return super().search(body=body, size=size)
            finally:
                self.docs = saved
        return super().search(body=body, size=size)

    def scroll(self, scroll_id: str, scroll: str) -> Dict[str, Any]:
        rest = self.scrolls[scroll_id]
        page, self.scrolls[scroll_id] = rest[: self._scroll_size], rest[self._scroll_size :]
        return {"_scroll_id": scroll_id, "hits": {"hits": page}}

    def clear_scroll(self, scroll_id: str) -> None:
        self.cleared.append(scroll_id)


def _src(es: _Es, monkeypatch: pytest.MonkeyPatch, **kw: Any) -> ElasticSource:
    kw.setdefault("base_query", {"query": {"match_all": {}}, "sort": [{"v": "asc"}]})
    src = elastic_source(url="http://es", index="i", size=4, **kw)
    monkeypatch.setattr(ElasticSource, "_get_client", lambda self: es)
    return src


def _ids(records: List[Dict[str, Any]]) -> List[int]:
    return sorted(r["data"]["id"] for r in records)


# -- snapshot mode: the shared PIT is lost --------------------------------------


def test_snapshot_job_fails_with_source_error_when_the_pit_is_gone(monkeypatch) -> None:
    es = _Es(list(range(30)))
    subs = list(_src(es, monkeypatch, docs_per_job=10).split({}))
    assert len(subs) == 3 and len({s.config["pit_id"] for s in subs}) == 1

    assert len(subs[0].fetch({})) == 10  # fine while the PIT lives
    es.pits = {k: False for k in es.pits}  # ES restart / keep_alive expired

    with pytest.raises(SourceError, match="No search context found") as err:
        subs[1].fetch({})
    assert isinstance(err.value.original_error, NotFoundError)


def test_sliced_job_fails_with_source_error_when_the_pit_is_gone(monkeypatch) -> None:
    es = _Es(list(range(12)))
    sub = list(_src(es, monkeypatch, num_slices=3).split({}))[0]
    es.pits = {k: False for k in es.pits}
    with pytest.raises(SourceError, match="No search context found"):
        sub.fetch({})


# -- max_slices -------------------------------------------------------------------


def test_max_slices_caps_snapshot_windows_without_losing_documents(monkeypatch) -> None:
    es = _Es(list(range(30)))
    subs = list(_src(es, monkeypatch, docs_per_job=1, max_slices=4).split({}))

    assert len(subs) == 4  # not 30
    assert [s.config["window"]["size"] for s in subs] == [8, 8, 8, 8]  # ceil(30 / 4)
    assert _ids([r for s in subs for r in s.fetch({})]) == list(range(30))


def test_max_slices_caps_range_windows(monkeypatch) -> None:
    es = _Es(list(range(1, 31)))
    src = _src(es, monkeypatch, docs_per_job=1, max_slices=3, window_mode="range")
    subs = list(src.split({}))

    assert len(subs) == 3
    assert _ids([r for s in subs for r in s.fetch({})]) == list(range(30))


def test_max_slices_of_one_means_a_single_job(monkeypatch) -> None:
    es = _Es(list(range(30)))
    src = _src(es, monkeypatch, docs_per_job=1, max_slices=1)
    assert list(src.split({})) == [src]
    assert es.opened == 0


def test_snapshot_window_fetch_honours_limit(monkeypatch) -> None:
    es = _Es(list(range(30)))
    subs = list(_src(es, monkeypatch, docs_per_job=10).split({}))
    assert len(subs[0].fetch({}, limit=3)) == 3


# -- an empty index ----------------------------------------------------------------


@pytest.mark.parametrize(
    "kw",
    [
        {"docs_per_job": 10},
        {"docs_per_job": 10, "window_mode": "range"},
        {"num_slices": 4},
        {},
    ],
    ids=["snapshot", "range", "sliced", "single"],
)
def test_empty_index_plans_no_jobs_and_opens_no_pit(monkeypatch, kw) -> None:
    es = _Es([])
    assert list(_src(es, monkeypatch, **kw).split({})) == []
    assert es.opened == 0 and es.searches == 0


def test_empty_index_scroll_fetch_returns_nothing(monkeypatch) -> None:
    es = _Es([])
    assert _src(es, monkeypatch).fetch({}) == []
    assert es.cleared == ["scroll1"]


# -- sliced scroll (num_slices, no docs_per_job) ------------------------------------


def test_sliced_plan_shares_one_pit_and_covers_every_doc_once(monkeypatch) -> None:
    es = _Es(list(range(20)))
    subs = list(_src(es, monkeypatch, num_slices=3).split({}))

    assert es.opened == 1
    assert [s.config["slice"] for s in subs] == [{"id": i, "max": 3} for i in range(3)]
    assert {s.config["pit_id"] for s in subs} == {"pit1"}
    assert _ids([r for s in subs for r in s.fetch({})]) == list(range(20))


def test_sliced_fetch_honours_limit(monkeypatch) -> None:
    es = _Es(list(range(20)))
    sub = list(_src(es, monkeypatch, num_slices=2).split({}))[0]
    assert len(sub.fetch({}, limit=2)) == 2


def test_single_slice_is_the_source_itself(monkeypatch) -> None:
    es = _Es(list(range(5)))
    src = _src(es, monkeypatch, num_slices=1)
    assert list(src.split({})) == [src]


# -- scroll path (one job, whole result) -------------------------------------------


def test_scroll_fetch_reads_every_page_and_clears_the_scroll(monkeypatch) -> None:
    es = _Es(list(range(10)))
    records = _src(es, monkeypatch).fetch({})
    assert _ids(records) == list(range(10))
    assert es.cleared == ["scroll1"]


def test_scroll_fetch_limit_stops_early(monkeypatch) -> None:
    es = _Es(list(range(10)))
    assert len(_src(es, monkeypatch).fetch({}, limit=5)) == 5


def test_split_jobs_one_job_per_scroll_page(monkeypatch) -> None:
    es = _Es(list(range(10)))
    jobs = list(_src(es, monkeypatch).split_jobs({}))

    assert [j.metadata["count"] for j in jobs] == [4, 4, 2]
    assert [j.metadata["page_num"] for j in jobs] == [0, 1, 2]
    assert all(j.metadata["scroll_id"] == "scroll1" for j in jobs)
    assert _ids([r for j in jobs for r in j.records]) == list(range(10))
    assert es.cleared == ["scroll1"]


def test_split_jobs_api_error_is_a_source_error(monkeypatch) -> None:
    es = _Es(list(range(10)))

    def lost(scroll_id: str, scroll: str) -> Dict[str, Any]:
        raise _not_found("No search context found for id [scroll1]")

    es.scroll = lost  # type: ignore[method-assign]
    with pytest.raises(SourceError, match="Failed to split jobs"):
        list(_src(es, monkeypatch).split_jobs({}))


# -- health_check ------------------------------------------------------------------


@pytest.mark.parametrize("status,healthy", [("green", True), ("yellow", True), ("red", False)])
def test_health_check_status(monkeypatch, status, healthy) -> None:
    assert _src(_Es([], health=status), monkeypatch).health_check() is healthy


def test_health_check_unreachable(monkeypatch) -> None:
    es = _Es([])

    def down() -> Dict[str, Any]:
        raise ConnectionError("refused")

    es.cluster = SimpleNamespace(health=down)
    assert _src(es, monkeypatch).health_check() is False
