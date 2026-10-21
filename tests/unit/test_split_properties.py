"""Property tests for job planning: the slices cover the source exactly once.

Whatever the input, ``split()`` must neither drop a record nor hand one to two
jobs — a gap loses data silently, an overlap writes it twice. The example-based
tests pin specific shapes; these let hypothesis look for the shape that breaks
(gaps in IDs, negative IDs, runs of equal sort values on a window edge, a batch
size larger than the data, ...).

Covered: ``StaticSource``, ``SqlSource`` ID-range windows on SQLite,
``ElasticSource`` ``window_mode="range"`` windows on the fake ES from
``test_elastic_range_windows``, and ``chunk()`` / ``plan_slices`` sizing for
``define_jobs``.
"""

import os
import tempfile
from math import ceil
from typing import Any, Dict, List, Tuple
from unittest import mock

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from sqlalchemy import create_engine, text

from reflowfy import chunk
from reflowfy.execution.job_runner import plan_slices
from reflowfy.sources.elastic import ElasticSource
from reflowfy.sources.sql import SqlSource
from reflowfy.sources.static import StaticSource
from test_elastic_range_windows import _FakeEs

FAST = settings(max_examples=60, deadline=None)
IO = settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

json_scalars = st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=5))
records_st = st.lists(
    st.one_of(json_scalars, st.dictionaries(st.text(max_size=3), json_scalars, max_size=3)),
    max_size=40,
)


# --- StaticSource ----------------------------------------------------------------


@FAST
@given(records=records_st)
def test_static_split_yields_every_record_once(records: List[Any]):
    slices = list(StaticSource(records).split({}))

    assert len(slices) == (1 if records else 0)  # no no-op job for empty input
    assert [r for s in slices for r in s.fetch({})] == records


# --- chunk() / define_jobs sizing ------------------------------------------------


@FAST
@given(records=st.lists(st.integers(), max_size=200), size=st.integers(1, 50))
def test_chunk_sizes_and_preserves_order(records: List[int], size: int):
    chunks = chunk(records, size=size)

    assert [r for c in chunks for r in c] == records  # exactly once, in order
    assert len(chunks) == ceil(len(records) / size)
    assert all(len(c) == size for c in chunks[:-1])  # only the last is short
    if chunks:
        assert 1 <= len(chunks[-1]) <= size


@FAST
@given(records=st.lists(st.integers(), max_size=120), size=st.integers(1, 30))
def test_chunked_plan_becomes_one_job_per_chunk(records: List[int], size: int):
    slices = list(plan_slices(chunk(records, size=size), {}))

    assert len(slices) == ceil(len(records) / size)
    assert [r for s in slices for r in s.fetch({})] == records


# --- SqlSource ID-range windows on SQLite ----------------------------------------


def _sql_split_and_read(ids: List[int], batch_size: int) -> List[Tuple[Dict[str, int], List[int]]]:
    """Split a SQLite table of ``ids`` and fetch each slice: ``[(window, ids read)]``."""
    with tempfile.TemporaryDirectory() as tmp:
        url = f"sqlite:///{os.path.join(tmp, 'p.db')}"
        engine = create_engine(url)
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"))
            if ids:
                conn.execute(
                    text("INSERT INTO t (id, v) VALUES (:id, :v)"),
                    [{"id": i, "v": f"r{i}"} for i in ids],
                )
        engine.dispose()

        source = SqlSource(url, "SELECT id, v FROM t", id_column="id", batch_size=batch_size)
        subs = list(source.split({}))
        fetched = [[r["data"]["id"] for r in sub.fetch({})] for sub in subs]
        for s in [source, *subs]:
            if s._engine is not None:  # pyright: ignore[reportPrivateUsage]
                s._engine.dispose()  # pyright: ignore[reportPrivateUsage]
    return [(sub.config["slice"], got) for sub, got in zip(subs, fetched)]


@IO
@given(
    ids=st.sets(st.integers(-500, 500), max_size=60).map(sorted),
    batch_size=st.integers(1, 120),
)
def test_sql_id_range_windows_cover_each_row_once(ids: List[int], batch_size: int):
    planned = _sql_split_and_read(ids, batch_size)
    seen = [i for _, got in planned for i in got]

    assert sorted(seen) == ids  # none missing, none twice

    if not ids:
        assert planned == []  # an empty table plans no job
        return
    windows = [w for w, _ in planned]
    assert windows[0]["lo"] == ids[0]
    assert windows[-1]["lo"] <= ids[-1] < windows[-1]["hi"]
    for w in windows:
        assert w["hi"] - w["lo"] == batch_size
    for left, right in zip(windows, windows[1:]):
        assert left["hi"] == right["lo"]  # half-open [lo, hi): adjacent, no overlap
    assert len(windows) == ceil((ids[-1] - ids[0] + 1) / batch_size)
    for w, got in planned:  # each row lands in the window its id belongs to
        assert all(w["lo"] <= i < w["hi"] for i in got)


# --- ElasticSource range windows on the fake ES ----------------------------------


def _range_source(es: _FakeEs, docs_per_job: int, order: str, page: int) -> ElasticSource:
    return ElasticSource(
        url="http://es",
        index="i",
        base_query={"query": {"match_all": {}}, "sort": [{"v": order}]},
        size=page,
        docs_per_job=docs_per_job,
        window_mode="range",
    )


@IO
@given(
    # a small value domain forces long runs of ties that straddle window edges
    values=st.lists(st.integers(0, 8), min_size=1, max_size=60),
    docs_per_job=st.integers(1, 25),
    order=st.sampled_from(["asc", "desc"]),
    page=st.integers(1, 7),
)
def test_elastic_range_windows_read_every_doc_once(
    values: List[int], docs_per_job: int, order: str, page: int
):
    es = _FakeEs(values, order)
    src = _range_source(es, docs_per_job, order, page)

    with mock.patch.object(ElasticSource, "_get_client", lambda self: es):
        subs = list(src.split({}))
        if ceil(len(values) / docs_per_job) <= 1:
            # one job: the source itself, unwindowed (it scrolls, which the fake lacks)
            assert subs == [src] and es.opened == 0
            return
        ids = [r["data"]["id"] for sub in subs for r in sub.fetch({})]

    assert sorted(ids) == list(range(len(values)))  # none missing, none twice
    assert all(open_ is False for open_ in es.pits.values())  # every PIT closed

    windows: List[Dict[str, Any]] = [s.config["window"] for s in subs]
    assert windows[0]["after"] is None and windows[-1]["upto"] is None
    for left, right in zip(windows, windows[1:]):
        assert left["upto"] == right["after"]  # (a, b] windows: adjacent, no overlap
    edges = [w["upto"] for w in windows[:-1]]
    assert edges == sorted(edges, reverse=(order == "desc"))
    assert len(set(edges)) == len(edges)  # strictly monotone: no empty duplicate window
    assert len(subs) <= ceil(len(values) / docs_per_job)
