"""SqlSource against a real SQLite file: fetch, split, split_jobs, health_check.

No server needed — SQLite runs the same ``SELECT ... FROM (<query>) AS sub`` wrapping
the source builds for Postgres/MySQL, so the planning arithmetic is exercised for real.
"""

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest
from sqlalchemy import create_engine as real_create_engine

import reflowfy.sources.sql as sql_module

from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.base import SourceError
from reflowfy.sources.sql import SqlSource, sql_source


def _make_db(path: Path, ids: List[int]) -> str:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)")
    conn.executemany(
        "INSERT INTO events (id, status, amount) VALUES (?, ?, ?)",
        [(i, "active" if i % 2 else "inactive", i * 10) for i in ids],
    )
    conn.commit()
    conn.close()
    return f"sqlite:///{path}"


@pytest.fixture(autouse=True)
def dispose_engines(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """SqlSource has no close(); dispose its pooled engines so sqlite files are released."""
    engines: List[Any] = []

    def tracking(*args: Any, **kwargs: Any) -> Any:
        engine = real_create_engine(*args, **kwargs)
        engines.append(engine)
        return engine

    monkeypatch.setattr(sql_module, "create_engine", tracking)
    yield
    for engine in engines:
        engine.dispose()


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return _make_db(tmp_path / "events.db", list(range(1, 11)))


@pytest.fixture
def empty_db_url(tmp_path: Path) -> str:
    return _make_db(tmp_path / "empty.db", [])


def _ids(records: List[Dict[str, Any]]) -> List[int]:
    return sorted(r["data"]["id"] for r in records)


# -- fetch -------------------------------------------------------------------


def test_fetch_resolves_template_params(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM events WHERE status = '{{ status }}' ORDER BY id")

    records = src.fetch({"status": "active"})

    assert _ids(records) == [1, 3, 5, 7, 9]
    assert records[0] == {"data": {"id": 1, "status": "active", "amount": 10}}


def test_fetch_limit_caps_rows(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM events ORDER BY id")
    assert _ids(src.fetch({}, limit=3)) == [1, 2, 3]


def test_fetch_bad_sql_is_a_source_error(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM no_such_table")
    with pytest.raises(SourceError, match="Failed to fetch data"):
        src.fetch({})


# -- split (manager-side planning, metadata only) ------------------------------


def test_split_by_id_range_covers_every_row_exactly_once(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM events", id_column="id", batch_size=4)

    subs = list(src.split({}))

    # ids 1..10 in windows of 4: [1,5) [5,9) [9,13)
    assert [s.config["slice"] for s in subs] == [
        {"lo": 1, "hi": 5},
        {"lo": 5, "hi": 9},
        {"lo": 9, "hi": 13},
    ]
    fetched = [r for s in subs for r in s.fetch({})]
    assert _ids(fetched) == list(range(1, 11))
    # a sub-source is already narrowed: it must not split again by id
    assert all(s.config["id_column"] is None for s in subs)


def test_split_with_gaps_plans_windows_from_min_to_max(tmp_path: Path) -> None:
    url = _make_db(tmp_path / "gaps.db", [1, 2, 50])
    subs = list(sql_source(url, "SELECT * FROM events", id_column="id", batch_size=10).split({}))

    # MIN/MAX planning: one window per 10 ids between 1 and 50, empty ones included
    assert len(subs) == 5
    per_window = [len(s.fetch({})) for s in subs]
    assert per_window == [2, 0, 0, 0, 1]


def test_split_single_row_is_one_window(tmp_path: Path) -> None:
    url = _make_db(tmp_path / "one.db", [42])
    subs = list(sql_source(url, "SELECT * FROM events", id_column="id").split({}))
    assert len(subs) == 1
    assert _ids(subs[0].fetch({})) == [42]


def test_split_empty_table_plans_no_jobs(empty_db_url: str) -> None:
    by_id = sql_source(empty_db_url, "SELECT * FROM events", id_column="id")
    no_id = sql_source(empty_db_url, "SELECT * FROM events")
    assert list(by_id.split({})) == []
    assert list(no_id.split({})) == []  # probes for a row instead of a no-op job


def test_split_without_id_column_is_one_job(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM events")
    assert list(src.split({})) == [src]


def test_split_respects_template_params(db_url: str) -> None:
    src = sql_source(
        db_url,
        "SELECT * FROM events WHERE status = '{{ status }}'",
        id_column="id",
        batch_size=100,
    )
    subs = list(src.split({"status": "inactive"}))
    assert _ids([r for s in subs for r in s.fetch({})]) == [2, 4, 6, 8, 10]


# -- split_jobs (legacy, fetches records while planning) -----------------------


def test_split_jobs_by_id_range_skips_empty_windows(tmp_path: Path) -> None:
    url = _make_db(tmp_path / "gaps.db", [1, 2, 3, 30])
    src = sql_source(url, "SELECT * FROM events", id_column="id", batch_size=10)

    jobs = list(src.split_jobs({}))

    assert [_ids(j.records) for j in jobs] == [[1, 2, 3], [30]]
    assert jobs[0].metadata == {"id_range": {"start": 1, "end": 11}, "page_num": 0, "count": 3}
    assert jobs[1].metadata["page_num"] == 2  # window [21, 31) after the empty [11, 21)


def test_split_jobs_by_offset_pages_until_short_page(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM events ORDER BY id", batch_size=4)

    jobs = list(src.split_jobs({}))

    assert [j.metadata["offset"] for j in jobs] == [0, 4, 8]
    assert [j.metadata["count"] for j in jobs] == [4, 4, 2]
    assert _ids([r for j in jobs for r in j.records]) == list(range(1, 11))


def test_split_jobs_by_offset_exact_multiple_ends_on_empty_page(tmp_path: Path) -> None:
    url = _make_db(tmp_path / "eight.db", list(range(1, 9)))
    jobs = list(sql_source(url, "SELECT * FROM events ORDER BY id", batch_size=4).split_jobs({}))
    assert [j.metadata["count"] for j in jobs] == [4, 4]


def test_split_jobs_empty_table_yields_nothing(empty_db_url: str) -> None:
    assert (
        list(sql_source(empty_db_url, "SELECT * FROM events", id_column="id").split_jobs({})) == []
    )
    assert list(sql_source(empty_db_url, "SELECT * FROM events").split_jobs({})) == []


def test_split_jobs_bad_sql_is_a_source_error(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM missing", id_column="id")
    with pytest.raises(SourceError, match="Failed to split jobs"):
        list(src.split_jobs({}))


# -- health_check ------------------------------------------------------------


def test_health_check_up(db_url: str) -> None:
    assert sql_source(db_url, "SELECT 1").health_check() is True


def test_health_check_down(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path}/no/such/dir/x.db"
    assert sql_source(url, "SELECT 1").health_check() is False


def test_engine_is_created_once(db_url: str) -> None:
    src = sql_source(db_url, "SELECT 1")
    assert src._get_engine() is src._get_engine()


# -- wire round-trip ----------------------------------------------------------


def test_source_factory_round_trip_of_a_planned_slice(db_url: str) -> None:
    src = sql_source(db_url, "SELECT * FROM events", id_column="id", batch_size=4)
    sub = list(src.split({}))[1]

    descriptor = SourceFactory.serialize(sub)
    rebuilt = SourceFactory.create(descriptor["type"], descriptor["config"])

    assert descriptor["type"] == "SqlSource"
    assert isinstance(rebuilt, SqlSource)
    assert _ids(rebuilt.fetch({})) == [5, 6, 7, 8]


def test_engine_kwargs_reach_create_engine(db_url: str) -> None:
    src = SqlSource(connection_url=db_url, query="SELECT 1", echo=True)
    assert src._get_engine().echo is True
