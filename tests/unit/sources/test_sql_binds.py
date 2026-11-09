"""sql_source: template values are bind parameters, identifiers need ``| ident``,
a trailing ``;`` is dropped before wrapping, and ``fetch(limit)`` uses fetchmany."""

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterator, List

import pytest
from sqlalchemy import create_engine as real_create_engine

import reflowfy.sources.sql as sql_module
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.sql import (
    SqlSource,
    SqlTemplateError,
    render_sql_template,
    sql_source,
    strip_trailing_semicolon,
)


@pytest.fixture(autouse=True)
def dispose_engines(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
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
    path = tmp_path / "events.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)")
    conn.executemany(
        "INSERT INTO events VALUES (?, ?, ?)",
        [(i, "active" if i % 2 else "inactive", i * 10) for i in range(1, 11)],
    )
    conn.commit()
    conn.close()
    return f"sqlite:///{path}"


def _ids(records: List[Dict[str, Any]]) -> List[int]:
    return sorted(r["data"]["id"] for r in records)


# -- rendering -----------------------------------------------------------------


class TestRender:
    def test_values_become_binds(self) -> None:
        sql, binds = render_sql_template(
            "SELECT * FROM t WHERE a = {{ a }} AND b > {{ b + 1 }}", {"a": "x", "b": 1}
        )
        assert sql == "SELECT * FROM t WHERE a = :reflowfy_p0 AND b > :reflowfy_p1"
        assert binds == {"reflowfy_p0": "x", "reflowfy_p1": 2}

    def test_quoted_value_on_its_own_loses_the_quotes(self) -> None:
        sql, binds = render_sql_template("WHERE d >= '{{ day }}'::timestamp", {"day": "2026"})
        # a space keeps `::` from swallowing the bind name
        assert sql == "WHERE d >= :reflowfy_p0 ::timestamp"
        assert binds == {"reflowfy_p0": "2026"}

    def test_value_inside_a_longer_literal_fails(self) -> None:
        with pytest.raises(SqlTemplateError, match=r"string literal '%\{\{ … \}\}%'"):
            render_sql_template("WHERE s LIKE '%{{ q }}%'", {"q": "secret-value"})

    def test_error_never_shows_the_value(self) -> None:
        with pytest.raises(SqlTemplateError) as err:
            render_sql_template("SELECT * FROM {{ t }}", {"t": "s3cr3t"})
        assert "s3cr3t" not in str(err.value)

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM {{ t }}",
            "SELECT * FROM a JOIN {{ t }} ON 1=1",
            "SELECT {{ t }}.col FROM x",
            "SELECT x.{{ t }} FROM x",
            "SELECT * FROM x ORDER BY {{ t }}",
            "SELECT CAST(a AS {{ t }}) FROM x",
            "INSERT INTO {{ t }} VALUES (1)",
        ],
    )
    def test_value_in_identifier_position_points_at_ident(self, query: str) -> None:
        with pytest.raises(SqlTemplateError, match=r"identifier position.*\| ident"):
            render_sql_template(query, {"t": "events"})

    def test_value_in_quoted_identifier_points_at_ident(self) -> None:
        with pytest.raises(SqlTemplateError, match=r"quoted identifier.*\| ident"):
            render_sql_template('SELECT "{{ t }}" FROM x', {"t": "c"})

    def test_ident_is_spliced(self) -> None:
        sql, binds = render_sql_template(
            "SELECT * FROM {{ t | ident }} ORDER BY {{ c | ident }}",
            {"t": "public.events", "c": "id"},
        )
        assert sql == "SELECT * FROM public.events ORDER BY id"
        assert binds == {}

    def test_ident_rejects_injection(self) -> None:
        with pytest.raises(SqlTemplateError, match="not an identifier"):
            render_sql_template("SELECT * FROM {{ t | ident }}", {"t": "x; DROP TABLE y"})

    def test_list_is_an_expanding_bind_with_or_without_parentheses(self) -> None:
        bare, _ = render_sql_template("WHERE id IN {{ ids }}", {"ids": [1, 2]})
        paren, binds = render_sql_template("WHERE id IN ( {{ ids }} )", {"ids": [1, 2]})
        assert bare == paren == "WHERE id IN :reflowfy_p0"
        assert binds == {"reflowfy_p0": [1, 2]}

    def test_scalar_in_parentheses_keeps_them(self) -> None:
        sql, _ = render_sql_template("WHERE (a = {{ a }})", {"a": 1})
        assert sql == "WHERE (a = :reflowfy_p0)"

    def test_missing_parameter_fails(self) -> None:
        with pytest.raises(SqlTemplateError, match="no runtime parameter 'nope'"):
            render_sql_template("SELECT {{ nope }}", {})
        with pytest.raises(SqlTemplateError, match="no runtime parameter 'nope'"):
            render_sql_template("WHERE a = '{{ nope }}'", {})

    def test_values_in_comments_are_dropped(self) -> None:
        sql, binds = render_sql_template(
            "-- uses {{ param_name }}\nSELECT 1 /* {{ x }} */ WHERE a = {{ x }}", {"x": 5}
        )
        assert sql == "-- uses \nSELECT 1 /*  */ WHERE a = :reflowfy_p2"
        assert binds == {"reflowfy_p2": 5}

    def test_escaped_quotes_and_control_blocks(self) -> None:
        sql, binds = render_sql_template(
            "SELECT 'it''s' {% if on %}WHERE a = {{ a }}{% endif %}", {"on": True, "a": 1}
        )
        assert sql == "SELECT 'it''s' WHERE a = :reflowfy_p0"
        assert binds == {"reflowfy_p0": 1}

    def test_bind_followed_by_a_word_gets_a_space(self) -> None:
        sql, _ = render_sql_template("LIMIT {{ n }}OFFSET 0", {"n": 1})
        assert sql == "LIMIT :reflowfy_p0 OFFSET 0"

    def test_bad_syntax(self) -> None:
        with pytest.raises(SqlTemplateError, match="Invalid template syntax"):
            render_sql_template("SELECT {{ a ", {})

    def test_undefined_attribute(self) -> None:
        with pytest.raises(SqlTemplateError, match="Missing required parameter"):
            render_sql_template("SELECT {{ a.b.c }}", {})

    def test_strip_trailing_semicolon(self) -> None:
        assert strip_trailing_semicolon("SELECT 1 ;  \n") == "SELECT 1"
        assert strip_trailing_semicolon("SELECT 1;;") == "SELECT 1;"  # only one
        assert strip_trailing_semicolon("SELECT 1") == "SELECT 1"


# -- against SQLite ----------------------------------------------------------------


class TestFetch:
    def test_injection_attempt_is_just_a_value(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM events WHERE status = {{ status }}")
        assert src.fetch({"status": "x' OR '1'='1"}) == []
        assert _ids(src.fetch({"status": "active"})) == [1, 3, 5, 7, 9]

    def test_ident_and_value_together(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM {{ table | ident }} WHERE amount > {{ min }}")
        assert _ids(src.fetch({"table": "events", "min": 80})) == [9, 10]

    def test_list_values(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM events WHERE id IN ({{ ids }})")
        assert _ids(src.fetch({"ids": [2, 4]})) == [2, 4]

    def test_hand_written_params(self, db_url: str) -> None:
        src = sql_source(
            db_url,
            "SELECT * FROM events WHERE id IN :ids AND status = {{ status }}",
            params={"ids": [1, 2, 3]},
        )
        assert _ids(src.fetch({"status": "active"})) == [1, 3]

    def test_params_must_not_clash_with_generated_binds(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT {{ a }}", params={"reflowfy_p0": 1})
        with pytest.raises(SqlTemplateError, match="collide"):
            src.fetch({"a": 1})

    def test_limit_uses_fetchmany_with_a_trailing_semicolon(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM events ORDER BY id DESC;")
        assert _ids(src.fetch({}, limit=2)) == [9, 10]

    def test_template_off_keeps_the_query_verbatim(self, db_url: str) -> None:
        src = SqlSource(
            db_url, "SELECT * FROM events WHERE id = :x", params={"x": 3}, template=False
        )
        assert _ids(src.fetch({})) == [3]


class TestSplit:
    def test_slices_carry_bound_sql_and_rebuild_on_the_worker(self, db_url: str) -> None:
        src = sql_source(
            db_url,
            "SELECT * FROM {{ t | ident }} WHERE status = '{{ status }}';",
            id_column="id",
            batch_size=4,
        )
        subs = list(src.split({"t": "events", "status": "inactive"}))

        assert all(s.config["template"] is False for s in subs)
        assert subs[0].config["params"] == {
            "reflowfy_p0": "inactive",  # `| ident` is spliced, not a value
            "reflowfy_lo": 2,
            "reflowfy_hi": 6,
        }
        assert "{{" not in subs[0].config["query"]
        rebuilt = [
            SourceFactory.create(d["type"], d["config"])
            for d in (SourceFactory.serialize(s) for s in subs)
        ]
        # the worker has no "status" param: the value travels in the slice
        assert _ids([r for s in rebuilt for r in s.fetch({})]) == [2, 4, 6, 8, 10]

    def test_no_id_column_probe_with_trailing_semicolon(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM events WHERE id > {{ n }} ;")
        assert list(src.split({"n": 3})) == [src]
        assert list(src.split({"n": 99})) == []

    def test_trailing_line_comment_does_not_swallow_the_wrapper(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM events -- all of them", id_column="id")
        assert _ids([r for s in src.split({}) for r in s.fetch({})]) == list(range(1, 11))

    def test_id_column_must_be_an_identifier(self, db_url: str) -> None:
        src = sql_source(db_url, "SELECT * FROM events", id_column="id; DROP TABLE events")
        with pytest.raises(SqlTemplateError, match="id_column"):
            list(src.split({}))

    def test_legacy_split_jobs_with_binds_and_semicolon(self, db_url: str) -> None:
        by_id = sql_source(
            db_url, "SELECT * FROM events WHERE amount > {{ m }};", id_column="id", batch_size=5
        )
        by_offset = sql_source(
            db_url, "SELECT * FROM events WHERE amount > {{ m }} ORDER BY id;", batch_size=3
        )
        assert [_ids(j.records) for j in by_id.split_jobs({"m": 50})] == [[6, 7, 8, 9, 10]]
        assert [_ids(j.records) for j in by_offset.split_jobs({"m": 50})] == [
            [6, 7, 8],
            [9, 10],
        ]


def test_quoted_identifiers_without_values_pass_through() -> None:
    sql, binds = render_sql_template('SELECT "Col" FROM x WHERE a = {{ a }}', {"a": 1})
    assert sql == 'SELECT "Col" FROM x WHERE a = :reflowfy_p0'
    assert binds == {"reflowfy_p0": 1}


def test_ident_of_a_missing_parameter() -> None:
    with pytest.raises(SqlTemplateError, match="no runtime parameter 'tbl'"):
        render_sql_template("SELECT * FROM {{ tbl | ident }}", {})
    # ...but in a comment it is dropped like any other value
    assert render_sql_template("-- {{ tbl | ident }}\nSELECT 1", {}) == ("-- \nSELECT 1", {})
