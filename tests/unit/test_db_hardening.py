"""Dead database sessions must not block a replacement process.

A manager force-killed mid-plan left a session `idle in transaction` on the server for
10+ minutes (TCP keepalives default to two hours). The replacement's startup ran
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, which takes an exclusive table lock even
when it has nothing to do, queued behind that session, and never finished.
"""

from typing import Any, List
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError

import reflowfy.reflow_manager.database as database
from reflowfy.reflow_manager.db_settings import async_connect_args, sync_connect_args

PG = "postgresql://u:p@db:5432/reflowfy"
PG_ASYNC = "postgresql+asyncpg://u:p@db:5432/reflowfy"


def test_postgres_sessions_ask_the_server_to_probe_the_client():
    args = sync_connect_args(PG)
    for setting in ("tcp_keepalives_idle=30", "tcp_keepalives_interval=10", "tcp_keepalives_count=3"):
        assert f"-c {setting}" in str(args["options"])  # server-side: drops a dead peer in ~60s
    assert args["keepalives"] == 1  # and the client side too
    assert async_connect_args(PG_ASYNC) == {
        "server_settings": {
            "tcp_keepalives_idle": "30",
            "tcp_keepalives_interval": "10",
            "tcp_keepalives_count": "3",
        }
    }


def test_other_databases_get_no_postgres_only_settings():
    assert sync_connect_args("sqlite:///x.db") == {}
    assert async_connect_args("sqlite+aiosqlite:///x.db") == {}


def test_keepalives_can_be_turned_off_for_pgbouncer(monkeypatch):
    monkeypatch.setenv("DB_SESSION_KEEPALIVES", "false")
    assert sync_connect_args(PG) == {}
    assert async_connect_args(PG_ASYNC) == {}


def test_keepalive_timings_are_configurable(monkeypatch):
    monkeypatch.setenv("DB_KEEPALIVE_IDLE_SECONDS", "5")
    assert "tcp_keepalives_idle=5" in str(sync_connect_args(PG)["options"])


class _FakeEngine:
    """Records every statement; raises for those matching ``lock_on``."""

    def __init__(self, lock_on: str = ""):
        self.statements: List[str] = []
        self.lock_on = lock_on

    def begin(self):
        engine = self

        class _Ctx:
            def __enter__(self_inner):
                conn = MagicMock()

                def execute(clause):
                    sql = str(clause)
                    if engine.lock_on and engine.lock_on in sql:
                        raise OperationalError(sql, {}, Exception("canceling statement: lock timeout"))
                    engine.statements.append(sql)

                conn.execute = execute
                return conn

            def __exit__(self_inner, *exc):
                return False

        return _Ctx()


def _patch(monkeypatch, engine: _FakeEngine, missing: str = "") -> None:
    columns = {
        "jobs": ["job_id", "error_traceback", "attempts"],
        "executions": [
            "execution_id", "deduplicated_jobs", "rate_limit", "driver_id",
            "driver_heartbeat_at", "plan_complete",
        ],
    }
    if missing:
        for cols in columns.values():
            if missing in cols:
                cols.remove(missing)

    class _Inspector:
        def get_columns(self, table: str) -> List[Any]:
            return [{"name": name} for name in columns[table]]

    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setattr("sqlalchemy.inspect", lambda eng: _Inspector())


def test_existing_columns_are_skipped_without_touching_the_table(monkeypatch):
    """The usual restart: everything exists, so no ALTER ... ADD COLUMN may run — that
    statement is the one that waits for a lock."""
    engine = _FakeEngine()
    _patch(monkeypatch, engine)

    database._apply_column_migrations()

    assert not [s for s in engine.statements if "ADD COLUMN" in s]


def test_a_missing_column_is_added_with_a_lock_timeout(monkeypatch):
    engine = _FakeEngine()
    _patch(monkeypatch, engine, missing="plan_complete")

    database._apply_column_migrations()

    adds = [i for i, s in enumerate(engine.statements) if "plan_complete" in s]
    assert len(adds) == 1
    assert "lock_timeout" in engine.statements[adds[0] - 1]  # set in the same transaction


def test_optional_migrations_give_up_quietly_when_the_table_is_locked(monkeypatch):
    engine = _FakeEngine(lock_on="autovacuum")
    _patch(monkeypatch, engine)

    database._apply_column_migrations()  # does not raise: it only tunes performance


def test_a_missing_column_that_cannot_be_added_is_an_error(monkeypatch):
    """A column the code needs and cannot get must fail startup loudly (init_db retries),
    not be skipped."""
    engine = _FakeEngine(lock_on="plan_complete")
    _patch(monkeypatch, engine, missing="plan_complete")

    with pytest.raises(OperationalError):
        database._apply_column_migrations()
