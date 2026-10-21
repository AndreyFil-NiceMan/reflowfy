"""database.py: init_db's startup retry, the get_db dependency and close_db.

Column migrations are covered by test_db_hardening.py. The module's own engine points at
Postgres, so every test here swaps it (or SessionLocal) for SQLite or a recorder.
"""

from typing import Any, List

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import OperationalError

import reflowfy.reflow_manager.database as db_mod


def _db_error() -> OperationalError:
    return OperationalError("connect", {}, Exception("the database system is starting up"))


@pytest.fixture
def sleeps(monkeypatch) -> List[float]:
    recorded: List[float] = []
    monkeypatch.setattr(db_mod.time, "sleep", recorded.append)
    return recorded


def test_init_db_retries_until_postgres_is_ready(monkeypatch, sleeps):
    engine = create_engine("sqlite://")
    monkeypatch.setattr(db_mod, "engine", engine)
    attempts = {"n": 0}

    def migrations() -> None:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _db_error()

    monkeypatch.setattr(db_mod, "_apply_column_migrations", migrations)

    db_mod.init_db(max_retries=5, retry_delay=0.5)

    assert attempts["n"] == 3
    assert sleeps == [0.5, 0.5]
    assert {"executions", "jobs", "dlq_jobs", "pipeline_schedules"} <= set(
        inspect(engine).get_table_names()
    )


def test_init_db_gives_up_after_max_retries(monkeypatch, sleeps):
    monkeypatch.setattr(db_mod, "engine", create_engine("sqlite://"))
    errors: List[Exception] = []

    def migrations() -> None:
        errors.append(_db_error())
        raise errors[-1]

    monkeypatch.setattr(db_mod, "_apply_column_migrations", migrations)

    with pytest.raises(RuntimeError, match="Database init failed after 3 attempts") as info:
        db_mod.init_db(max_retries=3, retry_delay=2.0)

    assert info.value.__cause__ is errors[-1]
    assert sleeps == [2.0, 2.0]  # no pointless wait after the last attempt


class _Session:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def test_get_db_yields_a_session_and_closes_it(monkeypatch):
    session = _Session()
    monkeypatch.setattr(db_mod, "SessionLocal", lambda: session)

    gen = db_mod.get_db()
    assert next(gen) is session
    assert session.closed == 0
    with pytest.raises(StopIteration):
        next(gen)

    assert session.closed == 1


def test_get_db_closes_the_session_when_the_request_raises(monkeypatch):
    session = _Session()
    monkeypatch.setattr(db_mod, "SessionLocal", lambda: session)

    gen = db_mod.get_db()
    next(gen)
    with pytest.raises(ValueError, match="handler failed"):
        gen.throw(ValueError("handler failed"))

    assert session.closed == 1


def test_get_db_works_as_a_fastapi_dependency(monkeypatch):
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    sessions: List[_Session] = []

    def factory() -> _Session:
        sessions.append(_Session())
        return sessions[-1]

    monkeypatch.setattr(db_mod, "SessionLocal", factory)
    app = FastAPI()

    @app.get("/ok")
    def ok(db: Any = Depends(db_mod.get_db)) -> dict:
        return {"open": db.closed == 0}

    @app.get("/boom")
    def boom(db: Any = Depends(db_mod.get_db)) -> dict:
        raise RuntimeError("handler crashed")

    client = TestClient(app, raise_server_exceptions=False)
    assert client.get("/ok").json() == {"open": True}
    assert client.get("/boom").status_code == 500

    assert [s.closed for s in sessions] == [1, 1]  # closed after success and after a crash


def test_close_db_disposes_the_engine(monkeypatch):
    disposed: List[bool] = []
    monkeypatch.setattr(
        db_mod, "engine", type("E", (), {"dispose": lambda self: disposed.append(True)})()
    )

    db_mod.close_db()

    assert disposed == [True]
