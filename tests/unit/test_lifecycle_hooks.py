"""Lifecycle hooks, the /run callback and chaining, against a real (SQLite) database."""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.execution_summary import ExecutionSummary
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.reflow_manager import lifecycle
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.store.models import Base, Execution, Job, RateLimitState
from reflowfy.sources.static import StaticSource


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch):
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)

PIPE = "lifecycle_hooks_pipeline"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class _Calls:
    def __init__(self) -> None:
        self.calls: List[Any] = []
        self.lock = threading.Lock()
        self.raise_error: Optional[BaseException] = None
        self.sleep = 0.0
        self.chain: Optional[Dict[str, Any]] = None


CALLS = _Calls()


class _HookPipeline(AbstractPipeline[RuntimeParams]):
    name = PIPE

    def define_source(self, runtime_params: Any) -> Any:
        return StaticSource([{"id": 1}])

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        raise AssertionError("not sent")

    def _record(self, kind: str, summary: ExecutionSummary, params: Any) -> None:
        with CALLS.lock:
            CALLS.calls.append((kind, summary, dict(params)))
        if CALLS.sleep:
            time.sleep(CALLS.sleep)
        if CALLS.raise_error is not None:
            raise CALLS.raise_error
        if CALLS.chain is not None:
            CALLS.chain["child"] = lifecycle.run_pipeline(PIPE, {"step": 2})
            CALLS.chain["current"] = lifecycle.current_execution_id()

    def on_execution_complete(self, summary: ExecutionSummary, runtime_params: Any) -> None:
        self._record("complete", summary, runtime_params)

    def on_execution_failed(self, summary: ExecutionSummary, runtime_params: Any) -> None:
        self._record("failed", summary, runtime_params)

    def on_execution_cancelled(self, summary: ExecutionSummary, runtime_params: Any) -> None:
        self._record("cancelled", summary, runtime_params)


class _NoHooksPipeline(_HookPipeline):
    name = "lifecycle_no_hooks_pipeline"
    on_execution_complete = AbstractPipeline.on_execution_complete  # type: ignore[assignment]
    on_execution_failed = AbstractPipeline.on_execution_failed  # type: ignore[assignment]
    on_execution_cancelled = AbstractPipeline.on_execution_cancelled  # type: ignore[assignment]


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    CALLS.__init__()  # type: ignore[misc]
    pipelines = {PIPE: _HookPipeline(), _NoHooksPipeline.name: _NoHooksPipeline()}
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipelines.get(name))
    yield
    lifecycle.wait_for_hooks(timeout=10)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/hooks.db",
        connect_args={"timeout": 30, "check_same_thread": False},
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _execution(db: Session, state: str, eid: str = "e", pipeline: str = PIPE, **cols: Any) -> None:
    cols.setdefault("plan_complete", True)
    cols.setdefault("created_at", _now() - timedelta(seconds=10))
    db.add(
        Execution(
            execution_id=eid,
            pipeline_name=pipeline,
            state=state,
            runtime_params=cols.pop("runtime_params", {"day": "mon"}),
            **cols,
        )
    )
    db.commit()


def _job(db: Session, job_id: str, state: str, eid: str = "e", **cols: Any) -> None:
    db.add(Job(job_id=job_id, execution_id=eid, job_payload={}, state=state, **cols))
    db.commit()


def _row(db: Session, eid: str = "e") -> Execution:
    db.expire_all()
    return db.query(Execution).filter(Execution.execution_id == eid).one()


# ----- summary ---------------------------------------------------------------------------


def test_the_summary_counts_jobs_and_records(db) -> None:
    _execution(db, "failed", error_message="1 job failed", completed_at=_now())
    _job(
        db,
        "a",
        "completed",
        processed_records=8,
        stats={"records_input": 10, "records_skipped": 2, "retry_count": 1},
    )
    _job(db, "b", "completed", processed_records=0, stats={"records_input": 0, "skip_reason": "x"})
    _job(db, "c", "deduplicated", stats={"records_input": 5, "records_deduplicated": 5})
    _job(db, "d", "failed", processed_records=3, error_message="E" * 400, stats={"records_input": 3})
    _job(db, "f", "failed", error_message="boom", stats="not a dict")
    _job(db, "g", "cancelled")

    summary = lifecycle.build_summary(db, _row(db))

    assert summary.state == "failed" and not summary.succeeded
    assert (summary.jobs_total, summary.jobs_completed, summary.jobs_failed) == (6, 2, 2)
    assert (summary.jobs_deduplicated, summary.jobs_cancelled, summary.jobs_skipped) == (1, 1, 1)
    assert (summary.records_in, summary.records_out) == (18, 8)  # a failed job sent nothing
    assert (summary.records_skipped, summary.records_deduplicated, summary.retries) == (2, 5, 1)
    assert summary.duration_seconds is not None and 9 <= summary.duration_seconds <= 12
    assert summary.error_message == "1 job failed"
    assert len(summary.first_errors) == 2
    assert {len(e) for e in summary.first_errors} == {300, 4}
    assert summary.to_dict()["first_errors"] == summary.first_errors


# ----- firing ----------------------------------------------------------------------------


def test_the_hook_for_the_state_runs_once_with_the_summary_and_params(db, engine, caplog) -> None:
    caplog.set_level("INFO", logger="reflowfy")
    _execution(db, "completed", completed_at=_now())
    _job(db, "a", "completed", processed_records=1, stats={"records_input": 1})

    assert lifecycle.fire_execution_hooks(engine, "e") is True
    assert lifecycle.fire_execution_hooks(engine, "e") is False  # already fired

    [(kind, summary, params)] = CALLS.calls
    assert kind == "complete"
    assert summary.execution_id == "e" and summary.records_out == 1
    assert params == {"day": "mon"}
    row = _row(db)
    assert row.hooks_fired_at is not None and row.hook_error is None
    assert row.state == "completed"
    assert "hook_fired" in [getattr(r, "event", None) for r in caplog.records]


@pytest.mark.parametrize("state, kind", [("failed", "failed"), ("cancelled", "cancelled")])
def test_failed_and_cancelled_hooks(db, engine, state: str, kind: str) -> None:
    _execution(db, state)
    lifecycle.fire_execution_hooks(engine, "e")
    assert [c[0] for c in CALLS.calls] == [kind]


def test_exactly_once_under_two_concurrent_callers(db, engine) -> None:
    _execution(db, "completed")
    barrier = threading.Barrier(2)
    results: List[bool] = []

    def fire() -> None:
        barrier.wait()
        results.append(lifecycle.fire_execution_hooks(engine, "e"))

    threads = [threading.Thread(target=fire) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == [False, True]
    assert len(CALLS.calls) == 1


def test_an_execution_still_running_or_unknown_fires_nothing(db, engine) -> None:
    _execution(db, "running")
    assert lifecycle.fire_execution_hooks(engine, "e") is False
    assert lifecycle.fire_execution_hooks(engine, "nope") is False
    assert _row(db).hooks_fired_at is None
    assert CALLS.calls == []


def test_a_pipeline_without_hooks_is_only_marked(db, engine) -> None:
    _execution(db, "completed", pipeline=_NoHooksPipeline.name)
    assert lifecycle.fire_execution_hooks(engine, "e") is True
    assert _row(db).hooks_fired_at is not None
    # An unregistered pipeline has no hooks either.
    _execution(db, "completed", eid="e2", pipeline="gone")
    assert lifecycle.fire_execution_hooks(engine, "e2") is True


def test_a_failing_hook_is_stored_and_never_changes_the_state(db, engine, caplog) -> None:
    CALLS.raise_error = RuntimeError("warehouse down")
    _execution(db, "completed")

    assert lifecycle.fire_execution_hooks(engine, "e") is True

    row = _row(db)
    assert row.state == "completed"
    assert row.hook_error == "on_execution_complete raised RuntimeError: warehouse down"
    assert "hook_failed" in [getattr(r, "event", None) for r in caplog.records]


def test_a_hook_that_calls_sys_exit_does_not_end_the_manager(db, engine) -> None:
    CALLS.raise_error = SystemExit(1)
    _execution(db, "failed")
    lifecycle.fire_execution_hooks(engine, "e")
    assert "SystemExit" in (_row(db).hook_error or "")


def test_a_hook_that_runs_too_long_is_reported(db, engine, monkeypatch) -> None:
    monkeypatch.setenv("HOOK_TIMEOUT_SECONDS", "0.05")
    CALLS.sleep = 3.0  # far beyond the timeout, so a loaded machine still tells them apart
    _execution(db, "completed")

    started = time.time()
    lifecycle.fire_execution_hooks(engine, "e")

    assert time.time() - started < 2.0  # the hook's 3 s are never waited out
    assert _row(db).hook_error == "on_execution_complete timed out after 0.05s"


# ----- callback ----------------------------------------------------------------------------


class _Response:
    def __init__(self, status: int) -> None:
        self.status_code = status
        self.request = httpx.Request("POST", "http://cb")


def test_the_callback_gets_the_summary_after_the_hook(db, engine, monkeypatch, caplog) -> None:
    caplog.set_level("INFO", logger="reflowfy")
    posted: List[Any] = []

    def post(url: str, json: Any, timeout: float) -> _Response:
        posted.append((url, json, timeout, len(CALLS.calls)))
        return _Response(204)

    monkeypatch.setattr(httpx, "post", post)
    _execution(db, "completed", callback_url="https://hooks.example/done?token=secret")

    lifecycle.fire_execution_hooks(engine, "e")

    [(url, body, timeout, hooks_before)] = posted
    assert url.endswith("token=secret") and timeout == 10.0
    assert body["execution_id"] == "e" and body["state"] == "completed"
    assert hooks_before == 1  # the hook ran first
    assert _row(db).hook_error is None
    assert "token" not in caplog.text  # only the host is logged
    assert "hooks.example" in caplog.text


def test_a_failed_callback_is_stored(db, engine, monkeypatch) -> None:
    monkeypatch.setattr(httpx, "post", lambda *a, **k: _Response(500))
    _execution(db, "completed", pipeline=_NoHooksPipeline.name, callback_url="http://cb/x")
    lifecycle.fire_execution_hooks(engine, "e")
    assert _row(db).hook_error == "callback to cb failed: HTTP 500"


def test_an_unreachable_callback_and_a_failed_hook_are_both_stored(db, engine, monkeypatch):
    def post(*args: Any, **kwargs: Any) -> Any:
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(httpx, "post", post)
    CALLS.raise_error = ValueError("bad")
    _execution(db, "failed", callback_url="http://cb/x")
    lifecycle.fire_execution_hooks(engine, "e")
    assert _row(db).hook_error == (
        "on_execution_failed raised ValueError: bad; callback to cb failed: ConnectError"
    )


# ----- every terminal transition fires them ------------------------------------------------


def test_installed_listener_fires_on_terminal_transitions_and_drops_the_bucket(db) -> None:
    lifecycle.install()
    lifecycle.install()  # idempotent
    _execution(db, "running")
    db.add(RateLimitState(pipeline_name=f"{PIPE}:e", tokens=1, max_tokens=1, refill_rate=1,
                          last_update=_now()))
    db.commit()

    ExecutionManager(db).update_execution_state("e", "completed")
    assert lifecycle.wait_for_hooks(timeout=10)

    assert [c[0] for c in CALLS.calls] == ["complete"]
    assert db.query(RateLimitState).count() == 0

    # Not terminal: nothing.
    ExecutionManager(db).update_execution_state("e", "paused")
    assert lifecycle.wait_for_hooks(timeout=10)
    assert len(CALLS.calls) == 1


def test_cancel_fires_after_the_jobs_are_cancelled(db) -> None:
    lifecycle.install()
    _execution(db, "running")
    _job(db, "a", "pending")
    _job(db, "b", "completed", stats={"records_input": 1})

    result = ReflowManager(db, execution_mode="local").cancel_execution("e")
    assert result is not None and result.cancelled_jobs == 1
    assert lifecycle.wait_for_hooks(timeout=10)

    [(kind, summary, _)] = CALLS.calls
    assert kind == "cancelled" and summary.jobs_cancelled == 1


def test_retry_failed_reopens_the_hooks(db, engine) -> None:
    _execution(db, "failed")
    lifecycle.fire_execution_hooks(engine, "e")
    ExecutionManager(db).set_hook_error("e", "x")
    ExecutionManager(db).reopen_execution("e")
    row = _row(db)
    assert row.hooks_fired_at is None and row.hook_error is None


def test_a_listener_that_raises_does_not_break_the_transition(db, caplog) -> None:
    from reflowfy.reflow_manager.execution import add_terminal_listener, remove_terminal_listener

    def broken(bind: Any, eid: str, state: str) -> None:
        raise RuntimeError("listener bug")

    add_terminal_listener(broken)
    try:
        _execution(db, "running")
        assert ExecutionManager(db).update_execution_state("e", "failed") is not None
    finally:
        remove_terminal_listener(broken)
        remove_terminal_listener(broken)  # no-op the second time
    assert "Terminal-state listener failed" in caplog.text


def test_terminal_work_never_raises(monkeypatch, caplog) -> None:
    def boom(*args: Any) -> None:
        raise RuntimeError("db down")

    monkeypatch.setattr(lifecycle, "_drop_rate_bucket", boom)
    monkeypatch.setattr(lifecycle, "fire_execution_hooks", boom)
    lifecycle.run_terminal_work(None, "e")
    assert "Could not delete the rate-limit bucket" in caplog.text
    assert "Lifecycle hooks could not be fired" in caplog.text


def test_wait_for_hooks_times_out(monkeypatch) -> None:
    release = threading.Event()
    monkeypatch.setattr(lifecycle, "run_terminal_work", lambda bind, eid: release.wait(5))
    lifecycle.on_terminal(None, "e", "completed")
    assert lifecycle.wait_for_hooks(timeout=0.05) is False
    release.set()
    assert lifecycle.wait_for_hooks() is True


# ----- chaining -------------------------------------------------------------------------------


def test_run_pipeline_from_a_hook_records_the_parent(db, engine, monkeypatch) -> None:
    driven: List[Any] = []
    started = threading.Event()

    def drive(*args: Any) -> None:
        driven.append(args)
        started.set()

    monkeypatch.setattr(lifecycle, "_drive_child", drive)
    CALLS.chain = {}
    _execution(db, "completed")

    lifecycle.fire_execution_hooks(engine, "e")

    child_id = CALLS.chain["child"]
    assert CALLS.chain["current"] == "e"
    child = _row(db, child_id)
    assert child.parent_execution_id == "e"
    assert child.state == "pending" and child.runtime_params == {"step": 2}
    # The child is started in its own thread: wait for it rather than poll for a fixed
    # second, which a loaded machine (coverage, a busy CI runner) overran.
    assert started.wait(timeout=30)
    assert driven[0][1:] == (child_id, PIPE, {"step": 2}, None, None)
    assert lifecycle.current_execution_id() is None  # only inside the hook


def test_run_pipeline_rejects_unknown_pipelines_and_bad_params() -> None:
    with pytest.raises(ValueError, match="not found in registry"):
        lifecycle.run_pipeline("nope")

    class _Strict:
        def apply_defaults(self, params: Any) -> Any:
            return params

        def validate_parameters(self, params: Any) -> List[str]:
            return ["day is required"]

    pipeline_registry.get = lambda name: _Strict()  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="Invalid parameters: day is required"):
        lifecycle.run_pipeline("strict")


def test_run_pipeline_outside_a_hook_uses_the_manager_database(engine, monkeypatch) -> None:
    import reflowfy.reflow_manager.database as database

    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=engine))
    driven: List[Any] = []
    monkeypatch.setattr(lifecycle, "_drive_child", lambda *args: driven.append(args))

    eid = lifecycle.run_pipeline(
        PIPE, {"x": 1}, parent_execution_id="p", rate_limit=5, execution_id="c1",
        callback_url="http://cb",
    )

    assert eid == "c1"
    with Session(bind=engine) as db:
        row = db.query(Execution).one()
        assert (row.parent_execution_id, row.rate_limit, row.callback_url) == ("p", 5, "http://cb")


def test_drive_child_runs_the_plan_and_fails_the_execution_on_error(db, engine, monkeypatch):
    import reflowfy.reflow_manager.factory as factory

    _execution(db, "pending")
    seen: List[Any] = []

    class _Manager:
        def run_pipeline_jobs(self, **kwargs: Any) -> None:
            seen.append(kwargs)

    monkeypatch.setattr(factory, "make_manager", lambda db, mode=None: _Manager())
    lifecycle._drive_child(engine, "e", PIPE, {"a": 1}, 7.0, "local")
    assert seen == [
        {"execution_id": "e", "pipeline_name": PIPE, "runtime_params": {"a": 1},
         "rate_limit_override": 7.0}
    ]

    class _Broken:
        def run_pipeline_jobs(self, **kwargs: Any) -> None:
            raise RuntimeError("plan blew up")

    monkeypatch.setattr(factory, "make_manager", lambda db, mode=None: _Broken())
    lifecycle._drive_child(engine, "e", PIPE, {}, None, None)
    row = _row(db)
    assert row.state == "failed" and row.error_message == "plan blew up"


def test_drive_child_survives_a_failure_to_mark_failed(engine, monkeypatch, caplog):
    import reflowfy.reflow_manager.factory as factory

    def broken_manager(db: Any, mode: Any = None) -> Any:
        raise RuntimeError("no kafka config")

    monkeypatch.setattr(factory, "make_manager", broken_manager)
    monkeypatch.setattr(
        ExecutionManager, "update_execution_state", lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("db down")
        )
    )
    lifecycle._drive_child(engine, "e", PIPE, {}, None, None)
    assert "Failed to mark execution as failed" in caplog.text


def test_make_manager_reads_the_environment(db, monkeypatch) -> None:
    from reflowfy.reflow_manager.factory import make_manager

    monkeypatch.setenv("EXECUTION_MODE", "distributed")
    assert make_manager(db, mode="local").execution_mode == "local"
    assert make_manager(db).execution_mode == "distributed"


# ----- service wiring ---------------------------------------------------------------------------


def test_run_accepts_and_persists_a_callback_url(engine, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from reflowfy.reflow_manager.database import get_db

    factory = sessionmaker(bind=engine)
    monkeypatch.setenv("EXECUTION_MODE", "local")
    monkeypatch.setattr(app_module, "_dispatch_pipeline_jobs", lambda **kw: None)
    monkeypatch.setattr(app_module, "get_pipeline_scheduler", lambda: None)

    def db_override() -> Any:
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app_module.app.dependency_overrides[get_db] = db_override
    try:
        client = TestClient(app_module.app)
        bad = client.post(
            "/run", json={"pipeline_name": PIPE, "callback_url": "ftp://x/y"}
        )
        assert bad.status_code == 422
        ok = client.post(
            "/run",
            json={"pipeline_name": PIPE, "execution_id": "r", "callback_url": "https://cb/x"},
        )
        assert ok.status_code == 202
    finally:
        app_module.app.dependency_overrides.clear()
    with Session(bind=engine) as db:
        assert db.query(Execution).one().callback_url == "https://cb/x"


def test_execution_stats_report_the_hooks(db) -> None:
    _execution(db, "failed", rate_limit=30.0, parent_execution_id="p")
    _job(db, "a", "pending", next_attempt_at=_now() + timedelta(minutes=1))
    ExecutionManager(db).claim_hooks("e")
    ExecutionManager(db).set_hook_error("e", "callback to cb failed: HTTP 500")

    stats = ReflowManager(db, execution_mode="local").get_execution_stats("e")

    assert stats is not None
    assert stats["hooks"]["error"] == "callback to cb failed: HTTP 500"
    assert stats["hooks"]["fired_at"] is not None
    assert (stats["rate_limit"], stats["parent_execution_id"]) == (30.0, "p")
    assert stats["jobs_retry_scheduled"] == 1
    as_dict = _row(db).to_dict()
    assert as_dict["hook_error"] and "callback_url" not in as_dict


async def test_startup_installs_the_hooks_and_warns_about_max_jobs_per_minute(
    monkeypatch, caplog
) -> None:
    from reflowfy.reflow_manager.execution import _terminal_listeners

    monkeypatch.setenv("MAX_JOBS_PER_MINUTE", "6000")
    monkeypatch.setattr(app_module, "init_db", lambda: None)
    monkeypatch.setattr(app_module, "setup_logging", lambda **kw: None)
    monkeypatch.setattr(
        "reflowfy.core.pipeline_discovery.discover_and_load_pipelines", lambda: None
    )

    async def no_recovery() -> None:
        return None

    monkeypatch.setattr(app_module, "_recover_interrupted_executions", no_recovery)
    monkeypatch.setattr(app_module, "init_dlq_scheduler", lambda f: None)
    monkeypatch.setattr(app_module, "init_pipeline_scheduler", lambda f: (_ for _ in ()).throw(
        RuntimeError("skip")
    ))
    monkeypatch.setattr(app_module, "init_content_dedup_scheduler", lambda: None)

    await app_module._startup()

    assert lifecycle.on_terminal in _terminal_listeners
    assert "MAX_JOBS_PER_MINUTE is no longer applied" in caplog.text


async def test_shutdown_waits_for_hooks_in_flight(monkeypatch, caplog) -> None:
    for name in ("stop_pipeline_scheduler", "stop_dlq_scheduler", "stop_content_dedup_scheduler"):
        monkeypatch.setattr(app_module, name, lambda: None)
    monkeypatch.setattr(lifecycle, "wait_for_hooks", lambda timeout=None: False)
    await app_module._shutdown()
    assert "lifecycle hooks still running" in caplog.text
