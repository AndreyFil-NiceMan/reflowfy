"""LocalDispatcher: in-process dispatch, rate limiting, and failure isolation."""

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

import reflowfy.reflow_manager.local_dispatcher as local_mod
from reflowfy.core.registry import pipeline_registry
from reflowfy.reflow_manager.dispatcher import TOKEN_WAIT_SECONDS
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.worker.executor import WorkerExecutor


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@contextmanager
def capture_logs(name: str) -> Iterator[List[str]]:
    target = logging.getLogger(name)
    handler = _ListHandler()
    old_level = target.level
    target.addHandler(handler)
    target.setLevel(logging.DEBUG)
    try:
        yield handler.messages
    finally:
        target.removeHandler(handler)
        target.setLevel(old_level)


class FakeRateLimiter:
    def __init__(self, tokens: int = 10**9):
        self.tokens = tokens
        self.calls: List[Dict[str, Any]] = []

    def acquire_token(
        self, pipeline_name: str, rate_limit: Optional[float] = None, max_wait: float = 300.0
    ) -> bool:
        self.calls.append(
            {"pipeline": pipeline_name, "rate_limit": rate_limit, "max_wait": max_wait}
        )
        if self.tokens <= 0:
            return False
        self.tokens -= 1
        return True


class FakeExecutor:
    def __init__(self, fail: Optional[Dict[str, BaseException]] = None):
        self.fail = fail or {}
        self.ran: List[str] = []
        self.closed = 0

    async def execute_job(self, job: Dict[str, Any]) -> bool:
        self.ran.append(job["job_id"])
        error = self.fail.get(job["job_id"])
        if error is not None:
            raise error
        return True

    async def close(self) -> None:
        self.closed += 1


def _dispatcher(
    monkeypatch, tokens: int = 10**9, fail: Optional[Dict[str, BaseException]] = None
) -> "tuple[LocalDispatcher, List[FakeExecutor]]":
    d = LocalDispatcher(rate_limiter=FakeRateLimiter(tokens))  # type: ignore[arg-type]
    created: List[FakeExecutor] = []

    def create() -> FakeExecutor:
        created.append(FakeExecutor(fail))
        return created[-1]

    monkeypatch.setattr(d, "_create_executor", create)
    return d, created


def _jobs(n: int) -> List[Dict[str, Any]]:
    return [{"job_id": f"j{i}"} for i in range(n)]


# ---------------------------------------------------------------- batch


class TestDispatchJobsBatch:
    async def test_runs_every_job_in_order_with_one_executor(self, monkeypatch):
        d, created = _dispatcher(monkeypatch)
        reported: List[str] = []

        n = await d.dispatch_jobs_batch(
            _jobs(3), "p", on_sent=lambda j: reported.append(j["job_id"])
        )

        assert n == 3
        assert reported == ["j0", "j1", "j2"]
        (executor,) = created
        assert executor.ran == ["j0", "j1", "j2"]
        assert executor.closed == 1
        assert d.rate_limiter.calls == []  # no rate limit -> no tokens taken

    async def test_a_failing_job_does_not_stop_the_batch(self, monkeypatch):
        d, created = _dispatcher(monkeypatch, fail={"j1": RuntimeError("boom")})
        reported: List[str] = []

        with capture_logs(local_mod.logger.name) as logs:
            n = await d.dispatch_jobs_batch(
                _jobs(3), "p", on_sent=lambda j: reported.append(j["job_id"])
            )

        assert n == 3  # "first N handled" stays true: the failure is on the job row
        assert reported == ["j0", "j1", "j2"]
        assert created[0].ran == ["j0", "j1", "j2"]
        assert "Local job execution failed" in logs

    async def test_rate_limit_takes_a_token_per_job(self, monkeypatch):
        d, _ = _dispatcher(monkeypatch)
        await d.dispatch_jobs_batch(_jobs(2), "p", rate_limit=30.0)
        assert (
            d.rate_limiter.calls
            == [{"pipeline": "p", "rate_limit": 30.0, "max_wait": TOKEN_WAIT_SECONDS}] * 2
        )

    async def test_no_token_returns_what_was_run(self, monkeypatch):
        d, created = _dispatcher(monkeypatch, tokens=1)
        n = await d.dispatch_jobs_batch(_jobs(3), "p", rate_limit=1.0)
        assert n == 1
        assert created[0].ran == ["j0"]
        assert created[0].closed == 1

    async def test_should_stop_ends_the_batch(self, monkeypatch):
        d, created = _dispatcher(monkeypatch)
        n = await d.dispatch_jobs_batch(
            _jobs(5), "p", should_stop=lambda **kw: len(created[0].ran) >= 2
        )
        assert n == 2
        assert created[0].ran == ["j0", "j1"]

    async def test_executor_closed_when_the_batch_is_interrupted(self, monkeypatch):
        d, created = _dispatcher(monkeypatch, fail={"j0": KeyboardInterrupt()})
        with pytest.raises(KeyboardInterrupt):
            await d.dispatch_jobs_batch(_jobs(2), "p")
        assert created[0].closed == 1

    async def test_close_is_a_noop(self, monkeypatch):
        d, created = _dispatcher(monkeypatch)
        await d.close()
        assert created == []


# ---------------------------------------------------------------- single job


class TestDispatchJob:
    async def test_runs_the_job_with_a_fresh_executor(self, monkeypatch):
        d, created = _dispatcher(monkeypatch)
        assert await d.dispatch_job({"job_id": "a"}, "p") is True
        assert await d.dispatch_job({"job_id": "b"}, "p") is True
        assert [e.ran for e in created] == [["a"], ["b"]]
        assert [e.closed for e in created] == [1, 1]
        assert d.rate_limiter.calls == []

    async def test_rate_limited_job_waits_up_to_five_minutes(self, monkeypatch):
        d, _ = _dispatcher(monkeypatch)
        assert await d.dispatch_job({"job_id": "a"}, "p", rate_limit=2.0) is True
        assert d.rate_limiter.calls == [{"pipeline": "p", "rate_limit": 2.0, "max_wait": 300.0}]

    async def test_rate_limit_timeout_skips_the_job(self, monkeypatch):
        d, created = _dispatcher(monkeypatch, tokens=0)
        with capture_logs(local_mod.logger.name) as logs:
            assert await d.dispatch_job({"job_id": "a"}, "p", rate_limit=2.0) is False
        assert created == []
        assert "Rate limit timeout, skipping job" in logs

    async def test_job_exception_returns_false_and_closes(self, monkeypatch):
        d, created = _dispatcher(monkeypatch, fail={"a": RuntimeError("x")})
        with capture_logs(local_mod.logger.name) as logs:
            assert await d.dispatch_job({"job_id": "a"}, "p") is False
        assert created[0].closed == 1
        assert "Local dispatch failed" in logs


# ---------------------------------------------------------------- real executor


def test_create_executor_builds_a_worker_executor(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path}/x.db")
    executor = LocalDispatcher(rate_limiter=FakeRateLimiter())._create_executor()  # type: ignore[arg-type]
    assert isinstance(executor, WorkerExecutor)
    assert executor.database_url.endswith("/x.db")


class _Dest:
    def __init__(self, sink: List[Any]):
        self.sink = sink
        self.config: Dict[str, Any] = {}

    async def health_check(self) -> bool:
        return True

    async def send_with_retry(self, records: List[Any], params: Dict[str, Any]) -> None:
        if any(r.get("poison") for r in records):
            raise RuntimeError("destination rejected the batch")
        self.sink.extend(records)

    async def close(self) -> None:
        pass


@pytest.mark.real_claim
async def test_failed_job_is_marked_failed_and_the_batch_continues(monkeypatch, tmp_path):
    """In-process, end to end on SQLite: the failing job's row is `failed` with its
    error, the jobs around it complete."""
    url = f"sqlite+aiosqlite:///{tmp_path}/local.db"
    monkeypatch.setenv("DATABASE_URL", url)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            Execution.__table__.insert().values(
                execution_id="e", pipeline_name="p", state="running", created_at=now, updated_at=now
            )
        )
        for job_id in ("j0", "j1", "j2"):
            await conn.execute(
                Job.__table__.insert().values(
                    job_id=job_id,
                    execution_id="e",
                    job_payload={},
                    state="dispatched",
                    attempts=0,
                    created_at=now,
                    updated_at=now,
                )
            )
    await engine.dispose()

    sink: List[Any] = []

    class Pipe:
        name = "local-dispatch-p"

        def define_transformations(self, records, params):
            return []

        def define_destination(self, records, params):
            return _Dest(sink)

    monkeypatch.setattr(pipeline_registry, "get", lambda name: Pipe())

    def job(job_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "schema_version": 2,
            "execution_id": "e",
            "job_id": job_id,
            "pipeline_name": "local-dispatch-p",
            "source": {"type": "StaticSource", "config": {"records": [record]}},
            "metadata": {"runtime_params": {}},
        }

    d = LocalDispatcher(rate_limiter=FakeRateLimiter())  # type: ignore[arg-type]
    n = await d.dispatch_jobs_batch(
        [job("j0", {"id": 0}), job("j1", {"id": 1, "poison": True}), job("j2", {"id": 2})], "p"
    )

    assert n == 3
    assert sink == [{"id": 0}, {"id": 2}]
    check = create_async_engine(url)
    async with check.connect() as conn:
        rows = {r.job_id: r for r in await conn.execute(Job.__table__.select())}
    await check.dispose()
    assert {k: r.state for k, r in rows.items()} == {
        "j0": "completed",
        "j1": "failed",
        "j2": "completed",
    }
    assert rows["j1"].error_message == "destination rejected the batch"
