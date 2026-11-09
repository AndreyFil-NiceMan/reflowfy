"""Rate limit: precedence, persistence, per-execution buckets, and no hidden default."""

import logging
import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.exceptions import PipelineError
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.reflow_manager.dispatcher import KafkaDispatcher
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.models import Base, Execution, Job, RateLimitState
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner, resolve_rate_limit
from reflowfy.reflow_manager.rate_limiter import RateLimiter, execution_bucket_key
from reflowfy.sources.static import StaticSource


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch):
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)


@pytest.fixture
def db(tmp_path) -> Session:
    engine = create_engine(f"sqlite:///{tmp_path}/rate.db", connect_args={"timeout": 30})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()
    engine.dispose()


class _Dynamic:
    name = "dyn"
    rate_limit = 12.0

    def __init__(self, dynamic: Any) -> None:
        self.dynamic = dynamic

    def define_rate_limit(self, params: Dict[str, Any]) -> Any:
        return self.dynamic(params) if callable(self.dynamic) else self.dynamic


@pytest.mark.parametrize(
    "override, persisted, dynamic, expected",
    [
        (90.0, 30.0, 20.0, 90.0),  # /run override wins
        (None, 30.0, 20.0, 30.0),  # then what the execution persisted
        (None, None, 20.0, 20.0),  # then define_rate_limit(params)
        (None, None, None, 12.0),  # define_rate_limit None -> the attribute
        (None, None, 0, None),  # 0 means unlimited, as before
    ],
)
def test_precedence(override: Any, persisted: Any, dynamic: Any, expected: Any) -> None:
    assert resolve_rate_limit(_Dynamic(dynamic), {}, override, persisted) == expected


def test_define_rate_limit_sees_the_params() -> None:
    pipeline = _Dynamic(lambda p: 600 if p.get("env") == "prod" else None)
    assert resolve_rate_limit(pipeline, {"env": "prod"}) == 600.0
    assert resolve_rate_limit(pipeline, {"env": "dev"}) == 12.0


def test_nothing_set_is_unlimited() -> None:
    assert resolve_rate_limit(SimpleNamespace(rate_limit=None), {}) is None
    assert resolve_rate_limit(SimpleNamespace(), {}) is None


@pytest.mark.parametrize("bad, error", [(-5, ValueError), ("fast", TypeError), (True, TypeError)])
def test_invalid_rates_are_rejected(bad: Any, error: type) -> None:
    with pytest.raises(error):
        resolve_rate_limit(_Dynamic(bad), {})


def test_a_failing_define_rate_limit_is_named() -> None:
    def boom(params: Any) -> Any:
        raise KeyError("env")

    with pytest.raises(PipelineError, match="define_rate_limit of pipeline 'dyn'"):
        resolve_rate_limit(_Dynamic(boom), {})


# ----- planning persists the resolved rate; resume reuses it ----------------------------


class _RatePipeline(AbstractPipeline[RuntimeParams]):
    name = "rate_resolution_pipeline"

    def define_source(self, runtime_params: Any) -> Any:
        return StaticSource([{"id": 1}])

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        raise AssertionError("not sent")

    def define_rate_limit(self, runtime_params: Any) -> Any:
        return 45


def test_planning_persists_define_rate_limit_and_drives_with_it(db, monkeypatch) -> None:
    pipeline = _RatePipeline()
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipeline)
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), SimpleNamespace())
    seen: List[Any] = []
    monkeypatch.setattr(
        runner, "_drive_execution", lambda eid, name, rate: seen.append(rate) or (0, 0, 0)
    )
    result = runner.run_pipeline(_RatePipeline.name, {}, "e")

    assert seen == [45.0]
    db.expire_all()
    assert db.query(Execution).one().rate_limit == 45.0
    assert result["rate_limit"] == 45.0


def test_job_retry_travels_in_every_job_message(db, monkeypatch) -> None:
    from reflowfy.core.job_retry import JobRetryConfig

    pipeline = _RatePipeline(job_retry=JobRetryConfig(max_retries=3))
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipeline)
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), SimpleNamespace())
    monkeypatch.setattr(runner, "_drive_execution", lambda *a: (0, 0, 0))
    runner.run_pipeline(_RatePipeline.name, {}, "e")

    payload = db.query(Job).one().job_payload
    assert payload["metadata"]["job_retry"]["max_retries"] == 3
    assert JobRetryConfig.from_payload(payload) == JobRetryConfig(max_retries=3)


def test_resume_of_an_old_execution_resolves_the_rate_from_the_pipeline(db, monkeypatch):
    pipeline = _RatePipeline()
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipeline)
    db.add(
        Execution(
            execution_id="e",
            pipeline_name=_RatePipeline.name,
            state="running",
            plan_complete=True,
            runtime_params={},
        )
    )
    db.add(Job(job_id="j", execution_id="e", job_payload={}, state="pending"))
    db.commit()
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), SimpleNamespace())
    seen: List[Any] = []
    monkeypatch.setattr(
        runner, "_drive_execution", lambda eid, name, rate: seen.append(rate) or (0, 0, 0)
    )
    runner.resume_execution("e")
    assert seen == [45.0]


# ----- buckets --------------------------------------------------------------------------------


def test_no_rate_is_unlimited_and_touches_no_bucket(db) -> None:
    limiter = RateLimiter(db)
    assert limiter.acquire_token("k", None, max_wait=0)
    assert limiter.consume_tokens("k", 1, None)
    assert limiter.can_dispatch("k", 1, None)
    assert db.query(RateLimitState).count() == 0


def test_buckets_are_per_execution_and_deleted(db) -> None:
    limiter = RateLimiter(db)
    a, b = execution_bucket_key("p", "e1"), execution_bucket_key("p", "e2")
    assert a == "p:e1"
    assert limiter.consume_tokens(a, 1, 60.0)
    assert limiter.consume_tokens(b, 1, 6000.0)
    rates = {s.pipeline_name: s.refill_rate for s in db.query(RateLimitState)}
    assert rates == {a: 1.0, b: 100.0}  # neither overwrote the other's rate
    assert limiter.delete_bucket(a) == 1
    assert limiter.delete_bucket(a) == 0
    assert [s.pipeline_name for s in db.query(RateLimitState)] == [b]


async def test_kafka_dispatch_without_a_rate_takes_no_token() -> None:
    class _Limiter:
        default_rate = None

        def acquire_token(self, *args: Any, **kwargs: Any) -> bool:
            return RateLimiter.acquire_token(self, *args, **kwargs)  # type: ignore[arg-type]

        def consume_tokens(self, *args: Any) -> bool:
            raise AssertionError("no bucket for an unlimited execution")

    dispatcher = KafkaDispatcher("k:9092", "t", _Limiter())  # type: ignore[arg-type]

    class _Producer:
        async def send_and_wait(self, **kwargs: Any) -> None:
            return None

    async def producer() -> Any:
        return _Producer()

    dispatcher._get_producer = producer  # type: ignore[method-assign]
    sent = await dispatcher.dispatch_jobs_batch([{"job_id": "a"}, {"job_id": "b"}], "k", None)
    assert sent == 2


def test_the_drive_loop_deletes_the_bucket_once_the_execution_ended(db) -> None:
    db.add(Execution(execution_id="e", pipeline_name="p", state="completed", plan_complete=True))
    db.commit()
    limiter = RateLimiter(db)
    limiter.consume_tokens("p:e", 1, 60.0)

    class _Dispatcher:
        rate_limiter = limiter

        async def close(self) -> None:
            pass

    runner = PipelineRunner(ExecutionManager(db), JobManager(db), _Dispatcher())  # type: ignore[arg-type]
    asyncio.run(runner._drive_async("e", "p", 60.0, "me"))
    assert db.query(RateLimitState).count() == 0


def test_a_paused_execution_keeps_its_bucket(db) -> None:
    db.add(Execution(execution_id="e", pipeline_name="p", state="paused", plan_complete=True))
    db.commit()
    limiter = RateLimiter(db)
    limiter.consume_tokens("p:e", 1, 60.0)
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), SimpleNamespace(rate_limiter=limiter))
    runner._drop_rate_bucket("e", "p:e")
    assert db.query(RateLimitState).count() == 1


def test_bucket_cleanup_errors_are_logged_not_raised(db, caplog) -> None:
    class _Broken:
        def delete_bucket(self, key: str) -> int:
            raise RuntimeError("db down")

    db.add(Execution(execution_id="e", pipeline_name="p", state="failed", plan_complete=True))
    db.commit()
    runner = PipelineRunner(ExecutionManager(db), JobManager(db), SimpleNamespace(rate_limiter=_Broken()))
    runner._drop_rate_bucket("e", "p:e")
    assert "Could not delete the rate-limit bucket" in caplog.text
    # A dispatcher without a rate limiter (test doubles) is skipped quietly.
    PipelineRunner(ExecutionManager(db), JobManager(db), SimpleNamespace())._drop_rate_bucket(
        "e", "p:e"
    )
