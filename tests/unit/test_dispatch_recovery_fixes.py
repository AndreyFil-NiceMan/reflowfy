"""Continuous dispatch, job leases and worker claims.

Regressions found by the OpenShift scale test (2026-10-17), and the redesign
that replaced the batch barrier:

- A batch whose dispatch stopped early (Kafka error / rate-limit timeout) marked
  every job `dispatched`, including the ones never sent; resume only waited on
  dispatched jobs, so they were stranded forever (556 jobs in the 2M run).
- resume_execution dropped the /run rate_limit override.
- A worker shut down mid-job kept its content-hash claim, so the redelivered
  message was marked deduplicated and the unwritten rest of the job was lost.
- Kafka redelivery (rebalance, scale-down) ran a job twice and wrote it twice.
- Pause only took effect between 1000-job batches; a batch timeout failed
  healthy executions whose workers were merely slow.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.pipeline_runner as runner_mod
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.models import Base, Execution, Job
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner
from reflowfy.worker.executor import WorkerExecutor


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def db() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    session.add(Execution(execution_id="e", pipeline_name="p", state="running", total_jobs=0))
    session.commit()
    return session


def _add_jobs(db: Session, n: int, state: str = "pending", **cols: Any) -> List[str]:
    ids = [f"j{len(db.query(Job).all()) + i}" for i in range(n)]
    for job_id in ids:
        db.add(
            Job(
                job_id=job_id,
                execution_id="e",
                job_payload={"job_id": job_id},
                batch_number=1,
                state=state,
                **cols,
            )
        )
    db.commit()
    return ids


def _states(db: Session) -> Dict[str, str]:
    db.expire_all()
    return {j.job_id: j.state for j in db.query(Job).all()}


class _FakeDispatcher:
    """Sends in order; optionally completes each job (an instant worker)."""

    def __init__(self, db: Session, send_limit: int = 10**9, complete: bool = True):
        self.db = db
        self.send_limit = send_limit
        self.complete = complete
        self.calls: List[int] = []

    async def dispatch_jobs_batch(self, jobs, pipeline_name, rate_limit=None,
                                  on_sent=None, should_stop=None, on_unsendable=None):
        self.calls.append(len(jobs))
        sent = 0
        for payload in jobs:
            if (should_stop and should_stop()) or sent >= self.send_limit:
                break
            sent += 1
            if on_sent:
                on_sent(payload)
            if self.complete:
                self.db.query(Job).filter(Job.job_id == payload["job_id"]).update(
                    {"state": "completed"}
                )
                self.db.commit()
        return sent

    async def close(self):
        pass


def _runner(db: Session, dispatcher: Any) -> PipelineRunner:
    return PipelineRunner(
        execution_manager=ExecutionManager(db), job_manager=JobManager(db), dispatcher=dispatcher
    )


@pytest.fixture(autouse=True)
def _fast_loop(monkeypatch):
    monkeypatch.setattr(runner_mod, "CHECKPOINT_POLL_INTERVAL", 0.01)


def test_only_sent_jobs_are_marked_dispatched(db):
    """Sending stopped after 2 of 5: those 2 are dispatched, 3 stay pending."""
    ids = _add_jobs(db, 5)
    runner = _runner(db, _FakeDispatcher(db, send_limit=2, complete=False))
    jobs = JobManager(db).get_pending_jobs("e", limit=5)
    assert ExecutionManager(db).claim_driver("e", "me", lease_seconds=120)

    sent = asyncio.run(runner._send_jobs("e", "p:e", None, "me", jobs))

    assert sent == 2
    states = _states(db)
    assert [states[i] for i in ids] == ["dispatched"] * 2 + ["pending"] * 3
    db.expire_all()
    assert db.query(Job).filter(Job.job_id == ids[0]).one().dispatched_at is not None


def test_drive_loop_runs_to_completion(db):
    _add_jobs(db, 7)
    dispatcher = _FakeDispatcher(db)

    dispatched, completed, failed = _runner(db, dispatcher)._drive_execution("e", "p", None)

    assert (completed, failed) == (7, 0)
    assert set(_states(db).values()) == {"completed"}
    db.expire_all()
    assert db.query(Execution).one().state == "completed"


def test_drive_loop_retries_jobs_a_failed_send_left_pending(db):
    """A Kafka error mid-chunk no longer strands anything: the loop sends the rest."""
    _add_jobs(db, 5)
    dispatcher = _FakeDispatcher(db, send_limit=2)

    _runner(db, dispatcher)._drive_execution("e", "p", None)

    assert set(_states(db).values()) == {"completed"}
    assert len(dispatcher.calls) >= 3  # 2 + 2 + 1


def test_in_flight_window_caps_how_many_are_fetched(db, monkeypatch):
    monkeypatch.setattr(runner_mod, "MAX_IN_FLIGHT_JOBS", 3)
    running = _add_jobs(db, 2, state="running", updated_at=_now())
    _add_jobs(db, 5)
    runner = _runner(db, _FakeDispatcher(db))
    limits: List[int] = []
    real_get = runner.job_manager.get_pending_jobs

    def spy(execution_id, limit):
        limits.append(limit)
        if len(limits) == 1:
            # first fetch saw 2 in flight: only 1 free slot. Let the workers finish them.
            db.query(Job).filter(Job.job_id.in_(running)).update(
                {"state": "completed"}, synchronize_session=False
            )
            db.commit()
        return real_get(execution_id, limit)

    monkeypatch.setattr(runner.job_manager, "get_pending_jobs", spy)

    runner._drive_execution("e", "p", None)

    assert limits[0] == 1
    assert max(limits) <= 3
    assert set(_states(db).values()) == {"completed"}


def test_paused_execution_stops_without_finalizing(db):
    _add_jobs(db, 3)
    ExecutionManager(db).update_execution_state("e", "paused")
    dispatcher = _FakeDispatcher(db)

    _runner(db, dispatcher)._drive_execution("e", "p", None)

    assert dispatcher.calls == []
    db.expire_all()
    assert db.query(Execution).one().state == "paused"
    assert set(_states(db).values()) == {"pending"}


def test_pause_mid_chunk_stops_sending(db):
    _add_jobs(db, 4)

    class _PausingDispatcher(_FakeDispatcher):
        async def dispatch_jobs_batch(self, jobs, pipeline_name, rate_limit=None,
                                      on_sent=None, should_stop=None, on_unsendable=None):
            sent = 0
            for payload in jobs:
                if should_stop():
                    break
                sent += 1
                on_sent(payload)
                if sent == 1:
                    ExecutionManager(self.db).update_execution_state("e", "paused")
                    await asyncio.sleep(1.1)  # past the 1s state-check throttle
            return sent

    _runner(db, _PausingDispatcher(db, complete=False))._drive_execution("e", "p", None)

    assert sorted(_states(db).values()) == ["dispatched", "pending", "pending", "pending"]


def test_reclaim_expired_leases(db):
    old = _now() - timedelta(hours=2)
    lost_worker = _add_jobs(db, 1, state="running", updated_at=old, attempts=1)
    out_of_attempts = _add_jobs(db, 1, state="running", updated_at=old, attempts=3)
    lost_message = _add_jobs(db, 1, state="dispatched", dispatched_at=old)
    alive = _add_jobs(db, 1, state="running", updated_at=_now(), attempts=1)
    queued = _add_jobs(db, 1, state="dispatched", dispatched_at=_now())

    result = JobManager(db).reclaim_expired_leases(
        "e", running_lease_seconds=1800, dispatch_lease_seconds=3600, max_attempts=3
    )

    assert result == {"failed": 1, "requeued_running": 1, "requeued_dispatched": 1}
    states = _states(db)
    assert states[lost_worker[0]] == "pending"
    assert states[out_of_attempts[0]] == "failed"
    assert states[lost_message[0]] == "pending"
    assert states[alive[0]] == "running"
    assert states[queued[0]] == "dispatched"


def test_resume_reuses_persisted_rate_limit(db, monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: SimpleNamespace(rate_limit=4))
    db.query(Execution).update({"rate_limit": 30.0})
    db.commit()
    _add_jobs(db, 1)
    runner = _runner(db, _FakeDispatcher(db))
    seen = {}

    def fake_drive(execution_id, pipeline_name, effective_rate_limit):
        seen["rate"] = effective_rate_limit
        return 1, 1, 0

    monkeypatch.setattr(runner, "_drive_execution", fake_drive)
    runner.resume_execution("e")

    assert seen["rate"] == 30.0  # not the class rate_limit of 4


# ----- worker side --------------------------------------------------------------


def _payload(dedup: bool = False) -> Dict[str, Any]:
    return {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "p",
        "dedup_check": dedup,
        "source": {"type": "StaticSource", "config": {"records": [{"id": 1}]}},
        "metadata": {"runtime_params": {}},
    }


class _CancellingDest:
    async def health_check(self):
        return True

    async def send_with_retry(self, records, params):
        raise asyncio.CancelledError  # pod terminated mid-write

    async def close(self):
        pass


def _register(monkeypatch, destination: Any) -> None:
    from reflowfy.core.registry import pipeline_registry

    pipe = SimpleNamespace(
        name="p",
        define_transformations=lambda records, params: [],
        define_destination=lambda records, params: destination,
    )
    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipe)


async def test_claim_released_when_job_cancelled(monkeypatch):
    import reflowfy.worker.executor as executor_mod

    _register(monkeypatch, _CancellingDest())
    monkeypatch.setattr(executor_mod, "held_by_other_job", AsyncMock(return_value=False))
    monkeypatch.setattr(
        executor_mod, "claim_record_hashes", AsyncMock(side_effect=lambda sf, h, j: set(h))
    )
    released = AsyncMock()
    monkeypatch.setattr(executor_mod, "release_record_hashes", released)

    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_claim_job", AsyncMock(return_value=None))
    monkeypatch.setattr(ex, "_update_job_in_db", AsyncMock())

    with pytest.raises(asyncio.CancelledError):
        await ex.execute_job(_payload(dedup=True))

    released.assert_awaited_once()


async def test_redelivered_job_is_skipped(monkeypatch):
    dest = MagicMock()
    _register(monkeypatch, dest)
    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_claim_job", AsyncMock(return_value="completed"))
    update = AsyncMock()
    monkeypatch.setattr(ex, "_update_job_in_db", update)

    ok = await ex.execute_job(_payload())

    assert ok is True  # commit the offset: the job belongs to the delivery that ran it
    update.assert_not_awaited()  # must not overwrite that delivery's result
    dest.send_with_retry.assert_not_called()


@pytest.mark.real_claim
async def test_claim_job_transitions(tmp_path):
    """pending/dispatched -> running once; every later delivery sees the blocking state."""
    from sqlalchemy.ext.asyncio import create_async_engine

    url = f"sqlite+aiosqlite:///{tmp_path}/claims.db"
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            Execution.__table__.insert().values(
                execution_id="e", pipeline_name="p", state="running",
                created_at=_now(), updated_at=_now(),
            )
        )
        await conn.execute(
            Job.__table__.insert().values(
                job_id="j", execution_id="e", job_payload={}, state="dispatched",
                attempts=0, created_at=_now(), updated_at=_now(),
            )
        )
    await engine.dispose()

    ex = WorkerExecutor(database_url=url)
    try:
        assert await ex._claim_job("j") is None  # claimed
        assert await ex._claim_job("j") == "running"  # a redelivery while it runs
        assert await ex._claim_job("untracked") is None  # no row: run it
    finally:
        await ex.close()

    check = create_async_engine(url)
    async with check.connect() as conn:
        attempts = (await conn.execute(Job.__table__.select())).one().attempts
    await check.dispose()
    assert attempts == 1


def test_drive_loop_survives_a_database_outage(db, monkeypatch):
    """A DB error used to end the loop and strand the execution until a restart."""
    from sqlalchemy.exc import OperationalError

    _add_jobs(db, 3)
    runner = _runner(db, _FakeDispatcher(db))
    real_counts = runner.job_manager.get_job_counts
    calls = {"n": 0}

    def flaky(execution_id):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise OperationalError("SELECT 1", {}, Exception("connection refused"))
        return real_counts(execution_id)

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(runner.job_manager, "get_job_counts", flaky)
    monkeypatch.setattr(runner_mod.asyncio, "sleep", no_sleep)

    runner._drive_execution("e", "p", None)

    assert set(_states(db).values()) == {"completed"}
    db.expire_all()
    assert db.query(Execution).one().state == "completed"



# ----- dispatch-loop ownership lease ---------------------------------------------


def test_driver_lease_claim_heartbeat_takeover(db):
    em = ExecutionManager(db)
    assert em.claim_driver("e", "A", lease_seconds=120) is True
    assert em.claim_driver("e", "B", lease_seconds=120) is False  # A is alive
    assert em.heartbeat_driver("e", "A") is True
    # A stalls past the lease: B takes over, and A learns it on its next heartbeat
    db.query(Execution).update({"driver_heartbeat_at": _now() - timedelta(seconds=300)})
    db.commit()
    assert em.claim_driver("e", "B", lease_seconds=120) is True
    assert em.heartbeat_driver("e", "A") is False
    em.release_driver("e", "A")  # not the owner: no-op
    db.expire_all()
    assert db.query(Execution).one().driver_id == "B"
    em.release_driver("e", "B")
    db.expire_all()
    assert db.query(Execution).one().driver_id is None


def test_drive_loop_stands_by_then_takes_over_a_dead_loop(db, monkeypatch):
    """A force-killed manager never releases its lease; the next one must not give
    up (the advisory lock did, leaving the execution stuck) but take over once the
    dead loop's heartbeat is stale."""
    _add_jobs(db, 2)
    db.query(Execution).update({"driver_id": "dead-pod", "driver_heartbeat_at": _now()})
    db.commit()
    monkeypatch.setattr(runner_mod, "DRIVER_RETRY_SECONDS", 0.01)
    real_sleep = asyncio.sleep
    waits = {"n": 0}

    async def sleep_and_age(seconds):
        waits["n"] += 1
        if waits["n"] == 3:  # the dead pod's heartbeat goes stale
            db.query(Execution).update(
                {"driver_heartbeat_at": _now() - timedelta(seconds=600)}
            )
            db.commit()
        await real_sleep(0)

    monkeypatch.setattr(runner_mod.asyncio, "sleep", sleep_and_age)

    _runner(db, _FakeDispatcher(db))._drive_execution("e", "p", None)

    assert set(_states(db).values()) == {"completed"}
    db.expire_all()
    execution = db.query(Execution).one()
    assert execution.state == "completed"
    assert execution.driver_id is None  # released on exit


def test_drive_loop_stops_when_another_loop_took_over(db):
    _add_jobs(db, 3)

    class _Thief(_FakeDispatcher):
        async def dispatch_jobs_batch(self, jobs, pipeline_name, rate_limit=None,
                                      on_sent=None, should_stop=None, on_unsendable=None):
            self.db.query(Execution).update({"driver_id": "other"})
            self.db.commit()
            return 0

    dispatcher = _Thief(db)
    _runner(db, dispatcher)._drive_execution("e", "p", None)

    assert set(_states(db).values()) == {"pending"}  # left for the new owner
    db.expire_all()
    assert db.query(Execution).one().driver_id == "other"


def test_release_drivers_on_host_frees_only_that_hosts_leases(db):
    db.add(Execution(execution_id="e2", pipeline_name="p", state="running"))
    db.commit()
    db.query(Execution).filter(Execution.execution_id == "e").update(
        {"driver_id": "pod-a:12:abcd", "driver_heartbeat_at": _now()}
    )
    db.query(Execution).filter(Execution.execution_id == "e2").update(
        {"driver_id": "pod-b:7:ef01", "driver_heartbeat_at": _now()}
    )
    db.commit()

    assert ExecutionManager(db).release_drivers_on_host("pod-a") == 1

    db.expire_all()
    owners = {e.execution_id: e.driver_id for e in db.query(Execution).all()}
    assert owners == {"e": None, "e2": "pod-b:7:ef01"}


# ----- worker heartbeat ----------------------------------------------------------


@pytest.mark.real_claim
async def test_heartbeat_refreshes_updated_at_only_while_running(tmp_path, monkeypatch):
    """A running job's updated_at is refreshed so the manager's running lease does
    not expire on a job that is merely long."""
    from sqlalchemy.ext.asyncio import create_async_engine

    monkeypatch.setenv("JOB_HEARTBEAT_SECONDS", "0.05")
    url = f"sqlite+aiosqlite:///{tmp_path}/hb.db"
    engine = create_async_engine(url)
    old = _now() - timedelta(hours=1)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            Execution.__table__.insert().values(
                execution_id="e", pipeline_name="p", state="running",
                created_at=old, updated_at=old,
            )
        )
        for job_id, state in (("run", "running"), ("done", "completed")):
            await conn.execute(
                Job.__table__.insert().values(
                    job_id=job_id, execution_id="e", job_payload={}, state=state,
                    attempts=1, created_at=old, updated_at=old,
                )
            )
    await engine.dispose()

    ex = WorkerExecutor(database_url=url)
    try:
        beats = [asyncio.create_task(ex._heartbeat_job(j)) for j in ("run", "done")]
        await asyncio.sleep(0.4)
        for beat in beats:
            beat.cancel()
    finally:
        await ex.close()

    check = create_async_engine(url)
    async with check.connect() as conn:
        rows = {r.job_id: r.updated_at for r in (await conn.execute(Job.__table__.select()))}
    await check.dispose()
    assert rows["run"] > _now() - timedelta(minutes=1)  # refreshed
    assert rows["done"] == old  # a finished job is left alone


def test_a_beating_job_survives_the_running_lease(db):
    """The manager side of the contract: a fresh updated_at keeps a running job."""
    beating = _add_jobs(db, 1, state="running", updated_at=_now(), attempts=1)
    silent = _add_jobs(db, 1, state="running", updated_at=_now() - timedelta(minutes=10), attempts=1)

    result = JobManager(db).reclaim_expired_leases(
        "e", running_lease_seconds=300, dispatch_lease_seconds=3600, max_attempts=3
    )

    assert result["requeued_running"] == 1
    states = _states(db)
    assert states[beating[0]] == "running"
    assert states[silent[0]] == "pending"


# ----- retry failed jobs ---------------------------------------------------------


def test_requeue_failed_resets_jobs_and_filters_by_error(db):
    a = _add_jobs(db, 1, state="failed", attempts=3, error_message="No search context found")
    b = _add_jobs(db, 1, state="failed", attempts=3, error_message="Destination health check failed")
    ok = _add_jobs(db, 1, state="completed")
    jm = JobManager(db)

    assert jm.requeue_failed("e", error_contains="search context") == 1
    states = _states(db)
    assert (states[a[0]], states[b[0]], states[ok[0]]) == ("pending", "failed", "completed")
    db.expire_all()
    job = db.query(Job).filter(Job.job_id == a[0]).one()
    assert (job.attempts, job.error_message, job.completed_at) == (0, None, None)

    assert jm.requeue_failed("e") == 1  # no filter: the rest
    assert _states(db)[b[0]] == "pending"
    assert jm.requeue_failed("e") == 0


def test_retry_failed_then_drive_loop_finishes_the_execution(db):
    """A failed execution, retried, runs its failed jobs and ends completed."""
    _add_jobs(db, 2, state="completed")
    _add_jobs(db, 3, state="failed", attempts=1, error_message="boom")
    ExecutionManager(db).update_execution_state("e", "failed", error_message="1 failed")
    assert JobManager(db).requeue_failed("e") == 3
    execution = ExecutionManager(db).reopen_execution("e")
    assert execution is not None and execution.state == "running"
    assert execution.completed_at is None and execution.error_message is None

    _runner(db, _FakeDispatcher(db))._drive_execution("e", "p", None)

    assert set(_states(db).values()) == {"completed"}
    db.expire_all()
    assert db.query(Execution).one().state == "completed"


# ----- a job that can never be sent --------------------------------------------------


class _Producer:
    """Rejects messages over a size, like a broker's max.request.size."""

    def __init__(self, limit: int):
        self.limit = limit
        self.sent: List[str] = []

    async def send_and_wait(self, topic, value):
        if len(value) > self.limit:
            from aiokafka.errors import MessageSizeTooLargeError

            raise MessageSizeTooLargeError(f"{len(value)} bytes is more than {self.limit}")
        import json

        self.sent.append(json.loads(value)["job_id"])


def _kafka_dispatcher(producer: _Producer):
    from reflowfy.reflow_manager.dispatcher import KafkaDispatcher

    rate_limiter = MagicMock()
    rate_limiter.acquire_token.return_value = True
    dispatcher = KafkaDispatcher("kafka:9092", "reflow.jobs", rate_limiter)

    async def get_producer():
        return producer

    dispatcher._get_producer = get_producer  # type: ignore[method-assign]
    return dispatcher


async def test_kafka_dispatcher_reports_an_oversized_job_and_sends_the_rest():
    producer = _Producer(limit=200)
    jobs = [{"job_id": "small-1"}, {"job_id": "big", "pad": "x" * 500}, {"job_id": "small-2"}]
    unsendable: List[str] = []

    sent = await _kafka_dispatcher(producer).dispatch_jobs_batch(
        jobs, "p", on_unsendable=lambda job, reason: unsendable.append(job["job_id"] + ":" + reason)
    )

    assert sent == 2 and producer.sent == ["small-1", "small-2"]
    assert len(unsendable) == 1 and unsendable[0].startswith("big:")
    assert "too large" in unsendable[0]


async def test_a_token_that_arrives_after_the_chunk_is_spent_still_sends_its_job():
    """At 1 job/min the token only arrives once the 30 s chunk is over. The re-check
    after the token must ignore the chunk limit, or every token is burned unsent."""
    producer = _Producer(limit=10_000)
    top_checks = iter([False])  # the loop-top check passes; the token wait then outlasts the chunk

    def should_stop(chunk: bool = True) -> bool:
        return next(top_checks, True) if chunk else False  # chunk=False: no pause, no lost lease

    sent = await _kafka_dispatcher(producer).dispatch_jobs_batch(
        [{"job_id": "j1"}], "p", should_stop=should_stop
    )

    assert sent == 1 and producer.sent == ["j1"]


async def test_a_transient_kafka_error_still_stops_the_batch_for_a_retry():
    from aiokafka.errors import KafkaConnectionError

    class _Down(_Producer):
        async def send_and_wait(self, topic, value):
            raise KafkaConnectionError("broker down")

    unsendable: List[str] = []
    sent = await _kafka_dispatcher(_Down(10**9)).dispatch_jobs_batch(
        [{"job_id": "a"}, {"job_id": "b"}], "p", on_unsendable=lambda j, r: unsendable.append(r)
    )

    assert sent == 0 and unsendable == []  # not poison: the loop will try again


def test_an_unsendable_job_fails_the_job_and_the_run_ends(db):
    """It used to be retried every two seconds for ever, with the execution stuck running."""
    _add_jobs(db, 3)

    class _Dispatcher(_FakeDispatcher):
        async def dispatch_jobs_batch(self, jobs, pipeline_name, rate_limit=None, on_sent=None,
                                      should_stop=None, on_unsendable=None):
            for payload in jobs:
                if payload["job_id"] == "j1":
                    on_unsendable(payload, "The job message is too large for Kafka")
                    continue
                on_sent(payload)
                self.db.query(Job).filter(Job.job_id == payload["job_id"]).update({"state": "completed"})
                self.db.commit()
            return 2

    _runner(db, _Dispatcher(db))._drive_execution("e", "p", None)

    states = _states(db)
    assert (states["j0"], states["j1"], states["j2"]) == ("completed", "failed", "completed")
    db.expire_all()
    assert db.query(Execution).one().state == "failed"
    assert "too large" in db.query(Job).filter(Job.job_id == "j1").one().error_message
