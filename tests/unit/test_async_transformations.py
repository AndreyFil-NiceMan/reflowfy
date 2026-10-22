"""Async transformations: awaited on the caller's loop, per-record steps run concurrently.

Covers the async core (``apply_transformations_iteratively_async`` /
``run_job_records_async``), its sync wrappers, ``AsyncTransformation``,
``@transformation`` on an ``async def``, ``@record_transformation`` and the worker
path through ``execute_job``.
"""

import asyncio
import contextvars
import time
from typing import Any, Dict, List

import pytest

from reflowfy import (
    AsyncTransformation,
    BaseTransformation,
    RecordTransformation,
    SkipJob,
    TransformationError,
    record_transformation,
    transformation,
)
from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.execution.job_runner import run_job_records, run_job_records_async
from reflowfy.execution.transformation_runner import (
    apply_transformations_iteratively,
    apply_transformations_iteratively_async,
)
from reflowfy.sources.static import StaticSource
from reflowfy.transformations.base import default_record_concurrency
from reflowfy.worker.executor import JobStats, WorkerExecutor


class _Pipeline:
    """Stand-in pipeline: a fixed (or computed) transformation list."""

    name = "ut_async_pipeline"

    def __init__(self, steps: Any, destination: Any = None):
        self._steps = steps
        self._destination = destination

    def define_transformations(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
        return self._steps(params) if callable(self._steps) else self._steps

    def define_destination(self, records: List[Any], params: Dict[str, Any]) -> Any:
        return self._destination


@transformation("ut_async_double")
async def double(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    await asyncio.sleep(0)
    return [{**r, "v": r["v"] * 2} for r in records]


@transformation("ut_async_set_flag")
async def set_flag(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    runtime_params["flag"] = True
    return records


@transformation("ut_sync_tag")
def tag(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    return [{**r, "tag": "t"} for r in records]


class Offset(AsyncTransformation):
    name = "ut_async_offset"

    def __init__(self, by: int):
        self.by = by
        self.validated: List[str] = []

    def validate_input(self, records: List[Any]) -> None:
        self.validated.append("in")

    def validate_output(self, records: List[Any]) -> None:
        self.validated.append("out")

    async def apply_async(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        await asyncio.sleep(0)
        return [{**r, "v": r["v"] + self.by} for r in records]


def _records(n: int) -> List[Dict[str, Any]]:
    return [{"id": i, "v": i} for i in range(n)]


class _InFlight:
    """Counts concurrent calls, so a test can check the cap really held."""

    def __init__(self) -> None:
        self.now = 0
        self.peak = 0

    async def hold(self, seconds: float) -> None:
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            await asyncio.sleep(seconds)
        finally:
            self.now -= 1


# --------------------------------------------------------------------------------
# Whole-batch async transformations
# --------------------------------------------------------------------------------


def test_an_async_decorated_function_builds_an_async_transformation():
    step = double()
    assert isinstance(step, AsyncTransformation)
    assert not isinstance(step, RecordTransformation)
    assert step.name == "ut_async_double"


async def test_the_async_core_awaits_an_async_transformation():
    out, applied = await apply_transformations_iteratively_async(
        _Pipeline([double()]), _records(3), {}
    )
    assert [r["v"] for r in out] == [0, 2, 4]
    assert [(s.name, s.mode, s.concurrency) for s in applied] == [
        ("ut_async_double", "async", None)
    ]


def test_the_sync_wrapper_gives_the_same_result():
    out, applied = apply_transformations_iteratively(_Pipeline([double()]), _records(3), {})
    assert [r["v"] for r in out] == [0, 2, 4]
    assert applied[0].mode == "async"


async def test_the_sync_wrapper_works_inside_a_running_loop():
    # LocalExecutor.execute runs inside FastAPI's loop: the wrapper must not deadlock.
    out, _ = apply_transformations_iteratively(_Pipeline([double()]), _records(2), {})
    assert [r["v"] for r in out] == [0, 2]


def test_a_subclass_keeps_its_constructor_state_and_runs_its_validation_hooks():
    step = Offset(by=10)
    out, applied = apply_transformations_iteratively(_Pipeline([step]), _records(2), {})
    assert [r["v"] for r in out] == [10, 11]
    assert step.validated == ["in", "out"]
    assert applied[0].mode == "async"


def test_calling_apply_directly_on_an_async_transformation_still_works():
    assert Offset(by=1).apply([{"v": 1}], {}) == [{"v": 2}]


async def test_sync_and_async_steps_mix_and_re_resolution_sees_async_writes():
    def steps(params: Dict[str, Any]) -> List[Any]:
        chain: List[Any] = [set_flag(), tag()]
        if params.get("flag"):
            chain.append(double())  # only revealed once the async step ran
        return chain

    params: Dict[str, Any] = {}
    out, applied = await apply_transformations_iteratively_async(
        _Pipeline(steps), _records(2), params
    )
    assert [s.name for s in applied] == ["ut_async_set_flag", "ut_sync_tag", "ut_async_double"]
    assert [s.mode for s in applied] == ["async", "sync", "async"]
    assert out == [{"id": 0, "v": 0, "tag": "t"}, {"id": 1, "v": 2, "tag": "t"}]
    assert params["flag"] is True


async def test_an_async_failure_is_a_transformation_error_with_the_chain_so_far():
    @transformation("ut_async_boom")
    async def boom(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        raise KeyError("user_id")

    with pytest.raises(TransformationError) as info:
        await apply_transformations_iteratively_async(_Pipeline([tag(), boom()]), [{"id": 1}], {})
    err = info.value
    assert err.transformation_name == "ut_async_boom"
    assert err.step_index == 1
    assert [s.name for s in err.applied_steps] == ["ut_sync_tag"]
    assert err.culprit_index == 0  # the KeyError heuristic still applies


async def test_skip_job_from_an_async_transformation_drops_the_job():
    @transformation("ut_async_skip")
    async def skip(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        raise SkipJob("nothing to score")

    params: Dict[str, Any] = {}
    result = await run_job_records_async(StaticSource(_records(2)), _Pipeline([skip()]), params)
    assert result == ([], [], [], None)
    assert params["skip_reason"] == "nothing to score"


# --------------------------------------------------------------------------------
# Per-record transformations
# --------------------------------------------------------------------------------


def test_a_record_transformation_is_built_from_an_async_function():
    @record_transformation("ut_rec_identity", concurrency=4, timeout=2.5)
    async def identity(record: Any, runtime_params: Any) -> Any:
        return record

    step = identity()
    assert isinstance(step, RecordTransformation)
    assert step.concurrency == 4 and step.timeout == 2.5
    assert step.effective_concurrency() == 4


async def test_per_record_keeps_input_order_and_none_drops_a_record():
    @record_transformation("ut_rec_order", concurrency=8)
    async def slow_first(record: Any, runtime_params: Any) -> Any:
        # Earlier records finish last, so completion order is the reverse of input.
        await asyncio.sleep(0.002 * (10 - record["id"]))
        return None if record["id"] == 3 else {**record, "seen": True}

    out, applied = await apply_transformations_iteratively_async(
        _Pipeline([slow_first()]), _records(10), {}
    )
    assert [r["id"] for r in out] == [0, 1, 2, 4, 5, 6, 7, 8, 9]
    assert all(r["seen"] for r in out)
    step = applied[0]
    assert (step.mode, step.concurrency, step.records_in, step.records_out) == (
        "per_record",
        8,
        10,
        9,
    )


@pytest.mark.parametrize("cap", [1, 3])
async def test_per_record_never_exceeds_its_concurrency(cap: int):
    flight = _InFlight()

    @record_transformation(f"ut_rec_cap_{cap}", concurrency=cap)
    async def held(record: Any, runtime_params: Any) -> Any:
        await flight.hold(0.01)
        return record

    started = time.monotonic()
    out, _ = await apply_transformations_iteratively_async(_Pipeline([held()]), _records(9), {})
    assert len(out) == 9
    assert flight.peak == cap
    # 9 records at `cap` at a time take about 9/cap rounds of 10 ms.
    assert time.monotonic() - started >= 0.01 * (9 // cap)


async def test_per_record_runs_concurrently_rather_than_one_by_one():
    @record_transformation("ut_rec_fast", concurrency=20)
    async def wait(record: Any, runtime_params: Any) -> Any:
        await asyncio.sleep(0.05)
        return record

    started = time.monotonic()
    await apply_transformations_iteratively_async(_Pipeline([wait()]), _records(20), {})
    assert time.monotonic() - started < 0.5  # one by one would be 1 s


async def test_the_first_failing_record_fails_the_step_naming_it_and_cancels_the_rest():
    cancelled: List[int] = []

    @record_transformation("ut_rec_fail", concurrency=4)
    async def fail_on_2(record: Any, runtime_params: Any) -> Any:
        await asyncio.sleep(0)  # let the whole first batch start
        if record["id"] == 2:
            raise ValueError("bad payload")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append(record["id"])
            raise
        return record

    started = time.monotonic()
    with pytest.raises(TransformationError) as info:
        await apply_transformations_iteratively_async(
            _Pipeline([tag(), fail_on_2()]), _records(8), {}
        )
    assert time.monotonic() - started < 5  # did not wait for the sleepers
    err = info.value
    assert err.transformation_name == "ut_rec_fail"
    assert err.culprit_index == 2
    assert err.culprit_record == {"id": 2, "v": 2, "tag": "t"}
    assert err.step_index == 1
    assert isinstance(err.original_error, ValueError)
    assert "record 2: ValueError: bad payload" in str(err)
    # The other records in flight were cancelled; records 4-7 never started.
    assert sorted(cancelled) == [0, 1, 3]


async def test_skip_job_from_one_record_drops_the_whole_job():
    @record_transformation("ut_rec_skip", concurrency=2)
    async def skip_on_1(record: Any, runtime_params: Any) -> Any:
        if record["id"] == 1:
            raise SkipJob("record 1 is not ready")
        await asyncio.sleep(0.05)
        return record

    params: Dict[str, Any] = {}
    result = await run_job_records_async(
        StaticSource(_records(4)), _Pipeline([skip_on_1()]), params
    )
    assert result == ([], [], [], None)
    assert params["skip_reason"] == "record 1 is not ready"


async def test_a_record_that_exceeds_its_timeout_fails_the_job():
    @record_transformation("ut_rec_timeout", timeout=0.05)
    async def hang_on_2(record: Any, runtime_params: Any) -> Any:
        if record["id"] == 2:
            await asyncio.sleep(10)
        return record

    with pytest.raises(TransformationError) as info:
        await apply_transformations_iteratively_async(_Pipeline([hang_on_2()]), _records(3), {})
    assert info.value.culprit_index == 2
    assert isinstance(info.value.original_error, TimeoutError)
    assert "timed out after 0.05s" in str(info.value)


async def test_a_timeout_raised_by_the_authors_own_code_keeps_its_message():
    @record_transformation("ut_rec_own_timeout")
    async def own_timeout(record: Any, runtime_params: Any) -> Any:
        raise asyncio.TimeoutError("upstream gave up")

    with pytest.raises(TransformationError) as info:
        await apply_transformations_iteratively_async(_Pipeline([own_timeout()]), _records(1), {})
    assert "upstream gave up" in str(info.value)
    assert "timed out after" not in str(info.value)


async def test_a_record_transformation_with_no_records_does_no_work():
    step = _Pooled()
    assert await step.apply_async([], {}) == []
    assert step.events == []  # no client opened for nothing


class _Pooled(RecordTransformation):
    """Opens one shared resource per step, like an HTTP client."""

    name = "ut_rec_pooled"
    concurrency = 3

    def __init__(self, fail_on: int = -1):
        self.fail_on = fail_on
        self.events: List[str] = []
        self.client: Any = None

    async def setup(self, runtime_params: Dict[str, Any]) -> None:
        self.events.append("setup")
        self.client = object()

    async def teardown(self) -> None:
        self.events.append("teardown")

    async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Any:
        assert self.client is not None
        if record["id"] == self.fail_on:
            raise RuntimeError("boom")
        return record


async def test_setup_and_teardown_run_once_per_step():
    step = _Pooled()
    out, _ = await apply_transformations_iteratively_async(_Pipeline([step]), _records(7), {})
    assert len(out) == 7
    assert step.events == ["setup", "teardown"]


async def test_teardown_runs_when_a_record_fails():
    step = _Pooled(fail_on=1)
    with pytest.raises(TransformationError):
        await apply_transformations_iteratively_async(_Pipeline([step]), _records(4), {})
    assert step.events == ["setup", "teardown"]


async def test_the_default_hooks_do_nothing():
    @record_transformation("ut_rec_hooks")
    async def identity(record: Any, runtime_params: Any) -> Any:
        return record

    step = identity()
    assert isinstance(step, RecordTransformation)
    await step.setup({})
    await step.teardown()


def test_a_record_transformation_needs_an_async_function():
    with pytest.raises(TypeError, match="needs an `async def`"):

        @record_transformation("ut_rec_sync")
        def not_async(record: Any, runtime_params: Any) -> Any:
            return record


@pytest.mark.parametrize(
    "kwargs, message",
    [({"concurrency": 0}, "concurrency must be >= 1"), ({"timeout": 0}, "timeout must be > 0")],
)
def test_record_transformation_rejects_bad_limits(kwargs: Dict[str, Any], message: str):
    with pytest.raises(ValueError, match=message):
        record_transformation("ut_rec_bad", **kwargs)


def test_concurrency_defaults_to_the_environment(monkeypatch: pytest.MonkeyPatch):
    @record_transformation("ut_rec_env")
    async def identity(record: Any, runtime_params: Any) -> Any:
        return record

    monkeypatch.delenv("TRANSFORM_CONCURRENCY", raising=False)
    assert identity().effective_concurrency() == 16
    monkeypatch.setenv("TRANSFORM_CONCURRENCY", "4")
    assert identity().effective_concurrency() == 4


@pytest.mark.parametrize("value, message", [("lots", "must be an integer"), ("0", "must be >= 1")])
def test_a_bad_concurrency_setting_is_rejected(
    monkeypatch: pytest.MonkeyPatch, value: str, message: str
):
    monkeypatch.setenv("TRANSFORM_CONCURRENCY", value)
    with pytest.raises(ValueError, match=message):
        default_record_concurrency()


# --------------------------------------------------------------------------------
# The caller's event loop stays free
# --------------------------------------------------------------------------------


async def _ticks_while(coro: Any) -> int:
    ticks = 0
    done = False

    async def ticker() -> None:
        nonlocal ticks
        while not done:
            ticks += 1
            await asyncio.sleep(0.01)

    task = asyncio.ensure_future(ticker())
    try:
        await coro
    finally:
        done = True
        await task
    return ticks


class _SleepySync(BaseTransformation):
    name = "ut_sync_sleepy"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        time.sleep(0.3)  # blocking, like a sync HTTP client
        return records


async def test_a_blocking_sync_transformation_does_not_stall_the_loop():
    ticks = await _ticks_while(
        apply_transformations_iteratively_async(_Pipeline([_SleepySync()]), _records(1), {})
    )
    assert ticks >= 10  # a stalled loop would tick once


async def test_a_blocking_fetch_does_not_stall_the_loop():
    class _SleepySource:
        def fetch(self, runtime_params: Dict[str, Any]) -> List[Any]:
            time.sleep(0.3)
            return _records(1)

    ticks = await _ticks_while(run_job_records_async(_SleepySource(), _Pipeline([]), {}))
    assert ticks >= 10


def test_the_sync_job_runner_runs_async_steps():
    records, transformed, applied, destination = run_job_records(
        StaticSource(_records(2)), _Pipeline([double()], destination="dest"), {}
    )
    assert [r["v"] for r in transformed] == [0, 2]
    assert len(records) == 2 and destination == "dest"
    assert applied[0].mode == "async"


_bound: contextvars.ContextVar[str] = contextvars.ContextVar("ut_bound", default="")


async def test_run_coroutine_sync_inside_a_running_loop_keeps_the_context():
    async def read() -> str:
        return _bound.get()

    _bound.set("job-1")
    assert run_coroutine_sync(read()) == "job-1"


def test_run_coroutine_sync_without_a_loop():
    async def answer() -> int:
        return 42

    assert run_coroutine_sync(answer()) == 42


# --------------------------------------------------------------------------------
# Worker path
# --------------------------------------------------------------------------------


class _Dest:
    def __init__(self) -> None:
        self.sent: List[Any] = []

    async def health_check(self) -> bool:
        return True

    async def send_with_retry(self, records: List[Any], params: Dict[str, Any]) -> None:
        self.sent.extend(records)

    async def close(self) -> None:
        pass


def _payload(records: List[Any]) -> Dict[str, Any]:
    return {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "ut_async_pipeline",
        "source": {"type": "StaticSource", "config": {"records": records}},
        "metadata": {"runtime_params": {}},
    }


@record_transformation("ut_rec_worker", concurrency=5)
async def enrich(record: Any, runtime_params: Any) -> Any:
    if record.get("fail"):
        raise RuntimeError("users API answered 500")
    await asyncio.sleep(0.01)
    return {**record, "user": f"user-{record['id']}"}


async def _run_worker(monkeypatch: pytest.MonkeyPatch, records: List[Any]) -> Any:
    from reflowfy.core.registry import pipeline_registry

    dest = _Dest()
    monkeypatch.setattr(
        pipeline_registry, "get", lambda name: _Pipeline([enrich(), tag()], destination=dest)
    )
    ex = WorkerExecutor(database_url="postgresql://x/y")
    saved: List[JobStats] = []

    async def save(execution_id: str, job_id: str, stats: JobStats) -> None:
        saved.append(stats)

    monkeypatch.setattr(ex, "_update_job_in_db", save)
    ok = await ex.execute_job(_payload(records))
    return ok, dest, saved[-1]


async def test_the_worker_runs_a_per_record_step_and_records_its_time(
    monkeypatch: pytest.MonkeyPatch,
):
    ok, dest, stats = await _run_worker(monkeypatch, [{"id": i} for i in range(10)])
    assert ok is True
    assert [r["user"] for r in dest.sent] == [f"user-{i}" for i in range(10)]
    assert set(stats.transformation_times) == {"ut_rec_worker", "ut_sync_tag"}
    assert stats.records_output == 10


async def test_a_failing_record_fails_the_worker_job_naming_it(monkeypatch: pytest.MonkeyPatch):
    ok, dest, stats = await _run_worker(monkeypatch, [{"id": 0}, {"id": 1, "fail": True}])
    assert ok is False
    assert dest.sent == []
    assert stats.error_type == "TransformationError"
    assert "record 1: RuntimeError: users API answered 500" in stats.error
