"""Step retries (``RetryConfig``) on a source's fetch, a transformation and a destination.

Covers the engine (``core/retry.py``), each place it is wired in, the job message for a
source's retry, and what the worker records (``step_retries`` stats, the counter).
"""

import logging
from typing import Any, Dict, Iterator, List, Optional

import pytest
from prometheus_client import REGISTRY

from reflowfy import (
    AsyncSource,
    AsyncTransformation,
    BaseSource,
    BaseTransformation,
    RecordTransformation,
    RetryConfig,
    SkipJob,
    SkipRecord,
    TransformationError,
    transformation,
)
from reflowfy.core.exceptions import DestinationError, PermanentDestinationError, SourceError
from reflowfy.core.retry import (
    count_retries,
    destination_defaults,
    retry_async,
    retry_sync,
    step_defaults,
)
from reflowfy.destinations.base import BaseDestination
from reflowfy.execution.job_runner import fetch_records, plan_slices, run_job_records_async
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.elastic import elastic_source
from reflowfy.sources.static import StaticSource
from reflowfy.worker.executor import WorkerExecutor
from reflowfy.worker.job_stats import JobStats

FAST = dict(min_wait_seconds=0.0, max_wait_seconds=0.0, multiplier=0.0)


def fast(max_attempts: int = 3, **kwargs: Any) -> RetryConfig:
    return RetryConfig(max_attempts=max_attempts, **{**FAST, **kwargs})


class Flaky:
    """Raises ``error`` the first ``failures`` calls, then returns ``value``."""

    def __init__(self, failures: int, error: Exception, value: Any = "ok"):
        self.failures = failures
        self.error = error
        self.value = value
        self.calls = 0

    def __call__(self) -> Any:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return self.value


# ── RetryConfig ──────────────────────────────────────────────────────────────


class TestRetryConfig:
    def test_defaults_unchanged_for_destinations(self):
        config = RetryConfig()
        assert (config.max_attempts, config.min_wait_seconds) == (3, 1.0)
        assert (config.max_wait_seconds, config.multiplier) == (60.0, 2.0)
        assert config.retry_on is None and config.never_retry is None

    @pytest.mark.parametrize(
        "kwargs, error",
        [
            ({"max_attempts": 0}, ValueError),
            ({"max_attempts": True}, TypeError),
            ({"max_attempts": 2.0}, TypeError),
            ({"min_wait_seconds": -1}, ValueError),
            ({"multiplier": -1}, ValueError),
            ({"retry_on": ("x",)}, TypeError),
            ({"never_retry": 3}, TypeError),
        ],
    )
    def test_rejects_bad_values(self, kwargs: Dict[str, Any], error: type):
        with pytest.raises(error):
            RetryConfig(**kwargs)

    def test_single_class_and_list_accepted(self):
        assert RetryConfig(retry_on=KeyError).retry_on == (KeyError,)
        assert RetryConfig(never_retry=[KeyError]).never_retry == (KeyError,)  # type: ignore[arg-type]

    def test_wait_exponential_between_bounds(self):
        config = RetryConfig(min_wait_seconds=1, max_wait_seconds=10, multiplier=2)
        assert [config.wait_for(n) for n in (1, 2, 3, 4)] == [2.0, 4.0, 8.0, 10.0]
        assert RetryConfig(min_wait_seconds=5, multiplier=1).wait_for(1) == 5.0

    def test_retry_after_replaces_backoff_capped(self):
        config = RetryConfig(max_wait_seconds=10)
        error = DestinationError("api", "busy")
        error.retry_after = 3  # type: ignore[attr-defined]
        assert config.wait_for(1, error) == 3.0
        error.retry_after = 99  # type: ignore[attr-defined]
        assert config.wait_for(1, error) == 10.0

    def test_step_defaults(self):
        config = RetryConfig()
        defaults = step_defaults()
        assert config.should_retry(SourceError("s", "down", None), defaults)
        assert config.should_retry(TimeoutError(), defaults)
        assert config.should_retry(ConnectionError(), defaults)
        assert not config.should_retry(KeyError("x"), defaults)
        assert not config.should_retry(RuntimeError("x"), defaults)

    def test_destination_defaults(self):
        config = RetryConfig()
        defaults = destination_defaults()
        assert config.should_retry(DestinationError("d", "x"), defaults)
        assert not config.should_retry(PermanentDestinationError("d", "x"), defaults)
        assert not config.should_retry(TimeoutError(), defaults)

    def test_own_lists_and_never_retry_wins(self):
        config = RetryConfig(retry_on=(RuntimeError,), never_retry=(NotImplementedError,))
        assert config.should_retry(RuntimeError(), step_defaults())
        assert not config.should_retry(NotImplementedError(), step_defaults())
        # never_retry left None keeps the step's: a KeyError is still never retried.
        assert not RetryConfig(retry_on=(KeyError,)).should_retry(KeyError(), step_defaults())

    def test_cause_chain_matched(self):
        try:
            try:
                raise ConnectionError("reset")
            except ConnectionError as inner:
                raise RuntimeError("wrapped") from inner
        except RuntimeError as outer:
            assert RetryConfig().should_retry(outer, step_defaults())

    @pytest.mark.parametrize("error", [SkipJob("x"), SkipRecord("x")])
    def test_skips_never_retried(self, error: Exception):
        assert not RetryConfig(retry_on=(Exception,)).should_retry(error, step_defaults())

    def test_dict_round_trip(self):
        config = RetryConfig(
            max_attempts=5,
            min_wait_seconds=0.5,
            max_wait_seconds=9,
            multiplier=3,
            retry_on=(SourceError, KeyError),
            never_retry=(PermanentDestinationError,),
        )
        assert RetryConfig.from_dict(config.to_dict()) == config
        assert RetryConfig.from_dict(RetryConfig().to_dict()) == RetryConfig()

    def test_from_dict_drops_unknown_class(self):
        data = RetryConfig(retry_on=(KeyError,)).to_dict()
        data["retry_on"].append("nowhere.Missing")
        assert RetryConfig.from_dict(data).retry_on == (KeyError,)


# ── the engine ───────────────────────────────────────────────────────────────


class TestEngine:
    async def test_retries_until_success_and_counts(self):
        flaky = Flaky(2, ConnectionError("refused"))

        async def call() -> Any:
            return flaky()

        records: List[logging.LogRecord] = []
        handler = logging.Handler()
        handler.emit = records.append  # type: ignore[method-assign]
        log = logging.getLogger("reflowfy.core.retry")
        log.addHandler(handler)
        level = log.level
        log.setLevel(logging.INFO)
        try:
            with count_retries() as counts:
                assert await retry_async(call, fast(), step="s") == "ok"
        finally:
            log.removeHandler(handler)
            log.setLevel(level)
        assert flaky.calls == 3
        assert counts == {"s": 2}
        events = [r for r in records if getattr(r, "event", None) == "step_retry"]
        assert [getattr(r, "attempt") for r in events] == [1, 2]
        assert getattr(events[0], "error_type") == "ConnectionError"
        assert "refused" not in events[0].getMessage()

    async def test_gives_up_after_max_attempts(self):
        flaky = Flaky(9, TimeoutError())

        async def call() -> Any:
            return flaky()

        with pytest.raises(TimeoutError):
            await retry_async(call, fast(3), step="s")
        assert flaky.calls == 3

    async def test_not_retryable_raises_at_once(self):
        flaky = Flaky(1, KeyError("x"))

        async def call() -> Any:
            return flaky()

        with pytest.raises(KeyError):
            await retry_async(call, fast(), step="s")
        assert flaky.calls == 1

    async def test_none_or_one_attempt_calls_once(self):
        for config in (None, fast(1)):
            flaky = Flaky(1, TimeoutError())

            async def call() -> Any:
                return flaky()

            with pytest.raises(TimeoutError):
                await retry_async(call, config, step="s")
            with pytest.raises(TimeoutError):
                retry_sync(Flaky(1, TimeoutError()), config, step="s")
            assert flaky.calls == 1

    def test_sync(self):
        flaky = Flaky(1, ConnectionError())
        with count_retries() as counts:
            assert retry_sync(flaky, fast(), step="t") == "ok"
        assert counts == {"t": 1}
        with pytest.raises(KeyError):
            retry_sync(Flaky(1, KeyError()), fast(), step="t")
        with pytest.raises(ConnectionError):
            retry_sync(Flaky(5, ConnectionError()), fast(2), step="t")

    async def test_waits(self, monkeypatch: pytest.MonkeyPatch):
        waits: List[float] = []

        async def sleep(seconds: float) -> None:
            waits.append(seconds)

        monkeypatch.setattr("reflowfy.core.retry.asyncio.sleep", sleep)
        flaky = Flaky(2, ConnectionError())

        async def call() -> Any:
            return flaky()

        await retry_async(call, RetryConfig(min_wait_seconds=1, multiplier=2), step="s")
        assert waits == [2.0, 4.0]

    def test_counts_only_inside_block(self):
        retry_sync(Flaky(1, ConnectionError()), fast(), step="t")  # no block: not counted
        with count_retries({"t": 1}) as counts:
            retry_sync(Flaky(1, ConnectionError()), fast(), step="t")
        assert counts == {"t": 2}


# ── sources ──────────────────────────────────────────────────────────────────


class FlakyRetrySource(BaseSource):
    """Fails ``failures`` fetches (class-level counter: the worker rebuilds it)."""

    calls = 0

    def __init__(self, failures: int = 1, records: Optional[List[Any]] = None):
        super().__init__({"failures": failures, "records": records or [{"id": 1}]})

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:
        type(self).calls += 1
        if type(self).calls <= self.config["failures"]:
            raise SourceError("flaky", "down", None)
        return list(self.config["records"])

    def health_check(self) -> bool:
        return True


class FlakyGeneratorSource(FlakyRetrySource):
    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:
        type(self).calls += 1
        failing = type(self).calls <= self.config["failures"]

        def rows() -> Iterator[Any]:
            yield {"id": 1}
            if failing:
                raise ConnectionError("cursor lost")  # fails while being read
            yield {"id": 2}

        return rows()


class FlakyAsyncRetrySource(AsyncSource):
    calls = 0

    def __init__(self, failures: int = 1):
        super().__init__({"failures": failures})

    async def fetch_async(self, runtime_params: Dict[str, Any], limit: Optional[int] = None):
        type(self).calls += 1
        if type(self).calls <= self.config["failures"]:
            raise TimeoutError()
        return [{"id": 1}]

    def health_check(self) -> bool:
        return True


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    for cls in (FlakyRetrySource, FlakyGeneratorSource, FlakyAsyncRetrySource):
        cls.calls = 0


class TestSourceRetry:
    async def test_sync_fetch_retried(self):
        source = FlakyRetrySource(failures=2).with_retry(fast())
        with count_retries() as counts:
            assert await fetch_records(source, {}) == [{"id": 1}]
        assert FlakyRetrySource.calls == 3
        assert counts == {"source:FlakyRetrySource": 2}

    async def test_generator_failing_while_read_is_retried(self):
        source = FlakyGeneratorSource(failures=1).with_retry(fast())
        assert await fetch_records(source, {}) == [{"id": 1}, {"id": 2}]
        assert FlakyGeneratorSource.calls == 2

    async def test_async_fetch_retried(self):
        source = FlakyAsyncRetrySource(failures=1).with_retry(fast())
        assert await fetch_records(source, {}) == [{"id": 1}]
        assert FlakyAsyncRetrySource.calls == 2

    async def test_no_retry_by_default(self):
        with pytest.raises(SourceError):
            await fetch_records(FlakyRetrySource(failures=1), {})
        assert FlakyRetrySource.calls == 1

    async def test_exhausted_fails_the_job(self):
        source = FlakyRetrySource(failures=5).with_retry(fast(2))
        with pytest.raises(Exception, match="down"):
            await run_job_records_async(source, _Pipeline([]), {})
        assert FlakyRetrySource.calls == 2

    def test_with_retry_checks_type(self):
        with pytest.raises(TypeError, match="RetryConfig"):
            FlakyRetrySource().with_retry({"max_attempts": 3})  # type: ignore[arg-type]
        source = FlakyRetrySource()
        assert source.with_retry(None) is source and source.retry is None

    def test_factory_retry_kwarg(self):
        config = fast()
        source = elastic_source(url="http://es:9200", index="i", base_query={}, retry=config)
        assert source.retry is config
        assert elastic_source(url="http://es:9200", index="i", base_query={}).retry is None

    def test_split_slices_keep_retry(self):
        class Splitting(FlakyRetrySource):
            def split(self, runtime_params: Dict[str, Any]) -> Iterator[BaseSource]:
                yield Splitting(records=[{"id": 1}])  # new instances, no retry of their own
                yield Splitting(records=[{"id": 2}]).with_retry(fast(7))

        config = fast()
        slices = [s.source for s in plan_slices(Splitting().with_retry(config), {})]
        assert [s.retry for s in slices] == [config, fast(7)]

    def test_descriptor_carries_retry(self):
        config = fast(4, retry_on=(SourceError,))
        descriptor = SourceFactory.serialize(FlakyRetrySource().with_retry(config))
        assert descriptor["retry"]["max_attempts"] == 4
        rebuilt = SourceFactory.from_descriptor(descriptor)
        assert isinstance(rebuilt, FlakyRetrySource)
        assert rebuilt.retry == config

    def test_descriptor_without_retry_unchanged(self):
        descriptor = SourceFactory.serialize(FlakyRetrySource())
        assert set(descriptor) == {"type", "config"}
        assert SourceFactory.from_descriptor(descriptor).retry is None


# ── transformations ──────────────────────────────────────────────────────────


class _Pipeline:
    name = "ut_step_retry"

    def __init__(self, steps: List[Any]):
        self._steps = steps

    def define_transformations(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
        return self._steps

    def define_destination(self, records: List[Any], params: Dict[str, Any]) -> Any:
        return None


async def run_steps(steps: List[Any], records: List[Any]) -> List[Any]:
    _, out, _, _ = await run_job_records_async(StaticSource(records), _Pipeline(steps), {})
    return out


class TestTransformationRetry:
    async def test_sync_batch_step(self):
        calls = {"n": 0}

        @transformation("ut_retry_sync_batch", retry=fast())
        def step(records: Any, params: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError("down")
            return [{**r, "ok": True} for r in records]

        with count_retries() as counts:
            assert await run_steps([step], [{"id": 1}]) == [{"id": 1, "ok": True}]
        assert calls["n"] == 2
        assert counts == {"ut_retry_sync_batch": 1}

    async def test_async_batch_step(self):
        calls = {"n": 0}

        @transformation("ut_retry_async_batch", retry=fast())
        async def step(records: Any, params: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                raise TimeoutError()
            return records

        assert await run_steps([step], [{"id": 1}]) == [{"id": 1}]
        assert calls["n"] == 2

    async def test_per_record_async_retries_only_failing_record(self):
        calls: Dict[int, int] = {}

        @transformation("ut_retry_per_record_async", per_record=True, retry=fast())
        async def step(record: Any, params: Any) -> Any:
            calls[record["id"]] = calls.get(record["id"], 0) + 1
            if record["id"] == 2 and calls[2] < 3:
                raise ConnectionError()
            return record

        records = [{"id": n} for n in range(1, 4)]
        with count_retries() as counts:
            assert await run_steps([step], records) == records
        assert calls == {1: 1, 2: 3, 3: 1}
        assert counts == {"ut_retry_per_record_async": 2}

    async def test_per_record_sync(self):
        calls = {"n": 0}

        @transformation("ut_retry_per_record_sync", per_record=True, retry=fast())
        def step(record: Any, params: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                raise ConnectionError()
            return record

        assert await run_steps([step], [{"id": 1}, {"id": 2}]) == [{"id": 1}, {"id": 2}]
        assert calls["n"] == 3

    async def test_per_record_timeout_retried(self):
        import asyncio

        calls = {"n": 0}

        @transformation("ut_retry_per_record_timeout", per_record=True, timeout=0.05, retry=fast())
        async def step(record: Any, params: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                await asyncio.sleep(1)
            return record

        assert await run_steps([step], [{"id": 1}]) == [{"id": 1}]
        assert calls["n"] == 2

    async def test_on_error_skip_after_exhausted(self):
        calls = {"n": 0}

        @transformation("ut_retry_then_skip", per_record=True, on_error="skip", retry=fast(2))
        def step(record: Any, params: Any) -> Any:
            calls["n"] += 1
            raise ConnectionError()

        _, out, applied, _ = await run_job_records_async(
            StaticSource([{"id": 1}]), _Pipeline([step]), {}
        )
        assert out == [] and calls["n"] == 2
        assert applied[-1].skip_reasons == {"ConnectionError": 1}

    async def test_bug_not_retried(self):
        calls = {"n": 0}

        @transformation("ut_retry_keyerror", per_record=True, retry=fast())
        def step(record: Any, params: Any) -> Any:
            calls["n"] += 1
            return record["missing"]

        with pytest.raises(TransformationError):
            await run_steps([step], [{"id": 1}])
        assert calls["n"] == 1

    async def test_skip_record_not_retried(self):
        calls = {"n": 0}

        @transformation("ut_retry_skiprecord", per_record=True, retry=fast(retry_on=(Exception,)))
        def step(record: Any, params: Any) -> Any:
            calls["n"] += 1
            raise SkipRecord("no")

        assert await run_steps([step], [{"id": 1}]) == []
        assert calls["n"] == 1

    async def test_no_retry_by_default(self):
        calls = {"n": 0}

        @transformation("ut_retry_default_off")
        def step(records: Any, params: Any) -> Any:
            calls["n"] += 1
            raise ConnectionError()

        with pytest.raises(TransformationError):
            await run_steps([step], [{"id": 1}])
        assert calls["n"] == 1

    async def test_subclass_class_attribute(self):
        class Batch(BaseTransformation):
            name = "ut_retry_subclass_batch"
            retry = fast()
            calls = 0

            def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
                type(self).calls += 1
                if type(self).calls == 1:
                    raise ConnectionError()
                return records

        class PerRecord(RecordTransformation):
            name = "ut_retry_subclass_record"
            retry = fast()
            calls = 0

            async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Any:
                type(self).calls += 1
                if type(self).calls == 1:
                    raise ConnectionError()
                return record

        class Whole(AsyncTransformation):
            name = "ut_retry_subclass_async"
            retry = fast()
            calls = 0

            async def apply_async(self, records: List[Any], runtime_params: Dict[str, Any]):
                type(self).calls += 1
                if type(self).calls == 1:
                    raise ConnectionError()
                return records

        assert await run_steps([Batch(), PerRecord(), Whole()], [{"id": 1}]) == [{"id": 1}]
        assert (Batch.calls, PerRecord.calls, Whole.calls) == (2, 2, 2)

    def test_bad_retry_rejected(self):
        with pytest.raises(TypeError, match="RetryConfig"):
            transformation("ut_retry_bad", retry=3)  # type: ignore[call-overload]
        with pytest.raises(TypeError, match="RetryConfig"):

            class Bad(BaseTransformation):  # pyright: ignore[reportUnusedClass]
                name = "ut_retry_bad_attr"
                retry = 3  # type: ignore[assignment]

                def apply(self, records: List[Any], runtime_params: Dict[str, Any]):
                    return records

    def test_decorated_class_carries_retry(self):
        config = fast()

        @transformation("ut_retry_carried", retry=config)
        def step(records: Any, params: Any) -> Any:
            return records

        assert step.retry is config


# ── destinations ─────────────────────────────────────────────────────────────


class FlakyDestination(BaseDestination):
    def __init__(self, error: Exception, failures: int, retry_config: RetryConfig):
        super().__init__({}, retry_config)
        self.error = error
        self.failures = failures
        self.calls = 0

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error

    async def health_check(self) -> bool:
        return True


class TestDestinationRetry:
    async def test_default_retries_destination_error_and_counts(self):
        destination = FlakyDestination(DestinationError("d", "x"), 2, fast())
        with count_retries() as counts:
            await destination.send_with_retry([{"id": 1}])
        assert destination.calls == 3
        assert counts == {"destination:FlakyDestination": 2}

    async def test_permanent_not_retried(self):
        destination = FlakyDestination(PermanentDestinationError("d", "404"), 1, fast())
        with pytest.raises(PermanentDestinationError):
            await destination.send_with_retry([])
        assert destination.calls == 1

    async def test_own_retry_on(self):
        destination = FlakyDestination(OSError("disk"), 1, fast(retry_on=(OSError,)))
        await destination.send_with_retry([])
        assert destination.calls == 2
        plain = FlakyDestination(OSError("disk"), 1, fast())
        with pytest.raises(OSError):
            await plain.send_with_retry([])


# ── worker ───────────────────────────────────────────────────────────────────


class TestWorkerRecords:
    def test_stats_only_when_retried(self):
        stats = JobStats()
        assert "step_retries" not in stats.to_dict()
        stats.step_retries["source:X"] = 2
        assert stats.to_dict()["step_retries"] == {"source:X": 2}

    def test_counter(self):
        def value() -> float:
            return (
                REGISTRY.get_sample_value(
                    "reflowfy_step_retries_total",
                    {"pipeline": "ut_step_retry_metric", "step": "enrich"},
                )
                or 0.0
            )

        before = value()
        stats = JobStats()
        stats.step_retries["enrich"] = 3
        stats.success = True
        WorkerExecutor.record_job_metrics("ut_step_retry_metric", stats)
        assert value() == before + 3
