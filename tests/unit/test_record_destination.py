"""@record_destination: a destination chosen per record.

The function is called once per record and returns that record's destination;
records whose destinations have the same class and config share one. Covered:
routing and grouping, skips (SkipRecord, on_error="skip"), failures before any
send, per-group retries, health/close, the job runner's reporting step, and the
worker (per-record dedup identity, stats, skip metrics and logs).
"""

import asyncio
from typing import Any, Dict, List, Optional

import pytest
from prometheus_client import REGISTRY
from sqlalchemy.ext.asyncio import create_async_engine

import reflowfy
from reflowfy import SkipJob, SkipRecord
from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig
from reflowfy.destinations.decorators import destination_registry, record_destination
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.record import RecordDestination, RecordDestinationError
from reflowfy.execution.job_runner import ROUTE_MODE, run_job_records_async
from reflowfy.store.models import Base
from reflowfy.sources.static import StaticSource
from reflowfy.worker.executor import JobStats, WorkerExecutor
from test_worker_executor_paths import (  # type: ignore[import-not-found]
    _payload,
    capture_logs,
    processed,
    register,  # noqa: F401  (fixture)
    stub_worker,  # noqa: F401  (fixture)
)

# ---------------------------------------------------------------- a fake sink


class Sink(BaseDestination):
    """Writes to a shared ledger keyed by topic; one instance per record, like the real ones."""

    ledger: Dict[str, List[Any]] = {}
    send_calls: Dict[str, int] = {}
    fail_topics: Dict[str, int] = {}  # topic -> sends that raise before one succeeds
    unhealthy: set = set()
    built = 0
    closed: List[str] = []
    send_delay = 0.0
    in_flight = 0
    peak = 0
    order: List[str] = []  # topics in the order their send started

    def __init__(self, topic: str, password: str = "s3cret", **extra: Any):
        Sink.built += 1
        super().__init__(
            {"topic": topic, "password": password, **extra},
            RetryConfig(max_attempts=3, min_wait_seconds=0, max_wait_seconds=0),
        )

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        topic = self.config["topic"]
        Sink.send_calls[topic] = Sink.send_calls.get(topic, 0) + 1
        Sink.order.append(topic)
        Sink.in_flight += 1
        Sink.peak = max(Sink.peak, Sink.in_flight)
        try:
            if Sink.send_delay:
                await asyncio.sleep(Sink.send_delay)
        finally:
            Sink.in_flight -= 1
        if Sink.fail_topics.get(topic, 0) > 0:
            Sink.fail_topics[topic] -= 1
            raise DestinationError("sink", "broker down")
        Sink.ledger.setdefault(topic, []).extend(records)

    async def health_check(self) -> bool:
        return self.config["topic"] not in Sink.unhealthy

    async def close(self) -> None:
        Sink.closed.append(self.config["topic"])


@pytest.fixture(autouse=True)
def reset_sink():
    Sink.ledger = {}
    Sink.send_calls = {}
    Sink.fail_topics = {}
    Sink.unhealthy = set()
    Sink.built = 0
    Sink.closed = []
    Sink.send_delay = 0.0
    Sink.in_flight = 0
    Sink.peak = 0
    Sink.order = []


@record_destination("rd_by_topic")
def by_topic(record: Any, params: Any) -> Sink:
    return Sink(topic=f"{params.get('env', 'dev')}.{record['engine']}")


@record_destination("rd_by_topic_serial", concurrency=1)
def by_topic_serial(record: Any, params: Any) -> Sink:
    return Sink(topic=f"dev.{record['engine']}")


@record_destination("rd_by_topic_skip", on_error="skip")
def by_topic_skip(record: Any, params: Any) -> Sink:
    topics = {"whisper": "t.whisper", "google": "t.google"}
    if record.get("engine") == "ignored":
        raise SkipRecord("engine ignored")
    return Sink(topic=topics[record["engine"]])


def _records(*engines: str) -> List[Dict[str, Any]]:
    return [{"id": i, "engine": e} for i, e in enumerate(engines, 1)]


async def _route_and_send(dest: RecordDestination, records: List[Any], params: Any = None) -> Any:
    result = await dest.route(records, params or {})
    await dest.send_with_retry(result.records, params or {})
    return result


# ---------------------------------------------------------------- the decorator


class TestDecorator:
    def test_builds_a_registered_destination_class(self):
        assert issubclass(by_topic, RecordDestination)
        assert by_topic.name == "rd_by_topic"
        assert by_topic.on_error == "fail"
        assert destination_registry.get("rd_by_topic") is by_topic
        assert isinstance(by_topic(), BaseDestination)

    def test_is_exported_from_the_package(self):
        assert reflowfy.record_destination is record_destination
        assert reflowfy.RecordDestination is RecordDestination
        assert reflowfy.RecordDestinationError is RecordDestinationError

    def test_bad_on_error_is_rejected(self):
        with pytest.raises(ValueError, match="on_error must be 'fail' or 'skip'"):
            record_destination("x", on_error="ignore")  # type: ignore[arg-type]

    @pytest.mark.parametrize("concurrency", [0, -1, True, 1.5])
    def test_bad_concurrency_is_rejected(self, concurrency):
        with pytest.raises(ValueError, match="concurrency must be an integer >= 1"):
            record_destination("x", concurrency=concurrency)

    def test_docstring_and_module_follow_the_function(self):
        @record_destination("rd_doc")
        def documented(record: Any, params: Any) -> Sink:
            """Routes by engine."""
            return Sink("t")

        assert documented.__doc__ == "Routes by engine."
        assert documented.__module__ == __name__
        assert documented.__name__ == "documented"

    async def test_options_are_passed_to_every_call(self):
        @record_destination("rd_options")
        def with_options(record: Any, params: Any, prefix: str = "none") -> Sink:
            return Sink(f"{prefix}.{record['engine']}")

        await _route_and_send(with_options(prefix="eu"), _records("a", "b"))
        assert sorted(Sink.ledger) == ["eu.a", "eu.b"]

    def test_subclass_without_a_function_cannot_route(self):
        class Bare(RecordDestination):
            name = "bare"

        with pytest.raises(RecordDestinationError, match="record 1 of 1: NotImplementedError"):
            asyncio.run(Bare().route([{"a": 1}], {}))


# ---------------------------------------------------------------- routing


class TestRouting:
    async def test_each_record_goes_to_its_own_topic(self):
        dest = by_topic()
        records = _records("whisper", "google", "whisper", "azure", "google")
        result = await _route_and_send(dest, records, {"env": "prod"})

        assert result.records == records and result.skip_reasons == {}
        assert [r["id"] for r in Sink.ledger["prod.whisper"]] == [1, 3]
        assert [r["id"] for r in Sink.ledger["prod.google"]] == [2, 5]
        assert [r["id"] for r in Sink.ledger["prod.azure"]] == [4]

    async def test_records_with_the_same_destination_share_one(self):
        dest = by_topic()
        await dest.route(_records(*["whisper"] * 100, *["google"] * 100), {})

        assert Sink.built == 200  # the function ran per record...
        assert len(dest.destinations) == 2  # ...but only two destinations are used
        await dest.send_with_retry(_records("x"), {})  # an unrouted record routes itself
        assert Sink.send_calls == {"dev.x": 1}

    async def test_one_send_per_destination(self):
        dest = by_topic()
        await _route_and_send(dest, _records(*["a", "b"] * 50))
        assert Sink.send_calls == {"dev.a": 1, "dev.b": 1}
        assert len(Sink.ledger["dev.a"]) == 50

    async def test_destinations_differing_in_credentials_are_kept_apart(self):
        @record_destination("rd_creds")
        def creds(record: Any, params: Any) -> Sink:
            return Sink("same", password=record["pw"])

        dest = creds()
        await dest.route([{"pw": "a"}, {"pw": "b"}, {"pw": "a"}], {})
        assert len(dest.destinations) == 2

    async def test_real_kafka_destinations_group_by_config(self):
        @record_destination("rd_kafka")
        def kafka(record: Any, params: Any) -> BaseDestination:
            return kafka_destination(bootstrap_servers="k:9092", topic=f"t.{record['engine']}")

        dest = kafka()
        await dest.route(_records("a", "b", "a"), {})
        assert [d.config["topic"] for d in dest.destinations] == ["t.a", "t.b"]

    async def test_async_function_routes_concurrently_in_order(self):
        in_flight = 0
        peak = 0

        @record_destination("rd_async", concurrency=4)
        async def lookup(record: Any, params: Any) -> Sink:
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            return Sink(f"t{record['id'] % 3}")

        dest = lookup()
        records = [{"id": i} for i in range(20)]
        result = await dest.route(records, {})
        assert result.records == records
        assert peak == 4
        await dest.send_with_retry(result.records, {})
        assert [r["id"] for r in Sink.ledger["t0"]] == [0, 3, 6, 9, 12, 15, 18]

    async def test_sync_function_runs_off_the_event_loop(self):
        loops: List[bool] = []

        @record_destination("rd_thread")
        def where(record: Any, params: Any) -> Sink:
            try:
                asyncio.get_running_loop()
                loops.append(True)
            except RuntimeError:
                loops.append(False)
            return Sink("t")

        await where().route([{"a": 1}], {})
        assert loops == [False]

    async def test_destination_for_an_unrouted_record_raises(self):
        with pytest.raises(KeyError, match="record was not routed"):
            by_topic().destination_for({"engine": "a"})

    async def test_routing_logs_the_destination_count(self):
        with capture_logs("reflowfy.destinations.record") as logs:
            await by_topic().route(_records("a", "b", "a"), {})
        assert "rd_by_topic routed 3 records to 2 destinations" in logs

    def test_repr_names_the_destination_never_its_credentials(self):
        dest = by_topic()
        assert repr(dest) == "by_topic('rd_by_topic')"
        asyncio.run(dest.route(_records("a"), {}))
        assert repr(dest) == "by_topic('rd_by_topic', 1 destinations (Sink))"
        assert "s3cret" not in repr(dest)

    def test_identity_config_names_the_record_destination(self):
        assert by_topic().identity_config() == {"record_destination": "rd_by_topic"}


# ---------------------------------------------------------------- skips and failures


class TestSkipsAndFailures:
    async def test_skip_record_drops_that_record_with_its_reason(self):
        dest = by_topic_skip()
        records = _records("whisper", "ignored", "google", "ignored")
        result = await _route_and_send(dest, records)
        assert [r["id"] for r in result.records] == [1, 3]
        assert result.skip_reasons == {"engine ignored": 2}
        assert sorted(Sink.ledger) == ["t.google", "t.whisper"]

    async def test_on_error_skip_uses_the_exception_type_never_the_message(self):
        dest = by_topic_skip()
        result = await _route_and_send(dest, _records("whisper", "secret-engine", "nope"))
        assert [r["id"] for r in result.records] == [1]
        assert result.skip_reasons == {"KeyError": 2}

    async def test_skip_record_works_with_on_error_fail_too(self):
        @record_destination("rd_skip_fail")
        def strict(record: Any, params: Any) -> Sink:
            if record["engine"] == "x":
                raise SkipRecord("x unsupported")
            return Sink("t")

        result = await strict().route(_records("a", "x"), {})
        assert result.skip_reasons == {"x unsupported": 1}

    async def test_on_error_fail_fails_naming_the_position_and_nothing_is_sent(self):
        @record_destination("rd_strict")
        def strict(record: Any, params: Any) -> Sink:
            return Sink({"whisper": "t.whisper"}[record["engine"]])

        with pytest.raises(RecordDestinationError) as excinfo:
            await strict().route(_records("whisper", "whisper", "secret-engine"), {})
        msg = str(excinfo.value)
        assert msg == "record_destination 'rd_strict' failed on record 3 of 3: KeyError"
        assert "secret-engine" not in msg
        assert isinstance(excinfo.value.original_error, KeyError)
        assert Sink.ledger == {}

    async def test_async_failure_fails_too(self):
        @record_destination("rd_async_fail", concurrency=2)
        async def boom(record: Any, params: Any) -> Sink:
            if record["id"] == 2:
                raise ValueError("bad")
            return Sink("t")

        with pytest.raises(RecordDestinationError, match="record 2 of 3: ValueError"):
            await boom().route([{"id": 1}, {"id": 2}, {"id": 3}], {})

    @pytest.mark.parametrize("on_error", ["fail", "skip"])
    async def test_a_return_that_is_not_a_destination_always_fails(self, on_error):
        @record_destination(f"rd_bad_return_{on_error}", on_error=on_error)
        def bad(record: Any, params: Any) -> Any:
            return None

        with pytest.raises(RecordDestinationError, match="returned NoneType for record 1 of 1"):
            await bad().route([{"a": 1}], {})

    async def test_returning_a_record_destination_fails(self):
        @record_destination("rd_nested")
        def nested(record: Any, params: Any) -> Any:
            return by_topic()

        with pytest.raises(RecordDestinationError, match="returned by_topic"):
            await nested().route([{"a": 1}], {})

    @pytest.mark.parametrize("on_error", ["fail", "skip"])
    async def test_skip_job_drops_the_job(self, on_error):
        @record_destination(f"rd_skipjob_{on_error}", on_error=on_error)
        def drop(record: Any, params: Any) -> Sink:
            raise SkipJob("not today")

        with pytest.raises(SkipJob, match="not today"):
            await drop().route([{"a": 1}], {})

    async def test_no_records_routes_nothing_and_does_not_warn(self):
        with capture_logs("reflowfy.destinations.record") as logs:
            result = await by_topic().route([], {})
        assert result == ([], {}) and logs == []

    async def test_every_record_skipped_warns(self):
        with capture_logs("reflowfy.destinations.record") as logs:
            result = await by_topic_skip().route(_records("nope", "ignored"), {})
        assert result.records == []
        assert "All 2 records skipped by rd_by_topic_skip — nothing to send" in logs

    async def test_skips_at_send_time_are_logged(self):
        dest = by_topic_skip()
        with capture_logs("reflowfy.destinations.record") as logs:
            await dest.send(_records("whisper", "nope"), {})
        assert "rd_by_topic_skip skipped 1 records at send time" in logs
        assert [r["id"] for r in Sink.ledger["t.whisper"]] == [1]


# ---------------------------------------------------------------- sending, health, close


class TestSendHealthClose:
    async def test_each_destination_retries_on_its_own(self):
        Sink.fail_topics = {"dev.b": 2}  # b fails twice, then succeeds
        dest = by_topic()
        await _route_and_send(dest, _records("a", "b"))
        assert Sink.send_calls == {"dev.a": 1, "dev.b": 3}  # a was not re-sent
        assert len(Sink.ledger["dev.a"]) == 1

    async def test_a_destination_failing_after_its_retries_fails_the_send(self):
        Sink.fail_topics = {"dev.b": 99}
        dest = by_topic_serial()
        await dest.route(_records("a", "b", "c"), {})
        with pytest.raises(DestinationError):
            await dest.send_with_retry(_records("a", "b", "c"), {})  # unrouted copies: route
        assert "dev.c" not in Sink.ledger  # stopped at the failing destination

    async def test_send_without_retry(self):
        Sink.fail_topics = {"dev.a": 1}
        dest = by_topic()
        result = await dest.route(_records("a"), {})
        with pytest.raises(DestinationError):
            await dest.send(result.records, {})
        assert Sink.send_calls == {"dev.a": 1}

    async def test_health_check_needs_every_destination_healthy(self):
        dest = by_topic()
        assert await dest.health_check()  # nothing routed
        await dest.route(_records("a", "b"), {})
        assert await dest.health_check()
        Sink.unhealthy = {"dev.b"}
        assert not await dest.health_check()

    async def test_close_closes_every_destination_and_raises_the_first_error(self):
        class Fragile(Sink):
            async def close(self) -> None:
                await super().close()
                raise RuntimeError(f"close {self.config['topic']}")

        @record_destination("rd_fragile")
        def fragile(record: Any, params: Any) -> Sink:
            return Fragile(record["engine"])

        dest = fragile()
        await dest.route(_records("a", "b"), {})
        with pytest.raises(RuntimeError, match="close a"):
            await dest.close()
        assert sorted(Sink.closed) == ["a", "b"]

    async def test_close_is_quiet_when_all_close(self):
        dest = by_topic()
        await dest.route(_records("a", "b"), {})
        await dest.close()
        assert sorted(Sink.closed) == ["dev.a", "dev.b"]


# ---------------------------------------------------------------- concurrency and timeout


class TestConcurrency:
    async def test_destinations_are_sent_to_concurrently_by_default(self, monkeypatch):
        monkeypatch.delenv("TRANSFORM_CONCURRENCY", raising=False)
        Sink.send_delay = 0.02
        dest = by_topic()
        assert dest.effective_concurrency() == 16
        await _route_and_send(dest, _records(*"abcde"))
        assert Sink.peak == 5
        assert {t: len(r) for t, r in Sink.ledger.items()} == {f"dev.{c}": 1 for c in "abcde"}

    async def test_concurrency_caps_destinations_in_flight(self):
        @record_destination("rd_conc_two", concurrency=2)
        def two(record: Any, params: Any) -> Sink:
            return Sink(record["engine"])

        Sink.send_delay = 0.02
        await _route_and_send(two(), _records(*"abcde"))
        assert Sink.peak == 2
        assert sorted(Sink.ledger) == list("abcde")

    async def test_the_env_default_applies(self, monkeypatch):
        monkeypatch.setenv("TRANSFORM_CONCURRENCY", "3")
        Sink.send_delay = 0.02
        dest = by_topic()
        assert dest.effective_concurrency() == 3
        await _route_and_send(dest, _records(*"abcde"))
        assert Sink.peak == 3

    async def test_concurrency_one_sends_in_order_one_at_a_time(self):
        Sink.send_delay = 0.01
        await _route_and_send(by_topic_serial(), _records("c", "a", "b", "a"))
        assert Sink.peak == 1
        assert Sink.order == ["dev.c", "dev.a", "dev.b"]  # first-seen order
        assert [r["id"] for r in Sink.ledger["dev.a"]] == [2, 4]  # order within a group

    async def test_plain_send_is_concurrent_too(self):
        Sink.send_delay = 0.02
        dest = by_topic()
        result = await dest.route(_records(*"abc"), {})
        await dest.send(result.records, {})
        assert Sink.peak == 3

    async def test_a_failure_cancels_the_sends_still_in_flight(self):
        class Slow(Sink):
            async def send(self, records: List[Any], metadata: Any = None) -> None:
                if self.config["topic"] == "boom":
                    raise DestinationError("sink", "down")
                await asyncio.sleep(5)  # cancelled long before this ends
                await super().send(records, metadata)

        @record_destination("rd_cancel")
        def cancel(record: Any, params: Any) -> Sink:
            return Slow(record["engine"])

        dest = cancel()
        result = await dest.route(_records("slow1", "boom", "slow2"), {})
        with pytest.raises(DestinationError):
            await asyncio.wait_for(dest.send(result.records, {}), timeout=2)
        assert Sink.ledger == {}

    async def test_health_checks_run_concurrently(self):
        in_flight = 0
        peak = 0

        class SlowHealth(Sink):
            async def health_check(self) -> bool:
                nonlocal in_flight, peak
                in_flight += 1
                peak = max(peak, in_flight)
                await asyncio.sleep(0.02)
                in_flight -= 1
                return self.config["topic"] != "sick"

        @record_destination("rd_health_conc")
        def slow_health(record: Any, params: Any) -> Sink:
            return SlowHealth(record["engine"])

        dest = slow_health()
        await dest.route(_records("a", "b", "c"), {})
        assert await dest.health_check() and peak == 3
        await dest.route(_records("sick"), {})
        assert not await dest.health_check()


class TestTimeout:
    @staticmethod
    def _slow(name: str, **kw: Any) -> Any:
        @record_destination(name, timeout=0.05, **kw)
        async def slow(record: Any, params: Any) -> Sink:
            if record["engine"] == "slow":
                await asyncio.sleep(5)
            return Sink(record["engine"])

        return slow

    def test_timeout_is_on_the_class(self):
        assert self._slow("rd_timeout_attr").timeout == 0.05
        assert by_topic.timeout is None

    async def test_a_slow_record_fails_the_job(self):
        with pytest.raises(RecordDestinationError) as excinfo:
            await self._slow("rd_timeout_fail")().route(_records("a", "slow"), {})
        assert str(excinfo.value) == (
            "record_destination 'rd_timeout_fail' failed on record 2 of 2: TimeoutError"
        )
        assert "timed out after 0.05s" in str(excinfo.value.original_error)

    async def test_a_slow_record_is_skipped_with_on_error_skip(self):
        dest = self._slow("rd_timeout_skip", on_error="skip")()
        result = await dest.route(_records("a", "slow", "b"), {})
        assert [r["id"] for r in result.records] == [1, 3]
        assert result.skip_reasons == {"TimeoutError": 1}

    def test_timeout_needs_an_async_function(self):
        with pytest.raises(TypeError, match="timeout needs an `async def` function"):

            @record_destination("rd_timeout_sync", timeout=1)
            def sync(record: Any, params: Any) -> Sink:
                return Sink("t")

    @pytest.mark.parametrize("timeout", [0, -1])
    def test_bad_timeout_is_rejected(self, timeout):
        with pytest.raises(ValueError, match="timeout must be > 0"):
            record_destination("x", timeout=timeout)


# ---------------------------------------------------------------- the job runner


class _Pipeline:
    def __init__(self, name: str, dest_cls: Any, transformations: Optional[List[Any]] = None):
        self.name = name
        self.dest_cls = dest_cls
        self.transformations = transformations or []
        self.calls = 0

    def define_transformations(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
        return list(self.transformations)

    def define_destination(self, records: List[Any], params: Dict[str, Any]) -> Any:
        self.calls += 1
        return self.dest_cls()


class TestJobRunner:
    async def test_routing_is_reported_as_a_step_with_its_skips(self):
        source = StaticSource(_records("whisper", "nope", "ignored", "google"))
        records, sent, applied, dest = await run_job_records_async(
            source, _Pipeline("rd-runner", by_topic_skip), {}
        )
        assert len(records) == 4
        assert [r["id"] for r in sent] == [1, 4]
        assert isinstance(dest, RecordDestination)
        step = applied[-1]
        assert (step.name, step.mode, step.records_in, step.records_out) == (
            "rd_by_topic_skip",
            ROUTE_MODE,
            4,
            2,
        )
        assert step.skip_reasons == {"KeyError": 1, "engine ignored": 1}
        assert step.concurrency is None  # a sync function

    async def test_async_function_reports_its_concurrency(self, monkeypatch):
        monkeypatch.setenv("TRANSFORM_CONCURRENCY", "7")

        @record_destination("rd_async_conc")
        async def route(record: Any, params: Any) -> Sink:
            return Sink("t")

        _, _, applied, _ = await run_job_records_async(
            StaticSource([{"a": 1}]), _Pipeline("rd-conc", route), {}
        )
        assert applied[-1].concurrency == 7

    async def test_nothing_skipped_leaves_skip_reasons_unset(self):
        _, _, applied, _ = await run_job_records_async(
            StaticSource(_records("a")), _Pipeline("rd-clean", by_topic), {}
        )
        assert applied[-1].skip_reasons is None

    async def test_skip_job_from_routing_drops_the_job(self):
        @record_destination("rd_runner_skipjob")
        def drop(record: Any, params: Any) -> Sink:
            raise SkipJob("engine offline")

        params: Dict[str, Any] = {}
        result = await run_job_records_async(
            StaticSource(_records("a")), _Pipeline("rd-skipjob", drop), params
        )
        assert result == ([], [], [], None)
        assert params["skip_reason"] == "engine offline"


# ---------------------------------------------------------------- the worker


def skipped_records(pipeline: str, step: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "reflowfy_records_skipped_total", {"pipeline": pipeline, "transformation": step}
        )
        or 0.0
    )


class TestWorker:
    async def test_routes_sends_and_reports(self, register, stub_worker):  # noqa: F811
        register(_Pipeline("rd-worker", by_topic_skip))
        before_skips = skipped_records("rd-worker", "rd_by_topic_skip")
        before_done = processed("rd-worker", "completed")

        with capture_logs() as logs:
            ok = await stub_worker.execute_job(
                _payload("rd-worker", records=_records("whisper", "nope", "google", "whisper"))
            )

        assert ok
        stats: JobStats = stub_worker.updates[-1]
        assert stats.success and stats.records_output == 3
        assert stats.destinations == 2
        assert stats.records_skipped == 1 and stats.record_skip_reasons == {"KeyError": 1}
        assert stats.to_dict()["destinations"] == 2
        assert "rd_by_topic_skip" in stats.transformation_times
        assert "rd_by_topic_skip skipped 1 records ('KeyError' x1)" in logs
        assert skipped_records("rd-worker", "rd_by_topic_skip") == before_skips + 1
        assert processed("rd-worker", "completed") == before_done + 1
        assert [r["id"] for r in Sink.ledger["t.whisper"]] == [1, 4]
        assert sorted(Sink.closed) == ["t.google", "t.whisper"]

    async def test_other_jobs_stats_have_no_destinations_key(self):
        assert "destinations" not in JobStats().to_dict()

    async def test_a_routing_failure_fails_the_job_with_nothing_sent(
        self, register, stub_worker  # noqa: F811
    ):
        @record_destination("rd_worker_strict")
        def strict(record: Any, params: Any) -> Sink:
            return Sink({"a": "t.a"}[record["engine"]])

        register(_Pipeline("rd-worker-fail", strict))
        ok = await stub_worker.execute_job(_payload("rd-worker-fail", records=_records("a", "b")))
        assert not ok
        stats = stub_worker.updates[-1]
        assert stats.error_type == "RecordDestinationError"
        assert stats.error == "record_destination 'rd_worker_strict' failed on record 2 of 2: KeyError"
        assert Sink.ledger == {}

    async def test_an_unhealthy_destination_fails_the_job(self, register, stub_worker):  # noqa: F811
        Sink.unhealthy = {"dev.b"}
        register(_Pipeline("rd-worker-unhealthy", by_topic))
        ok = await stub_worker.execute_job(
            _payload("rd-worker-unhealthy", records=_records("a", "b"))
        )
        assert not ok and stub_worker.updates[-1].error == "Destination health check failed"
        assert Sink.ledger == {}

    async def test_every_record_skipped_completes_with_nothing_sent(
        self, register, stub_worker  # noqa: F811
    ):
        register(_Pipeline("rd-worker-allskip", by_topic_skip))
        ok = await stub_worker.execute_job(
            _payload("rd-worker-allskip", records=_records("nope", "ignored"))
        )
        stats = stub_worker.updates[-1]
        assert ok and stats.success and stats.records_output == 0 and stats.records_skipped == 2
        assert Sink.ledger == {}


@pytest.fixture
async def dedup_worker(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path}/dedup.db"
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

    w = WorkerExecutor(database_url=url)
    updates: List[JobStats] = []

    async def record(execution_id: str, job_id: str, stats: JobStats) -> None:
        updates.append(stats)

    w._update_job_in_db = record  # type: ignore[method-assign]
    w.updates = updates  # type: ignore[attr-defined]
    yield w
    await w.close()


class TestWorkerDedup:
    async def _run(self, worker: Any, pipeline: str, job_id: str, records: List[Any]) -> JobStats:
        await worker.execute_job(_payload(pipeline, job_id=job_id, records=records, dedup_check=True))
        return worker.updates[-1]

    async def test_a_record_already_sent_to_its_topic_is_dropped(
        self, register, dedup_worker  # noqa: F811
    ):
        pipe = register(_Pipeline("rd-dedup", by_topic))
        first = await self._run(dedup_worker, "rd-dedup", "a", _records("x", "y"))
        second = await self._run(dedup_worker, "rd-dedup", "b", _records("x", "y", "z"))

        assert (first.records_output, second.records_output) == (2, 1)
        assert second.records_deduplicated == 2 and second.destinations == 3
        assert {t: len(r) for t, r in Sink.ledger.items()} == {"dev.x": 1, "dev.y": 1, "dev.z": 1}
        # Routing is not repeated after dedup (its skips would count twice).
        assert pipe.calls == 2

    async def test_the_same_record_to_another_topic_is_new_work(
        self, register, dedup_worker  # noqa: F811
    ):
        register(_Pipeline("rd-dedup-topic", by_topic))
        await self._run(dedup_worker, "rd-dedup-topic", "a", [{"id": 1, "engine": "x"}])

        @record_destination("rd_dedup_other_topic")
        def other(record: Any, params: Any) -> Sink:
            return Sink("elsewhere")

        register(_Pipeline("rd-dedup-topic", other))
        again = await self._run(dedup_worker, "rd-dedup-topic", "b", [{"id": 1, "engine": "x"}])
        assert again.records_output == 1 and not again.deduplicated
        assert set(Sink.ledger) == {"dev.x", "elsewhere"}

    async def test_records_sharing_a_destination_are_hashed_against_it(
        self, register, dedup_worker  # noqa: F811
    ):
        register(_Pipeline("rd-dedup-shared", by_topic))
        await self._run(dedup_worker, "rd-dedup-shared", "a", _records("x", "x"))
        again = await self._run(dedup_worker, "rd-dedup-shared", "b", _records("x", "x", "y"))
        assert again.records_deduplicated == 2 and again.records_output == 1
        assert {t: len(r) for t, r in Sink.ledger.items()} == {"dev.x": 2, "dev.y": 1}

    async def test_a_fully_deduplicated_job_is_deduplicated(
        self, register, dedup_worker  # noqa: F811
    ):
        register(_Pipeline("rd-dedup-full", by_topic))
        await self._run(dedup_worker, "rd-dedup-full", "a", _records("x"))
        again = await self._run(dedup_worker, "rd-dedup-full", "b", _records("x"))
        assert again.deduplicated and again.records_output == 0
