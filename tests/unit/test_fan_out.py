"""Several destinations: a list or fan_out(...) from define_destination, and a
@record_destination function that returns several (a per-record multicast).

Built on RecordDestination: every record joins each of its destinations' groups, each
group is sent and retried on its own, an optional one may fail without failing the job
(logged, ``destinations_failed_optional`` in the job's stats), and content dedup hashes
every (record, destination) pair.
"""

from typing import Any, Dict, List, Optional

import pytest

from reflowfy.destinations.base import BaseDestination
from reflowfy.destinations.decorators import record_destination
from reflowfy.destinations.fan_out import FanOut, as_destination, fan_out
from reflowfy.destinations.record import RecordDestination, RecordDestinationError
from reflowfy.execution.job_runner import ROUTE_MODE, run_job_records_async
from reflowfy.sources.static import StaticSource
from reflowfy.worker.executor import JobStats
from test_record_destination import (  # type: ignore[import-not-found]
    Sink,
    _records,
    dedup_worker,  # noqa: F401  (fixture)
    reset_sink,  # noqa: F401  (autouse fixture)
)
from test_worker_executor_paths import (  # type: ignore[import-not-found]
    _payload,
    capture_logs,
    processed,
    register,  # noqa: F401  (fixture)
    stub_worker,  # noqa: F401  (fixture)
)


class _Multi:
    """A pipeline whose define_destination returns whatever ``build()`` makes."""

    def __init__(self, name: str, build: Any):
        self.name = name
        self.build = build
        self.calls = 0

    def define_transformations(self, records: List[Any], params: Dict[str, Any]) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], params: Dict[str, Any]) -> Any:
        self.calls += 1
        return self.build()


async def _route_and_send(dest: RecordDestination, records: List[Any]) -> None:
    result = await dest.route(records, {})
    await dest.send_with_retry(result.records, {})


# ---------------------------------------------------------------- fan_out itself


class TestFanOut:
    async def test_every_record_goes_to_each_destination(self):
        dest = fan_out(Sink("a"), Sink("b"))
        records = _records("x", "y")
        await _route_and_send(dest, records)
        assert Sink.ledger == {"a": records, "b": records}
        assert len(dest.destinations) == 2 and dest.failed_optional == []

    def test_a_list_argument_is_unpacked(self):
        a, b = Sink("a"), Sink("b")
        assert [d for d, _ in fan_out([a, b]).targets()] == [a, b]
        assert [d for d, _ in fan_out((a,), b).targets()] == [a, b]

    def test_as_destination_turns_a_list_into_a_fan_out(self):
        a = Sink("a")
        assert isinstance(as_destination([a, Sink("b")]), FanOut)
        assert isinstance(as_destination((a,)), FanOut)
        assert as_destination(a) is a

    def test_bad_arguments_fail_at_construction(self):
        with pytest.raises(ValueError, match="at least one"):
            fan_out()
        with pytest.raises(TypeError, match="str is not a destination"):
            fan_out(Sink("a"), "topic")  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="FanOut is not a destination"):
            fan_out(fan_out(Sink("a")))
        with pytest.raises(ValueError, match="optional= destination must also be passed"):
            fan_out(Sink("a"), optional=[Sink("b")])

    async def test_identical_destinations_are_one_group(self):
        dest = fan_out(Sink("a"), Sink("a"))
        await _route_and_send(dest, _records("x"))
        assert len(dest.destinations) == 1 and Sink.ledger == {"a": _records("x")}

    async def test_each_destination_retries_on_its_own(self):
        Sink.fail_topics = {"b": 1}  # b fails once, then succeeds
        await _route_and_send(fan_out(Sink("a"), Sink("b")), _records("x"))
        assert Sink.send_calls == {"a": 1, "b": 2}  # a was not written twice

    async def test_a_required_destination_failing_fails_the_send(self):
        Sink.fail_topics = {"b": 9}
        with pytest.raises(Exception, match="broker down"):
            await _route_and_send(fan_out(Sink("a"), Sink("b")), _records("x"))

    async def test_an_optional_destination_failing_is_logged_and_counted(self):
        Sink.fail_topics = {"b": 9}
        b = Sink("b")
        dest = fan_out(Sink("a"), b, optional=[b])
        with capture_logs("reflowfy.destinations.record") as logs:
            await _route_and_send(dest, _records("x", "y"))
        assert Sink.ledger == {"a": _records("x", "y")}
        assert dest.failed_optional == ["Sink"]
        assert dest.is_optional(b) and not dest.is_optional(dest.destinations[0])
        assert any(
            "optional destination Sink failed, 2 records not written there" in m
            and "broker down" in m
            for m in logs
        )

    async def test_an_optional_failure_that_is_not_a_destination_error_logs_its_type(self):
        class Broken(Sink):
            async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None):
                raise RuntimeError("secret record value")

        b = Broken("b")
        dest = fan_out(Sink("a"), b, optional=[b])
        with capture_logs("reflowfy.destinations.record") as logs:
            result = await dest.route(_records("x"), {})
            await dest.send(result.records, {})  # plain send: same rule
        assert dest.failed_optional == ["Broken"]
        assert not any("secret record value" in m for m in logs)
        assert any(m.endswith(": RuntimeError") for m in logs)

    async def test_an_unhealthy_optional_destination_does_not_fail_the_health_check(self):
        Sink.unhealthy = {"b"}
        b = Sink("b")
        dest = fan_out(Sink("a"), b, optional=[b])
        await dest.route(_records("x"), {})
        with capture_logs("reflowfy.destinations.record") as logs:
            assert await dest.health_check()
        assert any("optional destination Sink is unhealthy" in m for m in logs)

        Sink.unhealthy = {"a"}
        assert not await dest.health_check()  # a required one still fails it


# ---------------------------------------------------------------- multicast per record


@record_destination("fo_multicast")
def multicast(record: Any, params: Any) -> Any:
    if record["engine"] == "both":
        return [Sink("t.a"), Sink("t.b")]
    if record["engine"] == "maybe":
        audit = Sink("t.audit")
        return fan_out(Sink("t.a"), audit, optional=[audit])
    return Sink(f"t.{record['engine']}")


class TestMulticast:
    async def test_a_record_function_may_return_several(self):
        dest = multicast()
        records = _records("both", "a", "b")
        await _route_and_send(dest, records)
        assert [r["id"] for r in Sink.ledger["t.a"]] == [1, 2]
        assert [r["id"] for r in Sink.ledger["t.b"]] == [1, 3]
        assert [d.config["topic"] for d in dest.destinations_for(records[0])] == ["t.a", "t.b"]
        assert dest.destination_for(records[0]).config["topic"] == "t.a"

    async def test_a_group_is_required_once_any_record_requires_it(self):
        Sink.fail_topics = {"t.audit": 9}
        dest = multicast()
        await _route_and_send(dest, _records("maybe", "a"))
        assert dest.failed_optional == ["Sink"] and "t.audit" not in Sink.ledger

        @record_destination("fo_audit_required")
        def audit_required(record: Any, params: Any) -> Any:
            if record["engine"] == "maybe":
                audit = Sink("t.audit")
                return fan_out(Sink("t.a"), audit, optional=[audit])
            return [Sink("t.a"), Sink("t.audit")]  # this record needs the audit

        Sink.fail_topics = {"t.audit": 9}
        with pytest.raises(Exception, match="broker down"):
            await _route_and_send(audit_required(), _records("maybe", "x"))

    @pytest.mark.parametrize("produced", [[], [Sink("a"), "x"], ["x"]])
    async def test_a_bad_list_fails_the_job(self, produced: Any):
        @record_destination("fo_bad_list")
        def bad(record: Any, params: Any) -> Any:
            return produced

        with pytest.raises(RecordDestinationError, match="returned list for record 1 of 1"):
            await bad().route(_records("x"), {})

    async def test_keep_only_narrows_a_records_destinations(self):
        dest = multicast()
        records = _records("both")
        result = await dest.route(records, {})
        b = dest.destinations_for(records[0])[1]
        dest.keep_only(records[0], [b])
        await dest.send_with_retry(result.records, {})
        assert set(Sink.ledger) == {"t.b"}


# ---------------------------------------------------------------- the job runner


class TestJobRunner:
    async def test_a_list_from_define_destination_is_routed_as_a_fan_out(self):
        pipe = _Multi("fo-runner", lambda: [Sink("a"), Sink("b")])
        records, sent, applied, dest = await run_job_records_async(
            StaticSource(_records("x", "y")), pipe, {}
        )
        assert isinstance(dest, FanOut) and len(dest.destinations) == 2
        assert (applied[-1].name, applied[-1].mode) == ("fan_out", ROUTE_MODE)
        assert sent == records


# ---------------------------------------------------------------- the worker


class TestWorker:
    async def test_an_optional_failure_completes_the_job_and_is_in_its_stats(
        self, register, stub_worker  # noqa: F811
    ):
        Sink.fail_topics = {"b": 9}

        def build() -> Any:
            b = Sink("b")
            return fan_out(Sink("a"), b, optional=[b])

        register(_Multi("fo-worker", build))
        before = processed("fo-worker", "completed")
        with capture_logs("reflowfy.destinations.record") as logs:
            ok = await stub_worker.execute_job(_payload("fo-worker", records=_records("x")))
        stats: JobStats = stub_worker.updates[-1]
        assert ok and stats.success
        assert stats.destinations == 2 and stats.destinations_failed_optional == 1
        assert stats.to_dict()["destinations_failed_optional"] == 1
        assert processed("fo-worker", "completed") == before + 1
        assert any("optional destination Sink failed" in m for m in logs)

    async def test_no_failures_reports_zero(self, register, stub_worker):  # noqa: F811
        register(_Multi("fo-worker-ok", lambda: [Sink("a"), Sink("b")]))
        assert await stub_worker.execute_job(_payload("fo-worker-ok", records=_records("x")))
        assert stub_worker.updates[-1].to_dict()["destinations_failed_optional"] == 0

    def test_other_jobs_have_no_optional_key(self):
        assert "destinations_failed_optional" not in JobStats().to_dict()

    async def test_a_required_failure_fails_the_job(self, register, stub_worker):  # noqa: F811
        Sink.fail_topics = {"b": 9}
        register(_Multi("fo-worker-fail", lambda: [Sink("a"), Sink("b")]))
        ok = await stub_worker.execute_job(_payload("fo-worker-fail", records=_records("x")))
        assert not ok and stub_worker.updates[-1].error_type == "DestinationError"


class TestWorkerDedup:
    async def _run(self, worker: Any, pipeline: str, job_id: str, records: List[Any]) -> JobStats:
        await worker.execute_job(
            _payload(pipeline, job_id=job_id, records=records, dedup_check=True)
        )
        return worker.updates[-1]

    async def test_each_record_destination_pair_is_claimed(
        self, register, dedup_worker  # noqa: F811
    ):
        build: Dict[str, Any] = {"targets": lambda: [Sink("a")]}
        register(_Multi("fo-dedup", lambda: build["targets"]()))
        first = await self._run(dedup_worker, "fo-dedup", "j1", _records("x", "y"))
        assert first.records_output == 2

        # Now to a and b: the records are already in a, so they go to b only.
        build["targets"] = lambda: [Sink("a"), Sink("b")]
        second = await self._run(dedup_worker, "fo-dedup", "j2", _records("x", "y"))
        assert second.records_output == 2 and second.records_deduplicated == 0
        assert len(Sink.ledger["a"]) == 2 and len(Sink.ledger["b"]) == 2

        third = await self._run(dedup_worker, "fo-dedup", "j3", _records("x", "y"))
        assert third.deduplicated and third.records_deduplicated == 2
        assert len(Sink.ledger["a"]) == 2 and len(Sink.ledger["b"]) == 2


def test_fan_out_is_a_record_destination_named_fan_out():
    assert issubclass(FanOut, RecordDestination) and FanOut.name == "fan_out"
    assert isinstance(fan_out(Sink("a")), BaseDestination)
