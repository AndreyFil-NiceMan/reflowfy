"""What a source's fetch, a pipeline hook and a record destination may be.

- ``fetch`` may return a list, a tuple, ``None`` (an empty slice) or any iterable — a
  generator works — and an ``AsyncSource`` implements ``async def fetch_async``.
  Anything else raises ``SourceContractError``. It used to be ``str()``-ed by
  ``to_json_safe`` and sent one character per record, the job reporting success.
- ``define_transformations``, ``define_destination`` and
  ``should_apply_transformation`` may be ``async def``. An async
  ``should_apply_transformation`` returning False used to be ignored (``bool`` of a
  coroutine is True). The other hooks are checked to be sync at class definition.
- ``@record_destination`` options are bound when the destination is built, like a
  transformation's. A typo used to fail every record at route time, and with
  ``on_error="skip"`` skipped them all while the job completed.
"""

import asyncio
from typing import Any, Dict, Iterator, List, Optional

import pytest

from reflowfy import AbstractPipeline, RuntimeParams, transformation
from reflowfy.core.async_utils import call_hook, call_hook_sync
from reflowfy.core.exceptions import PipelineError
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.job_retry import JobRetryConfig
from reflowfy.destinations.base import BaseDestination
from reflowfy.destinations.decorators import record_destination
from reflowfy.destinations.fan_out import fan_out
from reflowfy.destinations.kafka import kafka_destination
from reflowfy.destinations.record import RecordDestination
from reflowfy.execution.job_runner import fetch_records, run_job_records, run_job_records_async
from reflowfy.execution.transformation_runner import SKIPPED_MODE
from reflowfy.sources.base import AsyncSource, BaseSource, SourceContractError, materialize_fetch
from reflowfy.sources.mock import MockSource
from reflowfy.testing import CaptureDestination, UnmockedCallError, run_pipeline
from reflowfy.testing.cassette import Session, activate, patch_sources


# ---------------------------------------------------------------- sources


class Shaped(BaseSource):
    """A source whose fetch returns whatever ``make()`` builds."""

    def __init__(self, shape: str = "list") -> None:
        super().__init__({"shape": shape})

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:
        shape = self.config["shape"]
        rows = [{"id": 1}, {"id": 2}]
        if shape == "generator":
            return (row for row in rows)
        if shape == "tuple":
            return tuple(rows)
        if shape == "none":
            return None
        if shape == "dict":
            return {"id": 1}
        if shape == "str":
            return "id,name"
        if shape == "int":
            return 3
        return rows

    def health_check(self) -> bool:
        return True


class Yielding(BaseSource):
    """The natural way to stream rows: a generator method."""

    def __init__(self) -> None:
        super().__init__({})

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Iterator[Any]:
        for i in range(3):
            yield {"id": i}

    def health_check(self) -> bool:
        return True


class CoroutineFetch(BaseSource):
    """``async def fetch`` on a plain source: the mistake the error names."""

    def __init__(self) -> None:
        super().__init__({})

    async def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:  # type: ignore[override]
        return [{"id": 1}]

    def health_check(self) -> bool:
        return True


class Users(AsyncSource):
    """An async source; counts the calls it served."""

    calls = 0

    def __init__(self, count: int = 2, as_generator: bool = False) -> None:
        super().__init__({"count": count, "as_generator": as_generator})

    async def fetch_async(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:
        type(self).calls += 1
        await asyncio.sleep(0)
        rows = [{"user": i} for i in range(self.config["count"])]
        if self.config["as_generator"]:
            return (row for row in rows)
        return rows

    def health_check(self) -> bool:
        return True


class TestMaterializeFetch:
    @pytest.mark.parametrize(
        "shape, expected",
        [
            ("list", [{"id": 1}, {"id": 2}]),
            ("generator", [{"id": 1}, {"id": 2}]),
            ("tuple", [{"id": 1}, {"id": 2}]),
            ("none", []),
        ],
    )
    def test_accepted_shapes(self, shape: str, expected: List[Any]) -> None:
        source = Shaped(shape)
        assert materialize_fetch(source.fetch({}), source) == expected

    def test_a_list_is_returned_as_is(self) -> None:
        rows = [{"id": 1}]
        assert materialize_fetch(rows, Shaped()) is rows

    @pytest.mark.parametrize("shape, kind", [("dict", "dict"), ("str", "str"), ("int", "int")])
    def test_other_shapes_name_the_source_and_the_type(self, shape: str, kind: str) -> None:
        source = Shaped(shape)
        with pytest.raises(SourceContractError, match=f"Shaped.fetch returned {kind}; return a list"):
            materialize_fetch(source.fetch({}), source)

    def test_a_coroutine_points_at_async_source_and_is_closed(self) -> None:
        source = CoroutineFetch()
        coroutine = source.fetch({})
        with pytest.raises(SourceContractError, match="returned a coroutine.*subclasses AsyncSource"):
            materialize_fetch(coroutine, source)
        assert coroutine.cr_frame is None  # closed: no "never awaited" warning

    def test_an_awaitable_without_close_is_still_refused(self) -> None:
        class Waitable:
            def __await__(self) -> Iterator[Any]:
                yield

        with pytest.raises(SourceContractError, match="returned a coroutine"):
            materialize_fetch(Waitable(), Shaped())

    def test_a_contract_error_is_never_retried(self) -> None:
        # A TypeError: a bug in the source, not an outage.
        assert issubclass(SourceContractError, TypeError)
        assert not JobRetryConfig().should_retry(SourceContractError("x"))


class TestFetchInTheJobRunner:
    def test_a_generator_source_runs_end_to_end(self) -> None:
        records, out, _, _ = run_job_records(Yielding(), _Plain(), {})
        assert records == out == [{"id": 0}, {"id": 1}, {"id": 2}]

    def test_records_are_still_made_json_safe(self) -> None:
        import datetime

        class Dated(Shaped):
            def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:
                yield {"at": datetime.date(2026, 1, 2)}

        records, _, _, _ = run_job_records(Dated(), _Plain(), {})
        assert records == [{"at": "2026-01-02"}]

    def test_a_bad_shape_fails_the_fetch_step(self) -> None:
        with pytest.raises(PipelineError, match="source fetch .* raised SourceContractError"):
            run_job_records(Shaped("str"), _Plain(), {})

    def test_an_async_def_fetch_on_a_plain_source_fails_loudly(self) -> None:
        with pytest.raises(PipelineError, match="returned a coroutine"):
            run_job_records(CoroutineFetch(), _Plain(), {})

    async def test_an_async_source_is_awaited_on_the_loop(self) -> None:
        Users.calls = 0
        assert await fetch_records(Users(3), {}) == [{"user": 0}, {"user": 1}, {"user": 2}]
        records, _, _, _ = await run_job_records_async(Users(as_generator=True), _Plain(), {})
        assert records == [{"user": 0}, {"user": 1}]
        assert Users.calls == 2

    def test_an_async_source_keeps_a_sync_fetch(self) -> None:
        assert Users(2, as_generator=True).fetch({}) == [{"user": 0}, {"user": 1}]

    def test_async_source_is_abstract_and_not_registered(self) -> None:
        from reflowfy.sources.base import source_types

        assert "AsyncSource" not in source_types
        assert source_types["Users"] is Users


class TestFetchUnderTheTestingToolkit:
    def test_generator_and_async_sources_in_run_pipeline(self) -> None:
        for source in (Yielding(), Users(2)):
            result = run_pipeline(_Plain, source=source)
            assert result.ok, result.error
            assert len(result.sent) == len(result.jobs[0].fetched) > 0

    def test_a_bad_shape_is_the_runs_error(self) -> None:
        result = run_pipeline(_Plain, source=Shaped("str"))
        assert "SourceContractError" in str(result.error) and result.sent == []

    def test_the_jobs_own_async_source_records_are_kept(self) -> None:
        session = _recording()
        source = Users(2, as_generator=True)
        with activate(session):
            session.main_sources.add(id(source))
            assert asyncio.run(source.fetch_async({})) == [{"user": 0}, {"user": 1}]
        assert session.main_fetches[id(source)] == [{"user": 0}, {"user": 1}]
        assert session.recorded_source_calls == []

    def test_a_generator_main_source_is_kept_as_a_list(self) -> None:
        session = _recording()
        source = Yielding()
        with activate(session):
            session.main_sources.add(id(source))
            assert source.fetch({}) == [{"id": 0}, {"id": 1}, {"id": 2}]
        assert session.main_fetches[id(source)] == [{"id": 0}, {"id": 1}, {"id": 2}]

    def test_an_async_fetch_from_a_step_records_then_replays(self) -> None:
        Users.calls = 0
        recorded = _recording()
        with activate(recorded):
            first = asyncio.run(Users(2, as_generator=True).fetch_async({}))
        assert first == [{"user": 0}, {"user": 1}]
        assert [c["source"]["type"] for c in recorded.recorded_source_calls] == ["Users"]
        assert recorded.recorded_source_calls[0]["records"] == first

        replay = Session("replay", source_calls=recorded.recorded_source_calls, strict_sources=True)
        replay.in_job = True
        with activate(replay):
            assert asyncio.run(Users(2, as_generator=True).fetch_async({})) == first
            with pytest.raises(UnmockedCallError, match="a fetch on Users"):
                asyncio.run(Users(5).fetch_async({}))
        assert Users.calls == 1

    def test_a_stub_answers_an_async_fetch(self, mock_source: Any) -> None:
        Users.calls = 0
        mock_source(Users, records=[{"stubbed": True}], wrap=False)
        assert asyncio.run(Users().fetch_async({})) == [{"stubbed": True}]
        assert Users.calls == 0

    def test_outside_a_job_an_async_fetch_runs_for_real(self) -> None:
        Users.calls = 0
        with activate(Session("record")):  # not in a job: nothing recorded
            assert asyncio.run(Users(1).fetch_async({})) == [{"user": 0}]
        assert Users.calls == 1

    def test_the_sync_shim_of_an_async_source_is_one_call(self, mock_source: Any) -> None:
        # The sync fetch runs fetch_async inside it; the nested call passes through.
        patch_sources()
        assert Users(1).fetch({}) == [{"user": 0}]
        assert [c.source_type for c in mock_source.calls] == ["Users"]

    def test_patched_methods_are_restored(self) -> None:
        original_fetch = Users.__dict__.get("fetch_async")
        with activate(_recording()):
            assert Users.__dict__["fetch_async"] is not original_fetch
        assert Users.__dict__["fetch_async"] is original_fetch

    def test_a_plain_source_still_records_from_a_step(self) -> None:
        recorded = _recording()
        with activate(recorded):
            assert MockSource(data=[{"a": 1}]).fetch({}) == [{"data": {"a": 1}}]
        assert len(recorded.recorded_source_calls) == 1


def _recording() -> Session:
    session = Session("record")
    session.in_job = True
    return session


# ---------------------------------------------------------------- pipeline hooks


@transformation("contracts_mark")
def mark(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    return [{**r, "marked": True} for r in records]


class _Plain(AbstractPipeline[RuntimeParams]):
    name = "contracts_plain"

    def define_transformations(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
        return []

    def define_destination(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
        return CaptureDestination()


class AsyncHooks(AbstractPipeline[RuntimeParams]):
    """Every async-capable hook as an ``async def``; ``apply`` decides the step."""

    name = "contracts_async_hooks"
    apply = False

    async def define_transformations(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
        await asyncio.sleep(0)
        return [mark()]

    async def define_destination(self, records: List[Any], runtime_params: RuntimeParams) -> Any:
        await asyncio.sleep(0)
        return CaptureDestination()

    async def should_apply_transformation(
        self, transformation: Any, runtime_params: RuntimeParams, records: List[Any]
    ) -> bool:
        await asyncio.sleep(0)
        return self.apply


class TestAsyncHooks:
    def test_async_should_apply_returning_false_declines_the_step(self) -> None:
        pipeline = AsyncHooks()
        out = run_job_records(MockSource(data=[{"id": 1}]), pipeline, {})
        records, transformed, applied, destination = out
        assert transformed == records  # not marked
        assert [s.mode for s in applied] == [SKIPPED_MODE]
        assert isinstance(destination, CaptureDestination)

    def test_async_should_apply_returning_true_applies_it(self) -> None:
        pipeline = AsyncHooks()
        pipeline.apply = True
        _, transformed, applied, _ = run_job_records(MockSource(data=[{"id": 1}]), pipeline, {})
        assert transformed == [{"data": {"id": 1}, "marked": True}]
        assert [s.name for s in applied] == ["contracts_mark"]

    def test_run_pipeline_with_async_hooks(self) -> None:
        pipeline = AsyncHooks()
        pipeline.apply = True
        result = run_pipeline(pipeline, records=[{"id": 1}], destination="real")
        assert result.ok, result.error
        assert result.jobs[0].records == [{"data": {"id": 1}, "marked": True}]

    def test_a_frozen_step_under_an_async_define_transformations(self) -> None:
        from reflowfy.testing.runner import StepHooks, execute, private_copy
        from reflowfy.execution.job_runner import preview_runtime_params

        pipeline = AsyncHooks()
        pipeline.apply = True
        fork = private_copy(pipeline)
        flat = preview_runtime_params(fork.name, {})
        hooks = StepHooks(frozen={"contracts_mark": [[{"frozen": 1}]]})
        with activate(Session("passthrough")) as session:
            result = execute(
                fork, flat, lambda: [MockSource(data=[{"id": 1}])], session, hooks=hooks
            )
        assert result.ok, result.error
        assert result.sent == [{"frozen": 1}]

    def test_transformation_names_of_an_async_hook(self) -> None:
        assert AsyncHooks().get_transformation_names() == ["contracts_mark"]

        class AsyncIds(IdBasedPipeline[RuntimeParams]):
            name = "contracts_async_ids"

            def define_source(self, runtime_params: RuntimeParams) -> Any:
                return MockSource(data=[])

            async def define_transformations(self, records: List[Any], runtime_params: Any) -> Any:
                return [mark()]

            def define_destination(self, records: List[Any], runtime_params: Any) -> Any:
                return CaptureDestination()

        assert AsyncIds().get_transformation_names() == ["contracts_mark"]

    @pytest.mark.parametrize(
        "hook",
        [
            "define_source",
            "define_jobs",
            "define_parameters",
            "define_rate_limit",
            "on_execution_complete",
            "on_execution_failed",
            "on_execution_cancelled",
        ],
    )
    def test_sync_only_hooks_are_refused_at_class_definition(self, hook: str) -> None:
        async def body(self: Any, *args: Any) -> None:
            return None

        with pytest.raises(TypeError, match=f"Bad.{hook} must be a regular def"):
            type(
                "Bad",
                (_Plain,),
                {"name": f"contracts_bad_{hook}", hook: body},
            )

    async def test_call_hook(self) -> None:
        def sync(x: int) -> int:
            return x + 1

        async def coro(x: int) -> int:
            return x + 2

        def wrapper(x: int) -> Any:  # a sync wrapper around an async hook
            return coro(x)

        assert await call_hook(sync, 1) == 2
        assert await call_hook(coro, 1) == 3
        assert await call_hook(wrapper, 1) == 3

    def test_call_hook_sync(self) -> None:
        async def coro(x: int) -> int:
            return x * 2

        assert call_hook_sync(coro, 4) == 8
        assert call_hook_sync(lambda x: x * 3, 4) == 12


# ---------------------------------------------------------------- record destination options


@record_destination("contracts_by_region", on_error="skip")
def by_region(record: Any, runtime_params: Any, *, region: str) -> BaseDestination:
    return kafka_destination(bootstrap_servers="k:9092", topic=f"orders-{region}")


class TestRecordDestinationOptions:
    def test_a_typo_fails_when_the_destination_is_built(self) -> None:
        with pytest.raises(TypeError, match="record_destination 'contracts_by_region'.*'region'"):
            by_region(regoin="eu")

    def test_an_unknown_option_is_named(self) -> None:
        with pytest.raises(TypeError, match="unexpected keyword argument 'colour'"):
            by_region(region="eu", colour="red")

    def test_valid_options_reach_every_call(self) -> None:
        destination = by_region(region="eu")
        assert destination.options == {"region": "eu"}
        routed = asyncio.run(destination.route([{"id": 1}], {}))
        assert len(routed.records) == 1
        assert destination.destinations[0].config["topic"] == "orders-eu"

    def test_a_typo_no_longer_skips_every_record(self) -> None:
        class Routed(_Plain):
            name = "contracts_routed_typo"

            def define_destination(self, records: List[Any], runtime_params: Any) -> Any:
                return by_region(regoin="eu")

        result = run_pipeline(Routed, records=[{"id": 1}], send=False)
        assert "define_destination" in str(result.error) and "regoin" not in str(
            result.skip_reasons
        )

    def test_a_hand_written_subclass_keeps_taking_any_options(self) -> None:
        class Custom(RecordDestination):
            name = "contracts_custom"

            def destination_for_record(self, record: Any, runtime_params: Any) -> Any:
                return kafka_destination(bootstrap_servers="k:9092", topic=self.options["t"])

        assert Custom(t="x").options == {"t": "x"}
        assert fan_out(kafka_destination(bootstrap_servers="k:9092", topic="a")).options == {}
