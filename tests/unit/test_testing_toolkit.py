"""reflowfy.testing: run_pipeline, the transformation helpers and the assertions."""

import asyncio
import logging
from typing import Any, Dict, Iterator, List, Literal

import httpx
import pytest
from typing_extensions import Annotated, NotRequired, TypedDict

from reflowfy import (
    AbstractPipeline,
    BaseTransformation,
    IdBasedPipeline,
    Param,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    TransformationError,
    TypedRecord,
    api_destination,
    job,
    pipeline_registry,
    record_transformation,
    transformation,
)
from reflowfy.destinations.base import DestinationError
from reflowfy.sources.mock import MockSource
from reflowfy.sources.static import StaticSource
from reflowfy.testing import (
    CaptureDestination,
    UnmockedCallError,
    apply_transformation,
    assert_job_dropped,
    assert_ok,
    assert_sent,
    assert_skipped,
    assert_valid_records,
    make_params,
    make_records,
    mock_network,
    record,
    run_pipeline,
)
from testing_support import isolated_registry


@pytest.fixture(autouse=True)
def registry() -> Iterator[None]:
    with isolated_registry():
        yield


class OrderParams(RuntimeParams, total=False):
    region: Annotated[NotRequired[Literal["eu", "us"]], Param(default="eu")]


@transformation("tk_add_tax")
def add_tax(records: List[Any], params: Dict[str, Any]) -> List[Any]:
    rate = 0.2 if params.get("region", "eu") == "eu" else 0.1
    return [{**r, "tax": round(r["data"]["amount"] * rate, 2)} for r in records]


@record_transformation("tk_drop_small")
async def drop_small(rec: Any, params: Dict[str, Any]) -> Any:
    if rec["data"]["amount"] < 1:
        raise SkipRecord("too small")
    return None if rec["data"]["amount"] > 1000 else rec


class Orders(AbstractPipeline[OrderParams]):
    name = "tk_orders"

    def define_source(self, runtime_params: OrderParams) -> Any:
        return MockSource(data=[{"id": i, "amount": 10.0 * i} for i in range(1, 6)], batch_size=2)

    def define_transformations(self, records: List[Any], runtime_params: OrderParams) -> Any:
        return [drop_small(), add_tax()]

    def define_destination(self, records: List[Any], runtime_params: OrderParams) -> Any:
        return api_destination(url="https://billing.internal/orders", per_record=True)


class TestRunPipeline:
    def test_records_replace_the_source_and_nothing_real_is_sent(self):
        result = run_pipeline(
            "tk_orders",
            params={"region": "us"},
            records=[{"id": 1, "amount": 10.0}, {"id": 2, "amount": 0.5}, {"id": 3, "amount": 5000}],
        )
        assert result.ok
        assert result.sent == [{"data": {"id": 1, "amount": 10.0}, "tax": 1.0}]
        assert result.records == result.sent
        assert result.fetched == 3
        assert result.skip_reasons == {"too small": 1}
        assert result.records_skipped == 1
        assert [s.name for s in result.steps] == ["tk_drop_small", "tk_add_tax"]
        assert result.destinations[0].config["url"] == "https://billing.internal/orders"
        assert result.params["region"] == "us" and result.params["pipeline_name"] == "tk_orders"
        assert "ok" in repr(result) and "1 sent" in repr(result)
        assert result.raise_for_error() is result

    def test_the_pipelines_own_source_plans_the_jobs(self):
        result = run_pipeline(Orders)
        assert [len(j.fetched) for j in result.jobs] == [2, 2, 1]
        assert len(result.sent) == 5 and all(j.sent for j in result.jobs)

    def test_limit_stops_fetching(self):
        result = run_pipeline("tk_orders", limit=3)
        assert result.fetched == 3 and len(result.jobs) == 2

    def test_jobs_with_their_own_params(self):
        seen: List[Any] = []

        class Tagged(AbstractPipeline[RuntimeParams]):
            name = "tk_tagged"

            def define_source(self, runtime_params):
                raise AssertionError("the source must not be built")

            def define_transformations(self, records, runtime_params):
                seen.append(runtime_params.get("tenant"))
                return []

            def define_destination(self, records, runtime_params):
                return CaptureDestination()

        result = run_pipeline(
            Tagged, jobs=[job([{"data": 1}], tenant="a"), [{"n": 2}], []], wrap=True
        )
        assert seen == ["a", None]
        assert [j.records for j in result.jobs] == [[{"data": 1}], [{"data": {"n": 2}}]]
        assert result.jobs[0].job_params == {"tenant": "a"}

    def test_wrap_false_uses_records_as_they_are(self):
        result = run_pipeline(Orders, records=[{"data": {"id": 1, "amount": 2.0}}], wrap=False)
        assert result.sent[0]["data"] == {"id": 1, "amount": 2.0}

    def test_an_explicit_source(self):
        result = run_pipeline(Orders, source=StaticSource([{"data": {"id": 7, "amount": 1.0}}]))
        assert [r["data"]["id"] for r in result.sent] == [7]

    def test_only_one_input_may_be_given(self):
        with pytest.raises(ValueError, match="only one of records, jobs and source"):
            run_pipeline(Orders, records=[], jobs=[])

    def test_invalid_params_raise(self):
        with pytest.raises(ValueError, match="region"):
            run_pipeline("tk_orders", params={"region": "mars"}, records=[])

    def test_unknown_pipeline_name(self, monkeypatch):
        monkeypatch.setattr(
            "reflowfy.core.pipeline_discovery.discover_and_load_pipelines", lambda *a, **k: 0
        )
        with pytest.raises(LookupError, match="No pipeline named 'tk_nope'.*tk_orders"):
            run_pipeline("tk_nope")

    def test_a_name_triggers_discovery_once_it_is_missing(self, monkeypatch):
        def discover(*args: Any, **kwargs: Any) -> int:
            class Late(Orders):
                name = "tk_late"

            return 1

        monkeypatch.setattr(
            "reflowfy.core.pipeline_discovery.discover_and_load_pipelines", discover
        )
        assert run_pipeline("tk_late", records=[]).ok

    def test_an_unregistered_class_and_an_instance(self):
        class Plain(Orders):
            pass  # no `name` of its own in the class body: not registered separately

        pipeline_registry.unregister("tk_orders")
        assert run_pipeline(Plain, records=[{"id": 1, "amount": 3.0}]).ok
        assert run_pipeline(Plain(), records=[{"id": 1, "amount": 3.0}]).ok

    def test_the_registered_instance_is_left_alone(self):
        registered = pipeline_registry.get("tk_orders")
        assert registered is not None
        before = dict(vars(registered))
        run_pipeline("tk_orders", records=[{"id": 1, "amount": 3.0}])
        assert vars(registered) == before

    def test_send_false_stops_before_the_destination(self):
        result = run_pipeline(Orders, records=[{"id": 1, "amount": 3.0}], send=False)
        assert result.sent == [] and len(result.records) == 1
        assert result.destinations != []

    def test_a_destination_of_your_own_and_a_retried_failure(self, capture_destination):
        flaky = CaptureDestination(fail_times=1, error=DestinationError("x", "down"))
        result = run_pipeline(Orders, records=[{"id": 1, "amount": 3.0}], destination=flaky)
        assert result.ok and flaky.attempts == 2
        assert flaky.records == result.sent
        assert flaky.sends[0].metadata["pipeline_name"] == "tk_orders"
        assert asyncio.run(capture_destination.health_check()) is True

    def test_a_failing_destination_fails_the_run(self):
        broken = CaptureDestination(fail_times=5)
        result = run_pipeline(Orders, records=[{"id": 1, "amount": 3.0}], destination=broken)
        assert not result.ok and "told to fail" in str(result.error)
        assert result.sent == []
        with pytest.raises(RuntimeError):
            result.raise_for_error()
        with pytest.raises(AssertionError, match="tk_orders.*failed"):
            assert_ok(result)
        assert "failed: RuntimeError" in repr(result)

    def test_destination_real_sends_to_what_the_pipeline_defined(self, mock_http):
        mock_http.post("https://billing.internal/orders", json={"ok": True})
        result = run_pipeline(Orders, records=[{"id": 1, "amount": 3.0}], destination="real")
        assert result.ok
        assert mock_http.calls[0].json()["data"]["id"] == 1

    def test_a_failing_step_is_on_the_result_with_the_steps_before_it(self):
        result = run_pipeline(Orders, records=[{"id": 1}])  # no amount
        assert isinstance(result.error, TransformationError)
        assert result.error.transformation_name == "tk_drop_small"
        assert result.jobs[0].fetched == [{"data": {"id": 1}}]
        assert result.jobs[0].steps == []

    def test_a_source_that_is_not_a_base_source(self):
        class Duck:
            def fetch(self, runtime_params):
                return [{"data": {"id": 1}}, {"data": {"id": 2, "amount": 4.0}}]

        result = run_pipeline(Orders, source=Duck())
        assert isinstance(result.error, TransformationError)
        assert result.jobs[0].fetched == []  # only a BaseSource's fetch can be observed
        assert run_pipeline(Orders, source=Duck(), limit=0).jobs == []

    def test_a_failing_plan_is_on_the_result(self):
        class Broken(Orders):
            def define_source(self, runtime_params):
                raise RuntimeError("no source today")

        result = run_pipeline(Broken)
        assert "no source today" in str(result.error) and result.jobs == []

    def test_skip_job_and_skip_record_are_logged_like_the_worker(self):
        @transformation("tk_drop_job")
        def drop_job(records, params):
            raise SkipJob("nothing to do")

        class Dropping(Orders):
            name = "tk_dropping"

            def define_transformations(self, records, runtime_params):
                return [drop_small(), drop_job()]

        # A handler of its own: other tests configure the `reflowfy` logger not to
        # propagate, which hides its records from caplog.
        seen: List[logging.LogRecord] = []
        handler = logging.Handler()
        handler.emit = seen.append  # type: ignore[method-assign]
        testing_logger = logging.getLogger("reflowfy.testing")
        level = testing_logger.level
        testing_logger.addHandler(handler)
        testing_logger.setLevel(logging.INFO)
        try:
            result = run_pipeline(Dropping, records=[{"amount": 0.1}, {"amount": 5}])
            run_pipeline(Orders, records=[{"amount": 0.1}, {"amount": 5}])
        finally:
            testing_logger.removeHandler(handler)
            testing_logger.setLevel(level)
        assert result.ok and result.jobs_dropped == ["nothing to do"]
        assert result.jobs[0].fetched == [{"data": {"amount": 0.1}}, {"data": {"amount": 5}}]
        assert result.sent == [] and result.destinations == []
        events = [getattr(r, "event", None) for r in seen]
        assert "job_dropped" in events and "records_skipped" in events
        assert_job_dropped(result)
        assert_job_dropped(result, reason="nothing to do")
        with pytest.raises(AssertionError, match="dropped jobs: \\['nothing to do'\\]"):
            assert_job_dropped(result, reason="other")
        with pytest.raises(AssertionError, match="dropped jobs: none"):
            assert_job_dropped(run_pipeline(Orders, records=[]))


class TestIdBasedPipeline:
    def _pipeline(self) -> Any:
        class Users(IdBasedPipeline[RuntimeParams]):
            name = "tk_users"
            ids_batch_size = 2

            def define_source(self, runtime_params):
                return [{"user": i} for i in runtime_params["current_ids"]]

            def define_transformations(self, records, runtime_params):
                assert "input_ids" not in runtime_params
                return []

            def define_destination(self, records, runtime_params):
                return CaptureDestination()

        return Users

    def test_ids_plan_the_jobs_and_input_ids_is_stripped(self):
        result = run_pipeline(self._pipeline(), params={"input_ids": [1, 2, 3]})
        assert [j.records for j in result.jobs] == [[{"user": 1}, {"user": 2}], [{"user": 3}]]
        assert result.jobs[1].params["current_ids"] == [3]

    def test_supplied_records_need_no_input_ids(self):
        result = run_pipeline(self._pipeline(), records=[{"user": 9}])
        assert result.sent == [{"data": {"user": 9}}]

    def test_without_records_input_ids_is_required(self):
        with pytest.raises(ValueError, match="input_ids"):
            run_pipeline(self._pipeline())


class TestNetwork:
    def _pipeline(self, step: Any) -> Any:
        class Calls(Orders):
            def define_transformations(self, records, runtime_params):
                return [step()]

        return Calls

    def test_an_unmocked_request_raises_naming_the_step(self):
        @transformation("tk_calls_out")
        def calls_out(records, params):
            httpx.get("https://crm.internal/customers/1")
            return records

        with pytest.raises(UnmockedCallError, match="step 'tk_calls_out' made GET https://crm"):
            run_pipeline(self._pipeline(calls_out), records=[{"id": 1}])

    def test_an_unmocked_request_while_planning_names_the_pipeline(self):
        class Plans(Orders):
            def define_source(self, runtime_params):
                httpx.get("https://crm.internal/plan")
                return []

        with pytest.raises(UnmockedCallError, match="the pipeline made GET"):
            run_pipeline(Plans)

    def test_mock_network_outside_pytest_and_source_stubs(self):
        class Lookup(MockSource):
            pass

        @record_transformation("tk_lookup")
        async def lookup(rec, params):
            async with httpx.AsyncClient() as client:
                rec["tier"] = (await client.get("https://crm.internal/tier")).json()["tier"]
            rec["invoices"] = Lookup(data=[{"real": True}]).fetch(params)
            return rec

        with mock_network() as net:
            net.http.get("https://crm.internal/tier", json={"tier": "gold"})
            result = run_pipeline(
                self._pipeline(lookup),
                records=[{"id": 1}],
                source_stubs={"Lookup": [{"invoice": "A1"}]},
            )
        assert result.sent == [
            {"data": {"id": 1}, "tier": "gold", "invoices": [{"data": {"invoice": "A1"}}]}
        ]
        assert net.http.calls[0].step == "tk_lookup"
        assert net.sources.calls[0].source_type == "Lookup"

    def test_allow_network_lets_requests_through(self, monkeypatch):
        @transformation("tk_real_call")
        def real_call(records, params):
            return [{**r, "status": httpx.get("https://crm.internal/x").status_code} for r in records]

        monkeypatch.setattr(
            "reflowfy.testing.cassette._httpx_sync",
            lambda transport, request: httpx.Response(204),
        )
        result = run_pipeline(self._pipeline(real_call), records=[{"id": 1}], allow_network=True)
        assert result.sent[0]["status"] == 204


class TestApplyTransformation:
    def test_a_decorated_function_or_an_instance(self):
        out, skips = apply_transformation(
            add_tax, make_records([{"amount": 10.0}]), params={"region": "eu"}
        )
        assert out == [{"data": {"amount": 10.0}, "tax": 2.0}] and skips == {}
        assert apply_transformation(add_tax(), make_records([{"amount": 1.0}])).records[0]["tax"] == 0.2

    def test_a_per_record_step_reports_its_skips(self):
        result = apply_transformation(
            drop_small, make_records([{"amount": 0.5}, {"amount": 5}, {"amount": 5000}])
        )
        assert result.records == [{"data": {"amount": 5}}]
        assert result.skip_reasons == {"too small": 1}

    def test_a_failure_is_a_transformation_error(self):
        with pytest.raises(TransformationError) as excinfo:
            apply_transformation(add_tax, [{"data": {}}])
        assert isinstance(excinfo.value.original_error, KeyError)

    def test_a_step_without_a_name(self):
        class Anonymous(BaseTransformation):
            def apply(self, records, runtime_params):
                return records[::-1]

        assert apply_transformation(Anonymous(), [1, 2]).records == [2, 1]

    def test_building_inputs(self):
        assert record({"id": 1}, tax=2.0) == {"data": {"id": 1}, "tax": 2.0}
        assert make_records(["a"]) == [{"data": "a"}]
        params = make_params("tk_orders", extra=1)
        assert params["region"] == "eu" and params["extra"] == 1
        assert params["pipeline_name"] == "tk_orders" and params["execution_id"]


class TestAssertions:
    def test_assert_sent(self):
        result = run_pipeline(Orders, records=[{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}])
        first, second = record({"id": 1, "amount": 10.0}, tax=2.0), record(
            {"id": 2, "amount": 20.0}, tax=4.0
        )
        assert_sent(result, [first, second])
        assert_sent(result, [second, first], ordered=False)
        assert_sent(result, [record({"id": 1, "amount": 10.0}, tax=9), second], ignore=["tax"])
        with pytest.raises(AssertionError, match=r"sent\[0\].tax: 9 → 2.0"):
            assert_sent(result, [record({"id": 1, "amount": 10.0}, tax=9), second])
        with pytest.raises(AssertionError, match="sent: 1 item\\(s\\) → 2"):
            assert_sent(result, [first])

    def test_assert_sent_fails_on_a_failed_run(self):
        with pytest.raises(AssertionError, match="failed"):
            assert_sent(run_pipeline(Orders, records=[{"id": 1}]), [])

    def test_assert_skipped(self):
        result = run_pipeline(Orders, records=[{"amount": 0.1}, {"amount": 0.2}, {"amount": 3}])
        assert_skipped(result)
        assert_skipped(result, reason="too small")
        assert_skipped(result, reason="too small", count=2)
        assert_skipped(result, count=2)
        with pytest.raises(AssertionError, match="expected 1 record.*'too small'.*got 2"):
            assert_skipped(result, reason="too small", count=1)
        with pytest.raises(AssertionError, match="at least 1.*with a reason, got 0.*none"):
            assert_skipped(run_pipeline(Orders, records=[{"amount": 3}]))

    def test_assert_valid_records(self):
        class Order(TypedDict):
            id: int

        class Typed(Orders):
            record_type = TypedRecord[Order]

        assert_valid_records(TypedRecord[Order], [{"data": {"id": 1}}])
        assert_valid_records(Typed, [{"data": {"id": 1}}])
        with pytest.raises(AssertionError, match="record 1 of 1 does not match.*data.id"):
            assert_valid_records(Typed, [{"data": {}}])
