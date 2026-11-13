"""Distributed E2E: the framework-gaps features (PRs #105–#110) over Kafka and real workers.

Pipelines: ``pipelines/dist_gaps_pipelines.py``. Delivery is checked in the sink's
ledger; job outcomes in the manager.
"""

import time
from typing import Any, Callable, Dict, List

from .helpers import Manager, Sink, assert_exactly_once

Params = Callable[..., Dict[str, Any]]


def ids(n: int) -> List[int]:
    return list(range(1, n + 1))


def _wait_ids(sink: Sink, channel: str, count: int, timeout: float = 60) -> List[Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        got = sink.ids(channel)
        if len(got) >= count:
            return got
        time.sleep(0.5)
    return sink.ids(channel)


class TestCustomSourceOnWorkers:
    def test_a_user_source_is_rebuilt_on_the_workers_with_its_secret(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_source", params(total=30, per_job=10))
        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert stats["total_jobs"] == 3
        assert_exactly_once(sink, channel, ids(30))
        assert all(b["meta"]["token_resolved"] for b in sink.batches(channel))
        payload = manager.jobs(execution)[0]["job_payload"]["source"]
        assert payload["type"] == "DistGeneratedSource"
        assert payload["config"]["token"] == {"$secret": "HOSTNAME"}


class TestTransformations:
    def test_options_declined_step_and_skipped_records(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_transform", params(total=20, per_job=5))
        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, [i for i in ids(20) if i % 2 == 0])
        for batch in sink.batches(channel):
            assert batch["meta"]["labels"] == ["dist"]
            assert batch["meta"]["declined_ran"] is False
        reasons = [j["stats"]["record_skip_reasons"] for j in manager.jobs(execution)]
        assert reasons and all(r == {"ValueError": 2} or r == {"ValueError": 3} for r in reasons)

    def test_the_per_record_return_contract_fails_jobs(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_transform", params(total=10, per_job=5, scenario="contract"))
        stats = manager.wait(execution)

        assert stats["state"] == "failed", stats
        assert sink.ids(channel) == []
        assert all("a record, a list of records, or None" in (j["error_message"] or "") for j in manager.jobs(execution))


class TestDestinations:
    def test_fan_out_with_a_failing_optional_destination(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.rules(f"{channel}-opt", fail_next=1000)
        try:
            execution = manager.run("dist_gaps_dest", params(total=20, per_job=5, scenario="fanout"))
            stats = manager.wait(execution)
        finally:
            sink.reset(f"{channel}-opt")

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(20))
        assert all(j["stats"].get("destinations_failed_optional") for j in manager.jobs(execution))

    def test_define_destination_returning_a_list(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_dest", params(total=20, per_job=5, scenario="list"))
        try:
            assert manager.wait(execution)["state"] == "completed"
            assert_exactly_once(sink, channel, ids(20))
            assert_exactly_once(sink, f"{channel}-copy", ids(20))
        finally:
            sink.reset(f"{channel}-copy")

    def test_record_destination_multicast(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_dest", params(total=20, per_job=5, scenario="multicast"))
        try:
            assert manager.wait(execution)["state"] == "completed"
            assert_exactly_once(sink, channel, ids(20))
            assert_exactly_once(sink, f"{channel}-even", [i for i in ids(20) if i % 2 == 0])
        finally:
            sink.reset(f"{channel}-even")


class TestJobRetry:
    def test_failed_jobs_are_retried_on_workers_and_everything_arrives_once(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.rules(channel, fail_next=3, fail_status=503)
        execution = manager.run("dist_gaps_retry", params(total=30, per_job=5))
        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(30))
        retries = sum(int(j["stats"].get("retry_count") or 0) for j in manager.jobs(execution))
        assert retries == 3

    def test_a_404_is_neither_resent_nor_retried(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        # One failing answer: any retry, in the destination or of the job, would succeed.
        sink.rules(channel, fail_next=1, fail_status=404)
        execution = manager.run("dist_gaps_retry", params(total=1, per_job=1))
        stats = manager.wait(execution)

        assert stats["state"] == "failed", stats
        assert sink.ids(channel) == []
        job = manager.jobs(execution)[0]
        assert job["stats"].get("error_type") == "PermanentDestinationError", job["stats"]


class TestLifecycle:
    def test_complete_hook_and_chained_child_run(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_hooks", params(total=10, per_job=5, chain=True))
        try:
            assert manager.wait(execution)["state"] == "completed"
            assert _wait_ids(sink, f"{channel}-hook", 1) == ["complete"]
            summary = sink.batches(f"{channel}-hook")[0]["meta"]["summary"]
            assert summary["execution_id"] == execution
            assert summary["records_out"] == 10

            child_hook = _wait_ids(sink, f"{channel}-child-hook", 1, timeout=120)
            assert child_hook == ["complete"]
            child = sink.batches(f"{channel}-child-hook")[0]["meta"]["summary"]
            assert child["parent_execution_id"] == execution
            assert_exactly_once(sink, f"{channel}-child", ids(3))
        finally:
            for suffix in ("-hook", "-child", "-child-hook"):
                sink.reset(f"{channel}{suffix}")

    def test_failed_hook_fires_once(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_hooks", params(total=5, per_job=5, fail=True))
        try:
            assert manager.wait(execution)["state"] == "failed"
            _wait_ids(sink, f"{channel}-hook", 1)
            time.sleep(3)
            assert sink.ids(f"{channel}-hook") == ["failed"]
        finally:
            sink.reset(f"{channel}-hook")

    def test_callback_url(self, manager: Manager, sink: Sink, channel: str, params: Params):
        from .conftest import SINK_INTERNAL_URL

        callback = f"{SINK_INTERNAL_URL}/ingest/{channel}-cb"
        execution = manager.run("dist_gaps_hooks", params(total=5, per_job=5), callback_url=callback)
        try:
            assert manager.wait(execution)["state"] == "completed"
            deadline = time.time() + 30
            while time.time() < deadline and not sink.batches(f"{channel}-cb"):
                time.sleep(0.5)
            batches = sink.batches(f"{channel}-cb")
            assert len(batches) == 1, batches
            assert batches[0]["meta"]["execution_id"] == execution
        finally:
            sink.reset(f"{channel}-cb")

    def test_define_rate_limit_is_persisted(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_gaps_hooks", params(total=5, per_job=5, rpm=600))
        try:
            stats = manager.wait(execution)
            assert stats["state"] == "completed", stats
            assert stats["rate_limit"] == 600
        finally:
            sink.reset(f"{channel}-hook")
