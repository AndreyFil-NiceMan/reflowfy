"""Async transformations over Kafka and a real worker pool.

``dist_async`` is ``dist_abstract`` plus a ``RecordTransformation`` that looks every record up
in the sink's mock users API, ``concurrency`` calls at a time per job. The sink counts the calls
in flight per job (each call carries ``?job=<job_index>``), so a test can check the cap from
outside; delivery is checked on the sink's ledger, as everywhere in this suite.
"""

from typing import Any, Callable, Dict

import pytest

from tests.e2e_distributed.helpers import Manager, Sink, assert_exactly_once

Params = Callable[..., Dict[str, Any]]
PIPELINE = "dist_async"


def ids(n: int) -> range:
    return range(1, n + 1)


def delivered_users(sink: Sink, channel: str) -> Dict[str, str]:
    users: Dict[str, str] = {}
    for batch in sink.batches(channel):
        users.update(batch["meta"].get("users") or {})
    return users


class TestAsyncDelivery:
    def test_every_record_is_looked_up_and_delivered_exactly_once(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=500, per_job=50, concurrency=8))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert (stats["total_jobs"], stats["jobs_completed"]) == (10, 10), stats
        assert_exactly_once(sink, channel, ids(500))
        assert delivered_users(sink, channel) == {str(i): f"user-{i}" for i in ids(500)}
        assert sink.api_stats(channel)["calls"] == 500
        for item in manager.jobs(execution):
            assert set(item["stats"]["transformation_times"]) == {"dist_behaviors", "dist_lookup_user"}

    def test_the_lookups_overlap_within_a_job_but_never_exceed_the_cap(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.api_rules(channel, slow_ms=100)  # every lookup holds its slot for 100 ms
        execution = manager.run(PIPELINE, params(total=120, per_job=40, concurrency=5))

        assert manager.wait(execution)["state"] == "completed"

        per_job = sink.api_stats(channel)["peak_by_job"]
        assert set(per_job) == {"0", "1", "2"}, per_job
        assert all(2 <= peak <= 5 for peak in per_job.values()), per_job
        assert_exactly_once(sink, channel, ids(120))

    def test_concurrency_one_looks_records_up_one_at_a_time(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.api_rules(channel, slow_ms=20)
        execution = manager.run(PIPELINE, params(total=30, per_job=30, concurrency=1))

        assert manager.wait(execution)["state"] == "completed"
        assert sink.api_stats(channel)["peak_by_job"] == {"0": 1}
        assert_exactly_once(sink, channel, ids(30))

    def test_a_404_drops_only_that_record(self, manager: Manager, sink: Sink, channel: str, params: Params):
        sink.api_rules(channel, missing_ids=[7, 42])
        execution = manager.run(PIPELINE, params(total=100, per_job=25))

        assert manager.wait(execution)["state"] == "completed"
        assert_exactly_once(sink, channel, [i for i in ids(100) if i not in (7, 42)])


class TestAsyncFailures:
    def test_a_failing_lookup_fails_its_job_naming_the_record_and_retry_failed_finishes(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.api_rules(channel, status_by_id={"113": 500})  # job 2 (ids 101-150), index 12
        execution = manager.run(PIPELINE, params(total=200, per_job=50))

        stats = manager.wait(execution)

        assert stats["state"] == "failed", stats
        assert (stats["jobs_completed"], stats["jobs_failed"]) == (3, 1), stats
        assert_exactly_once(sink, channel, [i for i in ids(200) if not 101 <= i <= 150])
        error = str(manager.errors(execution))
        assert "dist_lookup_user" in error and "record 12" in error, error

        sink.api_rules(channel, status_by_id={})
        retry = manager.post(f"/executions/{execution}/retry-failed")

        assert retry.status_code == 200 and retry.json()["requeued"] == 1, retry.text
        assert manager.wait(execution)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(200))

    def test_a_transient_error_is_absorbed_by_the_authors_own_retry(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """The framework does not retry a record; a step that wants retries writes them."""
        sink.api_rules(channel, fail_first={"13": 2})  # 500 twice, then fine
        execution = manager.run(PIPELINE, params(total=50, per_job=50, retries=2))

        assert manager.wait(execution)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(50))
        assert sink.api_stats(channel)["calls"] == 52

    def test_skip_job_still_drops_a_job_before_the_async_step(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=100, per_job=50, skip_jobs="1"))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["jobs_failed"] == 0, stats
        assert_exactly_once(sink, channel, ids(50))
        assert sink.api_stats(channel)["calls"] == 50, "the dropped job must not look anything up"


class TestTheWorkerStaysResponsive:
    def test_a_long_async_step_does_not_get_the_worker_evicted(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """Each job spends ~25 s in the async step (50 lookups x 1 s, 2 at a time), far past
        the Kafka session timeout. The step awaits on the worker's own loop, so a stalled loop
        would miss heartbeats, the group would rebalance, and the job would run twice. It must
        run once: one attempt per job and nothing delivered twice."""
        sink.api_rules(channel, slow_ms=1000)
        execution = manager.run(PIPELINE, params(total=100, per_job=50, concurrency=2))

        stats = manager.wait(execution, timeout=600)

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(100))
        assert [item["attempts"] for item in manager.jobs(execution)] == [1, 1]
        assert sink.api_stats(channel)["calls"] == 100


@pytest.mark.chaos
class TestAsyncChaos:
    def test_a_worker_killed_during_an_async_step_loses_nothing(
        self, manager: Manager, sink: Sink, channel: str, params: Params, chaos: Any
    ):
        """The kill lands while jobs are awaiting lookups, before their write; the lease
        re-runs them. Nothing lost, nothing written twice."""
        sink.api_rules(channel, slow_ms=500)
        execution = manager.run(PIPELINE, params(total=200, per_job=20, concurrency=2), rate_limit=600)
        manager.wait_jobs(execution, at_least=2)

        chaos.kill_worker()

        stats = manager.wait(execution, timeout=900)
        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(200))
