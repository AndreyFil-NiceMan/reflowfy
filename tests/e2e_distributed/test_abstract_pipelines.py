"""AbstractPipeline over Kafka and a real worker pool.

Every test names the property it protects. Delivery is checked from outside, on the sink's
ledger, so "it completed" is never taken as proof that the data arrived.
"""

import threading
import time
from typing import Any, Callable, Dict

import httpx
import pytest

from tests.e2e_distributed.helpers import Manager, Sink, assert_exactly_once, job_states

Params = Callable[..., Dict[str, Any]]
PIPELINE = "dist_abstract"


def ids(n: int) -> range:
    return range(1, n + 1)


class TestDelivery:
    def test_every_record_is_delivered_exactly_once(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run(PIPELINE, params(total=1000, per_job=100))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert (stats["total_jobs"], stats["jobs_completed"], stats["jobs_failed"]) == (10, 10, 0)
        assert_exactly_once(sink, channel, ids(1000))

    def test_the_short_last_job_is_not_lost(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run(PIPELINE, params(total=1001, per_job=100))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == 11, stats
        assert_exactly_once(sink, channel, ids(1001))

    def test_many_tiny_jobs(self, manager: Manager, sink: Sink, channel: str, params: Params):
        """1,500 one-record jobs: the dispatch loop, claims and heartbeats at job-count scale."""
        execution = manager.run(PIPELINE, params(total=1500, per_job=1))

        stats = manager.wait(execution, timeout=600)

        assert stats["state"] == "completed" and stats["total_jobs"] == 1500, stats
        assert_exactly_once(sink, channel, ids(1500))

    def test_an_empty_plan_completes_with_no_jobs(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run(PIPELINE, params(total=0))

        stats = manager.wait(execution, timeout=60)

        assert stats["state"] == "completed" and stats["total_jobs"] == 0, stats
        assert sink.report(channel)["records"] == 0

    def test_transformations_can_filter_and_expand(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run(PIPELINE, params(total=200, per_job=50, drop_even=True, expand=True))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        expected = [i * 10 + n for i in ids(200) if i % 2 for n in (0, 1)]
        assert_exactly_once(sink, channel, expected)

    def test_large_records_travel_whole(self, manager: Manager, sink: Sink, channel: str, params: Params):
        """100 KB records, five per job: ~500 KB Kafka messages and ~500 KB HTTP bodies."""
        execution = manager.run(PIPELINE, params(total=50, per_job=5, pad_kb=100))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(50))

    def test_a_job_over_the_kafka_message_limit_never_completes_with_data_missing(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """Records ride inside the job message (1 MB default). A job that cannot fit must
        fail loudly — it must not end 'completed' with its records dropped."""
        execution = manager.run(PIPELINE, params(total=10, per_job=5, pad_kb=300))

        stats = manager.wait(execution, timeout=120)  # used to stay 'running' for ever

        assert stats["state"] == "failed" and stats["jobs_failed"] == 2, stats
        assert "too large" in str(manager.errors(execution))
        assert sink.report(channel)["records"] == 0


class TestJobOutcomes:
    def test_skipped_jobs_complete_and_deliver_nothing(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run(PIPELINE, params(total=500, per_job=100, skip_jobs="1,3"))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        kept = [i for i in ids(500) if (i - 1) // 100 not in (1, 3)]
        assert_exactly_once(sink, channel, kept)
        skipped = [j for j in manager.jobs(execution) if (j.get("stats") or {}).get("skip_reason")]
        assert len(skipped) == 2, "SkipJob must leave its reason on the job row"

    def test_a_failing_job_fails_the_execution_and_does_not_stop_the_others(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=500, per_job=100, fail_jobs="2"))

        stats = manager.wait(execution)

        assert stats["state"] == "failed", stats
        assert (stats["jobs_completed"], stats["jobs_failed"]) == (4, 1), stats
        kept = [i for i in ids(500) if (i - 1) // 100 != 2]
        assert_exactly_once(sink, channel, kept)
        assert "injected failure in job 2" in str(manager.errors(execution))

    def test_retry_failed_finishes_a_failed_execution_exactly_once(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.flag(channel, "broken")
        execution = manager.run(PIPELINE, params(total=400, per_job=100, fail_while_flag="broken"))
        stats = manager.wait(execution)
        assert stats["state"] == "failed" and stats["jobs_failed"] == 4, stats
        assert sink.report(channel)["records"] == 0

        sink.flag(channel, "fixed")
        retry = manager.post(f"/executions/{execution}/retry-failed")

        assert retry.status_code == 200 and retry.json()["requeued"] == 4, retry.text
        stats = manager.wait(execution)
        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(400))

    def test_retry_failed_with_a_filter_touches_only_matching_errors(
        self, manager: Manager, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=200, per_job=100, fail_jobs="0"))
        manager.wait(execution)

        none = manager.post(f"/executions/{execution}/retry-failed", params={"error_contains": "no such text"})
        some = manager.post(f"/executions/{execution}/retry-failed", params={"error_contains": "injected failure"})

        assert none.json()["requeued"] == 0
        assert some.json()["requeued"] == 1


class TestDeduplication:
    def test_the_same_data_twice_is_delivered_once(self, manager: Manager, sink: Sink, channel: str, params: Params):
        first = manager.run(PIPELINE, params(total=300, per_job=100), enable_duplicate_jobs=False)
        manager.wait(first)
        assert_exactly_once(sink, channel, ids(300))

        second = manager.run(PIPELINE, params(total=300, per_job=100), enable_duplicate_jobs=False)
        stats = manager.wait(second)

        assert stats["state"] == "completed", stats
        assert stats["deduplicated_jobs"] == 3, stats
        assert_exactly_once(sink, channel, ids(300))  # still once

    def test_the_same_records_to_another_destination_are_new_work(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        first = manager.run(PIPELINE, params(total=300, per_job=100, dest_tag="a"), enable_duplicate_jobs=False)
        manager.wait(first)

        second = manager.run(PIPELINE, params(total=300, per_job=100, dest_tag="b"), enable_duplicate_jobs=False)
        stats = manager.wait(second)

        assert stats["state"] == "completed" and stats["deduplicated_jobs"] == 0, stats
        assert sink.report(channel)["records"] == 600  # delivered once per destination

    @pytest.mark.characterization
    def test_a_volatile_request_body_is_new_work_by_default(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """Pins the safe default: the whole destination config, request body included, is its
        identity. A timestamp in the body therefore makes a repeat look new — a duplicate
        write, never a lost one. (Declare dedup_identity to opt out: next test.)"""
        first = manager.run(PIPELINE, params(total=200, per_job=100, volatile=True), enable_duplicate_jobs=False)
        manager.wait(first)

        second = manager.run(PIPELINE, params(total=200, per_job=100, volatile=True), enable_duplicate_jobs=False)
        stats = manager.wait(second)

        assert stats["deduplicated_jobs"] == 0, stats
        assert sink.report(channel)["records"] == 400

    def test_a_declared_dedup_identity_makes_a_volatile_body_deduplicate(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        first = manager.run(
            PIPELINE, params(total=200, per_job=100, volatile=True, stable_identity=True), enable_duplicate_jobs=False
        )
        manager.wait(first)

        second = manager.run(
            PIPELINE, params(total=200, per_job=100, volatile=True, stable_identity=True), enable_duplicate_jobs=False
        )
        stats = manager.wait(second)

        assert stats["deduplicated_jobs"] == 2, f"dedup missed identical content: {stats}"
        assert_exactly_once(sink, channel, ids(200))


class TestRateLimit:
    def test_the_per_request_rate_limit_spaces_the_jobs(self, manager: Manager, sink: Sink, channel: str, params: Params):
        """120 jobs/min = one job every 0.5 s: 20 jobs take about 9.5 s to dispatch."""
        execution = manager.run(PIPELINE, params(total=20, per_job=1), rate_limit=120)

        manager.wait(execution)

        arrivals = [b["at"] for b in sink.batches(channel)]
        span = max(arrivals) - min(arrivals)
        assert 7.0 <= span <= 40.0, f"20 jobs at 120/min arrived over {span:.1f}s"
        assert_exactly_once(sink, channel, ids(20))


class TestPauseResume:
    def test_pause_stops_dispatch_and_resume_finishes_exactly_once(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=40, per_job=1), rate_limit=60)  # 1 job/s
        manager.wait_jobs(execution, at_least=5)

        assert manager.post(f"/executions/{execution}/pause").status_code == 200
        time.sleep(6)  # let the jobs already in flight land
        settled = sink.report(channel)["records"]
        time.sleep(8)
        assert sink.report(channel)["records"] == settled, "jobs were dispatched while paused"
        assert 5 <= settled < 40

        assert manager.post(f"/executions/{execution}/resume").status_code == 200
        stats = manager.wait(execution)
        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(40))

    def test_pausing_a_finished_execution_is_refused(self, manager: Manager, channel: str, params: Params):
        execution = manager.run(PIPELINE, params(total=10, per_job=10))
        manager.wait(execution)

        assert manager.post(f"/executions/{execution}/pause").status_code == 409
        assert manager.stats(execution)["state"] == "completed"


class TestConcurrency:
    def test_concurrent_executions_do_not_mix(self, manager: Manager, sink: Sink, channel: str, params: Params):
        runs = {}
        for n in range(4):
            tag = f"{channel}-{n}"
            runs[tag] = manager.run(PIPELINE, {**params(total=400 + n * 10, per_job=50), "run": tag})

        for tag, execution in runs.items():
            assert manager.wait(execution)["state"] == "completed"
        for n, tag in enumerate(runs):
            assert_exactly_once(sink, tag, ids(400 + n * 10))
            sink.reset(tag)


    def test_runs_started_at_the_same_instant_each_get_their_own_plan(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """Sixteen /run calls released together. The registry shares one pipeline instance, and
        resolving it stored the plan on it, so simultaneous runs overwrote each other's plan:
        all sixteen got the wrong job count, most of them none, and finished 'completed'."""
        count = 16
        barrier = threading.Barrier(count)
        started: Dict[int, str] = {}

        def fire(k: int) -> None:
            barrier.wait(timeout=30)
            started[k] = manager.run(PIPELINE, {**params(total=k * 100, per_job=100), "run": f"{channel}-{k}"})

        threads = [threading.Thread(target=fire, args=(k,)) for k in range(1, count + 1)]
        [t.start() for t in threads]
        [t.join(timeout=60) for t in threads]
        assert len(started) == count

        for k, execution in sorted(started.items()):
            stats = manager.wait(execution, timeout=300)
            assert stats["state"] == "completed" and stats["total_jobs"] == k, (
                f"run {k} should plan {k} jobs, planned {stats['total_jobs']}: {stats}"
            )
            assert_exactly_once(sink, f"{channel}-{k}", ids(k * 100))
            sink.reset(f"{channel}-{k}")


class TestApi:
    def test_an_unknown_pipeline_is_a_404(self, manager: Manager):
        assert manager.try_run("no_such_pipeline_xyz", {}).status_code == 404

    def test_unknown_executions_are_404(self, manager: Manager):
        for path in ("stats", "jobs", "errors"):
            assert manager.http.get(f"/executions/does-not-exist/{path}").status_code == 404
        assert manager.post("/executions/does-not-exist/pause").status_code == 404
        assert manager.post("/executions/does-not-exist/retry-failed").status_code == 404

    def test_a_missing_required_param_is_rejected_up_front(self, manager: Manager):
        response = manager.try_run(PIPELINE, {"total": 10})

        assert response.status_code == 422, f"{response.status_code}: {response.text}"
        assert "run" in response.text

    def test_a_typo_in_a_parameter_name_is_reported_not_silently_ignored(self, manager: Manager, params: Params):
        response = manager.try_run(PIPELINE, params(totl=5, per_job=5))

        assert response.status_code == 202
        assert "totl" in str(response.json().get("warnings")), response.json()
        manager.wait(response.json()["execution_id"])

    def test_a_wrongly_typed_param_never_ends_as_a_success(self, manager: Manager, params: Params):
        response = manager.try_run(PIPELINE, params(total="lots"))
        if response.status_code == 202:
            stats = manager.wait(response.json()["execution_id"], timeout=60)
            assert stats["state"] == "failed", stats
        else:
            assert 400 <= response.status_code < 500, response.text

    def test_a_reused_execution_id_is_a_client_error_not_a_crash(self, manager: Manager, params: Params):
        execution = manager.run(PIPELINE, params(total=10, per_job=10))
        manager.wait(execution)

        again = manager.try_run(PIPELINE, params(total=10, per_job=10), execution_id=execution)

        assert again.status_code == 409, f"{again.status_code}: {again.text}"
        assert "psycopg2" not in again.text, "a database error leaked to the caller"
        assert manager.stats(execution)["state"] == "completed"  # the original is untouched

    def test_the_manager_survives_a_burst_of_runs(self, manager: Manager, params: Params):
        """Fifty runs accepted back to back; the manager stays healthy and answers every one."""
        executions = [manager.run(PIPELINE, params(total=5, per_job=5)) for _ in range(50)]
        for execution in executions:
            assert manager.wait(execution, timeout=300)["state"] == "completed"
        assert httpx.get(f"{manager.http.base_url}/health").status_code == 200


@pytest.mark.needs_es
class TestElasticsearchSource:
    """The worker path with a real source: windows are planned on the manager, fetched on workers."""

    @pytest.mark.parametrize("mode", ["snapshot", "range"])
    def test_every_doc_is_delivered_exactly_once(
        self, manager: Manager, sink: Sink, channel: str, es_params: Params, mode: str
    ):
        execution = manager.run(
            "dist_abstract_es", es_params(min_seq=1, max_seq=20000, docs_per_job=500, window_mode=mode)
        )

        stats = manager.wait(execution, timeout=600)

        assert stats["state"] == "completed" and stats["total_jobs"] >= 40, stats
        assert_exactly_once(sink, channel, ids(20000))
        assert job_states(manager, execution) == {"completed": stats["total_jobs"]}


@pytest.mark.chaos
class TestChaos:
    def test_a_killed_worker_loses_nothing(self, manager: Manager, sink: Sink, channel: str, params: Params, chaos: Any):
        """Jobs sleep before writing, so the kill lands while they are in flight; the manager's
        lease re-runs them. Nothing is lost, nothing is written twice (the sleep is before the write)."""
        execution = manager.run(PIPELINE, params(total=300, per_job=10, slow_s=2.0), rate_limit=600)
        manager.wait_jobs(execution, at_least=4)

        chaos.kill_worker()

        stats = manager.wait(execution, timeout=900)
        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(300))

    def test_a_restarted_manager_resumes_the_run(self, manager: Manager, sink: Sink, channel: str, params: Params, chaos: Any):
        execution = manager.run(PIPELINE, params(total=100, per_job=1), rate_limit=120)
        manager.wait_jobs(execution, at_least=10)

        chaos.restart_manager()

        stats = manager.wait(execution, timeout=600)
        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(100))

    def test_a_manager_killed_while_planning_fails_the_run_instead_of_completing_it(
        self, manager: Manager, sink: Sink, channel: str, params: Params, chaos: Any
    ):
        """The planner is slow (a slow source). Killed mid-plan, the run must end 'failed' with
        an explanation — not 'completed' having processed nothing."""
        execution = manager.run(PIPELINE, params(total=100, per_job=10, plan_delay_s=3.0))
        time.sleep(8)  # well inside the ~30 s plan

        chaos.restart_manager()

        stats = manager.wait(execution, timeout=600)
        assert stats["state"] == "failed", stats
        assert "stopped while planning" in str(stats.get("error_message")), stats
        assert sink.report(channel)["records"] == 0
