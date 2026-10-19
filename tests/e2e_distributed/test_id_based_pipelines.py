"""IdBasedPipeline over Kafka and a real worker pool.

An IdBasedPipeline plans from a list the caller passes (``input_ids``): one job per ID, or
per ``ids_batch_size`` IDs, or whatever ``define_jobs`` makes of them. The properties:
every ID is processed exactly once, jobs know *their* IDs (``current_ids``) and not the
whole list, and a source that fails never looks like a source with nothing to say.
"""

import threading
import time
from typing import Any, Callable, Dict, List

import pytest

from tests.e2e_distributed.helpers import Manager, Sink, assert_exactly_once

Params = Callable[..., Dict[str, Any]]


def ids(n: int) -> List[int]:
    return list(range(1, n + 1))


class TestPlanning:
    def test_default_is_one_job_per_id(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_b1", params(input_ids=ids(120)))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == 120, stats
        assert_exactly_once(sink, channel, ids(120))
        assert {b["size"] for b in sink.batches(channel)} == {1}

    def test_ids_batch_size_groups_ids_and_keeps_the_short_tail(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_ids_b7", params(input_ids=ids(100)))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == 15, stats  # 14 x 7 + 2
        assert sorted(b["size"] for b in sink.batches(channel)) == [2] + [7] * 14
        assert_exactly_once(sink, channel, ids(100))

    def test_ten_thousand_ids_in_batches_of_a_hundred(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_b100", params(input_ids=ids(10000)))

        stats = manager.wait(execution, timeout=300)

        assert stats["state"] == "completed" and stats["total_jobs"] == 100, stats
        assert_exactly_once(sink, channel, ids(10000))

    def test_define_jobs_can_fan_one_id_out_into_many_jobs(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run("dist_ids_fanout", params(input_ids=ids(30)))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == 90, stats
        assert_exactly_once(sink, channel, [f"{p}-{c}" for p in ids(30) for c in range(3)])
        # each child job knew which parent it belongs to (the params job(...) attached)
        for batch in sink.batches(channel):
            assert batch["meta"]["current_id"] is not None


class TestWhatAJobSees:
    def test_a_job_sees_only_its_own_ids_never_the_whole_list(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """``input_ids`` exists only in define_jobs; downstream hooks read current_ids. If the
        whole list leaked into every job, each of 100 jobs would carry 100 IDs."""
        execution = manager.run("dist_ids_b7", params(input_ids=ids(100)))
        manager.wait(execution)

        for batch in sink.batches(channel):
            meta = batch["meta"]
            assert meta["has_input_ids"] is False, "input_ids leaked into a job"
            assert 1 <= len(meta["current_ids"]) <= 7
            assert meta["current_id"] == meta["current_ids"][0]


class TestInputs:
    def test_unusual_ids_survive_the_round_trip(self, manager: Manager, sink: Sink, channel: str, params: Params):
        odd = ["a b", "ü-ß", "x/y", "0", "-5", "emoji-\U0001f600", 2**53 + 1, ""]
        execution = manager.run("dist_ids_b1", params(input_ids=odd))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == len(odd), stats
        assert_exactly_once(sink, channel, odd)

    @pytest.mark.characterization
    def test_repeated_ids_are_each_processed(self, manager: Manager, sink: Sink, channel: str, params: Params):
        """Pins current behaviour: the list is taken literally, so an ID listed twice runs
        twice. If the framework ever de-duplicates input_ids, change this deliberately."""
        execution = manager.run("dist_ids_b1", params(input_ids=[1, 1, 2, 3]))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == 4, stats
        assert sorted(sink.ids(channel)) == [1, 1, 2, 3]

    def test_an_empty_id_list_is_rejected_up_front(self, manager: Manager, sink: Sink, channel: str, params: Params):
        response = manager.try_run("dist_ids_b1", params(input_ids=[]))

        assert response.status_code == 422 and "input_ids" in response.text, response.text
        assert sink.report(channel)["records"] == 0

    def test_a_missing_id_list_is_rejected_up_front(self, manager: Manager, params: Params):
        response = manager.try_run("dist_ids_b1", params())

        assert response.status_code == 422 and "input_ids" in response.text, response.text

    def test_a_non_list_id_parameter_is_rejected(self, manager: Manager, params: Params):
        response = manager.try_run("dist_ids_b1", params(input_ids="1,2,3"))
        if response.status_code == 202:
            assert manager.wait(response.json()["execution_id"], timeout=60)["state"] == "failed"
        else:
            assert 400 <= response.status_code < 500, response.text


class TestJobOutcomes:
    def test_failing_ids_fail_only_their_own_job(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_b7", params(input_ids=ids(28), fail_ids="10"))

        stats = manager.wait(execution)

        # ids 8..14 share a job; the other three jobs are untouched
        assert stats["state"] == "failed" and stats["jobs_failed"] == 1, stats
        assert_exactly_once(sink, channel, [i for i in ids(28) if not 8 <= i <= 14])

    def test_retry_failed_completes_an_id_run(self, manager: Manager, sink: Sink, channel: str, params: Params):
        sink.flag(channel, "broken")
        execution = manager.run("dist_ids_b7", params(input_ids=ids(21), fail_while_flag="broken"))
        assert manager.wait(execution)["jobs_failed"] == 3

        sink.flag(channel, "fixed")
        assert manager.post(f"/executions/{execution}/retry-failed").json()["requeued"] == 3

        assert manager.wait(execution)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(21))


class TestApiSource:
    """IDBasedAPISource against the sink's mock users API, whose answers the test controls."""

    def test_per_id_requests_deliver_every_user(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_api", params(input_ids=ids(50)))

        stats = manager.wait(execution)

        assert stats["state"] == "completed" and stats["total_jobs"] == 10, stats
        assert_exactly_once(sink, channel, ids(50))

    @pytest.mark.characterization
    def test_a_404_means_the_user_does_not_exist_and_is_skipped(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """Pins current behaviour: an ID the API does not know is skipped, not an error."""
        sink.rules(f"api:{channel}", missing_ids=[7])
        execution = manager.run("dist_ids_api", params(input_ids=ids(20)))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, [i for i in ids(20) if i != 7])

    @pytest.mark.parametrize("status", [500, 503, 429, 401])
    def test_a_server_error_is_not_mistaken_for_a_missing_user(
        self, manager: Manager, sink: Sink, channel: str, params: Params, status: int
    ):
        """Only a 404 means 'no such user'. A 5xx / 429 / 401 says the data could not be read:
        the job must fail so it can be retried — completing with that user silently
        absent loses data and looks like success."""
        sink.rules(f"api:{channel}", status_by_id={"13": status})
        execution = manager.run("dist_ids_api", params(input_ids=ids(20)))

        stats = manager.wait(execution)

        assert stats["state"] == "failed" and stats["jobs_failed"] == 1, (
            f"HTTP {status} for user 13 ended as {stats['state']} with "
            f"{stats['jobs_failed']} failed jobs; delivered {len(set(sink.ids(channel)))}/20"
        )

    def test_a_transient_error_is_recovered_by_retry_failed(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.rules(f"api:{channel}", fail_first={"13": 1})  # 500 once, then fine
        execution = manager.run("dist_ids_api", params(input_ids=ids(20)))
        stats = manager.wait(execution)
        assert stats["state"] == "failed", f"a 500 was swallowed: {stats}"

        manager.post(f"/executions/{execution}/retry-failed")

        assert manager.wait(execution)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(20))

    def test_batch_mode_delivers_every_user(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_api_batch", params(input_ids=ids(50)))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert_exactly_once(sink, channel, ids(50))

    def test_batch_mode_server_error_fails_the_job(self, manager: Manager, sink: Sink, channel: str, params: Params):
        sink.rules(f"api:{channel}", status_by_id={"13": 500})
        execution = manager.run("dist_ids_api_batch", params(input_ids=ids(20)))

        stats = manager.wait(execution)

        assert stats["state"] == "failed" and stats["jobs_failed"] == 1, stats


class TestRateLimitAndPause:
    def test_rate_limit_spaces_id_jobs(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_b1", params(input_ids=ids(20)), rate_limit=120)

        manager.wait(execution)

        arrivals = [b["at"] for b in sink.batches(channel)]
        assert 7.0 <= max(arrivals) - min(arrivals) <= 40.0
        assert_exactly_once(sink, channel, ids(20))

    def test_pause_and_resume_an_id_run(self, manager: Manager, sink: Sink, channel: str, params: Params):
        execution = manager.run("dist_ids_b1", params(input_ids=ids(40)), rate_limit=60)
        manager.wait_jobs(execution, at_least=5)
        assert manager.post(f"/executions/{execution}/pause").status_code == 200
        time.sleep(6)
        settled = sink.report(channel)["records"]
        time.sleep(6)
        assert sink.report(channel)["records"] == settled

        assert manager.post(f"/executions/{execution}/resume").status_code == 200

        assert manager.wait(execution)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(40))


class TestConcurrency:
    def test_id_runs_started_at_the_same_instant_each_get_their_own_plan(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        count = 8
        barrier = threading.Barrier(count)
        started: Dict[int, str] = {}

        def fire(k: int) -> None:
            barrier.wait(timeout=30)
            started[k] = manager.run("dist_ids_b7", {**params(input_ids=ids(k * 7)), "run": f"{channel}-{k}"})

        threads = [threading.Thread(target=fire, args=(k,)) for k in range(1, count + 1)]
        [t.start() for t in threads]
        [t.join(timeout=60) for t in threads]
        assert len(started) == count

        for k, execution in sorted(started.items()):
            stats = manager.wait(execution, timeout=300)
            assert stats["state"] == "completed" and stats["total_jobs"] == k, (k, stats)
            assert_exactly_once(sink, f"{channel}-{k}", ids(k * 7))
            sink.reset(f"{channel}-{k}")

    def test_concurrent_id_runs_do_not_mix(self, manager: Manager, sink: Sink, channel: str, params: Params):
        runs = {}
        for n, pipeline in enumerate(["dist_ids_b1", "dist_ids_b7", "dist_ids_b100", "dist_ids_fanout"]):
            tag = f"{channel}-{n}"
            runs[tag] = (pipeline, manager.run(pipeline, {**params(input_ids=ids(60)), "run": tag}))

        for tag, (pipeline, execution) in runs.items():
            assert manager.wait(execution)["state"] == "completed", pipeline
            expected: List[Any] = (
                [f"{p}-{c}" for p in ids(60) for c in range(3)] if pipeline == "dist_ids_fanout" else ids(60)
            )
            assert_exactly_once(sink, tag, expected)
            sink.reset(tag)


@pytest.mark.chaos
class TestChaos:
    def test_a_killed_worker_loses_no_ids(self, manager: Manager, sink: Sink, channel: str, params: Params, chaos: Any):
        execution = manager.run("dist_ids_b7", params(input_ids=ids(140), slow_s=2.0), rate_limit=600)
        manager.wait_jobs(execution, at_least=4)

        chaos.kill_worker()

        assert manager.wait(execution, timeout=900)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(140))

    def test_a_restarted_manager_resumes_an_id_run(
        self, manager: Manager, sink: Sink, channel: str, params: Params, chaos: Any
    ):
        execution = manager.run("dist_ids_b1", params(input_ids=ids(100)), rate_limit=120)
        manager.wait_jobs(execution, at_least=10)

        chaos.restart_manager()

        assert manager.wait(execution, timeout=600)["state"] == "completed"
        assert_exactly_once(sink, channel, ids(100))
