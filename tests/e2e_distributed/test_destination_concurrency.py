"""Per-record ``api_destination`` with ``concurrency``, over Kafka and a real worker pool.

``dist_per_record`` is ``dist_abstract`` delivered one ``PUT <sink>/record/<run>/<id>`` per
record. The id in the URL is filled from the record; the sink checks it matches the body
(a mismatch lands in the ledger as ``mismatch:…``, so ``assert_exactly_once`` catches it),
and counts the requests of one job open at once (each carries ``?job=<job_index>``), so a
test checks the concurrency cap from outside, as ``test_async_pipelines`` does for lookups.
"""

from typing import Any, Callable, Dict

from tests.e2e_distributed.helpers import Manager, Sink, assert_exactly_once

Params = Callable[..., Dict[str, Any]]
PIPELINE = "dist_per_record"


def ids(n: int) -> range:
    return range(1, n + 1)


class TestPerRecordDelivery:
    def test_every_record_is_its_own_request_exactly_once(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=300, per_job=50, dest_concurrency=8))

        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert (stats["total_jobs"], stats["jobs_completed"]) == (6, 6), stats
        assert_exactly_once(sink, channel, ids(300))
        # One request per record: as many deliveries as records, each of size 1.
        assert {b["size"] for b in sink.batches(channel)} == {1}

    def test_key_field_sends_only_the_payload(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(
            PIPELINE, params(total=40, per_job=20, use_key_field=True, dest_concurrency=4)
        )

        assert manager.wait(execution)["state"] == "completed"
        # The sink keys a body by its "id"; the payload carries one, the record's own
        # "internal" field would not be in it. Mismatches would show up as unexpected ids.
        assert_exactly_once(sink, channel, ids(40))


class TestPerRecordConcurrency:
    def test_requests_overlap_within_a_job_but_never_exceed_the_cap(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.rules(channel, record_slow_ms=100)  # every request holds its slot for 100 ms
        execution = manager.run(PIPELINE, params(total=120, per_job=40, dest_concurrency=5))

        assert manager.wait(execution)["state"] == "completed"

        per_job = sink.record_stats(channel)["peak_by_job"]
        assert set(per_job) == {"0", "1", "2"}, per_job
        assert all(2 <= peak <= 5 for peak in per_job.values()), per_job
        assert_exactly_once(sink, channel, ids(120))

    def test_default_concurrency_sends_one_at_a_time(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        sink.rules(channel, record_slow_ms=20)
        execution = manager.run(PIPELINE, params(total=30, per_job=30))

        assert manager.wait(execution)["state"] == "completed"
        assert sink.record_stats(channel)["peak_by_job"] == {"0": 1}
        assert_exactly_once(sink, channel, ids(30))


class TestPerRecordFailure:
    def test_a_failed_record_fails_its_job_and_retry_failed_finishes_it(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """A retried job is sent from scratch, so the records of the failed job that had
        already been accepted arrive a second time: per-record mode does not resume where
        it stopped (the docstring of ``api_destination`` says so). Only that job may repeat."""
        sink.rules(channel, record_fail_ids=[113])  # job 2 (ids 101-150), its 13th record
        execution = manager.run(PIPELINE, params(total=200, per_job=50))

        stats = manager.wait(execution)

        assert stats["state"] == "failed", stats
        assert (stats["jobs_completed"], stats["jobs_failed"]) == (3, 1), stats
        error = str(manager.errors(execution))
        assert "record 13 of 50: HTTP 503" in error, error
        first_pass = sink.ids(channel)
        assert 113 not in first_pass
        # One at a time: the failed job stopped at its 13th record.
        assert sorted(i for i in first_pass if 101 <= i <= 150) == list(range(101, 113))

        sink.flag(channel, "fixed")
        retry = manager.post(f"/executions/{execution}/retry-failed")

        assert retry.status_code == 200 and retry.json()["requeued"] == 1, retry.text
        assert manager.wait(execution)["state"] == "completed"
        got = sink.ids(channel)
        assert sorted(set(got)) == list(ids(200))
        repeated = {i for i in got if got.count(i) > 1}
        assert repeated == set(range(101, 113)), sorted(repeated)
