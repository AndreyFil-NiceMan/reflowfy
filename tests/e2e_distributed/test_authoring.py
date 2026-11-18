"""Distributed E2E: the authoring API changed after 1.1.6 (#120, #121, #123) over Kafka
and real workers.

Pipeline: ``pipelines/dist_authoring_pipelines.py`` (``dist_authoring``). Deliveries are
checked in the sink's ledger, job outcomes in the manager.
"""

from typing import Any, Callable, Dict, List

from .helpers import Manager, Sink, assert_exactly_once

Params = Callable[..., Dict[str, Any]]
PIPELINE = "dist_authoring"


def _steps_ids(total: int) -> List[int]:
    """Mirrors dist_auth_each (5k skipped, 3k dropped, 7k doubled) and dist_auth_fragile (1)."""
    out: List[int] = []
    for i in range(1, total + 1):
        if i % 5 == 0 or i % 3 == 0 or i == 1:
            continue
        out.append(i)
        if i % 7 == 0:
            out.append(i + 1000)
    return out


def _skip_totals(jobs: List[Dict[str, Any]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for job in jobs:
        for reason, count in ((job["stats"] or {}).get("record_skip_reasons") or {}).items():
            totals[reason] = totals.get(reason, 0) + count
    return totals


class TestDecoratorApiOnWorkers:
    def test_bare_steps_default_names_and_plain_def_per_record(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=40, per_job=5))
        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert stats["total_jobs"] == 8
        assert_exactly_once(sink, channel, _steps_ids(40))
        assert all(b["meta"]["tags"] == ["batch"] for b in sink.batches(channel))

        jobs = manager.jobs(execution)
        assert _skip_totals(jobs) == {"multiple of five": 8, "KeyError": 1}
        for job in jobs:
            times = job["stats"]["transformation_times"]
            assert {"dist_auth_tag", "dist_auth_each", "dist_auth_fragile_v2"} <= set(times)

    def test_a_bare_record_destination_routes_every_record(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        try:
            execution = manager.run(PIPELINE, params(total=30, per_job=10, scenario="route"))
            stats = manager.wait(execution)

            assert stats["state"] == "completed", stats
            assert_exactly_once(sink, f"{channel}-even", list(range(2, 31, 2)))
            assert_exactly_once(sink, f"{channel}-odd", list(range(1, 31, 2)))
            for job in manager.jobs(execution):
                assert job["stats"]["destinations"] == 2, job["stats"]
                assert "dist_auth_by_parity" in job["stats"]["transformation_times"]
        finally:
            sink.reset(f"{channel}-even")
            sink.reset(f"{channel}-odd")


class TestSkipRecordThenSkipJobOnWorkers:
    def test_the_dropped_job_keeps_the_records_it_skipped_first(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        execution = manager.run(PIPELINE, params(total=20, per_job=5, scenario="skip_then_drop"))
        stats = manager.wait(execution)

        assert stats["state"] == "completed", stats
        assert stats["jobs_failed"] == 0, stats
        assert_exactly_once(sink, channel, list(range(6, 21, 2)))

        jobs = manager.jobs(execution)
        dropped = [j for j in jobs if (j["stats"] or {}).get("skip_reason")]
        assert len(dropped) == 1, [j["stats"] for j in jobs]
        assert dropped[0]["stats"]["skip_reason"] == "first job"
        assert dropped[0]["stats"]["record_skip_reasons"] == {"odd": 3}
        others = [j for j in jobs if j is not dropped[0]]
        # The odd ids outside the first job (7, 9, 11, 13, 15, 17, 19) were skipped too.
        assert _skip_totals(others) == {"odd": 7}


class TestRemovalsOnTheManager:
    def test_a_callback_url_is_ignored(
        self, manager: Manager, sink: Sink, channel: str, params: Params
    ):
        """#119: callback_url is gone; sent anyway, the run proceeds and nothing is POSTed."""
        callback = f"{params()['sink_url']}/ingest/{channel}-callback"
        try:
            execution = manager.run(PIPELINE, params(total=10), callback_url=callback)
            stats = manager.wait(execution)

            assert stats["state"] == "completed", stats
            assert_exactly_once(sink, channel, _steps_ids(10))
            assert sink.ids(f"{channel}-callback") == []
            assert "callback_url" not in stats
        finally:
            sink.reset(f"{channel}-callback")
