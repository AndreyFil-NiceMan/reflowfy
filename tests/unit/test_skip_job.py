"""SkipJob from any hook drops the job: no destination, no records, no failure."""

import pytest

from reflowfy import SkipJob
from reflowfy.execution.job_runner import run_job_records


class _StaticSource:
    def __init__(self, records):
        self._records = records

    def fetch(self, runtime_params):
        return self._records


class _Drop:
    name = "drop"

    def apply(self, records, runtime_params):
        raise SkipJob("nothing useful")


class _Pipeline:
    name = "p"

    def __init__(self, where):
        self.where = where

    def define_transformations(self, records, runtime_params):
        return [_Drop()] if self.where == "transformation" else []

    def define_destination(self, records, runtime_params):
        if self.where == "destination":
            raise SkipJob("nothing ready")
        raise AssertionError("destination must not be resolved in this test")


@pytest.mark.parametrize("where", ["transformation", "destination"])
def test_skip_job_drops_the_job(where):
    runtime_params = {}
    records, transformed, applied, destination = run_job_records(
        _StaticSource([{"id": 1}]), _Pipeline(where), runtime_params
    )
    assert (records, transformed, applied, destination) == ([], [], [], None)
    # The reason is what the worker/local executor log and store in job stats.
    assert runtime_params["skip_reason"] in ("nothing useful", "nothing ready")


async def test_worker_path_drops_the_job(monkeypatch):
    """The Kafka worker path (WorkerExecutor.execute_job) drops the job too."""
    from reflowfy.core.registry import pipeline_registry
    from reflowfy.worker.executor import WorkerExecutor

    class _WorkerPipeline:
        name = "p"

        def define_transformations(self, records, params):
            return []

        def define_destination(self, records, params):
            raise SkipJob("worker drop")

    monkeypatch.setattr(pipeline_registry, "get", lambda name: _WorkerPipeline())

    recorded = {}

    async def _capture(execution_id, job_id, stats):
        recorded["stats"] = stats

    executor = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(executor, "_update_job_in_db", _capture)

    ok = await executor.execute_job(
        {
            "schema_version": 2,
            "execution_id": "e",
            "job_id": "j",
            "pipeline_name": "p",
            "source": {"type": "StaticSource", "config": {"records": [{"id": 1}]}},
            "metadata": {
                "batch_id": "b",
                "created_at": "t",
                "batch_number": 1,
                "total_batches": 1,
                "retry_count": 0,
                "is_retry": False,
                "runtime_params": {},
                "source_metadata": None,
            },
        }
    )

    stats = recorded["stats"]
    assert ok is True, "a dropped job is not a failure"
    # No destination was ever resolved, so nothing could be sent.
    assert stats.skip_reason == "worker drop"
    assert stats.success is True
    assert stats.records_output == 0


def test_skipped_job_is_its_own_metric_status():
    """A dropped job must not be counted as an ordinary completed one."""
    from prometheus_client import REGISTRY

    from reflowfy.worker.executor import JobStats, WorkerExecutor

    stats = JobStats()
    stats.success = True
    stats.skip_reason = "nothing ready"
    assert stats.to_dict()["skip_reason"] == "nothing ready"

    def count(status):
        return (
            REGISTRY.get_sample_value(
                "reflowfy_jobs_processed_total",
                {"pipeline": "skip-metrics", "status": status},
            )
            or 0
        )

    before_skipped, before_completed = count("skipped"), count("completed")
    WorkerExecutor.record_job_metrics("skip-metrics", stats)
    assert count("skipped") == before_skipped + 1
    assert count("completed") == before_completed
