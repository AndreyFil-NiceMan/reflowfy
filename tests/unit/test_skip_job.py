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


# --- SkipRecord before SkipJob: both are reported, not only the drop -------------


from reflowfy import SkipRecord, transformation  # noqa: E402


@transformation("skip_job_test_skip_one", per_record=True, concurrency=1)
async def _skip_one(record, params):
    if record["id"] == 1:
        raise SkipRecord("bad id")
    if params.get("where") == "same_step":
        raise SkipJob("stop here")
    return record


def _skip_first_then(where):
    """A per-record step that skips record 1; the job is then dropped ``where``
    (read from runtime_params, so the step can be registered once)."""

    class _P:
        name = "p"

        def define_transformations(self, records, runtime_params):
            runtime_params["where"] = where
            steps = [_skip_one()]
            if where == "later_step":
                steps.append(_Drop())
            return steps

        def define_destination(self, records, runtime_params):
            if where == "destination":
                raise SkipJob("nothing ready")
            raise AssertionError("destination must not be reached")

    return _P()


@pytest.mark.parametrize("where", ["later_step", "same_step", "destination"])
def test_records_skipped_before_a_drop_are_reported(where):
    runtime_params = {}
    records, transformed, applied, destination = run_job_records(
        _StaticSource([{"id": 1}, {"id": 2}]), _skip_first_then(where), runtime_params
    )
    assert (records, transformed, destination) == ([], [], None)
    assert runtime_params["skip_reason"]
    skipping = [s for s in applied if s.records_skipped]
    assert [(s.name, s.skip_reasons) for s in skipping] == [("skip_job_test_skip_one", {"bad id": 1})]


async def test_worker_stores_and_logs_both_skip_record_and_skip_job(monkeypatch):
    import logging

    from reflowfy.core.registry import pipeline_registry
    from reflowfy.worker.executor import WorkerExecutor

    monkeypatch.setattr(pipeline_registry, "get", lambda name: _skip_first_then("later_step"))
    recorded = {}

    async def _capture(execution_id, job_id, stats):
        recorded["stats"] = stats

    executor = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(executor, "_update_job_in_db", _capture)

    events = []

    class _Events(logging.Handler):
        def emit(self, record):
            events.append(getattr(record, "event", None))

    # Directly on the worker's logger: the 'reflowfy' logger may not propagate.
    worker_logger = logging.getLogger("reflowfy.worker.executor")
    handler, old_level = _Events(), worker_logger.level
    worker_logger.addHandler(handler)
    worker_logger.setLevel(logging.INFO)
    try:
        ok = await executor.execute_job(
            {
                "schema_version": 2,
                "execution_id": "e",
                "job_id": "j",
                "pipeline_name": "p",
                "source": {
                    "type": "StaticSource",
                    "config": {"records": [{"id": 1}, {"id": 2}]},
                },
                "metadata": {"runtime_params": {}},
            }
        )
    finally:
        worker_logger.removeHandler(handler)
        worker_logger.setLevel(old_level)

    stats = recorded["stats"]
    assert ok is True
    assert stats.skip_reason == "nothing useful"
    assert stats.records_skipped == 1
    assert stats.record_skip_reasons == {"bad id": 1}
    assert "records_skipped" in events and "job_dropped" in events
    assert events.index("records_skipped") < events.index("job_dropped")
