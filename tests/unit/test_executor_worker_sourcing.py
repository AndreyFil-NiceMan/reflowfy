from reflowfy.worker.executor import WorkerExecutor


class _FakeDest:
    def __init__(self, sink):
        self.sink = sink

    async def health_check(self):
        return True

    async def send_with_retry(self, records, params):
        self.sink.extend(records)


class _FakePipeline:
    name = "p"

    def define_transformations(self, records, params):
        return []

    def define_destination(self, records, params):
        return _FakeDest([])


async def _async_noop(*a, **k):
    return None


async def test_worker_fetches_static_source_and_runs(monkeypatch):
    captured = []
    pipe = _FakePipeline()
    pipe.define_destination = lambda records, params: _FakeDest(captured)  # noqa: E731

    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipe)

    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_update_job_in_db", _async_noop)

    payload = {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "p",
        "source": {"type": "StaticSource", "config": {"records": [{"id": 1}, {"id": 2}]}},
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
    ok = await ex.execute_job(payload)
    assert ok is True
    assert captured == [{"id": 1}, {"id": 2}]


async def test_worker_applies_transformations(monkeypatch):
    captured = []

    class _Pipe:
        name = "p"

        def define_transformations(self, records, params):
            return []  # not used by the iterative runner path below

        def define_destination(self, records, params):
            return _FakeDest(captured)

    # A pipeline whose iterative transform uppercases the "v" field.
    from reflowfy.execution import transformation_runner

    def _fake_iter(pipeline, records, runtime_params):
        out = [{**r, "v": r["v"].upper()} for r in records]
        return out, [("upper", 0.0)]

    monkeypatch.setattr(transformation_runner, "apply_transformations_iteratively", _fake_iter)
    # executor.py imported the symbol directly, so patch it there too:
    from reflowfy.worker import executor as executor_mod

    monkeypatch.setattr(executor_mod, "apply_transformations_iteratively", _fake_iter)

    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: _Pipe())

    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_update_job_in_db", _async_noop)

    payload = {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "p",
        "source": {"type": "StaticSource", "config": {"records": [{"v": "a"}, {"v": "b"}]}},
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
    ok = await ex.execute_job(payload)
    assert ok is True
    assert captured == [{"v": "A"}, {"v": "B"}]


async def test_worker_empty_records_short_circuits(monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: _FakePipeline())
    ex = WorkerExecutor(database_url="postgresql://x/y")
    updated = {}

    async def _capture(execution_id, job_id, stats):
        updated["called"] = True

    monkeypatch.setattr(ex, "_update_job_in_db", _capture)

    payload = {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "p",
        "source": {"type": "StaticSource", "config": {"records": []}},
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
    ok = await ex.execute_job(payload)
    assert ok is True
    assert updated.get("called") is True  # status still recorded for the no-op job


async def test_worker_missing_pipeline_fails(monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: None)
    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_update_job_in_db", _async_noop)

    payload = {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "missing",
        "source": {"type": "StaticSource", "config": {"records": [{"v": "a"}]}},
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
    ok = await ex.execute_job(payload)
    assert ok is False  # execute_job catches the RuntimeError and reports failure


async def test_worker_normalizes_non_json_records(monkeypatch):
    import datetime as _dt

    captured = []

    class _Pipe:
        name = "p"

        def define_transformations(self, records, params):
            return []

        def define_destination(self, records, params):
            return _FakeDest(captured)

    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: _Pipe())

    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_update_job_in_db", _async_noop)

    dt = _dt.datetime(2026, 6, 24, 10, 0, 0)
    payload = {
        "schema_version": 2,
        "execution_id": "e",
        "job_id": "j",
        "pipeline_name": "p",
        "source": {"type": "StaticSource", "config": {"records": [{"ts": dt, "n": 1}]}},
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
    ok = await ex.execute_job(payload)
    assert ok is True
    # datetime must be stringified so downstream json.dumps in destinations won't crash
    assert captured == [{"ts": str(dt), "n": 1}]
    import json

    json.dumps(captured)  # must not raise
