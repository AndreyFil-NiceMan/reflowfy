from reflowfy.worker.executor import WorkerExecutor


class _FakeDest:
    def __init__(self, sink): self.sink = sink
    async def health_check(self): return True
    async def send_with_retry(self, records, params): self.sink.extend(records)


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
        "schema_version": 2, "execution_id": "e", "job_id": "j",
        "pipeline_name": "p",
        "source": {"type": "StaticSource", "config": {"records": [{"id": 1}, {"id": 2}]}},
        "metadata": {"batch_id": "b", "created_at": "t", "batch_number": 1,
                     "total_batches": 1, "retry_count": 0, "is_retry": False,
                     "runtime_params": {}, "source_metadata": None},
    }
    ok = await ex.execute_job(payload)
    assert ok is True
    assert captured == [{"id": 1}, {"id": 2}]
