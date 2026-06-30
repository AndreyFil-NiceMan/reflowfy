from reflowfy.worker.executor import WorkerExecutor


class _FakeDest:
    def __init__(self, sink, healthy=True):
        self.sink = sink
        self.healthy = healthy
        self.closed = False

    async def health_check(self):
        return self.healthy

    async def send_with_retry(self, records, params):
        self.sink.extend(records)

    async def close(self):
        self.closed = True


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


def _static_payload():
    return {
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


async def test_worker_closes_destination_on_success(monkeypatch):
    """Regression: the per-job destination must be closed so Kafka producers
    (started during health_check/send) don't leak a connection per job."""
    dest = _FakeDest([])
    pipe = _FakePipeline()
    pipe.define_destination = lambda records, params: dest  # noqa: E731

    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipe)

    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_update_job_in_db", _async_noop)

    ok = await ex.execute_job(_static_payload())
    assert ok is True
    assert dest.closed is True


async def test_worker_closes_destination_when_health_check_fails(monkeypatch):
    """Even when health_check fails (a producer was already started), the
    destination must still be closed."""
    dest = _FakeDest([], healthy=False)
    pipe = _FakePipeline()
    pipe.define_destination = lambda records, params: dest  # noqa: E731

    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: pipe)

    ex = WorkerExecutor(database_url="postgresql://x/y")
    monkeypatch.setattr(ex, "_update_job_in_db", _async_noop)

    ok = await ex.execute_job(_static_payload())
    assert ok is False
    assert dest.closed is True


async def test_worker_applies_transformations(monkeypatch):
    captured = []

    class _Pipe:
        name = "p"

        def define_transformations(self, records, params):
            return []  # not used by the iterative runner path below

        def define_destination(self, records, params):
            return _FakeDest(captured)

    # A pipeline whose iterative transform uppercases the "v" field.
    # The shared core (reflowfy.execution.job_runner) binds the symbol, so patch it there.
    from reflowfy.execution import job_runner

    def _fake_iter(pipeline, records, runtime_params):
        out = [{**r, "v": r["v"].upper()} for r in records]
        return out, [("upper", 0.0)]

    monkeypatch.setattr(job_runner, "apply_transformations_iteratively", _fake_iter)

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


async def test_v2_payload_survives_kafka_json_wire(monkeypatch):
    """Full wire path: manager normalize -> KafkaDispatcher json.dumps ->
    consumer json.loads -> worker execute_job. No other test/E2E covers this."""
    import datetime as _dt
    import json as _json

    from reflowfy.core.serialization import to_json_safe
    from reflowfy.reflow_manager.pipeline_runner import build_job_payload
    from reflowfy.sources.static import StaticSource

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

    ts = _dt.datetime(2026, 6, 24, 10, 0, 0)
    sub = StaticSource([{"ts": ts, "n": 1}])
    payload = build_job_payload(
        execution_id="e",
        job_id="j",
        pipeline_name="p",
        sub_source=sub,
        metadata={
            "batch_id": "b",
            "created_at": "t",
            "batch_number": 1,
            "total_batches": 1,
            "retry_count": 0,
            "is_retry": False,
            "runtime_params": {},
            "source_metadata": None,
        },
    )

    # Manager normalizes the payload (datetime -> str), KafkaDispatcher does a
    # plain json.dumps (no default=str), the consumer json.loads the bytes.
    normalized = to_json_safe(payload)
    wire_bytes = _json.dumps(normalized).encode("utf-8")  # KafkaDispatcher
    decoded = _json.loads(wire_bytes.decode("utf-8"))  # KafkaJobConsumer

    assert decoded["schema_version"] == 2
    ok = await ex.execute_job(decoded)
    assert ok is True
    # datetime arrived at the destination as a JSON-safe string, end to end
    assert captured == [{"ts": str(ts), "n": 1}]
