# Worker-Side Sourcing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ReflowManager a pure planner that ships small `source` descriptors, and the worker the executor of the whole `source → transform → destination` flow — fetching its own slice of data.

**Architecture:** A slice is a narrowed source. `BaseSource.split()` yields one self-contained sub-source per job (metadata-only, no bulk fetch). The manager serializes each as `{type, config}` into a v2 job message; the worker reconstructs it via `SourceFactory`, fetches, then resolves transformations and the destination dynamically against the real records. Transformations/destination are **not** on the wire.

**Tech Stack:** Python 3, `uv`, pytest (`asyncio_mode=auto`), SQLAlchemy, elasticsearch-py, boto3, httpx, aiokafka.

**Spec:** `docs/superpowers/specs/2026-06-24-worker-side-sourcing-design.md`

**Conventions:** Run everything with `uv run`. Line length 100. Commit messages have **no** Claude co-author trailer. Branch: `worker-side-sourcing` (already created).

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `reflowfy/sources/base.py` | `BaseSource.split()` default + `registry_type` | Modify |
| `reflowfy/factories/source_factory.py` | `(type,config)→instance`, register all built-ins, `serialize()` | Modify |
| `reflowfy/sources/static.py` | `split()` → `[self]` | Modify |
| `reflowfy/sources/mock.py` | `split()` → narrowed `MockSource`s by `batch_size` | Modify |
| `reflowfy/sources/sql.py` | `split()` → bounded id-range / offset `SqlSource`s | Modify |
| `reflowfy/sources/api.py` | `split()` → id-subset `IDBasedAPISource`s | Modify |
| `reflowfy/sources/elastic.py` | `split()` → PIT + sliced-scroll `ElasticSource`s; `fetch` honors slice | Modify |
| `reflowfy/sources/s3.py` | `split()` → explicit-key `S3Source`s; `fetch` honors `keys` | Modify |
| `reflowfy/core/execution_context.py` | flatten metadata (drop nested `metadata.metadata`) | Modify |
| `reflowfy/reflow_manager/pipeline_runner.py` | manager plans slices, builds v2 payload, new job-id, removes lag check | Modify |
| `reflowfy/worker/executor.py` | rebuild source, fetch, dynamic transform+dest; delete frozen fallback | Modify |
| `reflowfy/worker/consumer.py` | reject on `schema_version` mismatch | Modify |
| `tests/unit/test_source_factory.py` | round-trip + serialize | Create |
| `tests/unit/test_source_split.py` | per-source `split()` | Create |
| `tests/unit/test_pipeline_runner_payload.py` | v2 payload, job-id stability | Create |
| `tests/unit/test_executor_worker_sourcing.py` | worker rebuild→fetch→run | Create |

**Phasing:** Phase 1 (Tasks 1–4) delivers a working vertical slice — the default `split()` (one job per base source) + `StaticSource`, end-to-end through manager and worker. Phases 2–3 (Tasks 5–9) add real parallel `split()` per source type. Each task is independently testable.

---

## Phase 1 — Foundation + working vertical slice

### Task 1: Standardize the source serialization contract

**Files:**
- Modify: `reflowfy/sources/base.py` (add `registry_type` property)
- Modify: `reflowfy/factories/source_factory.py:33-88`
- Test: `tests/unit/test_source_factory.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_source_factory.py
"""Round-trip (type, config) -> instance for every built-in source."""

from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.static import StaticSource
from reflowfy.sources.mock import MockSource
from reflowfy.sources.api import IDBasedAPISource
from reflowfy.sources.sql import SqlSource


def test_serialize_returns_type_and_config():
    src = StaticSource([1, 2, 3])
    serialized = SourceFactory.serialize(src)
    assert serialized == {"type": "StaticSource", "config": {"records": [1, 2, 3]}}


def test_roundtrip_static_source():
    src = StaticSource([{"a": 1}])
    rebuilt = SourceFactory.create("StaticSource", src.config)
    assert isinstance(rebuilt, StaticSource)
    assert rebuilt.config == src.config


def test_roundtrip_mock_source():
    src = MockSource(data=[{"x": 1}], batch_size=5)
    rebuilt = SourceFactory.create("MockSource", src.config)
    assert isinstance(rebuilt, MockSource)
    assert rebuilt.config == src.config


def test_roundtrip_api_source():
    src = IDBasedAPISource(base_url="http://h", endpoint_template="/u/{id}", ids=[1, 2])
    rebuilt = SourceFactory.create("IDBasedAPISource", src.config)
    assert isinstance(rebuilt, IDBasedAPISource)
    assert rebuilt.config == src.config


def test_roundtrip_sql_source():
    src = SqlSource(connection_url="sqlite://", query="SELECT 1", id_column="id")
    rebuilt = SourceFactory.create("SqlSource", src.config)
    assert isinstance(rebuilt, SqlSource)
    assert rebuilt.config == src.config


def test_unknown_type_raises():
    import pytest
    with pytest.raises(ValueError, match="Unknown source type"):
        SourceFactory.create("NopeSource", {})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_factory.py -v`
Expected: FAIL — `SourceFactory.serialize` missing; `create("StaticSource", ...)` raises Unknown type (StaticSource not registered) and old `create` calls `cls(config)` positionally.

- [ ] **Step 3: Add `registry_type` to `BaseSource`**

In `reflowfy/sources/base.py`, add inside `class BaseSource` (after `__init__`):

```python
    @property
    def registry_type(self) -> str:
        """Stable type name used to serialize/reconstruct this source."""
        return self.__class__.__name__
```

- [ ] **Step 4: Rewrite the factory to reconstruct via `cls(**config)` and register all built-ins**

Replace `create` and `_register_builtin_sources` in `reflowfy/factories/source_factory.py`:

```python
    @classmethod
    def create(cls, type_name: str, config: Dict[str, Any]) -> BaseSource:
        """Reconstruct a source from its registry type name and config dict.

        Every built-in source stores ``config`` as exactly its constructor
        kwargs, so reconstruction is a uniform ``cls(**config)``.
        """
        if type_name not in cls._registry:
            available = ", ".join(sorted(cls._registry)) if cls._registry else "none"
            raise ValueError(
                f"Unknown source type: '{type_name}'. Available types: {available}"
            )
        return cls._registry[type_name](**config)

    @classmethod
    def serialize(cls, source: BaseSource) -> Dict[str, Any]:
        """Serialize a source instance to a ``{type, config}`` descriptor."""
        return {"type": source.registry_type, "config": source.config}
```

Replace `_register_builtin_sources` with registration by class name (keep import guards):

```python
def _register_builtin_sources() -> None:
    """Register built-in source types by class name."""
    from reflowfy.sources.static import StaticSource
    from reflowfy.sources.mock import MockSource

    SourceFactory.register("StaticSource", StaticSource)
    SourceFactory.register("MockSource", MockSource)

    for module, classname in (
        ("reflowfy.sources.elastic", "ElasticSource"),
        ("reflowfy.sources.sql", "SqlSource"),
        ("reflowfy.sources.s3", "S3Source"),
        ("reflowfy.sources.api", "IDBasedAPISource"),
    ):
        try:
            mod = __import__(module, fromlist=[classname])
            SourceFactory.register(classname, getattr(mod, classname))
        except ImportError:
            pass  # optional dependency (boto3, elasticsearch) not installed
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_factory.py -v`
Expected: PASS (6 passed).

- [ ] **Step 6: Commit**

```bash
git add reflowfy/sources/base.py reflowfy/factories/source_factory.py tests/unit/test_source_factory.py
git commit -m "feat(sources): uniform (type,config) round-trip via SourceFactory"
```

---

### Task 2: `BaseSource.split()` default + `StaticSource.split()`

**Files:**
- Modify: `reflowfy/sources/base.py`
- Modify: `reflowfy/sources/static.py`
- Test: `tests/unit/test_source_split.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_source_split.py
from reflowfy.sources.static import StaticSource
from reflowfy.sources.mock import MockSource


def test_default_split_yields_self():
    src = MockSource(data=[{"a": 1}], batch_size=1000)
    subs = list(src.split({}))
    assert subs == [src]  # default: one job, identity


def test_static_split_yields_self():
    src = StaticSource([1, 2, 3])
    subs = list(src.split({}))
    assert len(subs) == 1
    assert subs[0].config == {"records": [1, 2, 3]}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: FAIL — `BaseSource` has no `split` attribute.

- [ ] **Step 3: Add the default `split()` to `BaseSource`**

In `reflowfy/sources/base.py`, add to `class BaseSource` (after `get_runtime_parameters`):

```python
    def split(self, runtime_params: Dict[str, Any]) -> "Iterator[BaseSource]":
        """Manager-side planning. Yield one narrowed source per job.

        Metadata-only and cheap — MUST NOT fetch bulk data. The default
        yields ``self`` (a single job; the worker fetches the whole source).
        Sources that can shard cheaply override this to yield N sub-sources,
        each with its slice baked into ``config``.
        """
        yield self
```

Ensure `Iterator` is imported at the top of `base.py` (it already imports `from typing import ... Iterator ...`).

- [ ] **Step 4: Override `split()` on `StaticSource`**

In `reflowfy/sources/static.py`, add to `class StaticSource`:

```python
    def split(self, runtime_params: Dict[str, Any]) -> Iterator["StaticSource"]:
        """A static source is already one job — its records are in config."""
        yield self
```

Add `Iterator` to the `typing` import line in `static.py` (it imports `Iterator` already).

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS (2 passed).

- [ ] **Step 6: Commit**

```bash
git add reflowfy/sources/base.py reflowfy/sources/static.py tests/unit/test_source_split.py
git commit -m "feat(sources): add BaseSource.split() planning hook (default one job)"
```

---

### Task 3: Flatten job metadata (drop nested `metadata.metadata`)

**Files:**
- Modify: `reflowfy/core/execution_context.py:43-56`
- Test: `tests/unit/test_execution_context.py` (add cases)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_execution_context.py  (add)
from reflowfy.core.execution_context import (
    ExecutionContext,
    build_flat_runtime_params_from_metadata,
)


def test_to_dict_has_no_nested_metadata_key():
    ctx = ExecutionContext(execution_id="e1", pipeline_name="p", runtime_params={"x": 1})
    d = ctx.to_dict()
    assert "metadata" not in d           # nested dead field removed
    assert d["execution_id"] == "e1"
    assert d["runtime_params"] == {"x": 1}


def test_build_flat_params_still_works_without_nested_metadata():
    ctx = ExecutionContext(execution_id="e1", pipeline_name="p", runtime_params={"x": 1})
    flat = build_flat_runtime_params_from_metadata(ctx.to_dict())
    assert flat["x"] == 1
    assert flat["execution_id"] == "e1"
    assert flat["pipeline_name"] == "p"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_execution_context.py -v`
Expected: FAIL — `to_dict()` currently includes `"metadata"`.

- [ ] **Step 3: Remove the nested `metadata` field from `to_dict()`**

In `reflowfy/core/execution_context.py`, edit `ExecutionContext.to_dict` (`:43-56`) — delete the `"metadata": self.metadata,` line:

```python
    def to_dict(self) -> Dict[str, Any]:
        """Serialize context for job metadata."""
        return {
            "execution_id": self.execution_id,
            "batch_id": self.batch_id,
            "pipeline_name": self.pipeline_name,
            "runtime_params": self.runtime_params,
            "created_at": self.created_at.isoformat(),
            "batch_number": self.batch_number,
            "total_batches": self.total_batches,
            "retry_count": self.retry_count,
            "is_retry": self.is_retry,
        }
```

`build_flat_runtime_params_from_metadata` (`:111`) never reads `metadata["metadata"]`, so it needs no change.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_execution_context.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/core/execution_context.py tests/unit/test_execution_context.py
git commit -m "refactor(context): drop dead nested metadata.metadata from job context"
```

---

### Task 4: Manager plans slices + worker executes v2 message (vertical slice)

This task converts both `pipeline_runner` paths to ship v2 `source` descriptors via `split()`, rewrites `generate_job_id` to hash the descriptor, and rewrites the worker to rebuild→fetch→dynamic transform→dynamic destination. With the default `split()` this already works end-to-end for id-based pipelines and `StaticSource`.

**Files:**
- Modify: `reflowfy/reflow_manager/pipeline_runner.py:36-54, 456-521, 746-810, 947-1026, 573-578`
- Modify: `reflowfy/worker/executor.py:103-224, 226-?? (delete frozen), 317-338`
- Modify: `reflowfy/worker/consumer.py:122-153`
- Test: `tests/unit/test_pipeline_runner_payload.py` (create), `tests/unit/test_executor_worker_sourcing.py` (create)

#### 4a — `generate_job_id` hashes the source descriptor

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pipeline_runner_payload.py
from reflowfy.reflow_manager.pipeline_runner import generate_job_id


def test_job_id_stable_for_same_slice():
    src = {"type": "StaticSource", "config": {"records": [1, 2]}}
    a = generate_job_id("p", source=src, current_ids=[1, 2])
    b = generate_job_id("p", source=src, current_ids=[1, 2])
    assert a == b


def test_job_id_differs_for_different_slice():
    a = generate_job_id("p", source={"type": "S", "config": {"lo": 0}}, current_ids=None)
    b = generate_job_id("p", source={"type": "S", "config": {"lo": 1}}, current_ids=None)
    assert a != b
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_pipeline_runner_payload.py::test_job_id_stable_for_same_slice -v`
Expected: FAIL — `generate_job_id` has signature `(pipeline_name, transformations, records, source_metadata)`.

- [ ] **Step 3: Rewrite `generate_job_id`**

Replace `generate_job_id` (`pipeline_runner.py:36-54`):

```python
def generate_job_id(
    pipeline_name: str,
    source: Dict[str, Any],
    current_ids: Optional[list] = None,
) -> str:
    """Return a deterministic SHA256 job ID derived from the source slice.

    Used when enable_duplicate_jobs=False. The narrowed source ``config``
    encodes the slice (id-range, scroll/PIT id, key list, or — for
    StaticSource — the records themselves), so identical slices produce the
    same ID across runs. Volatile date/time keys are stripped from config.
    """
    stable = {
        "pipeline_name": pipeline_name,
        "source": {
            "type": source.get("type"),
            "config": _filter_volatile_keys(source.get("config", {}) or {}),
        },
        "current_ids": current_ids,
    }
    content = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()
```

(`_filter_volatile_keys` and `_DATE_KEY_PATTERNS` above it are unchanged and still used.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_pipeline_runner_payload.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add reflowfy/reflow_manager/pipeline_runner.py tests/unit/test_pipeline_runner_payload.py
git commit -m "feat(manager): hash job-id from source slice descriptor"
```

#### 4b — Manager builds v2 payloads via `split()` (both paths)

- [ ] **Step 6: Write the failing test**

```python
# tests/unit/test_pipeline_runner_payload.py  (add)
from reflowfy.reflow_manager.pipeline_runner import build_job_payload
from reflowfy.sources.static import StaticSource


def test_build_job_payload_v2_shape():
    sub = StaticSource([101, 102])
    payload = build_job_payload(
        execution_id="e1",
        job_id="j1",
        pipeline_name="user_sync",
        sub_source=sub,
        metadata={"batch_id": "b1", "created_at": "t", "batch_number": 1,
                  "total_batches": 1, "retry_count": 0, "is_retry": False,
                  "runtime_params": {"env": "prod"}, "current_ids": [101, 102],
                  "source_metadata": None},
    )
    assert payload["schema_version"] == 2
    assert payload["source"] == {"type": "StaticSource", "config": {"records": [101, 102]}}
    assert "records" not in payload
    assert "transformations" not in payload
    assert "destination" not in payload
    assert payload["metadata"]["current_ids"] == [101, 102]
```

- [ ] **Step 7: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_pipeline_runner_payload.py::test_build_job_payload_v2_shape -v`
Expected: FAIL — `build_job_payload` does not exist.

- [ ] **Step 8: Add `SCHEMA_VERSION` + `build_job_payload` helper**

Near the top of `pipeline_runner.py` (after the imports/constants), add:

```python
from reflowfy.factories.source_factory import SourceFactory

JOB_SCHEMA_VERSION = 2
```

Add this module-level helper (above `class PipelineRunner`):

```python
def build_job_payload(
    execution_id: str,
    job_id: str,
    pipeline_name: str,
    sub_source: Any,
    metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """Assemble the v2 worker job message for one narrowed sub-source."""
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "execution_id": execution_id,
        "job_id": job_id,
        "pipeline_name": pipeline_name,
        "source": SourceFactory.serialize(sub_source),
        "metadata": metadata,
    }
```

- [ ] **Step 9: Rewrite the AbstractPipeline Phase-1 loop**

In `_run_pipeline_jobs`, replace the per-`source_job` body (`pipeline_runner.py:456-532`, the `for source_job in pipeline.source.split_jobs(...)` block) with planning via `split()`:

```python
        base_source = pipeline.source
        for sub_source in base_source.split(enriched_params):
            context.batch_number = batch_number
            context_dict = context.to_dict()
            context_dict["runtime_params"] = dict(enriched_params)
            metadata = {**context_dict, "source_metadata": None}

            source_descriptor = SourceFactory.serialize(sub_source)
            if enable_duplicate_jobs:
                job_id = str(uuid.uuid4())
            else:
                job_id = generate_job_id(pipeline_name, source_descriptor, current_ids=None)
                if self.job_manager.get_job(job_id):
                    dedup_count += 1
                    continue

            job_payload = build_job_payload(
                execution_id, job_id, pipeline_name, sub_source, metadata
            )
            job_payload = self._serialize_for_json(job_payload)

            self.job_manager.create_job(
                execution_id=execution_id,
                job_id=job_id,
                job_payload=job_payload,
                batch_number=batch_number,
            )
            current_job_ids.append(job_id)
            job_count += 1
            if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                batch_number += 1
                current_job_ids = []
```

Note: `pipeline.source` is the AbstractPipeline's resolved source (already used elsewhere). The manager no longer calls `define_transformations`/`t.apply`/`define_destination` here — delete those lines.

- [ ] **Step 10: Rewrite the IdBasedPipeline Phase-1 loop**

In `_run_id_based_pipeline_jobs`, replace the per-`source_job` body (`pipeline_runner.py:746-821`) so that, for each id-batch, it plans the resolved source via `split()`:

```python
            resolved = pipeline.resolve_for_ids(params, ids_batch)
            source = resolved["source"]
            batch_params = resolved.get("batch_params", params)

            for sub_source in source.split(batch_params):
                context.batch_number = batch_number
                context_dict = context.to_dict()
                context_dict["runtime_params"] = dict(batch_params)
                metadata = {**context_dict, "current_ids": ids_batch, "source_metadata": None}

                source_descriptor = SourceFactory.serialize(sub_source)
                if enable_duplicate_jobs:
                    job_id = str(uuid.uuid4())
                else:
                    job_id = generate_job_id(pipeline_name, source_descriptor, current_ids=ids_batch)
                    if self.job_manager.get_job(job_id):
                        dedup_count += 1
                        continue

                job_payload = build_job_payload(
                    execution_id, job_id, pipeline_name, sub_source, metadata
                )
                job_payload = self._serialize_for_json(job_payload)

                self.job_manager.create_job(
                    execution_id=execution_id,
                    job_id=job_id,
                    job_payload=job_payload,
                    batch_number=batch_number,
                )
                current_job_ids.append(job_id)
                job_count += 1
                if len(current_job_ids) >= CHECKPOINT_BATCH_SIZE:
                    batch_number += 1
                    current_job_ids = []
```

- [ ] **Step 11: Remove the manager-side lag health check**

Delete `_check_destination_lag_health` (`pipeline_runner.py:947-1026`) and its call site (the `if job_count > 0:` lag block at `:573-578`). The worker's `destination.health_check()` covers it.

- [ ] **Step 12: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_pipeline_runner_payload.py -v`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add reflowfy/reflow_manager/pipeline_runner.py tests/unit/test_pipeline_runner_payload.py
git commit -m "feat(manager): plan slices via split(), emit v2 payloads, drop manager lag check"
```

#### 4c — Worker rebuilds the source, fetches, runs dynamic chain

- [ ] **Step 14: Write the failing test**

```python
# tests/unit/test_executor_worker_sourcing.py
from reflowfy.worker.executor import WorkerExecutor


class _FakePipeline:
    name = "p"
    def define_transformations(self, records, params):
        return []
    def define_destination(self, records, params):
        return _FakeDest(records)


class _FakeDest:
    def __init__(self, sink): self.sink = sink
    async def health_check(self): return True
    async def send_with_retry(self, records, params): self.sink.extend(records)


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


async def _async_noop(*a, **k):
    return None
```

- [ ] **Step 15: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_executor_worker_sourcing.py -v`
Expected: FAIL — `execute_job` reads `job_payload["records"]`, not `source`.

- [ ] **Step 16: Rewrite `execute_job` to source from the descriptor**

In `reflowfy/worker/executor.py`, replace the "Extract job data" + transform block of `execute_job` (`:120-165`) so it fetches from the source descriptor first, then resolves transforms/destination dynamically:

```python
        try:
            metadata = job_payload.get("metadata", {})
            source_descriptor = job_payload.get("source") or {}

            runtime_params = build_flat_runtime_params_from_metadata(metadata)

            # 1. Rebuild the planned source and fetch THIS slice (worker-side).
            from reflowfy.factories.source_factory import SourceFactory

            source = SourceFactory.create(
                source_descriptor["type"], source_descriptor["config"]
            )
            records = source.fetch(runtime_params)
            stats.records_input = len(records)

            if not records:
                print(f"⚠️  Job {job_id}: No records to process")
                stats.success = True
                stats.records_output = 0
                stats.end_time = time.time()
                await self._update_job_in_db(execution_id, job_id, stats)
                return True

            print(f"🔄 Processing job {job_id}: {len(records)} records")

            # 2. Dynamic, content-aware transform resolution (pipeline required).
            pipeline = pipeline_registry.get(_pipeline_name)
            if pipeline is None:
                raise RuntimeError(
                    f"Pipeline '{_pipeline_name}' not found in worker registry; "
                    "worker-side sourcing requires the pipeline to be discoverable."
                )
            transformed_records, applied = apply_transformations_iteratively(
                pipeline, records, runtime_params
            )
            for name, duration in applied:
                stats.transformation_times[name] = round(duration, 3)
                print(f"  ✓ {name}")

            stats.records_output = len(transformed_records)

            # 3. Dynamic destination resolution against real records.
            destination = pipeline.define_destination(transformed_records, runtime_params)
```

Keep the rest of `execute_job` from the health-check onward (`:182-208`) unchanged — it already does `destination.health_check()` and `send_with_retry`.

- [ ] **Step 17: Delete the frozen-fallback and unused destination builder**

Delete `_apply_frozen_transformations` (`executor.py:226-~265`) and `_create_destination` (`:317-338`). Remove now-unused imports at the top of `executor.py`: `ApiDestination`, `ConsoleDestination`, `KafkaDestination`, and `transformation_registry` (keep `apply_transformations_iteratively`, `build_flat_runtime_params_from_metadata`, `pipeline_registry`, `Job`).

- [ ] **Step 18: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_executor_worker_sourcing.py -v`
Expected: PASS.

- [ ] **Step 19: Reject mismatched schema versions in the consumer**

In `reflowfy/worker/consumer.py._process_message`, after `job_payload = json.loads(...)` (`:132`), add:

```python
            version = job_payload.get("schema_version")
            if version != 2:
                print(f"❌ Unsupported job schema_version={version!r}; skipping")
                await self.consumer.commit()
                return
```

- [ ] **Step 20: Run the full unit suite**

Run: `uv run pytest tests/unit/ -v`
Expected: PASS. Fix any test that still references removed payload keys (`records`, `transformations`, `transformation_specs`, `destination`) or `_apply_frozen_transformations` / `_create_destination` by updating them to the v2 shape.

- [ ] **Step 21: Commit**

```bash
git add reflowfy/worker/executor.py reflowfy/worker/consumer.py tests/unit/test_executor_worker_sourcing.py
git commit -m "feat(worker): source from v2 descriptor, dynamic transform+destination, drop frozen fallback"
```

---

## Phase 2 — Real parallel `split()` per source type

Each task makes a source shard cheaply (metadata-only) and makes its `fetch()` honor the slice baked into `config`. Each is independent; ship in any order.

### Task 5: `MockSource.split()` (used by tests/E2E for deterministic fan-out)

**Files:** Modify `reflowfy/sources/mock.py`; Test `tests/unit/test_source_split.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_source_split.py  (add)
def test_mock_split_by_batch_size():
    from reflowfy.sources.mock import MockSource
    src = MockSource(data=[{"i": i} for i in range(25)], batch_size=10)
    subs = list(src.split({}))
    assert len(subs) == 3
    assert [len(s.config["data"]) for s in subs] == [10, 10, 5]
    assert subs[0].fetch({}) == [{"i": i} for i in range(10)]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_split.py::test_mock_split_by_batch_size -v`
Expected: FAIL — `MockSource.split` falls back to default (one job).

- [ ] **Step 3: Implement `MockSource.split()`**

Add to `class MockSource` in `reflowfy/sources/mock.py`:

```python
    def split(self, runtime_params: Dict[str, Any]) -> Iterator["MockSource"]:
        """Slice the in-memory data into batch_size-sized MockSources."""
        data = self.config["data"]
        size = self.config["batch_size"]
        for i in range(0, len(data), size):
            yield MockSource(data=data[i : i + size], batch_size=size)
```

Add `Iterator` to the `typing` import line in `mock.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/sources/mock.py tests/unit/test_source_split.py
git commit -m "feat(sources): MockSource.split() shards in-memory data by batch_size"
```

---

### Task 6: `SqlSource.split()` — bounded id-range / offset windows (no bulk fetch)

**Files:** Modify `reflowfy/sources/sql.py`; Test `tests/unit/test_source_split.py`.

The narrowed `SqlSource` wraps the base query with a bounded `WHERE id >= lo AND id < hi`
(or `LIMIT/OFFSET`), so its plain `fetch()` returns only that window.

- [ ] **Step 1: Write the failing test** (mock the engine so no DB is needed)

```python
# tests/unit/test_source_split.py  (add)
def test_sql_split_id_range(monkeypatch):
    from reflowfy.sources.sql import SqlSource
    src = SqlSource(connection_url="sqlite://", query="SELECT * FROM t",
                    id_column="id", batch_size=100)

    class _Row:
        def __getitem__(self, i): return (0, 250)[i]
    class _Result:
        def fetchone(self): return _Row()
    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): return _Result()
    monkeypatch.setattr(src, "_get_engine", lambda: type("E", (), {"connect": lambda self: _Conn()})())

    subs = list(src.split({}))
    bounds = [(s.config["slice"]["lo"], s.config["slice"]["hi"]) for s in subs]
    assert bounds == [(0, 100), (100, 200), (200, 300)]
    assert "BETWEEN" in subs[0].config["query"] or ">=" in subs[0].config["query"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_split.py::test_sql_split_id_range -v`
Expected: FAIL — no `split` override; no `slice` in config.

- [ ] **Step 3: Implement `SqlSource.split()` + slice-aware construction**

Add to `class SqlSource` in `reflowfy/sources/sql.py`:

```python
    def split(self, runtime_params: Dict[str, Any]) -> Iterator["SqlSource"]:
        """Plan id-range windows using MIN/MAX only — no row fetch.

        Falls back to a single job (yield self) when no id_column is set,
        since offset windows would require counting/among-pages coordination.
        """
        resolved = self.resolve_parameters(runtime_params) or self.config
        id_column = resolved.get("id_column")
        if not id_column:
            yield self
            return

        base_query = resolved["query"]
        batch_size = resolved.get("batch_size", 1000)
        engine = self._get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(f"SELECT MIN({id_column}) AS lo, MAX({id_column}) AS hi "
                     f"FROM ({base_query}) AS sub")
            ).fetchone()
        if not row or row[0] is None:
            return
        lo, hi = int(row[0]), int(row[1])

        cur = lo
        while cur <= hi:
            nxt = cur + batch_size
            windowed = (
                f"SELECT * FROM ({base_query}) AS sub "
                f"WHERE {id_column} >= {cur} AND {id_column} < {nxt}"
            )
            sub = SqlSource(
                connection_url=resolved["connection_url"],
                query=windowed,
                id_column=None,                       # window is already bounded
                time_column=resolved.get("time_column"),
                batch_size=batch_size,
            )
            sub.config["slice"] = {"lo": cur, "hi": nxt}
            yield sub
            cur = nxt
```

The narrowed source has `id_column=None`, so its own `fetch()` runs the bounded query directly. `slice` is advisory metadata carried in config (it does not affect `fetch`, and reconstruction tolerates it because `SqlSource` accepts `**engine_kwargs`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS.

- [ ] **Step 5: Verify round-trip still holds for a narrowed SqlSource**

Run: `uv run pytest tests/unit/test_source_factory.py -v`
Expected: PASS (narrowed config reconstructs; `slice` lands in `engine_kwargs`).

- [ ] **Step 6: Commit**

```bash
git add reflowfy/sources/sql.py tests/unit/test_source_split.py
git commit -m "feat(sources): SqlSource.split() plans id-range windows (no bulk fetch)"
```

---

### Task 7: `IDBasedAPISource.split()` — id-subset / page sharding

**Files:** Modify `reflowfy/sources/api.py`; Test `tests/unit/test_source_split.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_source_split.py  (add)
def test_api_split_per_id_mode_groups_ids():
    from reflowfy.sources.api import IDBasedAPISource
    src = IDBasedAPISource(base_url="http://h", endpoint_template="/u/{id}",
                           ids=[1, 2, 3, 4, 5], batch_size=2)
    subs = list(src.split({}))
    assert [s.config["ids"] for s in subs] == [[1, 2], [3, 4], [5]]
    assert all(s.config["base_url"] == "http://h" for s in subs)


def test_api_split_batch_mode_yields_self():
    from reflowfy.sources.api import IDBasedAPISource
    src = IDBasedAPISource(base_url="http://h", endpoint_template="/batch",
                           ids=[1, 2, 3], method="POST", body={"ids": [1, 2, 3]})
    subs = list(src.split({}))
    assert len(subs) == 1  # one batch request -> one job
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_split.py::test_api_split_per_id_mode_groups_ids -v`
Expected: FAIL — no `split` override.

- [ ] **Step 3: Implement `IDBasedAPISource.split()`**

Add to `class IDBasedAPISource` in `reflowfy/sources/api.py`:

```python
    def split(self, runtime_params: Dict[str, Any]) -> Iterator["IDBasedAPISource"]:
        """Per-ID mode: group ids into batch_size chunks, one source each.

        Batch mode (no ``{id}`` in the template) is a single request, so it
        yields one job (self).
        """
        if not self._is_per_id_mode():
            yield self
            return

        ids = self._get_all_ids(runtime_params)
        size = self.config["batch_size"]
        c = self.config
        for i in range(0, len(ids), size):
            yield IDBasedAPISource(
                base_url=c["base_url"], endpoint_template=c["endpoint_template"],
                ids=ids[i : i + size], method=c["method"], headers=c["headers"],
                auth_type=c["auth_type"], auth_token=c["auth_token"],
                batch_size=size, timeout=c["timeout"], response_key=c["response_key"],
                body=c["body"], params=c["params"],
                health_check_enabled=c["health_check_enabled"],
            )
```

Add `Iterator` to the `typing` import in `api.py` (already imports `Iterator`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/sources/api.py tests/unit/test_source_split.py
git commit -m "feat(sources): IDBasedAPISource.split() shards ids per batch_size"
```

---

### Task 8: `S3Source.split()` — explicit key batches + key-aware `fetch()`

**Files:** Modify `reflowfy/sources/s3.py`; Test `tests/unit/test_source_split.py`.

The narrowed `S3Source` carries an explicit `keys` list; `fetch()` reads exactly those keys.

- [ ] **Step 1: Write the failing test** (mock the S3 client)

```python
# tests/unit/test_source_split.py  (add)
def test_s3_split_lists_keys_only(monkeypatch):
    from reflowfy.sources.s3 import S3Source
    src = S3Source(bucket="b", prefix="p/", page_size=2, read_content=False)

    pages = [{"Contents": [{"Key": "p/a", "Size": 1, "ETag": "x",
                            "LastModified": _Dt()},
                           {"Key": "p/b", "Size": 1, "ETag": "y",
                            "LastModified": _Dt()}]},
             {"Contents": [{"Key": "p/c", "Size": 1, "ETag": "z",
                            "LastModified": _Dt()}]}]

    class _Paginator:
        def paginate(self, **k): return iter(pages)
    class _Client:
        def get_paginator(self, n): return _Paginator()
    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    subs = list(src.split({}))
    assert [s.config["keys"] for s in subs] == [["p/a", "p/b"], ["p/c"]]


class _Dt:
    def isoformat(self): return "t"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_split.py::test_s3_split_lists_keys_only -v`
Expected: FAIL — no `split` override; `keys` unsupported.

- [ ] **Step 3: Support a `keys` config in `S3Source` and add `split()`**

In `S3Source.__init__`, the existing `**kwargs` already lets `keys` ride in `config`. Add a key-aware branch at the **top** of `fetch` (`s3.py`, inside `fetch`, right after `resolved_config` is computed):

```python
        explicit_keys = resolved_config.get("keys")
        if explicit_keys:
            records: List[Any] = []
            for key in explicit_keys:
                if resolved_config["read_content"]:
                    content = self._read_object_content(key)
                    records.extend(content if isinstance(content, list) else [content])
                else:
                    records.append({"key": key})
                if limit and len(records) >= limit:
                    return records[:limit]
            return records
```

Add `split()` to `class S3Source`:

```python
    def split(self, runtime_params: Dict[str, Any]) -> Iterator["S3Source"]:
        """List object keys (metadata-only) and yield page_size key batches."""
        resolved = self.resolve_parameters(runtime_params) or self.config
        client = self._get_client()
        page_size = resolved.get("page_size", 1000)
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=resolved["bucket"], Prefix=resolved["prefix"],
            PaginationConfig={"PageSize": page_size},
        )
        c = self.config
        for page in pages:
            keys = [o["Key"] for o in page.get("Contents", []) if self._matches_pattern(o["Key"])]
            if not keys:
                continue
            sub = S3Source(
                bucket=c["bucket"], prefix=c["prefix"], file_pattern=c["file_pattern"],
                page_size=page_size, read_content=c["read_content"],
                content_type=c["content_type"], region_name=c["region_name"],
                endpoint_url=c["endpoint_url"],
                aws_access_key_id=c["aws_access_key_id"],
                aws_secret_access_key=c["aws_secret_access_key"],
            )
            sub.config["keys"] = keys
            yield sub
```

Add `Iterator` to the `typing` import in `s3.py` (already imports `Iterator`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reflowfy/sources/s3.py tests/unit/test_source_split.py
git commit -m "feat(sources): S3Source.split() lists keys (metadata-only), fetch honors explicit keys"
```

---

### Task 9: `ElasticSource.split()` — PIT + sliced scroll + slice-aware `fetch()`

**Files:** Modify `reflowfy/sources/elastic.py`; Test `tests/unit/test_source_split.py`.

Each narrowed `ElasticSource` carries `pit_id` + `slice: {id, max}`; `fetch()` runs that
slice independently. The manager opens the PIT once (cheap) and yields N descriptors.

- [ ] **Step 1: Write the failing test** (mock the ES client)

```python
# tests/unit/test_source_split.py  (add)
def test_elastic_split_opens_pit_and_yields_slices(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource
    src = ElasticSource(url="http://es:9200", index="logs-*",
                        base_query={"query": {"match_all": {}}}, size=1000)
    src.config["num_slices"] = 4

    class _Client:
        def open_point_in_time(self, **k): return {"id": "PIT123"}
    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    subs = list(src.split({}))
    assert len(subs) == 4
    assert all(s.config["pit_id"] == "PIT123" for s in subs)
    assert [s.config["slice"] for s in subs] == [
        {"id": 0, "max": 4}, {"id": 1, "max": 4},
        {"id": 2, "max": 4}, {"id": 3, "max": 4},
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_source_split.py::test_elastic_split_opens_pit_and_yields_slices -v`
Expected: FAIL — no `split` override.

- [ ] **Step 3: Implement `ElasticSource.split()`**

Add to `class ElasticSource` in `reflowfy/sources/elastic.py`:

```python
    def split(self, runtime_params: Dict[str, Any]) -> Iterator["ElasticSource"]:
        """Open a PIT and yield one source per sliced-scroll slice.

        ``num_slices`` (config, default 1) controls parallelism. With 1 slice
        this is a single job. No documents are fetched here.
        """
        resolved = self.resolve_parameters(runtime_params) or self.config
        num_slices = int(resolved.get("num_slices", 1))
        if num_slices <= 1:
            yield self
            return

        client = self._get_client()
        pit = client.open_point_in_time(index=resolved["index"], keep_alive=resolved["scroll"])
        pit_id = pit["id"]
        c = self.config
        for i in range(num_slices):
            sub = ElasticSource(
                url=c["url"], index=c["index"], base_query=c["base_query"],
                scroll=c["scroll"], size=c["size"], auth=c["auth"],
                verify_certs=c["verify_certs"],
            )
            sub.config["pit_id"] = pit_id
            sub.config["slice"] = {"id": i, "max": num_slices}
            yield sub
```

- [ ] **Step 4: Make `fetch()` honor a `pit_id` + `slice`**

In `ElasticSource.fetch`, before the existing `client.search(...)` call, branch when a slice is present:

```python
        pit_id = resolved_config.get("pit_id")
        slice_spec = resolved_config.get("slice")
        if pit_id and slice_spec is not None:
            body = dict(resolved_config["base_query"])
            body["slice"] = slice_spec
            body["pit"] = {"id": pit_id, "keep_alive": resolved_config["scroll"]}
            records: List[Any] = []
            search_after = None
            while True:
                page_body = dict(body)
                if search_after is not None:
                    page_body["search_after"] = search_after
                page_body.setdefault("sort", ["_shard_doc"])
                resp = client.search(body=page_body, size=resolved_config["size"])
                resp = resp.body if hasattr(resp, "body") else resp
                hits = resp["hits"]["hits"]
                if not hits:
                    break
                records.extend(h["_source"] for h in hits)
                search_after = hits[-1]["sort"]
                if limit and len(records) >= limit:
                    return records[:limit]
            return records
```

Normalize `auth` to a tuple in `_get_client` (JSON round-trips it to a list):

```python
        auth = self.config.get("auth")
        if isinstance(auth, list):
            auth = tuple(auth)
```
and pass `basic_auth=auth`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add reflowfy/sources/elastic.py tests/unit/test_source_split.py
git commit -m "feat(sources): ElasticSource.split() via PIT + sliced scroll; fetch honors slice"
```

---

## Phase 3 — Integration & cleanup

### Task 10: Remove dead split_jobs callers + full suite + E2E

**Files:** `reflowfy/reflow_manager/pipeline_runner.py`, `tests/e2e/`, `CLAUDE.md` (schema note).

- [ ] **Step 1: Grep for stale payload-key readers**

Run: `grep -rn '"records"\|transformation_specs\|_check_destination_lag_health\|_apply_frozen_transformations\|_create_destination' reflowfy/`
Expected: only `_serialize_for_json` and unrelated hits remain. Update/remove any leftover readers in `pipeline_runner.py` (e.g. the old `resume_execution` still reads `job.job_payload` — that's fine; it ships stored v2 payloads unchanged).

- [ ] **Step 2: Run the full unit suite**

Run: `uv run pytest tests/unit/ -v`
Expected: PASS. Fix any remaining test referencing the old payload by rewriting it to the v2 shape (source descriptor + flat metadata).

- [ ] **Step 3: Lint, format, type-check**

Run:
```bash
uv run ruff check reflowfy/ && uv run black --check reflowfy/ && uv run mypy reflowfy/ && uv run pyright
```
Expected: clean. Fix issues inline (notably the removed imports in `executor.py`).

- [ ] **Step 4: Update the E2E id-based pipeline expectation**

In `tests/e2e/` (the id-based suite under `tests/e2e/test_pipelines/`), confirm the pipeline still passes with worker-side sourcing. The manager now ships a `source` descriptor; the worker fetches. No pipeline-author API changed (`define_source` still returns a source or a list). Run:
```bash
./scripts/run_e2e_tests.sh sources
```
Expected: PASS. If the suite asserts on payload shape, update assertions to the v2 schema.

- [ ] **Step 5: Document the v2 schema**

Add a short "Job message schema (v2)" subsection to `CLAUDE.md` under the worker description, linking the spec and showing the v2 example. Keep it under 15 lines.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: integrate worker-side sourcing (suite green, e2e, docs)"
```

- [ ] **Step 7: Final verification**

Run: `uv run pytest tests/unit/ -q && ./scripts/run_e2e_tests.sh sources schedule`
Expected: all green. Report results before declaring done.

---

## Self-Review Notes

- **Spec coverage:** `split()` abstraction (T2,T5–T9), v2 payload + source field (T4b), worker dynamic chain (T4c), frozen-fallback + manager lag-check removal (T4c/T4b), serialization standardization (T1), job-id from descriptor (T4a), metadata flatten (T3), per-source planning for sql/api/s3/elastic/mock (T5–T9), tests + E2E (T10). All spec sections map to a task.
- **Type consistency:** `SourceFactory.serialize`/`create`, `BaseSource.split`/`registry_type`, `build_job_payload`, `generate_job_id(pipeline_name, source, current_ids)` are used with identical signatures across tasks.
- **Open follow-ups (out of scope, noted in spec):** worker-side DLQ-reschedule backpressure; adaptive re-slicing; `num_slices`/`batch_size` autotuning.
