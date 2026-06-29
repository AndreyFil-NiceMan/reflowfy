# Worker-Side Content Deduplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore v1 content-based deduplication by moving it from the manager (descriptor-hash, pre-dispatch) to the worker (content-hash, post-fetch), so a scheduled pipeline re-processes only when the underlying data actually changed.

**Architecture:** The manager stops deduplicating and always dispatches jobs with fresh UUID job IDs, carrying a `dedup_check` flag in the v2 payload. After a worker fetches its slice's records, if `dedup_check` is set it computes a SHA256 content hash (v1 logic: pipeline name + transformation names + records), atomically *claims* that hash in a new `processed_content` table via `INSERT ... ON CONFLICT DO NOTHING`. Claim won → send to destination; claim lost → terminate the job as `deduplicated` (no send). On send failure the worker releases its own claim so a retry reprocesses. A background sweeper purges hashes older than 24h, which also bounds the rare worker-crash-mid-send case.

**Tech Stack:** Python 3, SQLAlchemy (sync for manager, async/asyncpg for worker), PostgreSQL (`ON CONFLICT` upsert), FastAPI, pytest (`asyncio_mode=auto`), Docker-based E2E.

**Branch:** `worker-side-sourcing` (current). Commit frequently per the steps below.

**Why this design (recorded decisions):**
- Dedup must be content-based, not descriptor-based, because the manager no longer fetches records under worker-side sourcing — only the worker sees the data. A scheduled run with changed data → new hash → runs; unchanged → skipped.
- `enable_duplicate_jobs=False` now means "worker content-dedups." `True` (default) keeps current always-run behavior.
- Retention = 24h. The same window is the TTL for a stuck claim left by a worker that crashed between claiming and sending (accepted, bounded trade-off).
- Efficiency trade-off accepted: deduplicated jobs are still dispatched and fetched before being dropped at the worker.

---

## File Structure

**Create:**
- `reflowfy/execution/content_dedup.py` — pure `compute_content_hash()` + async `claim_content_hash()` / `release_content_hash()` DB helpers (one responsibility: the dedup primitive, shared + unit-testable).
- `reflowfy/reflow_manager/content_dedup_scheduler.py` — background 24h retention sweeper (mirrors `pipeline_scheduler.py` / `dlq_scheduler.py`).
- `tests/e2e/test_pipelines/content_dedup_test_pipeline.py` — E2E pipeline whose data is driven by `runtime_params` so tests can hold data constant or change it.
- `tests/e2e/test_worker_content_dedup.py` — E2E behavior tests (observe deliveries at the mock destination).
- `tests/unit/test_content_dedup.py` — unit tests for the hash + claim/release + sweep.

**Modify:**
- `reflowfy/reflow_manager/models.py` — add `ProcessedContent` model.
- `reflowfy/worker/executor.py` — dedup branch in `execute_job`; `deduplicated` state in `_update_job_in_db`; `JobStats.deduplicated`.
- `reflowfy/reflow_manager/pipeline_runner.py` — drop manager-side dedup, always UUID, pass `dedup_check`; count `deduplicated` in `_sync_counts_from_db`, final-state, `_wait_for_batch_completion`; update `build_job_payload`.
- `reflowfy/reflow_manager/job_manager.py` — `get_job_counts` returns `deduplicated`.
- `reflowfy/reflow_manager/app.py` — start/stop the retention sweeper.
- `tests/e2e/test_deduplication.py` — update assertions to the new (worker-side) semantics.
- `tests/e2e/test_schedule.py` — update `TestScheduledPipelineNoDuplicateJobs` assertions.

---

## Task 1: E2E test pipeline driven by runtime params

**Files:**
- Create: `tests/e2e/test_pipelines/content_dedup_test_pipeline.py`

This pipeline embeds its records (derived from `runtime_params["payload"]`) into the `e2e_mock` source descriptor, so the worker reconstructs the exact same records and the content hash reflects `payload`. `enable_duplicate_jobs=False` turns worker dedup on.

- [ ] **Step 1: Create the pipeline**

```python
"""E2E pipeline for worker-side content deduplication tests.

Records are derived from runtime_params['payload'] and embedded in the
e2e_mock source descriptor, so two runs with the same payload fetch
identical records (worker dedups the second), and a changed payload
produces different records (worker re-processes).
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http


class E2EContentDedupPipeline(AbstractPipeline):
    """Worker-side content dedup pipeline (enable_duplicate_jobs=False)."""

    name = "e2e_content_dedup"
    enable_duplicate_jobs = False

    def define_source(self, runtime_params):
        payload = runtime_params.get("payload", "A")
        data = [{"id": 1, "payload": payload}]
        return e2e_mock(data=data, batch_size=1)

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return []
```

- [ ] **Step 2: Verify it imports under the e2e module**

Run: `uv run python -c "import tests.e2e.test_pipelines.content_dedup_test_pipeline as m; print(m.E2EContentDedupPipeline.name)"`
Expected: prints `e2e_content_dedup` (no exception).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_pipelines/content_dedup_test_pipeline.py
git commit -m "test(e2e): add content-dedup test pipeline driven by runtime params"
```

---

## Task 2: E2E behavior tests (write first, expect FAIL)

**Files:**
- Create: `tests/e2e/test_worker_content_dedup.py`

These encode the target behavior and will fail until Tasks 3–8 land. They observe deliveries at the mock HTTP destination (`GET /records`, `DELETE /reset`), mirroring `tests/e2e/test_runtime_params_flow.py`.

- [ ] **Step 1: Write the failing E2E tests**

```python
"""E2E: worker-side content deduplication.

enable_duplicate_jobs=False now means the WORKER deduplicates by record
content (not the manager by descriptor). Observed at the destination:
- same payload twice  -> second run delivers nothing, deduplicated_jobs=1
- changed payload     -> delivered again
"""

import os
import time

import httpx
import pytest

REFLOW_MANAGER_URL = os.getenv("E2E_REFLOW_MANAGER_URL", "http://localhost:8002")
MOCK_URL = os.getenv("E2E_MOCK_HTTP_URL", "http://localhost:8091")
POLL_INTERVAL = 2
MAX_WAIT = 60


def _run(client, payload, **extra):
    body = {
        "pipeline_name": "e2e_content_dedup",
        "runtime_params": {"payload": payload},
        **extra,
    }
    resp = client.post("/run", json=body)
    assert resp.status_code == 202, f"/run failed: {resp.text}"
    return resp.json()["execution_id"]


def _wait(client, execution_id):
    deadline = time.time() + MAX_WAIT
    while time.time() < deadline:
        stats = client.get(f"/executions/{execution_id}/stats").json()
        if stats.get("state") in ("completed", "failed"):
            return stats
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{execution_id} did not finish in {MAX_WAIT}s")


def _records():
    return httpx.get(f"{MOCK_URL}/records", timeout=10).json()


def _reset():
    httpx.delete(f"{MOCK_URL}/reset", timeout=10)


@pytest.fixture(scope="module")
def client(check_reflow_manager):
    with httpx.Client(base_url=REFLOW_MANAGER_URL, timeout=60.0) as c:
        yield c


class TestWorkerContentDedup:
    def test_same_payload_twice_delivers_once(self, client):
        payload = f"dup-{int(time.time())}"
        _reset()

        first = _wait(client, _run(client, payload))
        assert first["state"] == "completed", first
        after_first = len(_records())
        assert after_first >= 1, "first run must deliver records"

        second = _wait(client, _run(client, payload))
        assert second["state"] == "completed", second
        assert len(_records()) == after_first, (
            "second run with identical data must deliver nothing"
        )
        assert second["deduplicated_jobs"] >= 1, (
            "worker must report the skipped job as deduplicated"
        )
        assert second["jobs_failed"] == 0

    def test_changed_payload_delivers_again(self, client):
        base = f"chg-{int(time.time())}"
        _reset()

        _wait(client, _run(client, base + "-v1"))
        n_after_v1 = len(_records())
        assert n_after_v1 >= 1

        changed = _wait(client, _run(client, base + "-v2"))
        assert changed["state"] == "completed", changed
        assert len(_records()) > n_after_v1, (
            "changed data must be re-processed and delivered"
        )
        assert changed["deduplicated_jobs"] == 0

    def test_dedup_run_still_creates_and_dispatches_jobs(self, client):
        """Unlike the old manager-side dedup, jobs are now always created
        and dispatched; dedup happens at the worker."""
        payload = f"jobs-{int(time.time())}"
        _wait(client, _run(client, payload))
        second = _wait(client, _run(client, payload))
        assert second["total_jobs"] > 0, (
            "jobs are always created now; dedup is a worker outcome"
        )
        assert second["deduplicated_jobs"] == second["total_jobs"]
```

- [ ] **Step 2: Run to confirm they FAIL against current code**

Run: `./scripts/run_e2e_tests.sh --test-file tests/e2e/test_worker_content_dedup.py`
Expected: FAIL — `test_same_payload_twice_delivers_once` delivers nothing different yet, and `deduplicated_jobs` is not populated by the worker (no `ProcessedContent`, no worker dedup). This confirms the tests exercise unbuilt behavior.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_worker_content_dedup.py
git commit -m "test(e2e): worker-side content dedup behavior (failing, pre-impl)"
```

---

## Task 3: `ProcessedContent` model + table

**Files:**
- Modify: `reflowfy/reflow_manager/models.py`
- Test: `tests/unit/test_content_dedup.py`

- [ ] **Step 1: Write the failing unit test**

```python
# tests/unit/test_content_dedup.py
from reflowfy.reflow_manager.models import ProcessedContent


def test_processed_content_table_shape():
    cols = {c.name for c in ProcessedContent.__table__.columns}
    assert {"content_hash", "pipeline_name", "job_id", "execution_id", "created_at"} <= cols
    assert ProcessedContent.__table__.primary_key.columns.keys() == ["content_hash"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_content_dedup.py::test_processed_content_table_shape -v`
Expected: FAIL with `ImportError: cannot import name 'ProcessedContent'`.

- [ ] **Step 3: Add the model**

Append to `reflowfy/reflow_manager/models.py` (uses the existing `Base`, `Mapped`, `mapped_column`, `String`, `DateTime`, `datetime`, `timezone` already imported in that module):

```python
class ProcessedContent(Base):
    """Content-hash ledger for worker-side deduplication.

    One row per processed record-content hash. The worker claims a hash
    atomically (INSERT ... ON CONFLICT DO NOTHING) before sending to the
    destination; a duplicate claim means the content was already processed.
    Rows are purged after a retention window by the content dedup sweeper.
    """

    __tablename__ = "processed_content"

    content_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    job_id: Mapped[str] = mapped_column(String(255), nullable=False)
    execution_id: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
        index=True,
    )
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_content_dedup.py::test_processed_content_table_shape -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add reflowfy/reflow_manager/models.py tests/unit/test_content_dedup.py
git commit -m "feat(manager): ProcessedContent model for worker-side dedup ledger"
```

---

## Task 4: `compute_content_hash` (v1 logic, pure function)

**Files:**
- Create: `reflowfy/execution/content_dedup.py`
- Test: `tests/unit/test_content_dedup.py` (append)

- [ ] **Step 1: Write the failing unit test (append)**

```python
from reflowfy.execution.content_dedup import compute_content_hash


def test_hash_is_stable_for_same_content():
    a = compute_content_hash("p", ["t1", "t2"], [{"id": 1, "v": "x"}])
    b = compute_content_hash("p", ["t2", "t1"], [{"id": 1, "v": "x"}])  # order-insensitive transforms
    assert a == b
    assert len(a) == 64


def test_hash_changes_with_records():
    a = compute_content_hash("p", [], [{"id": 1, "v": "x"}])
    b = compute_content_hash("p", [], [{"id": 1, "v": "y"}])
    assert a != b


def test_hash_changes_with_pipeline_name():
    a = compute_content_hash("p1", [], [{"id": 1}])
    b = compute_content_hash("p2", [], [{"id": 1}])
    assert a != b
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_content_dedup.py -k hash -v`
Expected: FAIL with `ModuleNotFoundError: reflowfy.execution.content_dedup`.

- [ ] **Step 3: Implement the function**

```python
# reflowfy/execution/content_dedup.py
"""Worker-side content deduplication primitive.

`compute_content_hash` reproduces the v1 deterministic hash (pipeline name +
transformation names + record content). The async claim/release helpers run
against PostgreSQL using whatever AsyncSession factory the worker provides.
"""

import hashlib
import json
from typing import Any, List

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import delete

from reflowfy.reflow_manager.models import ProcessedContent


def compute_content_hash(
    pipeline_name: str,
    transformation_names: List[str],
    records: List[Any],
) -> str:
    """Deterministic SHA256 over stable job content (v1 semantics)."""
    stable = {
        "pipeline_name": pipeline_name,
        "transformations": sorted(transformation_names),
        "records": records,
    }
    content = json.dumps(stable, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_content_dedup.py -k hash -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add reflowfy/execution/content_dedup.py tests/unit/test_content_dedup.py
git commit -m "feat(execution): compute_content_hash (v1 content-hash logic)"
```

---

## Task 5: async claim / release helpers

**Files:**
- Modify: `reflowfy/execution/content_dedup.py`
- Test: `tests/unit/test_content_dedup.py` (append)

The claim is the atomic decision: `INSERT ... ON CONFLICT (content_hash) DO NOTHING`, returning `True` iff this caller inserted the row (won the claim). Release deletes only the caller's own claim row (matched by `content_hash` AND `job_id`).

- [ ] **Step 1: Write the failing unit test (append)**

Uses an in-memory async SQLite engine to exercise the SQLAlchemy logic without Postgres. (SQLite supports `INSERT OR IGNORE`; SQLAlchemy renders `on_conflict_do_nothing` appropriately for the dialect via the generic path below.)

```python
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from reflowfy.reflow_manager.models import Base
from reflowfy.execution.content_dedup import claim_content_hash, release_content_hash


@pytest.fixture
async def session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def test_first_claim_wins_second_loses(session_factory):
    won1 = await claim_content_hash(session_factory, "h1", "pipe", "job1", "ex1")
    won2 = await claim_content_hash(session_factory, "h1", "pipe", "job2", "ex2")
    assert won1 is True
    assert won2 is False


async def test_release_allows_reclaim(session_factory):
    assert await claim_content_hash(session_factory, "h2", "pipe", "jobA", "exA") is True
    await release_content_hash(session_factory, "h2", "jobA")
    assert await claim_content_hash(session_factory, "h2", "pipe", "jobB", "exB") is True


async def test_release_only_removes_own_claim(session_factory):
    assert await claim_content_hash(session_factory, "h3", "pipe", "owner", "exO") is True
    await release_content_hash(session_factory, "h3", "not-owner")  # no-op
    assert await claim_content_hash(session_factory, "h3", "pipe", "x", "exX") is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_content_dedup.py -k "claim or release or reclaim" -v`
Expected: FAIL with `ImportError: cannot import name 'claim_content_hash'`.

(If `aiosqlite` is missing, add it as a dev dependency: `uv add --dev aiosqlite`, then commit the dependency change with this task.)

- [ ] **Step 3: Implement the helpers (append to content_dedup.py)**

```python
async def claim_content_hash(
    session_factory,
    content_hash: str,
    pipeline_name: str,
    job_id: str,
    execution_id: str,
) -> bool:
    """Atomically claim a content hash. Returns True iff this caller inserted it."""
    async with session_factory() as db:
        stmt = (
            pg_insert(ProcessedContent)
            .values(
                content_hash=content_hash,
                pipeline_name=pipeline_name,
                job_id=job_id,
                execution_id=execution_id,
            )
            .on_conflict_do_nothing(index_elements=["content_hash"])
        )
        result = await db.execute(stmt)
        await db.commit()
        return (result.rowcount or 0) == 1


async def release_content_hash(session_factory, content_hash: str, job_id: str) -> None:
    """Release this caller's own claim so a retry can reprocess."""
    async with session_factory() as db:
        stmt = delete(ProcessedContent).where(
            ProcessedContent.content_hash == content_hash,
            ProcessedContent.job_id == job_id,
        )
        await db.execute(stmt)
        await db.commit()
```

Note: `on_conflict_do_nothing` is Postgres-specific (`pg_insert`). The SQLite test path also accepts it because SQLAlchemy's `sqlite` dialect implements `on_conflict_do_nothing`. If the SQLite test ever rejects it, gate the test on Postgres instead — production is always Postgres.

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_content_dedup.py -v`
Expected: PASS (all content-dedup unit tests).

- [ ] **Step 5: Commit**

```bash
git add reflowfy/execution/content_dedup.py tests/unit/test_content_dedup.py pyproject.toml uv.lock
git commit -m "feat(execution): atomic claim/release for content-hash ledger"
```

---

## Task 6: `deduplicated` job state in the worker

**Files:**
- Modify: `reflowfy/worker/executor.py`
- Test: `tests/unit/test_worker_dedup_state.py` (create)

- [ ] **Step 1: Write the failing unit test**

```python
# tests/unit/test_worker_dedup_state.py
from reflowfy.worker.executor import JobStats


def test_jobstats_defaults_not_deduplicated():
    s = JobStats()
    assert s.deduplicated is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_worker_dedup_state.py -v`
Expected: FAIL with `AttributeError: 'JobStats' object has no attribute 'deduplicated'`.

- [ ] **Step 3: Add the flag and state mapping**

In `reflowfy/worker/executor.py`, in `JobStats.__init__` (after `self.success = False`, around line 32):

```python
        self.deduplicated = False
```

In `_update_job_in_db` (around line 214), replace:

```python
                state = "completed" if stats.success else "failed"
```

with:

```python
                if stats.deduplicated:
                    state = "deduplicated"
                elif stats.success:
                    state = "completed"
                else:
                    state = "failed"
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_worker_dedup_state.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add reflowfy/worker/executor.py tests/unit/test_worker_dedup_state.py
git commit -m "feat(worker): deduplicated job state in JobStats and DB update"
```

---

## Task 7: Worker dedup branch in `execute_job`

**Files:**
- Modify: `reflowfy/worker/executor.py`

Hook the claim between fetch and send. Read `dedup_check` from the payload (Task 8 sets it on the manager side; absent → `False`, so this is safe to land first).

- [ ] **Step 1: Add the dedup branch**

In `execute_job`, after the transformation/records section and before the destination health check. Concretely, right after `stats.records_output = len(transformed_records)` (around line 152), insert:

```python
            # Worker-side content deduplication (enable_duplicate_jobs=False).
            dedup_check = bool(job_payload.get("dedup_check", False))
            claimed_content_hash = None
            if dedup_check:
                from reflowfy.execution.content_dedup import (
                    compute_content_hash,
                    claim_content_hash,
                )

                transformation_names = [name for name, _ in applied]
                content_hash = compute_content_hash(
                    _pipeline_name, transformation_names, records
                )
                won = await claim_content_hash(
                    self._async_session, content_hash, _pipeline_name, job_id, execution_id
                )
                if not won:
                    print(f"⏭️  Job {job_id}: content already processed — deduplicated")
                    stats.deduplicated = True
                    stats.success = True
                    stats.records_output = 0
                    stats.end_time = time.time()
                    await self._update_job_in_db(execution_id, job_id, stats)
                    return True
                claimed_content_hash = content_hash
```

- [ ] **Step 2: Release the claim on every failure path**

In the destination health-check failure branch (around line 155-161), before `return False`, add:

```python
                if claimed_content_hash:
                    from reflowfy.execution.content_dedup import release_content_hash
                    await release_content_hash(self._async_session, claimed_content_hash, job_id)
```

In the outer `except Exception` block (around line 182), after building the failed `stats` and before `await self._update_job_in_db(...)`, add:

```python
            if claimed_content_hash:
                try:
                    from reflowfy.execution.content_dedup import release_content_hash
                    await release_content_hash(self._async_session, claimed_content_hash, job_id)
                except Exception:
                    pass
```

Because `claimed_content_hash` is referenced in the `except` block, initialize it at the top of `execute_job` (right after `stats = JobStats()`, line 110) so it is always bound even if an error occurs before the dedup section:

```python
        claimed_content_hash = None
```

and remove the duplicate local initialization added in Step 1 (keep only the assignment `claimed_content_hash = content_hash`). The `dedup_check` read stays in Step 1's block.

- [ ] **Step 3: Type-check**

Run: `uv run mypy reflowfy/worker/executor.py`
Expected: no new errors.

- [ ] **Step 4: Commit**

```bash
git add reflowfy/worker/executor.py
git commit -m "feat(worker): content-hash claim/skip in execute_job with release-on-failure"
```

---

## Task 8: Manager — stop dedup, always UUID, pass `dedup_check`

**Files:**
- Modify: `reflowfy/reflow_manager/pipeline_runner.py`

- [ ] **Step 1: Extend `build_job_payload` with `dedup_check`**

Replace the function (around line 87) with:

```python
def build_job_payload(
    execution_id: str,
    job_id: str,
    pipeline_name: str,
    sub_source: Any,
    metadata: Dict[str, Any],
    dedup_check: bool = False,
) -> Dict[str, Any]:
    """Assemble the v2 worker job message for one narrowed sub-source."""
    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "execution_id": execution_id,
        "job_id": job_id,
        "pipeline_name": pipeline_name,
        "source": SourceFactory.serialize(sub_source),
        "dedup_check": dedup_check,
        "metadata": metadata,
    }
```

- [ ] **Step 2: Replace the manager-side dedup in `_run_pipeline_jobs`**

In `_run_pipeline_jobs`, replace the job-id/dedup block (currently lines ~488-499):

```python
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
```

with:

```python
            dedup_check = not enable_duplicate_jobs
            job_id = str(uuid.uuid4())

            job_payload = build_job_payload(
                execution_id, job_id, pipeline_name, sub_source, metadata, dedup_check=dedup_check
            )
```

Remove the now-unused `dedup_count = 0` initialization (line ~478) and the dedup-count persistence block (lines ~517-520):

```python
        # Persist dedup count so it shows in stats
        if dedup_count > 0:
            self.execution_manager.update_deduplicated_count(execution_id, dedup_count)
            print(f"  Deduplicated {dedup_count} jobs (content hash match)")
```

(`deduplicated_jobs` is now populated from worker results in Task 9's `_sync_counts_from_db` change.)

- [ ] **Step 3: Same change in `_run_id_based_pipeline_jobs`**

Replace the analogous block (currently lines ~711-724):

```python
                source_descriptor = SourceFactory.serialize(sub_source)
                if enable_duplicate_jobs:
                    job_id = str(uuid.uuid4())
                else:
                    job_id = generate_job_id(
                        pipeline_name, source_descriptor, current_ids=ids_batch
                    )
                    if self.job_manager.get_job(job_id):
                        dedup_count += 1
                        continue

                job_payload = build_job_payload(
                    execution_id, job_id, pipeline_name, sub_source, metadata
                )
```

with:

```python
                dedup_check = not enable_duplicate_jobs
                job_id = str(uuid.uuid4())

                job_payload = build_job_payload(
                    execution_id, job_id, pipeline_name, sub_source, metadata,
                    dedup_check=dedup_check,
                )
```

Remove the corresponding `dedup_count` init and persistence block in this method as well.

- [ ] **Step 4: Delete now-dead helpers**

Delete `generate_job_id` (lines ~40-61), `_filter_volatile_keys` (lines ~35-37), and `_DATE_KEY_PATTERNS` (line ~32) — they are no longer referenced. Verify with:

Run: `grep -rn "generate_job_id\|_filter_volatile_keys\|_DATE_KEY_PATTERNS" reflowfy/`
Expected: no matches in `reflowfy/` (only possibly in `tests/unit/` — handled in Task 11).

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check reflowfy/reflow_manager/pipeline_runner.py && uv run mypy reflowfy/reflow_manager/pipeline_runner.py`
Expected: clean (no unused-import/undefined-name errors).

- [ ] **Step 6: Commit**

```bash
git add reflowfy/reflow_manager/pipeline_runner.py
git commit -m "feat(manager): drop descriptor dedup, always-UUID jobs, pass dedup_check to worker"
```

---

## Task 9: Count `deduplicated` jobs as finished-success

**Files:**
- Modify: `reflowfy/reflow_manager/job_manager.py`
- Modify: `reflowfy/reflow_manager/pipeline_runner.py`
- Test: `tests/unit/test_dedup_accounting.py` (create)

- [ ] **Step 1: Write the failing unit test**

```python
# tests/unit/test_dedup_accounting.py
from reflowfy.reflow_manager.pipeline_runner import _finished_count


def test_finished_includes_deduplicated():
    # (completed, failed, deduplicated) all count as finished
    assert _finished_count(completed=2, failed=0, deduplicated=3) == 5
    assert _finished_count(completed=0, failed=1, deduplicated=0) == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_dedup_accounting.py -v`
Expected: FAIL with `ImportError: cannot import name '_finished_count'`.

- [ ] **Step 3: Add `deduplicated` to `get_job_counts`**

In `reflowfy/reflow_manager/job_manager.py`, in `get_job_counts`, add `"deduplicated": 0` to the `counts` dict initializer:

```python
        counts = {
            "total": 0,
            "pending": 0,
            "dispatched": 0,
            "completed": 0,
            "failed": 0,
            "deduplicated": 0,
        }
```

- [ ] **Step 4: Add the helper and fold deduplicated into accounting**

In `reflowfy/reflow_manager/pipeline_runner.py`, add a module-level helper (near the top, after imports):

```python
def _finished_count(completed: int, failed: int, deduplicated: int) -> int:
    """Jobs in a terminal state. Deduplicated jobs are a success outcome."""
    return completed + failed + deduplicated
```

In `_sync_counts_from_db`, read the new count and fold it in:

```python
        completed = job_counts.get("completed", 0)
        failed = job_counts.get("failed", 0)
        deduplicated = job_counts.get("deduplicated", 0)
        dispatched = job_counts.get("dispatched", 0) + completed + failed + deduplicated

        execution = self.execution_manager.get_execution(execution_id)
        if execution:
            execution.jobs_dispatched = dispatched
            execution.jobs_completed = completed + deduplicated
            execution.jobs_failed = failed
            execution.deduplicated_jobs = deduplicated
            self.execution_manager.db.commit()

        return (dispatched, completed + deduplicated, failed)
```

In `_wait_for_batch_completion`, count `deduplicated` as terminal/completed. In the per-job loop (around line where it checks `state == "completed"`):

```python
                if state in ("completed", "deduplicated"):
                    completed += 1
                elif state == "failed":
                    failed += 1
                else:
                    pending += 1
```

- [ ] **Step 5: Run unit test to verify it passes**

Run: `uv run pytest tests/unit/test_dedup_accounting.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add reflowfy/reflow_manager/job_manager.py reflowfy/reflow_manager/pipeline_runner.py tests/unit/test_dedup_accounting.py
git commit -m "feat(manager): count deduplicated jobs as finished-success in accounting"
```

---

## Task 10: 24h retention sweeper

**Files:**
- Create: `reflowfy/reflow_manager/content_dedup_scheduler.py`
- Modify: `reflowfy/reflow_manager/app.py`
- Test: `tests/unit/test_content_dedup_sweep.py` (create)

- [ ] **Step 1: Write the failing unit test**

```python
# tests/unit/test_content_dedup_sweep.py
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from reflowfy.reflow_manager.models import Base, ProcessedContent
from reflowfy.reflow_manager.content_dedup_scheduler import purge_expired_content


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_purge_removes_only_expired(session):
    now = datetime(2026, 6, 30, 12, 0, 0)
    session.add(ProcessedContent(
        content_hash="old", pipeline_name="p", job_id="j1", execution_id="e1",
        created_at=now - timedelta(hours=25),
    ))
    session.add(ProcessedContent(
        content_hash="fresh", pipeline_name="p", job_id="j2", execution_id="e2",
        created_at=now - timedelta(hours=1),
    ))
    session.commit()

    deleted = purge_expired_content(session, retention_hours=24, now=now)
    session.commit()

    remaining = {r.content_hash for r in session.query(ProcessedContent).all()}
    assert deleted == 1
    assert remaining == {"fresh"}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_content_dedup_sweep.py -v`
Expected: FAIL with `ModuleNotFoundError: reflowfy.reflow_manager.content_dedup_scheduler`.

- [ ] **Step 3: Implement the sweeper**

```python
# reflowfy/reflow_manager/content_dedup_scheduler.py
"""Background sweeper that purges expired processed_content rows.

Mirrors pipeline_scheduler.py: a daemon thread polling at an interval.
Retention defaults to 24h; the same window bounds the rare case of a
worker that crashed between claiming a hash and finishing the send.
"""

import os
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import delete
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.models import ProcessedContent
from reflowfy.reflow_manager.database import SessionLocal

CONTENT_DEDUP_RETENTION_HOURS = int(os.getenv("CONTENT_DEDUP_RETENTION_HOURS", "24"))
CONTENT_DEDUP_SWEEP_INTERVAL = int(os.getenv("CONTENT_DEDUP_SWEEP_INTERVAL_SECONDS", "3600"))


def purge_expired_content(db: Session, retention_hours: int, now: Optional[datetime] = None) -> int:
    """Delete processed_content rows older than retention_hours. Returns count."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(hours=retention_hours)
    result = db.execute(delete(ProcessedContent).where(ProcessedContent.created_at < cutoff))
    return result.rowcount or 0


class ContentDedupScheduler:
    """Daemon thread that periodically purges expired content hashes."""

    def __init__(
        self,
        retention_hours: int = CONTENT_DEDUP_RETENTION_HOURS,
        sweep_interval: int = CONTENT_DEDUP_SWEEP_INTERVAL,
    ):
        self.retention_hours = retention_hours
        self.sweep_interval = sweep_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        print(f"✅ Content Dedup Sweeper started (every {self.sweep_interval}s, retain {self.retention_hours}h)")

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

    def _run_loop(self) -> None:
        while self._running:
            db = SessionLocal()
            try:
                deleted = purge_expired_content(db, self.retention_hours)
                db.commit()
                if deleted:
                    print(f"🧹 Content Dedup Sweeper purged {deleted} expired hash(es)")
            except Exception as e:  # pragma: no cover
                db.rollback()
                print(f"❌ Content Dedup Sweeper error: {e}")
            finally:
                db.close()
            self._stop_event.wait(timeout=self.sweep_interval)


_sweeper: Optional[ContentDedupScheduler] = None


def init_content_dedup_scheduler() -> ContentDedupScheduler:
    global _sweeper
    _sweeper = ContentDedupScheduler()
    _sweeper.start()
    return _sweeper


def stop_content_dedup_scheduler() -> None:
    global _sweeper
    if _sweeper:
        _sweeper.stop()
        _sweeper = None
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/test_content_dedup_sweep.py -v`
Expected: PASS

- [ ] **Step 5: Wire into app startup/shutdown**

In `reflowfy/reflow_manager/app.py`, add the import near the other scheduler imports (around line 33):

```python
from reflowfy.reflow_manager.content_dedup_scheduler import (
    init_content_dedup_scheduler,
    stop_content_dedup_scheduler,
)
```

In the startup function, after the Pipeline Scheduler is initialized (around line 541), add:

```python
    print("\n🧹 Initializing Content Dedup Sweeper...")
    try:
        init_content_dedup_scheduler()
        print("✅ Content Dedup Sweeper initialized")
    except Exception as e:
        print(f"⚠️ Failed to initialize Content Dedup Sweeper: {e}")
```

In the shutdown handler (find where `stop_pipeline_scheduler()` is called), add alongside it:

```python
    stop_content_dedup_scheduler()
```

- [ ] **Step 6: Lint + commit**

Run: `uv run ruff check reflowfy/reflow_manager/content_dedup_scheduler.py reflowfy/reflow_manager/app.py`
Expected: clean

```bash
git add reflowfy/reflow_manager/content_dedup_scheduler.py reflowfy/reflow_manager/app.py tests/unit/test_content_dedup_sweep.py
git commit -m "feat(manager): 24h retention sweeper for processed_content"
```

---

## Task 11: Update obsolete dedup tests to new semantics

**Files:**
- Modify: `tests/e2e/test_deduplication.py`
- Modify: `tests/e2e/test_schedule.py`
- Check: `tests/unit/` for any reference to `generate_job_id`

The old contract was "second dedup run dispatches 0 jobs" (manager-side skip). New contract: jobs are always created/dispatched; the *worker* deduplicates, so `total_jobs > 0`, `jobs_failed == 0`, and `deduplicated_jobs == total_jobs` on a repeat run.

- [ ] **Step 1: Find stale assertions**

Run: `grep -rn "total_jobs\"\] == 0\|jobs_dispatched\", 0\|generate_job_id\|skip all jobs\|skip every job" tests/`
Expected: matches in `tests/e2e/test_deduplication.py`, `tests/e2e/test_schedule.py`, and possibly `tests/unit/`.

- [ ] **Step 2: Update `test_deduplication.py` repeat-run assertions**

In `TestDedupOff.test_consecutive_dedup_runs_skip_all_jobs`, replace the second-run assertions:

```python
        assert second_stats["state"] == "completed", (
            f"Second run should complete cleanly, got {second_stats['state']}"
        )
        assert second_stats["jobs_failed"] == 0
        assert second_stats["total_jobs"] == 0, (
            f"All jobs should be skipped on second run, got total_jobs={second_stats['total_jobs']}"
        )
```

with:

```python
        assert second_stats["state"] == "completed", (
            f"Second run should complete cleanly, got {second_stats['state']}"
        )
        assert second_stats["jobs_failed"] == 0
        # New semantics: jobs are always created/dispatched; the worker dedups.
        assert second_stats["total_jobs"] > 0, (
            "jobs are created every run now; dedup is a worker outcome"
        )
        assert second_stats["deduplicated_jobs"] == second_stats["total_jobs"], (
            "every job on the repeat run must be deduplicated by the worker"
        )
```

Apply the same substitution in `TestDedupOn.test_api_override_disables_duplicates` (its `total_jobs == 0` block).

- [ ] **Step 3: Update `test_schedule.py` no-duplicate assertion**

In `TestScheduledPipelineNoDuplicateJobs.test_second_run_with_same_data_produces_no_new_jobs`, replace the final block:

```python
        jobs_second_run = stats2.get("jobs_dispatched", 0)
        assert jobs_second_run == 0, (
            f"Second run with enable_duplicate_jobs=False should dispatch 0 jobs "
            f"(same data already processed), but dispatched {jobs_second_run}"
        )
```

with:

```python
        # New semantics: the second run still creates/dispatches jobs, but the
        # worker deduplicates them by content (same static data).
        assert stats2["state"] == "completed", stats2
        assert stats2["jobs_failed"] == 0
        assert stats2.get("deduplicated_jobs", 0) == stats2.get("total_jobs", 0), (
            "second run with identical data must be fully deduplicated by the worker"
        )
        assert stats2.get("total_jobs", 0) > 0, (
            "jobs are always created now; dedup is a worker outcome"
        )
```

- [ ] **Step 4: Remove/replace any `generate_job_id` unit tests**

If Step 1 found `generate_job_id` references under `tests/unit/`, delete those specific tests (the function no longer exists). Their behavior is superseded by `tests/unit/test_content_dedup.py`. Verify:

Run: `grep -rn "generate_job_id" tests/ reflowfy/`
Expected: no matches anywhere.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/test_deduplication.py tests/e2e/test_schedule.py tests/unit/
git commit -m "test: update dedup/schedule assertions to worker-side dedup semantics"
```

---

## Task 12: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Unit suite**

Run: `uv run pytest tests/unit/ -v`
Expected: PASS (including the new content-dedup, sweep, accounting, and worker-state tests).

- [ ] **Step 2: Lint + types**

Run: `uv run ruff check reflowfy/ && uv run black --check reflowfy/ && uv run mypy reflowfy/`
Expected: clean.

- [ ] **Step 3: Targeted E2E (the behavior we set out to fix)**

Run: `./scripts/run_e2e_tests.sh --test-file tests/e2e/test_worker_content_dedup.py`
Expected: PASS — same payload delivers once, changed payload delivers again, repeat run reports `deduplicated_jobs == total_jobs`.

- [ ] **Step 4: Regression E2E (dedup + schedule suites)**

Run: `./scripts/run_e2e_tests.sh --test-file tests/e2e/test_deduplication.py` then `./scripts/run_e2e_tests.sh --test-file tests/e2e/test_schedule.py`
Expected: PASS with the updated assertions.

- [ ] **Step 5: Refresh the knowledge graph**

Run: `graphify update .`
Expected: graph rebuilt (AST-only, no API cost).

- [ ] **Step 6: Final commit (if graph or formatting changed)**

```bash
git add -A
git commit -m "chore: refresh graphify graph after worker-side content dedup"
```

---

## Self-Review Notes (for the executor)

- **Spec coverage:** content-based dedup at the worker (Tasks 4–7), 24h retention (Task 10), scheduling fix is a consequence of Tasks 7–8 (manager no longer skips), observability via `deduplicated_jobs` (Tasks 6, 9), tests-first (Tasks 1–2 before implementation).
- **Race safety:** the only "decide to run" operation is the atomic `claim_content_hash` (Task 5). Failure releases the claim (Task 7). The unreleased-claim-after-crash edge is bounded by Task 10's sweep — documented, accepted.
- **Local mode:** covered, because `LocalDispatcher` runs jobs through `WorkerExecutor.execute_job` (`local_dispatcher.py:28`), the single hook point.
- **Name consistency:** payload field `dedup_check` (manager sets in Task 8, worker reads in Task 7); job state string `deduplicated` (Tasks 6, 9); helper `_finished_count` (Task 9).
