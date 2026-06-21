# Reflowfy DX Improvement Plan

## Current DX Grade: B+

Strong foundations (decorator API, auto-discovery, parameter validation) but significant gaps in observability, error visibility, and local dev ergonomics.

---

## Priority 1 — Quick Wins (1–2 weeks)

### 1.1 Worker Error Propagation ← Biggest pain point

**Problem:** When a worker fails (transformation exception, destination write error), the user sees `status: failed` with no root cause. Errors are `print()`-ed to stdout and lost.

**Fix:** Persist worker errors back to the `jobs` table.
- Add `error_message: str | None` and `error_traceback: str | None` columns to the Job model
- In `WorkerExecutor.execute()`, catch exceptions and `PATCH /executions/{id}/jobs/{job_id}` with the error details
- Expose via `GET /executions/{id}/errors` — return all failed jobs with their messages

**Impact:** Users go from "why did my pipeline fail?" to "record X failed because `KeyError: 'user_id'` in `uppercase_names` transformation at line 42."

---

### 1.2 Fix Checkpoint/Job Terminology Confusion

**Problem:** The HTTP API uses both `/checkpoints` and `/jobs` inconsistently. The model is called `JobCheckpoint` but represents a job batch. This confuses every new user.

**Fix:**
- Rename all API routes: `/checkpoints` → `/jobs`
- Rename `JobCheckpoint` model → `JobBatch`
- Keep a 1-release deprecation alias on `/checkpoints` with a deprecation header

---

### 1.3 Cron Validation at Registration

**Problem:** `schedule = "*/5 * * * *"` is only validated at scheduler init, not at class definition time. A typo in the cron expression fails silently or crashes at runtime.

**Fix:** In `AbstractPipeline.__init_subclass__`, validate the cron expression immediately using `croniter.is_valid()` and raise `ValueError` with a helpful message:

```
ValueError: Invalid cron expression '*/5 * * * ? *' in MyPipeline.schedule.
  Did you mean '*/5 * * * *'? (reflowfy uses 5-field cron, not 6-field)
```

---

### 1.4 Deduplication Audit Trail

**Problem:** When `enable_duplicate_jobs=False`, deduplicated jobs are silently dropped. Users have no way to know how many jobs were skipped or why.

**Fix:**
- Add a `deduplicated_count` field to `ExecutionStats`
- Log a single structured info line: `Deduplicated 1,432 jobs (content hash match)`
- Expose via `GET /executions/{id}/stats`

---

## Priority 2 — Medium Term (2–6 weeks)



### 2.3 Hide Async/Sync Bridging Complexity

**Problem:** `LocalDispatcher._run_async()` contains fragile nested-event-loop detection that users hit when testing in Jupyter or async frameworks. Users get cryptic `RuntimeError: Event loop is closed` errors.

**Fix:** Replace with `asyncio.run()` + `nest_asyncio` when inside a running loop:

```python
def _run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        nest_asyncio.apply(loop)  # Safe re-entry
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)
```

Add `nest_asyncio` to dependencies. One line change, eliminates the `ThreadPoolExecutor` workaround.


### 2.5 `ExecutionContext` Enrichment

**Problem:** Transformations receive minimal context (execution_id, pipeline_name, runtime_params). No way to know current batch number, retry count, or total jobs — forcing users to log blindly.

**Fix:** Extend `ExecutionContext`:

```python
@dataclass
class ExecutionContext:
    execution_id: str
    pipeline_name: str
    runtime_params: Dict
    # Add:
    batch_number: int       # Current batch (1-indexed)
    total_batches: int      # Total job count
    retry_count: int        # How many times this job has been retried
    is_retry: bool          # Convenience flag
```

---

## Priority 3 — Long Term (6–12 weeks)

### 3.1 Scheduling High-Availability

**Problem:** `PipelineScheduler` runs as a single background thread. If ReflowManager restarts mid-schedule, jobs may fire twice or not at all.

**Fix:** Add a database-backed distributed lock on schedule execution using `SELECT ... FOR UPDATE SKIP LOCKED` (already on PostgreSQL). Only one instance fires each scheduled run.
