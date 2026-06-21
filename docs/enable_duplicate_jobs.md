# Job Deduplication: `enable_duplicate_jobs`

## Overview

By default, every time you run a pipeline all jobs execute — even if the exact same data was already processed in a previous run.

The `enable_duplicate_jobs` flag lets you change this behavior:

| Value | Behavior |
|-------|----------|
| `True` *(default)* | Jobs run every time — same data can be processed multiple times |
| `False` | Each unique job runs **at most once** — re-running the same pipeline with the same data skips every job that was already seen |

Uniqueness is determined by a **SHA256 hash** of the job's stable content (records, pipeline name, transformations, destination). Dates and timestamps are excluded from the hash so they don't cause unnecessary re-runs.

---

## Setting a Default on the Pipeline

Add `enable_duplicate_jobs` as a class attribute on your pipeline:

```python
from reflowfy import AbstractPipeline

class MyPipeline(AbstractPipeline):
    name = "my_pipeline"
    enable_duplicate_jobs = False   # each unique job runs at most once
```

```python
class MyReplayPipeline(AbstractPipeline):
    name = "my_replay_pipeline"
    enable_duplicate_jobs = True    # allow re-processing (default behavior)
```

If you omit the attribute, the default is `True` (no deduplication — preserves backward compatibility).

---

## Overriding Per Request via the API

You can override the pipeline's default for a single run by passing `enable_duplicate_jobs` in the `POST /run` request body:

```bash
# Enforce deduplication even on a pipeline that has enable_duplicate_jobs=True
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "my_pipeline",
    "enable_duplicate_jobs": false
  }'
```

```bash
# Allow re-processing for this run even if the pipeline defaults to False
curl -X POST http://localhost:8001/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "my_pipeline",
    "enable_duplicate_jobs": true
  }'
```

**Priority:** The request-level value always wins over the pipeline's class attribute.

---

## How It Works

### When `enable_duplicate_jobs = False`

During **Phase 1** of job dispatch (saving jobs to the database), for each source job the framework:

1. Computes a **deterministic SHA256 hash** from the job's stable content:
   - `pipeline_name`
   - transformation names (sorted)
   - destination class name and config
   - records (the actual data)
   - source metadata — with all `*date*`, `*time*`, `*timestamp*`, `*created_at*`, `*updated_at*` keys stripped
2. Uses that hash as the `job_id` (64-character hex string).
3. Queries the database: **does a job with this ID already exist?**
   - **Yes (any state)** → skip the job, log `[no-dup] Skipping job <id>...`
   - **No** → create the job and dispatch it normally.

### When `enable_duplicate_jobs = True` (default)

Each job gets a random UUID as its `job_id`. No database lookup is performed. Jobs are always created and dispatched.

---

## What Is Included in the Hash

| Field | Included | Notes |
|-------|----------|-------|
| `pipeline_name` | Yes | |
| transformation names | Yes | Sorted alphabetically for stability |
| destination class name | Yes | |
| destination config | Yes | |
| records | Yes | The actual data payload |
| source metadata | Yes (filtered) | Keys containing `date`, `time`, `timestamp`, `created_at`, or `updated_at` are **excluded** |
| `execution_id` | **No** | Different on every run |
| `rate_limit` | **No** | Runtime config, not content |
| runtime context | **No** | May contain timestamps |

---

## Skip Behavior

A job is skipped if a record with the same hash ID **already exists in the database**, regardless of its state:

| Existing job state | Action |
|--------------------|--------|
| `pending` | Skipped |
| `dispatched` | Skipped |
| `completed` | Skipped |
| `failed` | Skipped |

If you want a failed job to retry, use `enable_duplicate_jobs=True` (or the request-level override) for that run.

---

## Checking Results in the API

After a run with deduplication enabled, inspect the execution stats:

```bash
curl http://localhost:8001/executions/{execution_id}/stats
```

```json
{
  "execution_id": "abc123",
  "pipeline_name": "my_pipeline",
  "state": "completed",
  "total_jobs": 0,
  "jobs_dispatched": 0,
  "jobs_completed": 0,
  "jobs_failed": 0
}
```

`total_jobs: 0` on a re-run means all jobs were skipped (they already existed in the DB from the previous run).

---

## Viewing the Pipeline Configuration

The `enable_duplicate_jobs` setting is part of the pipeline's metadata response:

```bash
curl http://localhost:8001/pipelines/my_pipeline
```

```json
{
  "name": "my_pipeline",
  "enable_duplicate_jobs": false,
  "rate_limit": {"jobs_per_second": 50},
  "parameters": [...],
  "transformations": [...]
}
```

---

## Common Use Cases

### Process each record exactly once (ETL idempotency)

```python
class LoadUsersPipeline(AbstractPipeline):
    name = "load_users"
    enable_duplicate_jobs = False
```

Running `load_users` daily: if the source data hasn't changed, no records are reprocessed. If new records appear, only they are processed.

### Allow full re-runs (backfill, debugging)

```python
class BackfillPipeline(AbstractPipeline):
    name = "backfill"
    enable_duplicate_jobs = True   # explicit, for clarity
```

Or override at request time without touching the pipeline definition:

```bash
curl -X POST http://localhost:8001/run \
  -d '{"pipeline_name": "load_users", "enable_duplicate_jobs": true}'
```

### Selective re-processing

Because the hash is based on record content, modifying even one field in a source record produces a new hash — that job runs again while unchanged records are still skipped.

---

## Running the E2E Tests

The feature has E2E tests in `tests/e2e/test_deduplication.py`. Run them with the full test suite:

```bash
./scripts/run_e2e_tests.sh
```

Or run only the deduplication tests:

```bash
pytest tests/e2e/test_deduplication.py -v
```

The test pipelines used are defined in `tests/e2e/test_pipelines/dedup_test_pipeline.py`.
