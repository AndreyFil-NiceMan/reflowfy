# Framework gaps — implementation plan (2026-11-09)

Status: **approved scope, not started.** Deferred ideas live in
`docs/superpowers/specs/2026-11-09-deferred-ideas.md`.

Decisions taken with the user:

- #23 uses a new `JobRetryConfig` class, **opt-in** (no `job_retry` = today's behaviour).
- #4 and #8 ship with their behaviour changes (changelog + error messages say what to change).
- #13 is deferred (saved idea).

Every item below follows CLAUDE.md "Observability": a new terminal state or silent
short-circuit gets a log line, a `JobStats` field and a label on an existing counter.

---

## Stage 1 — correctness and security

### #2 Custom sources run on workers

- `BaseSource.split_jobs` becomes non-abstract; default raises
  `NotImplementedError("not used since the v2 job schema")`. Built-ins keep theirs.
- `BaseSource.__init_subclass__` registers each concrete subclass in `SourceFactory`
  under `registry_type`. Discovery already imports user modules on every service.
- Reconstruction contract made explicit: `cls(**config)` by default, or a
  `from_config(config)` classmethod override. Checked when a job is serialized, so a
  source that cannot round-trip fails at planning with a clear message, not on the worker.
- Tests: custom source plans → serializes → `SourceFactory.create` rebuilds it; E2E
  pipeline with its own source.

### #3 Documented hooks that were never called

- Rate limit precedence: `/run` override > `executions.rate_limit` >
  `define_rate_limit(params)` > `rate_limit` attribute. Result persisted on the execution.
- Token bucket keyed per execution instead of per pipeline (concurrent runs no longer
  overwrite each other's rate). No rate limit means unlimited, not a hidden 6000/min.
- `should_apply_transformation` called by the transformation runner before each step;
  a declined step keeps its position (append-only rule intact) and is reported as an
  `AppliedStep` with `mode="skipped"`.

### #4 SQL: values become bind parameters  ⚠️ behaviour change

- In `sql_source` queries, `{{ expr }}` renders to a bind placeholder (`:p0`, …) and the
  value is bound, including in the queries `split()` wraps around it. Bound values travel
  in the slice config so the worker rebuilds them.
- Identifiers need `{{ table | ident }}` (validated `[A-Za-z_][A-Za-z0-9_.]*`). A
  `{{ }}` in identifier position fails with a message pointing at `| ident`.
- Optional `params={...}` for hand-written `:name` binds.

### #5 Credentials

- Each source declares `secret_fields`; every API response that contains a job payload
  (`/executions/{id}/jobs`, `/archive/jobs*`, `/stats/failures`, …) redacts them
  (password in URLs, `auth`, `sasl_password`, `aws_secret_access_key`, `auth_token`,
  `Authorization` headers).
- `secret("ENV_NAME")` reference: only the name is serialized; the worker resolves it from
  its environment. A literal secret that gets serialized logs a warning once per pipeline.
- Optional `REFLOWFY_API_TOKEN`: API and manager require `Authorization: Bearer` on all
  routes except `/health` and `/metrics`. Helm values + scaffolded compose updated.

### #6 SQL `LIMIT` and trailing `;`

- One trailing `;` (and whitespace) stripped before any wrapping — fixes `split()` too.
- `fetch(limit=n)` uses `result.fetchmany(n)` instead of rewriting SQL.

### #7 Worker blocks the loop after dedup

- The post-dedup `define_destination` runs in `asyncio.to_thread` inside `pipeline_step`.

### #8 `api_destination` errors  ⚠️ behaviour change

- 4xx except 408/425/429 → `PermanentDestinationError`, not retried.
- 429/503 `Retry-After` honoured, capped at `max_wait_seconds`.
- Health check: one probe per host per worker, cached 30 s, one retry before failing.
- Error messages: `HTTP 404 Not Found (application/json, 812 bytes)`; body only in a
  debug log, truncated to 200 chars. Same for the `id_based_api` source.

### #11 `@record_transformation(on_error="fail"|"skip")`

- Same semantics as `@record_destination`: `"skip"` drops a record whose call raised
  (timeouts included) with the exception type name as reason, via the existing skip path.
  Subclass form: `on_error` class attribute.

### #21 Top-level exports

- `s3_source` (lazy; missing boto3 → "pip install reflowfy[s3]"), `id_based_api_source`,
  `mock_source`, `RetryConfig`, `JobRetryConfig`, `secret`, `fan_out`, `run_pipeline`.

---

## Stage 2 — authoring

### #10 Options on `@transformation` / `@record_transformation`

- Keyword arguments at construction are stored and passed to the function:
  `def mult(records, params, *, factor)` used as `mult(factor=2)`.
- Bound against the function signature at construction (`inspect.signature.bind`), so a
  typo fails inside `define_transformations`, not mid-job.

### #14 Per-record steps return exactly one `Record` or `None`

- `apply_record` must return a `dict` (so `Record` / `TypedRecord`) or `None`. Anything
  else (list, tuple, str, scalar) fails the job with `TransformationError` naming the
  record position and returned type. Contract check — `on_error` does not apply.
- Typing: per-record fn becomes `(record, params) -> Record | None`.
- Check tests/examples for per-record steps returning non-dicts before landing.

---

## Stage 3 — destinations

### #22 Connection pool per worker process

- One process-wide `ConnectionPool` on the worker, keyed by the existing
  `_connection_key()` (Kafka: cluster + credentials + producer settings; API: host +
  client settings). Pipelines and executions targeting the same cluster with the same
  credentials share one producer/client; different credentials never share.
- Idle eviction after N minutes (`DESTINATION_POOL_IDLE_SECONDS`), close on drain /
  shutdown, evict-and-rebuild on error. Health check cached per key (with #8).
- Local mode / `reflowfy test`: pool scoped to the event loop (one execution on the
  manager, one job in the CLI).
- Delivery unchanged: a job is completed only after every ack.
- Observability: no new metric (pure resource sharing); info log on create/evict, debug on reuse.

### #17 Several destinations

- `define_destination` may return a list or `fan_out(d1, d2, optional=[d2])`: every
  record goes to each.
- A `@record_destination` function may return a list: per-record multicast.
- Built on `RecordDestination`: a record joins several groups; pooling, request budget
  and concurrent group sends unchanged. Dedup already hashes per destination.
- Each group retries on its own; failure of an `optional` destination is logged and
  counted (`destinations_failed_optional` in stats) without failing the job.

---

## Stage 4 — retries and lifecycle

### #23 Automatic job retries — `JobRetryConfig` (opt-in)

```
job_retry = JobRetryConfig(
    max_retries=2, backoff_seconds=30, multiplier=2, max_backoff_seconds=600,
    retry_on=(SourceError, DestinationError, TimeoutError, ConnectionError),
    never_retry=(RecordValidationError, StepOutputError, PermanentDestinationError),
)
```

- Pipeline class attribute (also accepted in `__init__`); `None` = today's behaviour.
- Defaults for `retry_on`/`never_retry` as above; bugs (`KeyError`, `TypeError`,
  `ValueError`/`TemplateError`) are never retried. `never_retry` wins over `retry_on`.
- Flow: worker fails a job, error matches, `attempts <= max_retries` → job back to
  `pending` with new column `next_attempt_at = now + backoff`, last error kept in stats.
  The drive loop skips pending jobs whose `next_attempt_at` is in the future; an
  execution cannot complete while retries are scheduled. Exhausted → `failed` as today.
- Separate from `JOB_MAX_ATTEMPTS` (lease expiry) — documented side by side.
- Config travels in the job payload metadata (worker decides) so the worker and manager
  agree without the manager re-reading the class.
- Observability: `jobs_processed_total{status="retry_scheduled"}`, `retry_count` +
  `error_history` (type + short message, bounded) in stats, log
  `Job failed (X), retry 1/2 in 30s`.
- Caveat in docs: a retried job runs from the start; use dedup to avoid resends.

### #25 Lifecycle hooks and chaining

- Optional pipeline methods: `on_execution_complete(summary, params)`,
  `on_execution_failed(...)`, `on_execution_cancelled(...)`. `summary`: job counts,
  records in/out, skips, duration, first errors.
- Called by the manager at the one place an execution turns terminal (drive loop end,
  `/cancel`, forced fail, planning-crash recovery), exactly once via
  `UPDATE executions SET hooks_fired_at = now() WHERE hooks_fired_at IS NULL`.
- Run in a thread with a timeout; hook failure is logged and stored
  (`executions.hook_error`), never changes the execution state.
- `run_pipeline("B", params)` helper for chaining; child stores `parent_execution_id`.
- `/run` accepts optional `callback_url` → POST of the same summary.
- Observability: `hook_fired` / `hook_failed` log events; hook outcome in execution stats.

---

Each stage is one PR with unit tests, E2E where a broker/API is involved, and CLAUDE.md
updated.
