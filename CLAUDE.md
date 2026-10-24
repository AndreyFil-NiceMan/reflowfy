# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Reflowfy is a horizontally scalable data movement and transformation framework. Users define pipelines that fetch from **sources** (Elastic, SQL, HTTP API, S3), apply **transformations**, and write to **destinations** (Kafka, HTTP API, Console). Work is sharded into independent **jobs** dispatched over Kafka and processed by a pool of workers, with PostgreSQL as the source of truth for execution state. It is also distributed as a `pip` package with a CLI (`reflowfy`) used to scaffold, run, and deploy user projects.

## Commands

This project uses **uv**. Prefix Python commands with `uv run`.

```bash
# Unit tests (no services required). pytest/pytest-cov live in the `dev` EXTRA,
# not the dev dependency group, so a plain `uv sync` leaves them out.
uv sync --all-extras
uv run pytest tests/unit/ -v
uv run pytest tests/unit/ --cov     # line+branch; fails under [tool.coverage.report]
                                    # fail_under (99) — CI (.github/workflows/tests.yml)
                                    # runs ruff, pyright and this on every PR
uv run pytest tests/unit/test_api_destination.py::TestClassName::test_name -v   # single test

# E2E tests (build wheel, spin up full Docker stack, run, teardown)
./scripts/run_e2e_tests.sh                 # all suites
./scripts/run_e2e_tests.sh sources         # sources | destinations | dx | schedule
./scripts/run_e2e_tests.sh --no-docker     # assume services already running
./scripts/run_e2e_tests.sh --keep-docker   # leave Docker up after tests
./scripts/run_e2e_tests.sh --test-file tests/e2e/test_dlq.py

# Lint / format / type-check (line-length 100; mypy strict; pyright strict)
uv run ruff check reflowfy/
uv run black reflowfy/
uv run mypy reflowfy/       # NOT clean: ~133 pre-existing errors, mostly no-untyped-def.
                            # The bar for a change is "the count does not go up".
                            # `redundant-cast` is disabled in [tool.mypy]: mypy and
                            # pyright disagree about isinstance-narrowed Any, and the
                            # casts are load-bearing for pyright.
uv run pyright              # config in [tool.pyright]; covers reflowfy/, pipelines/
                            # and tests/e2e/test_pipelines/. CLEAN (0 errors) — the
                            # reportUnknown* rules are ON (they were suppressed
                            # repo-wide until 2026-08-18). Keep it at zero.
                            # NOTE: pyright must resolve the venv or it reports
                            # ~2200 phantom errors (unresolved imports make every
                            # downstream type Unknown). If it does not pick up
                            # .venv automatically — e.g. in a git worktree — pass
                            # `--pythonpath /path/to/.venv/bin/python`.
uvx basedpyright            # also clean; stricter superset, useful as a second opinion

# Run the full local stack via the CLI (Docker Compose under the hood)
uv run python -m reflowfy.cli.main run --build        # add -d/--detach to background
uv run python -m reflowfy.cli.main check              # kubectl get pods — cluster health only,
                                                     # NOT local pipeline validation
uv run python -m reflowfy.cli.main test <pipeline>    # run one pipeline locally, no Docker
                                                     # -v/-vv, -p k=v, --no-input, --json, --dry-run

# Build the wheel
uv run python -m build
```

`pytest` runs in `asyncio_mode = auto` — async test functions need no decorator.

## Observability (check this for every feature)

Whatever the feature, before calling it done ask where it becomes invisible in
production, and wire the ones that apply. `reflowfy/observability/` already has all
three; none of them need new infrastructure:

- **Log it** — `get_logger(__name__)`, `logger.info` for a decision a user would ask
  about ("why did nothing arrive?"), `debug` for volume. Inside the worker the job's
  `log_context` already binds execution_id/job_id/pipeline_name, so don't re-pass
  them. Never log record contents or credentials.
- **Persist it on the job** — a new terminal outcome or a per-job number belongs in
  `JobStats` (`worker/executor.py`); it lands in the job row's `stats` JSON and is
  queryable after the fact. A log line alone is gone once it rotates.
- **Count it** — a new outcome gets a label value on the existing
  `reflowfy_jobs_processed_total` counter (as `skipped`/`deduplicated` do) rather than
  a new metric. Labels are low-cardinality only: never a message, an ID, or a record
  value.

The bar: a new terminal state or silent short-circuit must not be indistinguishable
from an ordinary success in logs, job stats *and* metrics. `### Dropping a job` below
is the worked example. Skip all three only for pure refactors and internal helpers —
and say so.

## Architecture

Three deployable services, all sharing the same package and the same PostgreSQL database. Each service auto-discovers user code on startup; they coordinate only through Postgres and Kafka, never by direct calls.

```
HTTP POST /run → API (FastAPI) ──┐
                                 ▼
                    ReflowManager service (FastAPI, :8001)
                       │  PipelineRunner: runs source, shards into jobs
                       │  RateLimiter:    token bucket (Postgres-backed)
                       │  Dispatcher:     Kafka (distributed) or Local (in-process)
                       ▼
          PostgreSQL (executions, jobs, checkpoints)  ◄── workers report status here
                       │
                       ▼  Kafka topic reflow.jobs (distributed mode only)
              Worker pool (KafkaJobConsumer → executor) → Destinations
```

- **`reflowfy/reflow_manager/`** — the orchestrator. `manager.py` (`ReflowManager`) is a slim coordinator composing `execution.py` (execution records), `job_manager.py` (job + checkpoint tracking), `rate_limiter.py` (token bucket), `dispatcher.py`/`local_dispatcher.py` (Kafka vs in-process), and `pipeline_runner.py` (runs the source, builds jobs). `app.py` is the FastAPI service exposing `/run`, plus routers for DLQ, stats, and schedules. SQL schemas live alongside as `schema.sql` and `dlq_schema.sql`.
- **`reflowfy/worker/`** — `consumer.py` (`KafkaJobConsumer`) pulls jobs off Kafka; `executor.py` applies transformations and writes to the destination, reporting state back to Postgres directly.
- **`reflowfy/api/`** — thin FastAPI front door that forwards run requests to the ReflowManager.
- **`reflowfy/core/`** — the user-facing pipeline model. `abstract_pipeline.py` defines `AbstractPipeline` (with `define_source` / `define_transformations` / `define_destination` / `define_parameters` hooks). `id_based_pipeline.py` is a specialization for ID-range sharding. `execution_context.py` carries runtime params/metadata through a run.
- **`reflowfy/cli/`** — `typer` app (`main.py`); each subcommand registers itself from `commands/` (`init`, `new`, `run`, `build`, `check`, `deploy`, `test`).
- **`reflowfy/sources/`, `destinations/`, `transformations/`** — built-in connectors plus the `@source`, `@destination`, `@transformation` decorators. `factories/` builds connector instances from serialized config.
- **`reflowfy/execution/`** — `LocalExecutor` vs `DistributedExecutor` behind `base.py`'s `ExecutionStatus` / `ExecutionState` state machine.
- **`reflowfy/helm/`** — packaged Helm charts (api / reflow-manager / worker) used by `reflowfy deploy` for OpenShift/Kubernetes (KEDA-autoscaled workers).

### Worker job message (v2 schema)

The manager dispatches a JSON message per planned slice on Kafka topic `reflow.jobs` (or in-process via `LocalDispatcher`). It carries `schema_version` (currently `2`), `execution_id`, `job_id`, `pipeline_name`, a self-contained `source: {type, config}` descriptor (the narrowed slice from `BaseSource.split()`, reconstructible via `SourceFactory.create`), and `metadata` (execution context: runtime params, batch/retry info) — **no records, transformations, or destination travel on the wire**. The worker rebuilds the source from the descriptor, calls `source.fetch()` to pull just that slice, then resolves transformations and the destination dynamically by looking up `pipeline_name` in the (auto-discovered) `pipeline_registry` and calling `pipeline.define_transformations`/`define_destination` against the real fetched records. `KafkaJobConsumer` rejects messages where `schema_version != 2`. Full design: `docs/superpowers/specs/2026-06-24-worker-side-sourcing-design.md`.

### Auto-registration (important)

Pipelines register themselves with **no explicit registry calls**. `AbstractPipeline` uses a metaclass (`PipelineMeta`) that instantiates and registers any subclass defining a `name` attribute at class-definition time. If `__init__` raises (e.g. missing config), registration is **silently skipped** — a pipeline that fails to construct simply won't appear, with no error. The `pipeline_registry` (`core/registry.py`) is a thread-safe singleton and registration is idempotent by name.

**The registered instance is shared — never `resolve()` it.** The registry holds one instance per pipeline and `resolve()` stores the plan on the instance (`_source`, `_resolved_params`). Executions of the same pipeline running at once in one manager process (a burst of `/run` calls, two schedules firing together) overwrote each other's plan: 16 simultaneous runs all got the wrong job count, most of them none, and finished `completed` having processed nothing. Callers resolve a private copy — `pipeline = pipeline.fork()` (`PipelineRunner`, `LocalExecutor`) — and `tests/unit/test_concurrent_executions.py` forces the interleaving. Anything new that stores per-execution state on a pipeline instance has the same problem.

`PipelineMeta` also records the pipeline's params TypedDict (see below) from the subscripted base, reading `namespace["__orig_bases__"]` before it instantiates the class.

`core/pipeline_discovery.py` (`discover_and_load_pipelines`) imports every module under the `PIPELINE_MODULE` directory (default `pipelines`) plus sibling `sources/`, `destinations/`, `transformations/` dirs, which triggers the metaclass and decorator registration. All three services call this on startup. In E2E, `PIPELINE_MODULE` is overridden to `tests.e2e.test_pipelines`.

### Typed runtime_params

`runtime_params` is one flat dict merging the user's parameters with the framework's reserved execution-context keys (built by `build_flat_runtime_params` in `core/execution_context.py`). `core/runtime_params.py` names that shape: `RuntimeParams` (a `total=False` TypedDict of the reserved keys), the `P` TypeVar bound to it, and the `Param` marker for defaults.

A pipeline declares its parameters **once**, as a type: `class MyPipeline(AbstractPipeline[MyParams])` where `MyParams(RuntimeParams, total=False)` adds its own keys. Every hook then receives `MyParams` instead of `Dict[str, Any]`, and `define_parameters()` is *derived* from it by `params_from_typeddict` (`core/abstract_pipeline.py`) — so validation, defaults, CLI prompts and the generated OpenAPI routes all follow from the same declaration. A hand-written `define_parameters()` override still wins.

**Not declaring one is deprecated.** `PipelineMeta` raises a `DeprecationWarning` at class-definition time for any registered pipeline whose `_params_type` is `None`, naming the pipeline and the fix. A pipeline with no parameters of its own migrates with one token — `AbstractPipeline[RuntimeParams]` — so nobody needs an empty TypedDict. Every pipeline under `pipelines/` and `tests/e2e/test_pipelines/` is migrated; the remaining unsubscripted ones live in `tests/unit/` on purpose, as backward-compat canaries. Nothing raises: an unsubscripted pipeline still registers and runs.

Notes:
- `P` defaults to `Any` (PEP 696, via `typing_extensions`), **not** to `RuntimeParams`. That is what keeps the ~130 unsubscripted `AbstractPipeline` subclasses working: defaulting to `RuntimeParams` makes every pre-existing hook annotated `Dict[str, Any]` a Liskov violation.
- TypedDicts are closed, so a pipeline that declares `MyParams` must also declare the enrichment keys its own code *writes* into `runtime_params` (see `tests/e2e/test_pipelines/runtime_params_enrichment_pipeline.py`). Untyped pipelines are unaffected.
- Reserved keys are all optional because the execution context is built per job: `define_source`/`define_jobs` run before `execution_id` et al. exist. Read them with `.get()`.
- `IdBasedPipeline` injects **`input_ids`** (the caller's whole list) and declares that parameter for you — do not put it in `define_parameters()`. It lives on `RuntimeParams` beside `current_ids`/`current_id`, and `define_jobs` is the only hook that reads it: it runs once, before any job exists, so there is no "current" slice yet. The manager strips `input_ids` from each job before dispatch (`define_jobs` / `pipeline_runner.py`), so downstream hooks read `current_ids`/`current_id` — reading `input_ids` there would mean every job processing every ID. It was renamed from `ids` on 2026-08-23 so it no longer collides with pipelines that legitimately declare their own `ids` parameter (`pipelines/api_example_pipeline.py`); `IDBasedAPISource._get_all_ids` accepts either.
- `BaseTransformation.apply` deliberately keeps `Dict[str, Any]` — a transformation is shared across pipelines, so it cannot name one pipeline's params type.
- Worked examples: `typed_params_pipeline.py` (+ `tests/e2e/test_typed_params.py`), `id_based_api_advanced_pipeline_test.py`, `define_jobs_test_pipeline.py`. Derivation is unit-tested in `tests/unit/test_typed_runtime_params.py`.

### Typed records and hook returns

`reflowfy` exports the names a pipeline author needs to annotate a hook without a deep import: `Record` (`Dict[str, Any]`), `Records` (`List[Record]`), `Transformations` (`Sequence[BaseTransformation]`), plus `BaseSource` and `BaseDestination`. They live in `core/types.py`.

They are aliases, not a second generic parameter. The framework's own signatures stay `List[Any]` because a source may yield non-dict records (raw S3 text, scalars), and `List[Any]` accepts a `Records`-annotated override — so `AbstractPipeline` did **not** need an `R` TypeVar next to `P`.

`@transformation` is typed `Callable[[TransformFn], type[BaseTransformation]]` with `TransformFn = Callable[[Any, Any], Any]`. `Any` in every position on purpose: it rejects a wrong-arity function (the mistake that used to reach production as a runtime `TypeError`) without rejecting an author who annotates `runtime_params` with their own params TypedDict — a closed TypedDict is not assignable from `Dict[str, Any]`.

**Prefer `@transformation` over subclassing `BaseTransformation` whenever a
pipeline's own params type should be checked inside the transformation.**
`BaseTransformation.apply(self, records, runtime_params: Dict[str, Any])` can't
be narrowed on a subclass to a specific params TypedDict — mypy/pyright reject
that override as an LSP violation (a parameter type may only widen, never
narrow). A `@transformation`-decorated function has no such override relationship,
so it can be annotated with the pipeline's real params type and get full
checking — mypy validates a function's body against its own annotations
regardless of what the decorator turns the name into afterward:

```python
class ElasticTestParams(RuntimeParams, total=False):
    filter_status: Annotated[NotRequired[Literal["active", "inactive"]], Param(default="active")]

@transformation("enrich_processing_info")
def enrich_processing_info(records: Records, runtime_params: ElasticTestParams) -> Records:
    runtime_params["filter_staus"]  # mypy: TypedDict has no key "filter_staus" — caught
    ...
```

Reach for a `BaseTransformation` subclass only when the transformation needs
its own constructor state (e.g. `FilterByStatus(allowed_status="active")`) —
a plain function can't carry that.

The package ships `reflowfy/py.typed` (PEP 561), so installs are type-checked by consumers. It is registered in both `[tool.setuptools.package-data]` and `MANIFEST.in`.

Where a third-party library genuinely has no types, the suppression is **file-scoped, never repo-wide**, with a header comment naming the library: `destinations/kafka.py`, `worker/consumer.py` and `reflow_manager/dispatcher.py` (aiokafka ships no stubs), and `sources/s3.py` (`reportTypedDictNotRequiredAccess`, because boto3-stubs marks `Key`/`Size`/`ETag` NotRequired). That rule stays live everywhere else on purpose — it is what catches `runtime_params["execution_id"]` subscripting on the all-optional `RuntimeParams`. `boto3.client("s3", ...)` passes every argument explicitly: boto3 is typed by overloads on the literal service name, and a `**kwargs` splat matches none of them, making the client and everything derived from it Unknown.


### Async transformations

A transformation may be a coroutine. The per-job core is async
(`run_job_records_async` / `apply_transformations_iteratively_async`) and the worker awaits it on
its own event loop; the sync `run_job_records` / `apply_transformations_iteratively` that
`LocalExecutor` and `reflowfy test` call drive the same coroutine through
`core/async_utils.run_coroutine_sync` (one loop per job; on a helper thread when a loop is
already running, e.g. inside FastAPI). Three kinds of step, freely mixed in one chain:

- **sync** (`apply`): run in `asyncio.to_thread`, as are `source.fetch`, `define_transformations`
  and `define_destination`. The loop must stay free: a blocked loop starves aiokafka's heartbeat,
  the group rebalances and the job runs twice.
- **async** (`AsyncTransformation.apply_async`, or `@transformation` on an `async def`): awaited.
  `AsyncTransformation.apply` still works for sync callers. It is a separate method name
  because overriding sync `apply` with an `async def` is an LSP error.
- **per_record** (`RecordTransformation.apply_record`, or `@record_transformation(name,
  concurrency=, timeout=)` on an `async def f(record, params) -> record | None`): every record
  awaited, `concurrency` at a time (default `TRANSFORM_CONCURRENCY`, 16) by a fixed pool of
  workers, not a task per record. Order is kept, `None` drops the record, the first failure
  cancels the rest and raises `TransformationError` with the exact `culprit_index` (via
  `RecordFailure`), `SkipJob` from any record drops the job, and `raise SkipRecord("why")`
  drops just that record *with* a reason (see below). Subclass it for a client shared by
  the step's records: `setup()` / `teardown()` run once around them. An `httpx.AsyncClient`
  costs ~40 ms of CPU to build, so one per record is slow *and* stalls the loop.

Steps stay sequential (re-resolution reads what earlier steps wrote to `runtime_params`), and the
concurrent records of one step share `runtime_params`: read it, don't write it. Retries and rate
limits on remote calls are the author's job; the framework only caps concurrency and the
per-record timeout. Don't keep async clients at module level: `reflowfy test` and local mode use
a new loop per job. Sources stay sync (an async `fetch` is a possible follow-up).

Observability: `AppliedStep.mode` / `.concurrency` (worker debug log, `reflowfy test -v`);
per-step durations stay in `JobStats.transformation_times`. Running a step async adds no job
state or metric: its outcomes are the existing ones (the one new counter is for `SkipRecord`, see
"Dropping a record"). Tests: `tests/unit/test_async_transformations.py`,
`tests/e2e/test_async_transform.py` (the mock API counts lookups in flight),
`tests/e2e_distributed/test_async_pipelines.py` (`dist_async`, per-job peak from the sink).

### Dropping a record

`raise SkipRecord("why")` inside a per-record transformation drops that one record; the job goes
on. Returning `None` drops it silently (a filter); `SkipRecord` keeps the reason. The step
returns its skips beside its records (`RecordTransformation.apply_records` → `RecordResults`), so
nothing is stored on the shared transformation instance. Raised from a sync or whole-batch async
step it fails the job saying to use `@record_transformation` (there is no "current record"
there). Observability: `AppliedStep.skip_reasons`; the worker logs `Job <id>: <step> skipped N
records ('reason' xN, …)`, stores `records_skipped` and `record_skip_reasons` in the job's
`stats`, and counts `reflowfy_records_skipped_total{pipeline, transformation}` — a new counter,
because a record outcome has no place on the per-job `jobs_processed_total` and adding a label to
`records_processed_total` would break its existing series. Reasons are counted, so they must be
short fixed phrases: at most 20 distinct per step (the rest go to "(other reasons)"), 200
characters each. `LocalExecutor` and `reflowfy test` show them too (`--json`: `records_skipped` and `skip_reasons` per step).

### API destination: per job or per record

`define_destination(records, params)` runs once per job and returns one destination; the
worker calls `send(records)` once. `kafka_destination` already writes one message per
record. `api_destination` by default sends **one request per job** with `body=` verbatim
(build it from `records`). `api_destination(url=".../users/{user_id}", per_record=True)`
works like Kafka instead: one request per record, the record as the body (`key_field="x"`
sends only `record["x"]`, same rule as Kafka), and `{field}` placeholders in the URL filled
from each record, URL-encoded (a missing field fails the job, not retried). A retry
re-sends the whole job, so the endpoint should be idempotent. Errors name the record's
position (`record 2 of 3: HTTP 404 …`), never its contents. No `dedup_identity` needed:
the records are not in the config. Tests: `tests/unit/test_api_destination.py::TestPerRecord`.

**`concurrency=N`** (both `kafka_destination` and per-record `api_destination`, default 1)
keeps N messages/requests in flight per job through `core/async_utils.for_each_concurrently`
— the same fixed worker pool record transformations use, not a task per record. 1 is the old
behaviour exactly (one at a time, in order); above 1 records start in order but may land
out of order, and the first failure cancels the rest. It is not part of the dedup hash:
`BaseDestination.identity_config()` is what `destination_identity` hashes, and destinations
drop tuning knobs there. `ApiDestination` also drops `per_record`/`key_field` when unset,
so adding those keys did not change every existing destination's hash (which would have
re-sent the last 24 h of records once after upgrade). A new config key on a destination
needs the same treatment. Tests: `tests/unit/test_destination_concurrency.py`; E2E
`tests/e2e/destinations/test_api_destination_per_record.py` (the mock webhook's
`/items/<run>/<id>` counts requests open per job) and `test_kafka_destination_concurrency.py`
(real broker, exactly-once and order at concurrency 1); distributed
`tests/e2e_distributed/test_destination_concurrency.py` (`dist_per_record`, sink `PUT /record/…`).

### Execution modes

`EXECUTION_MODE` env var selects `local` (in-process via `LocalDispatcher`, used by the default docker-compose) or `distributed` (Kafka via `KafkaDispatcher`). Same pipeline code runs in both.

### Dropping a job

`raise SkipJob("why")` from `source.fetch`, a transformation, `define_transformations`
or `define_destination` to drop the current job: `run_job_records` catches it and
returns the same empty-slice tuple an empty fetch does, so every caller (worker,
`LocalExecutor`, `reflowfy test`) treats it as a no-op — no destination write, no
failure. It subclasses `PipelineError`, so `pipeline_step` passes it through
untouched; `transformation_runner` re-raises it instead of wrapping it in a
`TransformationError`. The job records as `completed` with 0 records — there is no
separate `skipped` state. A job whose fetch simply returns nothing already behaved
this way. `define_source`/`define_jobs` run manager-side and are NOT covered.

Observability: the reason rides back on `runtime_params["skip_reason"]` (a reserved
key on `RuntimeParams`) rather than a 5th return value. The worker logs
`Job <id>: dropped by pipeline (<reason>)` inside the job's `log_context`
(execution_id/job_id/pipeline_name bound), stores `skip_reason` in the job's `stats`
JSON, and counts it as `reflowfy_jobs_processed_total{status="skipped"}` — so a drop
is not silently indistinguishable from an ordinary completed job. `LocalExecutor`
logs the same reason.

### Elasticsearch window modes

`elastic_source(..., docs_per_job=N)` plans `ceil(count / N)` jobs (capped by `max_slices`) after one `search_after` pre-scan of the sort keys. `window_mode` decides what a job reads from:

- `"snapshot"` (default): every job reads one PIT opened at planning, so the run sees the index as it was at the start, in exact `docs_per_job` windows. The PIT is shared state: an Elasticsearch restart, or workers stopped longer than `pit_keep_alive` (30m), destroys it and every remaining job fails with `No search context found`.
- `"range"`: windows are value ranges `(a, b]` of the **first `sort` key** (which must be a real field — `_doc`, `_shard_doc`, `_score` or no sort raise), and each job opens its own 5-minute PIT, pages through its range and closes it. An ES restart fails only the jobs in flight — re-run them with `POST /executions/{id}/retry-failed`. Documents sharing a boundary value all fall in one window (sizes are approximate; an all-equal field degenerates to one big job), and documents indexed or deleted while the run progresses may or may not appear. Use it for long runs. `tests/unit/test_elastic_range_windows.py` checks exactly-once reads on tied values with a fake ES; `tests/e2e/sources/test_elastic_source.py::TestElasticRangeWindows` does it on a real index.

### Dispatch loop, leases and redelivery

Phase 1 (planning) is unchanged: jobs are streamed into Postgres in groups of `CHECKPOINT_BATCH_SIZE`, each tagged with a `batch_number` that `/executions/{id}/stats` reports as "checkpoints". **Batches no longer gate dispatch.** Phase 2 (`PipelineRunner._drive_execution`) is one continuous loop per execution:

- keeps up to `MAX_IN_FLIGHT_JOBS` (env, default 1000) jobs `dispatched`/`running`, topping the window up as jobs finish; per job: rate-limit token → send → `mark_job_dispatched` immediately (so the DB never shows `pending` for a job already sent, and a send that fails mid-chunk leaves the rest `pending` to retry — it used to strand them as `dispatched`);
- checks the execution state about once a second: `paused` stops sending (in-flight jobs finish), and `POST /executions/{id}/resume` starts a new loop. One loop per execution, by a lease on the execution row (`executions.driver_id` / `driver_heartbeat_at`): the owner heartbeats about once a second, others stand by and take over once it is older than `DRIVER_LEASE_SECONDS` (default 60), and a loop that finds itself replaced stops. At startup a manager frees leases left by an earlier process on its own hostname (a container restart), so recovery is immediate there; a replacement pod with a new name waits out the lease. Not a Postgres advisory lock: a force-killed pod's session lock lingers on its zombie connection until TCP keepalive (hours) and blocked every resume;
- has **no batch timeout**: slow or stopped workers never fail the execution; it warns every 10 min with no progress instead;
- retries database errors with backoff (5 s doubling to 60 s) instead of exiting — the job rows are the state, so a Postgres restart just pauses dispatch;
- runs in one event loop, so the Kafka producer is created once per execution.

**Planning crashes.** Phase 1 holds a heartbeated lease on the execution (a thread refreshes `driver_heartbeat_at` every 10 s while planning blocks, e.g. enumerating 2M ES docs) and sets `executions.plan_complete` only after the last job is saved. A manager killed mid-plan leaves `plan_complete = false`, which used to look like "no unfinished jobs" and ended `completed` having processed nothing (or dispatched a fragment of the plan). Recovery (`PipelineRunner._recover_plan`, also for `pending` executions) waits while the planning lease is fresh — a rollout's old pod may still be planning — and once the lease is gone and the execution is older than `DRIVER_LEASE_SECONDS` it marks the execution `failed` with "The manager stopped while planning this execution… run it again". A plan cannot be continued (the source's position is not saved), so the fix is a new run. `plan_complete = NULL` (an execution from before the column) resumes as before.

**Dead database sessions.** Every Reflowfy connection asks the server to probe it with TCP keepalives (`reflow_manager/db_settings.py`: idle 30 s, 10 s × 3 probes, so a killed process's session is dropped in about a minute instead of the two-hour default). Without it a force-killed manager's `idle in transaction` session kept its locks, and the replacement's startup `ALTER TABLE … ADD COLUMN IF NOT EXISTS` queued behind it and never finished. Migrations now skip columns that exist (no lock taken) and give up after `lock_timeout` 5 s on the rest. `DB_SESSION_KEEPALIVES=false` turns the settings off (PgBouncer rejects startup options).

Job states: `pending → dispatched → running → completed | failed | deduplicated`. The **worker claims** a job before running it (`WorkerExecutor._claim_job`: `pending|dispatched → running`, `attempts + 1`). Any other delivery of that job — Kafka redelivery after a rebalance/scale-down/crash, or a lease re-send — sees it `running`/finished and is skipped, committed, logged and counted as `reflowfy_jobs_processed_total{status="redelivered"}`. A job with no row runs anyway, and the claim fails open on a DB error.

On SIGTERM the worker **drains**: it stops taking messages, lets the job in progress finish and commit, then leaves the consumer group (`KafkaJobConsumer.request_stop`; a second signal exits at once). A job longer than the pod's `terminationGracePeriodSeconds` (120 s in the chart) is still killed mid-way and re-run once its heartbeat lease expires, re-sending what it had already written.

**Leases** (swept every 60 s by the drive loop): a worker refreshes a running job's `updated_at` every `JOB_HEARTBEAT_SECONDS` (default 30), so `running` with no heartbeat for `JOB_RUNNING_LEASE_SECONDS` (default 300) → back to `pending`, or `failed` after `JOB_MAX_ATTEMPTS` (default 3) claims, counted as `status="lease_expired"`; `dispatched` but never claimed after `JOB_DISPATCH_LEASE_SECONDS` (default 3600) → re-sent. A long job is never mistaken for a dead one; a killed worker is noticed after about 5 minutes. The chart gives workers `terminationGracePeriodSeconds: 120` to finish the job in hand on SIGTERM.

**Stopping an execution.** `POST /executions/{id}/cancel` moves a pending, running or paused execution to `cancelled` (terminal), marks every job no worker has started — `pending` or `dispatched` — `cancelled`, and the dispatch loop and the planner stop within about a second (a blocking source call, e.g. the enumeration before the first slice, finishes first). Messages already in Kafka are skipped: the worker's claim (`pending|dispatched → running`) fails for a cancelled job, so it is committed and counted as `reflowfy_jobs_processed_total{status="cancelled"}`. Jobs already `running` cannot be interrupted and finish, so a little more may be delivered after the call returns. Stats show `jobs_cancelled`. A finished execution answers 409.

`PATCH /executions/{id}/state` is an administrative override and accepts only `failed` (force-fail a stuck run, with `error_message`) and `cancelled`, from a non-terminal state. Every other state has an owner and answers 422 with a pointer to it: `completed` is set by the system once all jobs finished, `paused`/`running` by `/pause` and `/resume` (which also restart the loop), a retry by `/retry-failed`. It used to set any string and any transition, e.g. `running → completed` while a loop was dispatching, or `completed → running` with nothing driving it.

**Pause during planning** cannot stop the planner half way — its position in the source lives in the manager's memory — so it means: planning finishes, nothing is dispatched until `/resume`.

**Retrying failures.** Nothing retries a `failed` job by itself (the DLQ is not a failure sink). `POST /executions/{id}/retry-failed[?error_contains=text]` requeues the execution's failed jobs (attempts reset, error cleared), reopens the execution and restarts its loop; a retried job runs from scratch, so one that failed after a partial write sends those records again unless the run used `enable_duplicate_jobs=false`. A paused execution answers 409; a running one just picks the requeued jobs up.

The `/run` `rate_limit` override is persisted on the execution (`executions.rate_limit`) and reused when a restarted manager resumes it.

### Content deduplication & DLQ

Job IDs are plain `uuid4`. Idempotency is enforced **worker-side, per record**: when `enable_duplicate_jobs=False` the manager sets `dedup_check` on the payload, and right before the send the worker hashes every record it is about to write (`execution/content_dedup.py` `compute_record_hash`: pipeline name + the record's identity + `destination_identity()` — the destination's class and config, **minus credentials** — + the params the plan attached with `job(...)`, minus the slice keys `current_id`/`current_ids`/`input_ids`). It claims those hashes in bulk in `processed_records` (raw 32-byte digest PK, `INSERT … ON CONFLICT DO NOTHING RETURNING`, 1000 per statement) and drops the records another job already holds. Overlapping slices, reordered records and a job with one changed record are all handled; a job with nothing new ends `deduplicated`, a partly new one `completed` with `records_deduplicated` in its stats. A claim already held by the *same* `job_id` counts as won (a job re-run after its worker died is not a duplicate of itself), the worker releases its claims on any exit including shutdown, and a job whose transformations dropped every record is not a duplicate. When records were dropped, `define_destination` is called again with the records kept: it saw them all, and an `api_destination` body is sent verbatim.

What is hashed is the record **as sent** (after transformations), so a transformation that stamps `processed_at` makes every record new work. `dedup_key` names what identifies a record instead:

```python
class OrdersById(AbstractPipeline[RuntimeParams]):
    name = "orders_by_id"
    enable_duplicate_jobs = False
    dedup_key = "order_id"                     # a field of the record

class OrdersPerTenant(AbstractPipeline[TenantParams]):
    name = "orders_per_tenant"
    enable_duplicate_jobs = False
    dedup_key = staticmethod(lambda record, params: (params.get("tenant"), record["order_id"]))
```

A callable gets `(record, runtime_params)` — use the params when part of the identity lives only there — and must be wrapped in `staticmethod` in a class body. A missing field fails the job (`KeyError`) instead of sending unchecked. A key that leaves a field out means a change to that field is **not** resent until the claim expires. `dedup_key` with `enable_duplicate_jobs = True` raises in `__init__`. Two traps: per-job params count, so jobs tagged differently (`job(rows, slice="a")`) never dedup against each other; and an `api_destination` whose body carries the records makes the body part of every hash, so it needs `dedup_identity=` (see `tests/e2e/test_pipelines/record_dedup_pipeline.py`, `tests/unit/test_record_dedup_worker.py`).

Observability: `Job <id>: N of M records already processed — dropped`, `records_deduplicated` on the job's stats, and `reflowfy_records_deduplicated_total{pipeline}` (a fully deduplicated job is still `jobs_processed_total{status="deduplicated"}`).

Size: one row per record sent, about 0.6–0.7 GB per 4M records (estimate), purged after `CONTENT_DEDUP_RETENTION_HOURS` (24) in chunks of `CONTENT_DEDUP_PURGE_CHUNK_SIZE` (10 000) per transaction.

**Upgrade from job-level dedup.** Dedup used to claim one hash per job in `processed_content`, and record hashes never match those. So the worker still computes the old job hashes (`compute_content_hash`, with and without the destination) and treats a job whose old hash another job holds as already written (`held_by_other_job`, `DEDUP_HONOR_LEGACY_HASH`, default true). Nothing writes `processed_content` any more; set the flag to `false` once the 24h retention has emptied it, and the guard, the table and `compute_content_hash` can then be removed.

This is content idempotency across executions. Within one execution, the job-level claim above already stops a redelivered message from running a job twice.

The **DLQ is not a failure sink** despite the name — nothing auto-populates it. It is a "run this pipeline later with these params" queue: `POST /dlq/schedule` takes a `job_payload` that is really *runtime params* (an unregistered pipeline answers 404, as `/run` does), and `dlq_scheduler.py` calls `run_pipeline` with them when due. Due jobs of one pipeline are grouped by identical params — one execution per distinct set, so every job runs with its own params (`/dlq/pipelines/{name}/dispatch` returns all of them as `execution_ids`). `dlq_routes.py` handles inspection. `pipeline_scheduler.py` runs cron-scheduled pipelines (5-field cron, validated at class-definition time in the metaclass).

### Distributed E2E suite

`tests/e2e_distributed/` tests AbstractPipeline and IdBasedPipeline over **Kafka and real workers** against a deployment you point it at (see its README); `tests/e2e` below only covers local mode. Pipelines write to a sink that keeps a delivery ledger, and each test asserts every record arrived exactly once. It skips as a whole when no deployment is reachable. `pytest -m chaos` kills pods and needs `E2E_DIST_KILL_WORKER_CMD` / `E2E_DIST_RESTART_MANAGER_CMD`.

## E2E test mechanics

`scripts/run_e2e_tests.sh` does **not** test the source tree in place. It builds a wheel, runs `reflowfy init` into a throwaway `e2e_workspace/`, patches the generated Dockerfiles to install the local wheel, rewrites `docker-compose.yml` (E2E ports 5433/8002/8003, container prefix `reflofy-e2e-`, `PIPELINE_MODULE=tests.e2e.test_pipelines`), brings up `docker-compose.e2e-infra.yml` (Kafka/ES/mock servers) plus the app, seeds test data, then runs `pytest tests/e2e/`. So changes to packaging, the CLI scaffolding, Dockerfiles, or compose files are all exercised by the E2E run, and a stale build will mask source edits — always rebuild.

**E2E only covers local mode.** The scaffold it runs (`reflowfy init`) sets `EXECUTION_MODE: local`, `KAFKA_BOOTSTRAP_SERVERS: 'ignored:9092'`, and defines **no worker service** — so every E2E exercises `LocalDispatcher`, and the production path (manager → Kafka → `KafkaJobConsumer` → worker) has unit coverage only (`tests/unit/test_define_jobs_worker_path.py`, `test_executor_worker_sourcing.py`). To exercise it, copy the ready-made `scripts/e2e-distributed.override.yml` into `e2e_workspace/docker-compose.override.yml` (it flips `reflow-manager` to `EXECUTION_MODE: distributed` and adds a `worker` from `Dockerfile.worker`), then:

```bash
./scripts/run_e2e_tests.sh --test-file tests/e2e/test_typed_params.py --keep-docker
cp scripts/e2e-distributed.override.yml e2e_workspace/docker-compose.override.yml
cd e2e_workspace && REFLOWFY_BASE_IMAGE=reflowfy-base:local docker compose up -d --build worker reflow-manager
cd .. && uv run pytest tests/e2e/test_typed_params.py   # now runs over Kafka
```

Note `docker compose up` reuses a stale `e2e_workspace-worker` image, so `--build` matters, and the override must not be in place during the script's own run or it will silently switch that run to distributed too.

## graphify knowledge graph

A graphify graph exists at `graphify-out/`. Per `AGENTS.md`: for architecture / cross-module "how does X relate to Y" questions, prefer `graphify query "..."`, `graphify path "A" "B"`, or `graphify explain "..."` over grep, and read `graphify-out/GRAPH_REPORT.md` first. After modifying code in a session, run `graphify update .` to keep it current (AST-only, no API cost).
