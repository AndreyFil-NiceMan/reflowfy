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

**Subfolders show in Swagger.** A pipeline in `pipelines/bob1_pipeline/pipeline1.py` gets its run route at `/pipelines/bob1_pipeline/<name>/run`, grouped under a `bob1_pipeline` Swagger tag (the folder alone, no `pipelines/` prefix) (`api/routes.py` `pipeline_folder`, from the class's `__module__` relative to `PIPELINE_MODULE`). There is no flat alias: a pipeline answers only on its folder path. Pipeline names stay globally unique; the folder is only for display and URLs. `GET /pipelines` reports `folder` and `run_path`. There is no per-pipeline `GET …/status` route (removed); use `GET /pipelines` or `/executions/{id}/status`.

### Typed runtime_params

`runtime_params` is one flat dict merging the user's parameters with the framework's reserved execution-context keys (built by `build_flat_runtime_params` in `core/execution_context.py`). `core/runtime_params.py` names that shape: `RuntimeParams` (a `total=False` TypedDict of the reserved keys), the `P` TypeVar bound to it, and the `Param` marker for defaults.

A pipeline declares its parameters **once**, as a type: `class MyPipeline(AbstractPipeline[MyParams])` where `MyParams(RuntimeParams, total=False)` adds its own keys. Every hook then receives `MyParams` instead of `Dict[str, Any]`, and `define_parameters()` is *derived* from it by `params_from_typeddict` (`core/abstract_pipeline.py`) — so validation, defaults, CLI prompts and the generated OpenAPI routes all follow from the same declaration. A hand-written `define_parameters()` override still wins.

**Declaring one is the recommended style, not a requirement.** A pipeline with no parameters of its own uses `AbstractPipeline[RuntimeParams]`, so nobody needs an empty TypedDict. Every pipeline under `pipelines/` and `tests/e2e/test_pipelines/` declares one; an unsubscripted `AbstractPipeline` (many in `tests/unit/`) still registers and runs, with no warning, and its hooks get `Any`.

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

**`TypedRecord[T]`** (`core/types.py`) is the shaped record: a generic TypedDict `{"data": T}`. Describe what the source returns (`class Order(TypedDict)`), annotate with `List[TypedRecord[Order]]`, and subclass it for keys transformations add (`class TaxedOrder(TypedRecord[Order]): tax: NotRequired[float]`). It is a new name, not a change to `Record`: a TypedDict is closed, so `Record` must stay `Dict[str, Any]` or every transformation that adds a key breaks. `typing_extensions.TypedDict` because a generic TypedDict needs it on 3.10.

The package ships `reflowfy/py.typed` (PEP 561), so installs are type-checked by consumers. It is registered in both `[tool.setuptools.package-data]` and `MANIFEST.in`.

Where a third-party library genuinely has no types, the suppression is **file-scoped, never repo-wide**, with a header comment naming the library: `destinations/kafka.py`, `worker/consumer.py` and `reflow_manager/dispatcher.py` (aiokafka ships no stubs), and `sources/s3.py` (`reportTypedDictNotRequiredAccess`, because boto3-stubs marks `Key`/`Size`/`ETag` NotRequired). That rule stays live everywhere else on purpose — it is what catches `runtime_params["execution_id"]` subscripting on the all-optional `RuntimeParams`. `boto3.client("s3", ...)` passes every argument explicitly: boto3 is typed by overloads on the literal service name, and a `**kwargs` splat matches none of them, making the client and everything derived from it Unknown.

### Validating records at runtime

The same TypedDict is checked against the real records with pydantic (`core/record_validation.py`), opt in, on the pipeline class:

```python
class OrdersPipeline(AbstractPipeline[OrderParams]):
    record_type = TypedRecord[Order]   # checked right after fetch()
    on_invalid_record = "skip"         # "fail" (default) fails the job
    validate_steps = True              # check each step's output against its return annotation
```

- **Validate only, strict.** Records are never replaced by pydantic's output: it would coerce (`"3.5"` → float) and silently drop undeclared keys. Strict means what passes is what the annotated code sees; an `int` still passes as `float`. Undeclared keys do not fail. One `TypeAdapter(List[T])` per type, cached; ~2.7 µs per record.
- **After fetch** (`job_runner.check_record_type`): runs before `define_transformations`, which then sees only the kept records. `"fail"` raises `RecordValidationError` (a `PipelineError`): `record 3 of 50 does not match TypedRecord[Order]: data.amount: missing`. `"skip"` drops them the way `SkipRecord` does, reason `invalid data.amount: missing`. The check is reported as an `AppliedStep` named `record_type` with `mode="validate"`, so the worker, `LocalExecutor` and `reflowfy test` show its time and skips with no extra code.
- **After each step** (`validate_steps`, `transformation_runner._check_step_output`): the promised type comes from `step_output_type` — an explicit `output_type` class attribute (a record type, not a list; how a `BaseTransformation` subclass declares one), else the `@transformation` function's `-> List[X]` (or `Tuple[X, ...]`) or the `@record_transformation` function's `-> X | None`. `-> Records`, `List[Any]`, a `dict` type, no annotation or an unresolvable forward reference: unchecked. A mismatch is a bug in that step, so it **always** fails the job with `StepOutputError` (a `TransformationError` naming the step and `culprit_index`), whatever `on_invalid_record` says.
- Reasons and messages name a field path and pydantic's error type, never a value; list positions inside a record become `[]` (`data.items[].price: float_type`) so reasons group.

Observability: a skip is the existing record-skip path — `records_skipped`/`record_skip_reasons` in the job's `stats`, the `… skipped N records` log line, `reflowfy_records_skipped_total{transformation="record_type"}`. A failure is an ordinary `failed` job whose `error_type` (stats and `reflowfy_jobs_failed_total`) is `RecordValidationError` (bad source data) or `StepOutputError` (a step broke its contract), so the two are told apart from each other and from other failures. Worked example: `pipelines/typed_records_pipeline.py` (`reflowfy test typed_records`); tests: `tests/unit/test_record_validation.py`.

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
  concurrency=, timeout=, on_error=)` on an `async def f(record, params) -> record | None`): every record
  awaited, `concurrency` at a time (default `TRANSFORM_CONCURRENCY`, 16) by a fixed pool of
  workers, not a task per record. Order is kept, `None` drops the record, the first failure
  cancels the rest and raises `TransformationError` with the exact `culprit_index` (via
  `RecordFailure`) — or, with `on_error="skip"`, drops that record instead (see "Dropping a
  record") — `SkipJob` from any record drops the job, and `raise SkipRecord("why")`
  drops just that record *with* a reason (see below). It must return **one record (a `dict`,
  so `Record`/`TypedRecord`) or `None`**: a list, tuple, string or scalar fails the job with
  `record N: RecordContractError: returned list; a per-record step returns one record or None`
  — a contract error, so `on_error="skip"` does not hide it (it used to pass a list on as one
  "record"). Subclass it for a client shared by
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

### Authoring transformations: options and `should_apply_transformation`

**Options.** A `@transformation` / `@record_transformation` function's parameters after
`(records|record, runtime_params)` are options, given as keyword arguments when the step is
built and passed to every call: `def multiply(records, params, *, factor)` is used as
`multiply(factor=2)`. The constructor binds them against the function's signature
(`inspect.signature(...).bind`, signature taken once at decoration), so an unknown or missing
option raises `TypeError` naming the transformation inside `define_transformations`, not
mid-job; they are kept on the instance as `.options`. `TransformFn` is
`Callable[Concatenate[Any, Any, ...], Any]` (the per-record one returns
`Awaitable[Mapping[str, Any] | None]`, `Mapping` so a TypedDict return is accepted): it still
rejects a function with fewer than two positional parameters, and admits the options.
`validate_steps` keeps reading the function's return annotation (`_transform_fn`).

**`should_apply_transformation(transformation, runtime_params, records)`** (a pipeline method,
default yes) is asked before each step, in a thread like other sync user code, with the records
as they are at that point. A declined step keeps its position — it counts as applied, so
re-resolution never offers it again (append-only rule) — and is reported as an `AppliedStep`
with `mode="skipped"` (`SKIPPED_MODE`), `records_in == records_out`, `duration` = the hook's
time; it is not run and not validated. The hook is not called at all when the pipeline does
not override it (no thread hop). If it raises, the job fails with a `TransformationError`
naming the step ("should_apply_transformation raised KeyError: …"); `SkipJob` drops the job.
Observability: info log `transform_declined` from the runner (so worker, `LocalExecutor` and
`reflowfy test` all have it), the worker's existing `transform_applied` debug line with
`mode=skipped`, `reflowfy test` prints `– <step>  not applied (should_apply_transformation
declined it)` at any verbosity and `"mode"` per step in `--json`. No job state or metric: the
job's outcome is unchanged. Tests: `tests/unit/test_transformation_authoring.py`.

### Dropping a record

`raise SkipRecord("why")` inside a per-record transformation drops that one record; the job goes
on. Returning `None` drops it silently (a filter); `SkipRecord` keeps the reason. The step
returns its skips beside its records (`RecordTransformation.apply_records` → `RecordResults`), so
nothing is stored on the shared transformation instance. Raised from a sync or whole-batch async
step it fails the job saying to use `@record_transformation` (there is no "current record"
there). `@record_transformation(..., on_error="skip")` (subclass form: `on_error = "skip"` class
attribute, checked at class definition; the decorator checks its value at decoration) drops a
record whose call raised — a timeout included — through the same path, reason
`type(exc).__name__` (never the message: it can carry record values, and reasons are counted),
the same semantics as `@record_destination`'s `on_error`. `"fail"` is the default; `SkipJob` and
`SkipRecord` keep their meaning either way, and a wrong return type still fails. Observability: `AppliedStep.skip_reasons`; the worker logs `Job <id>: <step> skipped N
records ('reason' xN, …)`, stores `records_skipped` and `record_skip_reasons` in the job's
`stats`, and counts `reflowfy_records_skipped_total{pipeline, transformation}` — a new counter,
because a record outcome has no place on the per-job `jobs_processed_total` and adding a label to
`records_processed_total` would break its existing series. Reasons are counted, so they must be
short fixed phrases: at most 20 distinct per step (the rest go to "(other reasons)"), 200
characters each. `LocalExecutor` and `reflowfy test` show them too (`--json`: `records_skipped` and `skip_reasons` per step). `reflowfy test` also logs both skips with the worker's events (`records_skipped`, `job_dropped`, plus `jobs_dropped` in `--json`), so `LOG_DESTINATION=elastic` ships them from a CLI run as `service.name: cli`. That works because `ElasticLogHandler` registers `close()` with `atexit` itself and drains its queue there: left to `logging.shutdown`, urllib3's own exit hooks emptied the client's pool first and a short process shipped nothing.

### API destination: per job or per record

`define_destination(records, params)` runs once per job and returns one destination; the
worker calls `send(records)` once. `kafka_destination` already writes one message per
record. `api_destination` by default sends **one request per job** with `body=` verbatim
(build it from `records`). `api_destination(url=".../users/{user_id}", per_record=True)`
works like Kafka instead: one request per record, the record as the body (`body_field="x"`
sends only `record["x"]`), and `{field}` placeholders in the URL filled
from each record, URL-encoded (a missing field fails the job, not retried). A retry
re-sends the whole job, so the endpoint should be idempotent. Errors name the record's
position (`record 2 of 3: HTTP 404 …`), never its contents. Query params are per record
too: `{field}` placeholders in `params=` values are filled from each record (`"{field}"`
alone keeps the value's type, so a list repeats the param; httpx does the encoding), and
`params_field="query"` merges the dict in `record["query"]` over `params` (`None` value
removes a key) and leaves that field out of the body. Same failure rule as the URL: a
missing placeholder field or a non-dict `params_field` fails the job before its request.
Its `identity_config()` is the whole config. No `dedup_identity` needed: the records are not
in the config. Headers are per record too: a `headers` value with a
`{field}` placeholder is filled per request (the others ride on the client), or `headers=` is a
function `record -> {str: str} | None` (per-record mode only; kept off `config`). A request
passes `headers=` to httpx only when it has its own. Tests:
`tests/unit/test_api_destination.py::TestPerRecord`, `test_destination_pool.py::TestPerRecordHeaders`.

**No `concurrency=` on the destinations (removed in 1.1.4).** `kafka_destination` sends a
job's messages as **one batch**: every message is handed to `producer.send()`, then all the
ack futures are awaited together (`asyncio.gather`), so aiokafka batches them. Order on a
partition is kept — aiokafka mutes a partition while its batch is in flight
(`aiokafka/producer/sender.py`). A message refused on hand-over stops the loop; every future
already handed over is still awaited; the error names `record N of M`. Measured on a local
broker (1000 messages, 3 topics): 27–38 ms batched vs 259–725 ms at the old default of one
ack at a time, and faster than the old `concurrency=16` (42–46 ms). Passing `concurrency=`
raises a `TypeError` saying so (it would otherwise fall into `**producer_config`). A
per-record `api_destination` keeps `send_concurrency` requests in flight — set by a
`@record_destination`, else `default_record_concurrency()` (`TRANSFORM_CONCURRENCY`, 16) —
through `core/async_utils.for_each_concurrently`, so requests may land out of order. Tests:
`tests/unit/test_destination_concurrency.py`; E2E
`tests/e2e/destinations/test_api_destination_per_record.py` (the mock webhook's
`/items/<run>/<id>` counts requests open per job; the pipeline's `concurrency` param wraps the
destination in a `@record_destination` with that limit) and `test_kafka_destination_concurrency.py`
(real broker: exactly-once and each job's order with batching); distributed
`tests/e2e_distributed/test_destination_concurrency.py` (`dist_per_record`, sink `PUT /record/…`).

**Per-record templates — one engine.** `core/record_templates.py` (`fill`, `fill_value`,
`lookup`) is the only `{field}` filler: Kafka `headers=`/`key=`, API `url`/`params`/`headers=`.
`{data.region}` walks nested dicts (a key containing the dot wins first); only `{` + dotted
identifier + `}` is a placeholder, nothing is evaluated (not `str.format`). A missing field
raises `TemplateError` (a `ValueError`: not retried) — `"<owner>: <what> placeholder {f} has no
matching field on record N of M"`. Both destinations build every record's templates **before
the first send**, so a bad record writes nothing (the API used to fill each request lazily and
had sent the records before it).

**Kafka value, key, headers.** `value_field="data"` sends `record["data"]` as the message
value. `key=` sets the
message key per record: a template (`"{customer_id}"`) or a function `record -> str | bytes |
None`; like headers it is not in `identity_config()`, a template is `config["key"]`, a function
stays off `config`. `kafka_destination(headers=...)` sets message headers: a `{name: str}`
mapping, `{field}` placeholders filled from each record (`{"tenant": "{tenant_id}"}`), or a
function `record -> {name: str | bytes | None} | None` for anything computed. Headers for every
record are built before the first send, so a missing field or a function that raises fails the
job (`ValueError`, not retried) with nothing written, naming the record's position, never its
contents. They replace a metadata header of the same name (`LocalExecutor` passes slice params
as `metadata`). A static mapping is `config["headers"]` only when set; a function is kept off
`config` (its repr is an address, like `ssl_context`). Neither is in `identity_config()`:
headers describe the message, not where it goes, so adding them does not change the dedup hash.
Tests: `tests/unit/test_kafka_destination_send.py::TestHeaders`; E2E
`tests/e2e/destinations/test_kafka_destination_headers.py` (real broker: static, function and
missing-field modes of `e2e_kafka_headers_test`, topic `e2e-kafka-headers`).

### A destination per record: `@record_destination`

`define_destination` runs once per job; `@record_destination("name", on_error="fail"|"skip",
concurrency=None, timeout=None)` turns a function `(record, runtime_params, **options) -> BaseDestination`
(sync or `async def`) into a `RecordDestination` subclass (`destinations/record.py`) that
`define_destination` returns (`return stt_engines()`; kwargs become `options`). It is for
records of one job going to different topics/clusters/credentials.

- **Routing is a step of the job runner**, not of the send: `run_job_records_async` calls
  `RecordDestination.route()` right after `define_destination`, so every record has its
  destination before dedup and before the first send. It is reported as an `AppliedStep`
  named after the destination with `mode="route"` (`job_runner.ROUTE_MODE`), so its time and
  skips appear wherever a transformation's do (worker, `LocalExecutor`, `reflowfy test`).
- **Grouping:** destinations with the same class, `config`, `retry_config` and `dedup_identity`
  (`_group_key`, JSON) share one — 200 records over 3 topics open 3 producers. Records are
  mapped to their group by `id(record)` (the route keeps them alive). Anything kept off
  `config` (a Kafka `headers` function, `ssl_context`) comes from the group's first record.
- **Failures:** `SkipRecord` skips with its reason; `on_error="skip"` skips any other exception
  with `type(exc).__name__` as the reason (never the message: it can carry record values, and
  reasons are counted); `"fail"` (default) raises `RecordDestinationError` (a `PipelineError`)
  naming the destination, the record's position and the exception type. A return that is not a
  `BaseDestination` (or is another `RecordDestination`) always fails. `SkipJob` drops the job.
- **Shared connections (`destinations/base.py` `ConnectionPool`).** One pool per
  `RecordDestination` (per job); `_group_index` sets `connection_pool` and `send_concurrency`
  on every group. Kafka takes its producer from the pool keyed by `_connection_key()` (all
  producer kwargs — cluster, credentials, compression, producer_config — **not** the topic; an
  `ssl_context` is keyed by identity), API its client keyed by scheme+host, client headers and
  timeout. `pool.get` creates each once under a per-key lock (groups send concurrently); the
  pool, not the destination, closes them, in `RecordDestination.close`. Keyed by host on
  purpose: one client for the whole job measured ~20% slower than one per host (httpcore pool
  contention). The shared client's keep-alive pool is sized to the concurrency (httpx keeps 20
  by default and churned connections above it). `health_check` runs once per
  `BaseDestination.health_key()` (Kafka: per cluster, or per topic+group when it measures lag;
  API: per host + client settings; default `None`: every destination). Measured (200 records,
  5 ms latency, plain HTTP): a destination per record went from 1,685 ms / 200 clients / 200
  HEADs / 156 ms loop stall to 341 ms / 1 / 1 / 22 ms, the template pattern unchanged.
- **One request budget.** `ConnectionPool(request_limit=effective_concurrency())` holds an
  `asyncio.Semaphore` (created lazily, in the sending loop); a pooled per-record API destination
  acquires it per request, so `concurrency` caps requests across **all** groups together (it
  used to multiply: groups × each destination's own `concurrency`).
- **Advice.** A job of `ADVICE_MIN_RECORDS` (10)+ records that built one destination per record
  logs a warning once per record destination per process (`destination_per_record`): per-record
  values belong in templates (encoding, grouping).
- **Worker:** dedup hashes each record against **its own** destination
  (`destination_identity(destination.destination_for(record))`), so one record to two topics is
  two writes; after dedup the destination is **not** re-resolved for a `RecordDestination`
  (each was built from its own record, and re-routing would count skips twice).
  `send_with_retry` sends the groups through `for_each_concurrently` (the record
  transformations' worker pool), each with its own retries, so a failing group does not
  re-send the ones already written; the first failure cancels the sends in flight.
  `health_check` (every group healthy) and `close` (all closed, first error raised after) run
  through the same pool.
- **Concurrency/timeout, as `@record_transformation`:** `effective_concurrency()` (the
  decorator's `concurrency`, else `TRANSFORM_CONCURRENCY`, 16) is the request budget above, the
  groups in flight and, for an `async def` function, records routed at once
  (`AppliedStep.concurrency` reports the latter; `None` for a sync function, which routes
  sequentially in one thread). `concurrency=1` sends one request / one group at a time. `timeout` (async only — a sync function in a
  thread cannot be interrupted, so it raises `TypeError` at decoration) wraps each call in
  `asyncio.wait_for` and turns a timeout into `TimeoutError("timed out after Ns")`, which then
  follows `on_error` (reason/type `TimeoutError`).
- Observability: the existing skip path (`records_skipped`/`record_skip_reasons`,
  `reflowfy_records_skipped_total{transformation=<name>}`, the `… skipped N records` line);
  `records_routed` info log `<name> routed N records to K destinations`; a warning when every
  record was skipped; `destinations` (count) in the job's `stats`, present only for a record
  destination. No new metric: a topic or cluster as a label is unbounded.
- **Sending outside the worker.** `LocalExecutor` used to call `send_with_retry` without
  awaiting it, so it never sent; `_send_and_close` now runs send and close in one loop (a
  Kafka producer belongs to its loop), and `reflowfy test` closes the destination in the loop
  that sent.
- Tests: `tests/unit/test_record_destination.py`, `test_destination_pool.py`,
  `test_record_templates.py`; E2E
  `tests/e2e/destinations/test_record_destination.py` (`e2e_record_destination_test`, one job to
  topics `e2e-rd-whisper`/`-google`/`-azure`, in route, skip and fail modes).

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

### Custom sources

A `BaseSource` subclass in the user's project runs on workers like a built-in one: every
concrete subclass registers itself in `SourceFactory` under its class name when the class is
defined (`BaseSource.__init_subclass__`; abstract ones are skipped), and discovery imports the
module on every service. A source needs `fetch` and `health_check`, and usually `split`
(default: one job); `split_jobs` is no longer abstract (unused since the v2 schema, it raises
`NotImplementedError`). The worker rebuilds each job's slice with the classmethod
`from_config(config)`: `cls(**config)` by default, `cls(config)` when `__init__` keeps
`BaseSource`'s `(self, config)` signature, or an override. `SourceFactory.serialize` proves
that contract at planning, once per `(type, config keys)` shape per process
(`check_round_trip`): JSON-encodable config, type registered to this very class, rebuild
succeeds and gives the same config — otherwise `SourceRoundTripError` naming the fix, instead
of every job failing on the worker. Two classes with one name log `source_type_replaced`.
Tests: `tests/unit/test_custom_sources.py`; E2E `tests/e2e/test_custom_source.py`
(`e2e_custom_source`).

### SQL source: bound values, `| ident`

`sql_source` query templates are rendered by `render_sql_template` (`sources/sql.py`), not the
generic resolver: every `{{ expr }}` **value is a bind parameter** (`:reflowfy_p0`…), never
spliced into the SQL. `'{{ x }}'` quoted on its own is bound in place of the literal (old
templates keep working; a space keeps `::timestamp` after it parseable); a list/tuple is an
expanding bind (`id IN {{ ids }}` or `IN ({{ ids }})`); a value in a comment is dropped; a
missing parameter fails. **Identifiers need `{{ name | ident }}`** (validated
`[A-Za-z_][A-Za-z0-9_.]*`): a plain `{{ }}` after `FROM`/`JOIN`/`INTO`/`UPDATE`/`TABLE`/`BY`/`AS`,
next to a `.`, inside `"…"` or inside a longer `'…'` literal raises `SqlTemplateError`
pointing at `| ident` — this was a behaviour change (it used to be string substitution, i.e.
injection). `params={...}` binds hand-written `:name`s; `id_column` is validated as an
identifier. `split()` wraps the rendered SQL (`WHERE id >= :reflowfy_lo AND id < :reflowfy_hi`)
and each slice carries the SQL and every value in `config["params"]` with `template=False`, so
the worker runs the same statement without the runtime params. One trailing `;` is dropped
before any wrapping (and the wrappers put `)` on a new line, so a trailing `--` comment is
harmless); `fetch(limit=n)` is `fetchmany(n)`. Tests: `tests/unit/sources/test_sql_binds.py`.

### Secrets and API auth

- **`secret("ENV")`** (`core/secrets.py`; export it from `reflowfy` in the exports PR) is a
  `str` subclass holding the variable's value but remembering its name. `SourceFactory.serialize`
  writes `{"$secret": "ENV"}`, `SourceFactory.create` reads the variable from the worker's own
  environment (`SecretError` naming it if unset; no default, since a default would not travel).
  The manager plans with it too (`split()` connects), so set it on both. A source config that
  holds a *literal* credential when serialized logs `literal_secret_serialized` once per
  pipeline/type/field.
- **Redaction.** `BaseSource.secret_fields` names a source's credential keys (Sql:
  `connection_url` — only the URL's password; Elastic: `auth`; S3: `aws_secret_access_key`;
  API: `auth_token`, `headers`). `redact_row` hides `job_payload`/`runtime_params` in every
  manager route returning job, execution, schedule or DLQ rows: declared fields, any
  credential-named key at any depth (`password`, `token`, `api_key`, `Authorization`,
  `secret`…) and the password of any `scheme://user:pass@host`. The stored rows are unchanged.
- **`REFLOWFY_API_TOKEN`** (`core/api_auth.py`): set (non-blank) on the API and the manager,
  every route but `/health` and `/metrics` needs `Authorization: Bearer <token>` (CORS
  preflight passes; the token is read per request). The API's `DistributedExecutor` sends it
  to the manager. Rejections log `auth_rejected` with `missing`/`invalid`, never the token.
  Helm: `auth.apiToken` or `auth.existingSecret` (api + reflow-manager only); compose:
  `${REFLOWFY_API_TOKEN:-}`. `/docs` is guarded too.
- Tests: `tests/unit/test_secrets.py`, `tests/unit/test_api_auth_and_redaction.py`.

### Dispatch loop, leases and redelivery

Phase 1 (planning) is unchanged: jobs are streamed into Postgres in groups of `CHECKPOINT_BATCH_SIZE`, each tagged with a `batch_number` that `/executions/{id}/stats` reports as "checkpoints". **Batches no longer gate dispatch.** Phase 2 (`PipelineRunner._drive_execution`) is one continuous loop per execution:

- keeps up to `MAX_IN_FLIGHT_JOBS` (env, default 1000) jobs `dispatched`/`running`, topping the window up as jobs finish; per job: rate-limit token → send → `mark_job_dispatched` immediately (so the DB never shows `pending` for a job already sent, and a send that fails mid-chunk leaves the rest `pending` to retry — it used to strand them as `dispatched`);
- checks the execution state about once a second: `paused` stops sending (in-flight jobs finish), and `POST /executions/{id}/resume` starts a new loop. One loop per execution, by a lease on the execution row (`executions.driver_id` / `driver_heartbeat_at`): the owner heartbeats about once a second, others stand by and take over once it is older than `DRIVER_LEASE_SECONDS` (default 60), and a loop that finds itself replaced stops. At startup a manager frees leases left by an earlier process on its own hostname (a container restart), so recovery is immediate there; a replacement pod with a new name waits out the lease. Not a Postgres advisory lock: a force-killed pod's session lock lingers on its zombie connection until TCP keepalive (hours) and blocked every resume;
- has **no batch timeout**: slow or stopped workers never fail the execution; it warns every 10 min with no progress instead;
- retries database errors with backoff (5 s doubling to 60 s) instead of exiting — the job rows are the state, so a Postgres restart just pauses dispatch;
- runs in one event loop, so the Kafka producer is created once per execution.

**Planning crashes.** Phase 1 holds a heartbeated lease on the execution (a thread refreshes `driver_heartbeat_at` every 10 s while planning blocks, e.g. enumerating 2M ES docs) and sets `executions.plan_complete` only after the last job is saved. A manager killed mid-plan leaves `plan_complete = false`, which used to look like "no unfinished jobs" and ended `completed` having processed nothing (or dispatched a fragment of the plan). Recovery (`PipelineRunner._recover_plan`, also for `pending` executions) waits while the planning lease is fresh — a rollout's old pod may still be planning — and once the lease is gone and the execution is older than `DRIVER_LEASE_SECONDS` it marks the execution `failed` with "The manager stopped while planning this execution… run it again". A plan cannot be continued (the source's position is not saved), so the fix is a new run. `plan_complete` is `NOT NULL DEFAULT FALSE`.

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

This is content idempotency across executions. Within one execution, the job-level claim above already stops a redelivered message from running a job twice.

The **DLQ is not a failure sink** despite the name — nothing auto-populates it. It is a "run this pipeline later with these params" queue: `POST /dlq/schedule` takes a `job_payload` that is really *runtime params* (an unregistered pipeline answers 404, as `/run` does), and `dlq_scheduler.py` calls `run_pipeline` with them when due. Due jobs of one pipeline are grouped by identical params — one execution per distinct set, so every job runs with its own params (`/dlq/pipelines/{name}/dispatch` returns all of them as `execution_ids`). `dlq_routes.py` handles inspection. `pipeline_scheduler.py` runs cron-scheduled pipelines (5-field cron, validated at class-definition time in the metaclass).

### Distributed E2E suite

`tests/e2e_distributed/` tests AbstractPipeline and IdBasedPipeline over **Kafka and real workers** against a deployment you point it at (see its README); `tests/e2e` below only covers local mode. Pipelines write to a sink that keeps a delivery ledger, and each test asserts every record arrived exactly once. It skips as a whole when no deployment is reachable. `pytest -m chaos` kills pods and needs `E2E_DIST_KILL_WORKER_CMD` / `E2E_DIST_RESTART_MANAGER_CMD`.

## E2E test mechanics

**Images.** `Dockerfile.base` puts reflowfy + deps in `/opt/venv` and ships uv; the scaffolded service Dockerfiles (`reflowfy/templates/Dockerfile.*`) are uv-only — no `requirements.txt` — and run `uv sync --locked --no-dev --no-install-project --no-install-package reflowfy --inexact` against the user project's `pyproject.toml`/`uv.lock` (scaffolded by `reflowfy init`, which runs `uv lock`), before `COPY src/`, then `USER 10001` (GID 0, for OpenShift). The base image, not the lock, decides the reflowfy version. `reflowfy build` / `run --build` refuse to start without both files (`cli/utils.check_uv_project`).

`scripts/run_e2e_tests.sh` does **not** test the source tree in place. It builds a wheel, runs `reflowfy init --no-lock` into a throwaway `e2e_workspace/`, unpins reflowfy in its `pyproject.toml` and runs `uv lock`, bakes the local wheel into a `reflowfy-base:local` image, rewrites `docker-compose.yml` (E2E ports 5433/8002/8003, container prefix `reflofy-e2e-`, `PIPELINE_MODULE=tests.e2e.test_pipelines`), brings up `docker-compose.e2e-infra.yml` (Kafka/ES/mock servers) plus the app, seeds test data, then runs `pytest tests/e2e/`. So changes to packaging, the CLI scaffolding, Dockerfiles, or compose files are all exercised by the E2E run, and a stale build will mask source edits — always rebuild.

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
