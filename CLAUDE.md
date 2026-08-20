# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Reflowfy is a horizontally scalable data movement and transformation framework. Users define pipelines that fetch from **sources** (Elastic, SQL, HTTP API, S3), apply **transformations**, and write to **destinations** (Kafka, HTTP API, Console). Work is sharded into independent **jobs** dispatched over Kafka and processed by a pool of workers, with PostgreSQL as the source of truth for execution state. It is also distributed as a `pip` package with a CLI (`reflowfy`) used to scaffold, run, and deploy user projects.

## Commands

This project uses **uv**. Prefix Python commands with `uv run`.

```bash
# Unit tests (no services required)
uv run pytest tests/unit/ -v
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
- `BaseTransformation.apply` deliberately keeps `Dict[str, Any]` — a transformation is shared across pipelines, so it cannot name one pipeline's params type.
- Worked examples: `typed_params_pipeline.py` (+ `tests/e2e/test_typed_params.py`), `id_based_api_advanced_pipeline_test.py`, `define_jobs_test_pipeline.py`. Derivation is unit-tested in `tests/unit/test_typed_runtime_params.py`.

### Typed records and hook returns

`reflowfy` exports the names a pipeline author needs to annotate a hook without a deep import: `Record` (`Dict[str, Any]`), `Records` (`List[Record]`), `Transformations` (`Sequence[BaseTransformation]`), plus `BaseSource` and `BaseDestination`. They live in `core/types.py`.

They are aliases, not a second generic parameter. The framework's own signatures stay `List[Any]` because a source may yield non-dict records (raw S3 text, scalars), and `List[Any]` accepts a `Records`-annotated override — so `AbstractPipeline` did **not** need an `R` TypeVar next to `P`.

`@transformation` is typed `Callable[[TransformFn], type[BaseTransformation]]` with `TransformFn = Callable[[Any, Any], Any]`. `Any` in every position on purpose: it rejects a wrong-arity function (the mistake that used to reach production as a runtime `TypeError`) without rejecting an author who annotates `runtime_params` with their own params TypedDict — a closed TypedDict is not assignable from `Dict[str, Any]`.

The package ships `reflowfy/py.typed` (PEP 561), so installs are type-checked by consumers. It is registered in both `[tool.setuptools.package-data]` and `MANIFEST.in`.

Where a third-party library genuinely has no types, the suppression is **file-scoped, never repo-wide**, with a header comment naming the library: `destinations/kafka.py`, `worker/consumer.py` and `reflow_manager/dispatcher.py` (aiokafka ships no stubs), and `sources/s3.py` (`reportTypedDictNotRequiredAccess`, because boto3-stubs marks `Key`/`Size`/`ETag` NotRequired). That rule stays live everywhere else on purpose — it is what catches `runtime_params["execution_id"]` subscripting on the all-optional `RuntimeParams`. `boto3.client("s3", ...)` passes every argument explicitly: boto3 is typed by overloads on the literal service name, and a `**kwargs` splat matches none of them, making the client and everything derived from it Unknown.


### Execution modes

`EXECUTION_MODE` env var selects `local` (in-process via `LocalDispatcher`, used by the default docker-compose) or `distributed` (Kafka via `KafkaDispatcher`). Same pipeline code runs in both.

### Content deduplication & DLQ

Job IDs are plain `uuid4`. Idempotency is enforced **worker-side, by content**: when `enable_duplicate_jobs=False` the manager sets `dedup_check` on the payload, and the worker hashes the job's content (`execution/content_dedup.py`: pipeline name + transformation names + fetched records + the job's own `job_params`) and claims that hash in the `processed_content` table. First claimant runs; a later job with the same hash is marked `deduplicated` and never written. Note the hash covers records, not the source descriptor, so it is computed after fetching.

The **DLQ is not a failure sink** despite the name — nothing auto-populates it. It is a "run this pipeline later with these params" queue: `POST /dlq/schedule` takes a `job_payload` that is really *runtime params*, and `dlq_scheduler.py` calls `run_pipeline` with them when due. `dlq_routes.py` handles inspection. `pipeline_scheduler.py` runs cron-scheduled pipelines (5-field cron, validated at class-definition time in the metaclass).

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
