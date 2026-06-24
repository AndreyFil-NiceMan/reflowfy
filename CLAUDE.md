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
uv run mypy reflowfy/
uv run pyright              # config in [tool.pyright]; scoped to reflowfy/

# Run the full local stack via the CLI (Docker Compose under the hood)
uv run python -m reflowfy.cli.main run --build        # add -d/--detach to background
uv run python -m reflowfy.cli.main check              # validate pipelines/config

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

`core/pipeline_discovery.py` (`discover_and_load_pipelines`) imports every module under the `PIPELINE_MODULE` directory (default `pipelines`) plus sibling `sources/`, `destinations/`, `transformations/` dirs, which triggers the metaclass and decorator registration. All three services call this on startup. In E2E, `PIPELINE_MODULE` is overridden to `tests.e2e.test_pipelines`.

### Execution modes

`EXECUTION_MODE` env var selects `local` (in-process via `LocalDispatcher`, used by the default docker-compose) or `distributed` (Kafka via `KafkaDispatcher`). Same pipeline code runs in both.

### Deterministic job IDs & DLQ

When `enable_duplicate_jobs=False`, `pipeline_runner.generate_job_id` hashes stable job content into a SHA256 ID so identical data yields the same job across runs (idempotency). Date/time-like keys are stripped before hashing (see `_DATE_KEY_PATTERNS`) so IDs stay stable. Failed jobs flow to a **dead-letter queue**; `dlq_routes.py`/`dlq_scheduler.py` handle inspection and scheduled retries. `pipeline_scheduler.py` runs cron-scheduled pipelines (5-field cron, validated at class-definition time in the metaclass).

## E2E test mechanics

`scripts/run_e2e_tests.sh` does **not** test the source tree in place. It builds a wheel, runs `reflowfy init` into a throwaway `e2e_workspace/`, patches the generated Dockerfiles to install the local wheel, rewrites `docker-compose.yml` (E2E ports 5433/8002/8003, container prefix `reflofy-e2e-`, `PIPELINE_MODULE=tests.e2e.test_pipelines`), brings up `docker-compose.e2e-infra.yml` (Kafka/ES/mock servers) plus the app, seeds test data, then runs `pytest tests/e2e/`. So changes to packaging, the CLI scaffolding, Dockerfiles, or compose files are all exercised by the E2E run, and a stale build will mask source edits — always rebuild.

## graphify knowledge graph

A graphify graph exists at `graphify-out/`. Per `AGENTS.md`: for architecture / cross-module "how does X relate to Y" questions, prefer `graphify query "..."`, `graphify path "A" "B"`, or `graphify explain "..."` over grep, and read `graphify-out/GRAPH_REPORT.md` first. After modifying code in a session, run `graphify update .` to keep it current (AST-only, no API cost).
