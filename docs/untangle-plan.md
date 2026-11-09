# Untangle plan (decision log)

Skill: `untangle-codebase`. Options chosen: **Comments** (yes), **Tests** (no).
Status: **plan only, no code changed yet.**

The first pass is incomplete: the four area audits (core, execution/worker,
manager/api, connectors/cli) hit a usage limit before they reported. Everything
below comes from scripted scans (import graph, git churn, grep evidence), so
treat it as the highest-leverage part of the list, not the whole list. Step 2 of
the execution order is to finish that audit.

## Evidence snapshot (2026-11-09)

- 108 modules, ~24k lines. Most churn: `worker/executor.py` (17 commits since
  June), `__init__.py` (12), `destinations/kafka.py` (11), `destinations/api.py`
  (10), `cli/commands/test.py` (9), `api/routes.py` (9).
- Package import edges that point the wrong way, from stable to volatile code:
  `core → execution`, `core → transformations` (both top-level imports);
  `execution → reflow_manager`, `worker → reflow_manager`. There are also lazy
  imports marked "import cycle" in `core/secrets.py` and `sources/base.py`.
- 111 `except Exception`, 12 sleeps, 98 environment-variable reads,
  about 200 bare job/execution state strings in 15 files, and 19 `ALTER TABLE`
  statements in `database.py`.

## Fixes, each with the problems it solves

### F1. One job pipeline that every driver calls
Make fetch → validate → transform → route → dedup → send → close → stats into a
single async `JobRun` in `execution/`. The worker, `LocalExecutor` and
`reflowfy test` become thin drivers: each claims and reports the job, and
renders the result in its own way.
- `run_job_records*` is called from 4 places (`worker/executor.py:332`,
  `execution/local_executor.py:175` and `:344`, `cli/commands/test.py:393`).
  Each caller then repeats the send, close, pool-scope and skip-logging steps
  on its own.
- `local_executor.py:175` and `:344`: `execute` and `_execute_id_based` are
  near-copies of the same per-job loop.
- The skip log line is formatted 4 times: `worker/executor.py:385`,
  `cli/commands/test.py:436`, `local_executor.py:26`, `destinations/record.py:418`.
- Who opens the loop pool and closes the destination in the loop that sent is a
  convention each driver re-implements. There are 4 variants:
  `worker/consumer.py:143`, `local_dispatcher.py:63/87`, `local_executor.py:48`,
  `cli/commands/test.py:343`.
- `isinstance(destination, RecordDestination)` branches at
  `worker/executor.py:441,451,482,592,628` and `job_runner.py:275`. Destination
  types are special-cased instead of every destination exposing
  `destination_for(record)`, which a plain destination would answer with itself.
  With that in place, dedup and re-resolution need no branch.
- History shows the same change landing in `worker/executor.py`,
  `local_executor.py` and `cli/commands/test.py` together.

### F2. Persistence becomes its own package below everything that uses it
Move `models.py`, `db_settings.py` and the content-dedup store into
`reflowfy/store/`, which `reflow_manager`, `worker` and `execution` all import.
Make the SQLAlchemy models the only schema definition.
- `execution/content_dedup.py:19` and `worker/executor.py:32-33` import
  `reflow_manager.models` and `db_settings`. The worker depends on the manager
  service package.
- The schema exists in 3 copies: `models.py` (7 tables),
  `reflow_manager/schema.sql` + `dlq_schema.sql` (5 tables, never loaded by any
  code, only mentioned in CLAUDE.md), and the 17 `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS` statements in `database.py`. Delete the `.sql` files. Derive the
  add-column migrations from the models, or move them to one versioned migration
  list.
- Job and execution states are about 200 bare strings (`pipeline_runner.py` 50,
  `job_manager.py` 25, `manager.py` 24, `execution.py` 21, `worker/executor.py`
  16, …), while `execution/base.py:9` already has an `ExecutionState` enum that
  only the API uses. Use one `JobState` and one `ExecutionState` enum in `store/`
  that every module uses, with the legal transitions defined next to them.

### F3. One typed settings object per concern, read once
- Kafka settings are read in 4 places, and they disagree on defaults:
  `kafka_config.py:24-62` (SASL password default `"reflowfy"`),
  `worker/main.py:48-59` (default `None`), `reflow_manager/factory.py:22-30`,
  `reflow_manager/app.py:648`. The helper in `kafka_config.py` is used only by
  `app.py:106`.
- `TRANSFORM_CONCURRENCY` is parsed in `transformations/base.py:151`, but
  `destinations/record.py:160` and `destinations/api.py:198` lazily import it
  from there. That needless link creates a `destinations → transformations`
  dependency.
- `count_skip_reason` and `top_skip_reasons` (`transformations/base.py:183,191`)
  are used by `destinations/record.py:196`, `worker/executor.py:34` and
  `job_runner.py:62`. They belong with the job stats (F1), not with
  transformations.

### F4. Make `core` the bottom layer
`core` should hold the pipeline model and types and import nothing from
`execution`, `transformations`, `sources` or `destinations`.
- `core/types.py:28` imports `transformations.base` at top level.
- `core/id_based_pipeline.py:78` imports `chunk` from `execution/job_runner.py:88`,
  a 3-line helper. Move it into core.
- `core/job_retry.py:40-62` lazily imports four error classes from sources,
  destinations and transformations to build its default retry lists. Move the
  error hierarchy (`SourceError`, `DestinationError`, `PermanentDestinationError`,
  `StepOutputError`, `TransformationError`) into `core/exceptions.py`, which
  already lazy-imports `TransformationError` at `:42`. The connector modules then
  re-export them.
- `core/secrets.py:221` and `sources/base.py:60` import `SourceFactory` lazily
  ("import cycle"). The factory is a registry keyed by class name that lives
  apart from `BaseSource.__init_subclass__`, which fills it. Put the registry in
  `sources/base.py` and serialization in `sources/`, and delete `factories/`
  (see F5).

### F5. Remove surplus
- `factories/destination_factory.py` (`DestinationFactory`): no production
  caller. Destinations have not travelled on the wire since the v2 job schema.
  Its only uses are the `factories/__init__.py` export and 3 tests.
- `sources/decorators.py:28` `SourceRegistry` and `destinations/decorators.py:31`
  `DestinationRegistry`: only exported from `reflowfy/__init__.py`, and nothing
  reads them. This duplicates `SourceFactory` registration (F4).
- `reflow_manager/manager.py:113-190`: `ReflowManager` methods forward one-to-one
  to `execution_manager` and `pipeline_runner`. Callers should use those directly,
  or the class should hold the execution lifecycle itself, which would be a deep
  module.
- `api/execution.py` `ExecutionTracker`: the API's own in-memory copy of
  execution state, which Postgres already owns. Check whether any route needs it
  beyond the lifetime of a request, and delete it if not. It is the only user of
  `execution/base.py`, which would go too.
- `destinations/schemas.py`: no production import, only 2 tests.

### F6. One place where run requests are validated
- `/run` validation runs as `validate_parameters(apply_defaults(...))` in
  `reflow_manager/app.py:431`, `lifecycle.py:482` and
  `abstract_pipeline.py:940`. Defaults are applied again in
  `pipeline_runner.py:448`, `lifecycle.py:302` (through `getattr`/`hasattr`,
  which is a special case for an attribute every pipeline has) and
  `local_executor.py:284`. Have one `pipeline.prepare_params(raw) -> params |
  errors`, called once at the entry point, and pass the prepared params on.

### F7. Exceptions and sleeps (special cases to triage, not to fix one by one)
- Classify all 111 `except Exception` blocks after F1 to F6 land. Many guard
  paths those fixes remove (lazy imports, duplicate drivers). For each one that
  remains, it either re-raises, records the failure as a job/execution outcome
  (CLAUDE.md "Observability"), or carries a comment naming the external reason.
- The background loops (`pipeline_scheduler.py`, `dlq_scheduler.py`,
  `content_dedup_scheduler.py`, the lease sweeper) each have their own
  sleep/backoff loop. Have one periodic-task helper with the backoff already
  used by the drive loop.

### Comments (chosen)
- Remove comments that restate the line below them, such as `# Get configuration
  from environment` (`worker/main.py:47`), `# Create consumer` (`:83`) and
  `# Update job status in PostgreSQL (async)` (`worker/executor.py:503`). A
  rough first grep finds at least 69 of these.
- Drop `# Import here to avoid circular dependency` comments together with the
  cycles F4 removes (e.g. `transformations/base.py:29`).
- Keep comments about external facts (aiokafka, PgBouncer, httpcore pool
  contention, Elasticsearch PIT behaviour).

## Execution order

1. **Safety net:** record the baseline of unit tests, coverage (≥ 99%), ruff,
   pyright (0 errors) and mypy's error count.
2. **Finish the audit:** resume the four area audits once the usage limit
   resets, add their findings under F1 to F7 (or new fixes), and re-run the
   import-graph script.
3. **F5 (surplus):** delete the dead code first, because it shrinks what every
   later step has to read.
4. **F4 + F3:** fix the layering and settings. These are mostly moves, with
   re-exports kept for the public `reflowfy` API.
5. **F2:** move persistence into `store/`, add the state enums and keep a single
   schema definition.
6. **F1:** build the one job pipeline. This is the biggest change, so do it last
   and on the cleanest base.
7. **F6, F7, comments.**
8. **Repeat:** audit again for special cases, hidden links, needless links and
   surplus, until nothing worth fixing is left. Then run the unit tests and the
   local plus distributed E2E suites to confirm behaviour has not changed.

Every step keeps `reflowfy`'s top-level exports, the v2 Kafka job message and the
database contents compatible. If a step cannot, it stops and asks first.
