# Untangle plan (decision log)

Skill: `untangle-codebase`. Options chosen: **Comments** (yes), **Tests** (no).
Status: **bug fixes landed; structural fixes not started.** First full audit pass done (2026-11-09):
scripted scans plus four area audits (core, execution/worker, manager/api,
connectors/cli). The headline claims were re-checked by hand: items marked ✔.

## Evidence snapshot

- 108 modules, ~24k lines. Most churn since June: `worker/executor.py` (20
  commits; it co-changed with `job_runner` 7×, `local_executor` 6× and
  `cli/commands/test.py` 5×), `__init__.py` (12), `destinations/kafka.py` (11),
  `destinations/api.py` (10), `api/routes.py` (9).
- Single features spread over many files: rate-limit + lifecycle (354c281)
  touched 8 manager files; redaction (6cdca34) touched 4 route modules.
- Package imports pointing up the stack: `core → execution, transformations`;
  `transformations ↔ core`; `execution, worker → reflow_manager`;
  `destinations → transformations`; plus lazy "import cycle" imports.
- 111 `except Exception`, 12 sleeps, 98 environment-variable reads, about 200
  bare state strings, 19 `ALTER TABLE` statements.

## Bugs found along the way: status

Fixed on this branch, each with a regression test that fails on the old code:

- **Fixed: credential leak.** `GET /executions/{id}/stats` returned
  `runtime_params` unredacted. `/executions/{id}/errors` returned messages and
  tracebacks that quote connection URLs with their passwords.
- **Fixed: secrets in reprs.** `BaseDestination.__repr__` printed
  `sasl_password`, `auth_token` and `Authorization` headers.
- **Fixed: unsaved job outcomes.** A database error while saving an outcome was
  logged once and dropped. The job stayed `running` until the lease sweep re-ran
  it, re-sending its records. The write is now retried (1, 2, 4 s), and a final
  failure logs `job_outcome_not_saved`.
- **Fixed: unbounded retries.** When the retry count could not be read, it
  counted as zero, so `max_retries` was never reached. The job now fails without
  scheduling a retry.
- **Fixed: silent claim release.** A failed release of content-dedup claims was
  swallowed; it is now logged, and on shutdown the release is shielded from a
  second cancel.
- **Fixed: SIGTERM during connect.** The first SIGTERM during the consumer's
  connect backoff force-exited with code 1. A stop during the backoff now exits
  cleanly.
- **Fixed: frozen API status.** The API's `/executions/{id}/status` was a snapshot
  that never changed. It now reads the manager live. `status_url` pointed at a
  route that does not exist.
- **Fixed: CLI exit codes.** `reflowfy run` exited 0 when `docker compose up`
  failed, and `deploy` claimed its routes were created whatever `oc` returned.
- **Fixed: conflicting timeouts.** The executor had a 120 s default while
  `get_executor` passed 30 s; there is now one 30 s default.

Checked and **not** bugs (the audit overstated them):

- *Scheduled runs skip validation.* `resolve()` validates. A scheduled run with
  bad params ends `failed` with "Invalid parameters: …", which is right when
  there is no caller to send a 422 to.
- *Kafka default conflicts in `kafka_config.py`.* Only tests call those
  helpers, so they move to the surplus step.
- *Elasticsearch `verify_certs` opposite defaults.* The log handler's off
  default is deliberate and documented (it works with ES 8's self-signed cert).
  Whether to flip it is a security-posture decision, not a bug.

Decided:

- **Decided (option A), fixed: DLQ runs blocked the queue.** The DLQ now calls
  `PipelineRunner.start_pipeline`, which validates and creates the execution
  synchronously (so bad params still retry/archive) and drives the run on its
  own thread. A run that fails later ends `failed`, like `/run`. F4 should fold
  the cron scheduler's `_create_execution`/`_dispatch_async`, `/run` and
  `lifecycle.run_pipeline` onto the same method.
- **`LocalExecutor` ID batching.** It is wrong (ignores `ids_batch_size`, calls
  `define_source` twice per ID), but `LocalExecutor` has no production caller:
  the API's local mode goes through the manager. It moves to the surplus step
  instead of being fixed.

## Fixes, each with the problems it solves

### F1. One shared job pipeline that every driver calls
One async `JobRun` (fetch → validate → transform → route → dedup → health →
send → close) returns a `JobOutcome` that carries records in/out, skip_reason,
applied steps, destinations and failed_optional. The worker, `LocalExecutor`
and `reflowfy test` become thin drivers: each claims and reports the job, and
presents the outcome its own way.
- `run_job_records*` has 4 callers, and each repeats send, close, pool scope and
  skip logging (`worker/executor.py:332`, `local_executor.py:175,344`,
  `cli/commands/test.py:393`).
- The skip log line is formatted 4 times (`worker/executor.py:385`,
  `test.py:436`, `local_executor.py:26`, `destinations/record.py:418`).
- Each driver handles an unhealthy destination differently: the worker raises
  `DestinationError` (`:455`), `test` raises `PipelineError` (`test.py:345`), and
  `LocalExecutor` never checks health.
- There are 3 flat-params builders (`test.py:470`, `local_executor.py:139`,
  `worker/executor.py:313`), and the `test` one drops batch/retry keys.
- `SkipJob`'s reason travels in `runtime_params["skip_reason"]`
  (`job_runner.py:297`) to avoid a fifth return value; 4 readers depend on it.
- Pool scope and close-in-the-sending-loop are set up 4 ways
  (`consumer.py:143`, `local_dispatcher.py:63/87`, `local_executor.py:48`,
  `test.py:343`). The event loop is driven with `asyncio.run` in one place and
  `run_coroutine_sync` in another.
- 6 `isinstance(destination, RecordDestination)` checks
  (`worker/executor.py:441,451,482,592,628`, `job_runner.py:275`) go away once
  every destination exposes `destination_for(record)`, which a plain
  destination answers with itself.
- `record_job_metrics` takes 11 parameters unpacked from `JobStats` (`:175`).
  `_execute_job_inner(…, _pipeline_name)` uses a parameter whose name says it
  is unused.
- The outcome is classified twice with different precedence: metrics status
  (`worker/executor.py:196-207`) vs DB state (`:850-857`). A skipped job is
  stored as `completed` but counted as `skipped`.
- The sync `apply_transformations_iteratively` (`transformation_runner.py:195`)
  is used only by tests.

### F2. Planning returns a value instead of storing it on the pipeline
`resolve()` returns a `ResolvedPlan(params, jobs)`. Each job is an envelope
`(source, job_params)`. `IdBasedPipeline.define_jobs` becomes the only ID
planner.
- `fork()` before `resolve()` is a convention callers must remember
  (`pipeline_runner.py:579`, `local_executor.py:121/282`). It is a shallow copy,
  so the class defaults `config = {}` and `schedules = []` stay shared
  (`abstract_pipeline.py:488,533`).
- `_resolved_params` is read with `getattr` (`pipeline_runner.py:590`,
  `local_executor.py:128`).
- `source.job_params` is set onto a foreign object (`id_based_pipeline.py:202`)
  and read back with `getattr` (`job_runner.py:136,146,169`,
  `pipeline_runner.py:657`). The worker reads job params by two routes,
  `job_payload["job_params"]` and `metadata.runtime_params`
  (`worker/executor.py:404`).
- ID batching is written 3 times: `id_based_pipeline.py:199`,
  `test.py:855-875` and `local_executor.py:285-331` (the bug above).
- The "strip `input_ids`" rule appears 3 times (`id_based_pipeline.py:203`,
  `pipeline_runner.py:630` with an `isinstance(IdBasedPipeline)` check,
  `content_dedup.py:71`).
- Dead: the `destination`/`transformations` properties
  (`abstract_pipeline.py:908,964-980`), the unread keys in the
  `id_based_pipeline.py:339` dict, the `resolve_for_id` shim (`:346`) and
  `get_required_parameters` (tests only).

### F3. Persistence becomes its own package, `reflowfy/store/`
`store/` holds the models, DB settings, the state enums with their legal
transitions, a `JobStore` port (claim, heartbeat, finish, previous retries,
record-hash claims) and the shared count and aggregate queries. The manager,
worker and execution packages all import it.
- `content_dedup.py:19` and `worker/executor.py:32-33` import the manager
  package's `models` and `db_settings`.
- The schema exists in 3 copies: `models.py`, `schema.sql` + `dlq_schema.sql`
  (never loaded) and 17 `ALTER TABLE … IF NOT EXISTS` statements in
  `database.py`.
- About 200 bare state strings, e.g. `"pending","dispatched"` at
  `worker/executor.py:787` and `job_manager.py:140`. `execution/base.py`'s
  enum is used only by the API.
- Job counts are aggregated in 4 places (`manager.py:279`,
  `stats_routes.py:133,240`, `schedule_routes.py:65`). The `jobs_dispatched`
  formula is written twice (`manager.py:227`, `pipeline_runner.py:1229`).
- `pipeline_runner.py:1189-1245` writes `Execution` columns directly.
- A claim returns `None` both for "no row" and for "claimed"
  (`worker/executor.py:800`). Releasing record claims is hand-coded 3 times
  (`:462,524,541`); it becomes `async with store.record_claims(…)`.
- The `DATABASE_URL` default literal appears 3 times.

### F4. One way to start and drive an execution
One `start_execution(name, params, …)` service validates, creates, drives in the
background and marks the execution failed on a crash. `/run`,
`lifecycle.run_pipeline`, the cron scheduler and the DLQ all call it.
`factory.make_manager` is the only place a manager is built.
- There are 4 start paths, each with its own fallback for marking the execution
  failed (`app.py:533`, `lifecycle.py:477`, `pipeline_runner.py:301`,
  `pipeline_scheduler.py:187`). That is where the validation bug and the DLQ
  lock bug come from.
- `app.py:110` `_make_manager` copies `factory.make_manager`.
- Defaults and validation are applied in 6 places (`app.py:431`,
  `lifecycle.py:302,482`, `abstract_pipeline.py:940`,
  `pipeline_runner.py:448`, `local_executor.py:284`), partly through
  `hasattr`/`getattr`.
- `_dlq_source` and `_dlq_job_ids` are magic keys smuggled through runtime
  params (`dlq_scheduler.py:235`), hidden by an underscore rule (`app.py:459`).
- Route handlers hold business logic: `/run` is ~140 lines (`app.py:394-531`),
  `/retry-failed` reaches through `manager.job_manager`/`execution_manager`,
  and `/state` holds the transition policy.
- `ReflowManager` (`manager.py:113-190`) only forwards calls. Dead:
  `update_job_counts`, `run_pipeline`, `close`, `max_jobs_per_minute`,
  `execution.update_deduplicated_count` and
  `pipeline_runner._set_job_counts`.
- `pipeline_runner.py` (1249 lines) has 6 jobs: rate-limit resolution, payload
  building, the plan lease, planning, the drive loop and accounting. Split it
  into planner, driver and accounting (accounting goes to `store/`).

### F5. Every route returns rows through one redacting serializer
Every route that returns a stored row goes through the same function or response
model, never `to_dict()` directly.
- This fixes the credential leak above, and the per-route redaction churn
  (6cdca34) goes away.
- Error mapping is copied per route (`app.py:509,608`, `dlq_routes.py:215,289`).

### F6. The API is a thin proxy to the manager
Delete `ExecutionTracker` and `execution/base.py`, proxy status to the manager,
and fold `LocalExecutor`/`DistributedExecutor` selection into one manager
client.
- `api/execution.py` keeps a stale in-memory copy of execution state.
- `get_executor("distributed", execution_mode="local")` (`api/app.py:146`)
  contradicts itself. The `mode == "local"` branch is unreachable from the API.
- `_run_body` (`api/routes.py:37`) takes `pipeline` and `pipeline_name`, plus
  both executors.

### F7. Settings read once, in one module per concern
- Kafka: env, defaults and SASL kwargs are built in 4–5 places
  (`kafka_config.py`, `worker/main.py:48`, `factory.py:22`, `app.py:648`,
  `destinations/kafka.py:183`, `consumer.py:95`, `dispatcher.py:133`). The topic
  and group defaults appear as literals 7 times. `kafka_config`'s public helpers
  are used only by tests.
- Env names drift: `ELASTICSEARCH_URL` vs `ELASTIC_URL`, `KAFKA_SERVERS` vs
  `KAFKA_BOOTSTRAP_SERVERS` (`templates/source_template.py:28`,
  `new_cmd.py:155,197`, `destinations/decorators.py:9`).
- Worker: the heartbeat (`worker/executor.py:746`) and the manager's lease
  (`pipeline_runner.py:50`) must agree, but nothing checks that they do. The
  consumer's start retries are hard-coded (`consumer.py:110`).
  `DRIVER_RETRY_SECONDS` and `PLAN_HEARTBEAT_SECONDS` are hard-coded next to
  env-driven lease values.
- `TRANSFORM_CONCURRENCY` lives in `transformations/base.py:151` and is restated
  in 4 docstrings. Destinations import it lazily from there.

### F8. A dependency-free core with one registry primitive
`core/contracts.py` holds the error hierarchy, `OnError` choices, the
reserved-key list (derived from `RuntimeParams`), the concurrency default, skip
counting and union helpers. One generic `Registry[T]` with a single duplicate
policy and `scoped()` for tests, and one registering metaclass. Delete
`factories/`.
- `core/types.py:28` and `core/exceptions.py:42` import `transformations`;
  `core/id_based_pipeline.py:78` imports `execution.chunk`;
  `record_validation.py:124,136` reads the private `_transform_fn`;
  `job_retry.py:40-62` lazily imports 4 error classes.
- `"fail"/"skip"` is defined 5 times. The reserved runtime keys are listed 4
  times (`runtime_params.py:57`, `execution_context.py:52,100,127`).
  `job_retry.from_dict` repeats the dataclass defaults. Union unwrapping exists
  twice.
- `transformations/base.py:356` `_apply_all` copies
  `async_utils.for_each_concurrently`.
- There are two near-identical metaclasses
  (`TransformationMeta`, `PipelineMeta`) and three duplicate-name policies
  (raise, skip, warn). Five test files clear `_transformations` by hand.
- Surplus: `TransformationRegistry` is never read in production. Also
  `DestinationFactory`, `SourceRegistry`/`DestinationRegistry`, the
  `try/except ImportError` blocks around core dependencies in both factories,
  the croniter guard, and `ParameterResolver` in core when only sources use it.
- The `"import cycle"` lazy imports in `core/secrets.py:221` and
  `sources/base.py:60` disappear once the source registry lives in
  `sources/base.py`.

### F9. A declared config spec per connector
One dataclass or pydantic spec per source and destination drives `__init__`, the
factory function, `split()` cloning, passthrough kwargs, `secret_fields` and
`repr` redaction.
- Constructor signatures are repeated in the factory functions
  (`kafka.py:655` vs `:80`, `api.py:497` vs `:75`, and every source).
- `split()` re-lists every config key (`sources/api.py:307`,
  `s3.py:315`, `sql.py:451`), so a missed key is silently dropped from jobs.
- Two `inspect.signature(__init__)` tricks (`kafka.py:46-65`,
  `sql.py:347`).
- Destinations have no `secret_fields` (the repr leak above). `record.py:493`
  works around it for one class only.
- `IDBasedAPISourceConfig` validates everything just to read `batch_size`.
  `dedup_identity` can only be set on `api_destination`.

### F10. Shared connector building blocks
- **Pooled connections in the base:** `get(key, create, close)` / `evict`,
  with the key format owned by `pool.py`. Kafka and the API destination each
  re-implement get, evict and key building (`kafka.py:239-264,416`,
  `api.py:184-226,442`), and `pool.describe_key` parses their string format.
- **One `core/per_record.py`** holding headers, field extraction and body
  encoding. Header building is duplicated with different rules for allowed
  values (`kafka.py:465-652`, `api.py:130,324`). `value_field` and `body_field`
  are the same idea under two names. The body-encoding rule appears 3 times.
- **One HTTP helper** (`http_send`, `http_probe`) shared by the API source and
  destination. Their error handling is copied (`sources/api.py:186,216` vs
  `destinations/api.py:427`) and their health probes differ (HEAD vs
  HEAD → OPTIONS → retry with a sleep).
- **Elasticsearch:** one pager (PIT/`search_after` loop plus a `_body()`
  normaliser) instead of 4–5 copies (`elastic.py:255-769`; `raw.body if
  hasattr` appears 9 times). Split `fetch`'s six modes into planners. Share
  one ES client builder with the log handler.
- **Delete legacy `split_jobs`** in all 6 sources: no production caller.
- `destinations/base.py:13` imports the per-loop pool registry, and
  `check_concurrency` (`:198`) describes a parameter that was removed.
  `record.py:319` imports and isinstance-checks its own subclass `FanOut`.
- `kafka.py:144` keeps a shim for the removed `concurrency` argument; it can be
  deleted once its deprecation period is over.

### F11. One periodic-task helper and one error policy
- Three scheduler classes have the same `start`/`stop`/`_run_loop` body and the
  same module-global init (`dlq_scheduler.py:55`, `pipeline_scheduler.py:45`,
  `content_dedup_scheduler.py:94`). The retry and sleep loops each use their own
  constants (`database.py:44`, `rate_limiter.py:236`, `pipeline_runner.py:804`).
  `content_dedup_scheduler` also purges executions, so execution retention
  should be split out of it.
- Scheduler start-up failures leave the service reporting healthy
  (`app.py:666-701`).
- Swallowed exceptions to decide one by one, after the earlier fixes have
  removed the paths many of them guard. Each one remaining must re-raise,
  record an outcome, or carry a comment naming its external reason:
  - `pipeline_scheduler.py:240`
  - `local_dispatcher.py:66,109`
  - `pipeline_runner.py:1140`
  - the heartbeat at `worker/executor.py:757`
  - `distributed_executor.py:184`
  - `get_transformation_names` (`abstract_pipeline.py:890`,
    `id_based_pipeline.py:253`): returns `[]` on any error
  - `record_validation.py:127`: turns validation off silently
  - `coerce` (`abstract_pipeline.py:319`)
  - unknown `auth_type` (`http_auth.py:31`)
  - silent health-check failures in the Kafka destination
    (`kafka.py:313,534`) and every source
  - tracing and logging (`tracing.py:104,117`, `logging.py:56`,
    `elastic_handler.py:62`)
  - pipeline registration failures (`abstract_pipeline.py:158`) and
    discovery failures (`pipeline_discovery.py:70`)

### F12. CLI: one project descriptor and one command runner
`cli/project.py` holds image names, release, namespace, a template locator, the
registry check and `run_cmd(check=True)`. `new` renders `templates/`.
- Image, release and service names are hard-coded (`build.py:29`,
  `deploy.py:113-117,259`, `check.py:18`). The registry check is copied
  (`build.py:123`, `deploy.py:79`).
- The template-path fallback is copied 6 times in `init_cmd.py`, plus
  `utils.get_dockerfiles_path`. `new_cmd.py` inlines its own scaffolds instead
  of using `templates/`.
- Docker is driven through both python-on-whales and raw subprocess calls. The
  cluster is driven through both `kubectl` (no namespace) and `oc -n`.

### F13. Observability names in one place
`observability/labels.py` holds the metric status label values and step-mode
constants.
- The label values are free strings (`consumer.py:255`, `pipeline_runner.py:1054`,
  `worker/executor.py:208`), and metric names are repeated in the Grafana JSON.
- Step-mode strings mix constants (`SKIPPED_MODE`, `ROUTE_MODE`) with literals
  (`"sync"`, `"async"`, `"per_record"`, `"validate"`). `getattr(t, "name",
  "<unknown>")` appears 7 times while `.name` is also used directly.

### Tests whose result misleads (noted, not acted on: the Tests option was not chosen)
- `tests/e2e/test_id_based_pipeline.py` (6 places) and `test_transformations.py`
  `pytest.skip` when a pipeline is not registered, so a registration failure
  shows up as a skip. F8/F11 should make registration failures loud. These
  tests should then fail instead of skipping.

### Comments (chosen)
- Remove the roughly 100+ comments that restate code, e.g. `worker/main.py:47`
  `# Get configuration from environment`, `worker/executor.py:454` `# Health
  check (async)`, `app.py:63` `# Create FastAPI app`, `elastic.py:794`
  `# Initialize scroll`, `metrics.py:5` `# Job metrics`,
  `transformations/registry.py:131` `# Global singleton instance`.
- Remove false comments: `consumer.py:268` "Don't commit - will retry",
  `destinations/base.py:42` "Metrics reporting", and
  `transformations/registry.py:15` "Workers use this registry…".
- Drop the "import cycle" / "circular dependency" comments together with the
  cycles F8 removes.
- Keep comments about external facts (aiokafka, PgBouncer, httpcore pool
  contention, Elasticsearch PIT behaviour).

## Execution order

1. **Safety net:** record the baseline of unit tests, coverage (≥ 99%), ruff,
   pyright (0) and mypy's error count.
2. **Bugs above:** ✅ done (see status).
3. **Surplus:** ✅ done. Removed, each with no production caller:
   - `LocalExecutor` and `get_executor`; the API's in-memory `ExecutionTracker`.
   - `PipelineRunner.run_pipeline`, the `ReflowManager` pass-throughs
     (`run_pipeline`, `update_job_counts`, `close`, `max_jobs_per_minute`),
     `ExecutionManager.update_job_counts` / `update_deduplicated_count`,
     `_set_job_counts`, and `app._make_manager` / `_get_kafka_config` (copies of
     `factory.make_manager`).
   - The legacy `split_jobs` in `BaseSource` and the six built-ins, plus
     `sources/schemas.py`.
   - `kafka_config.py`, `DestinationFactory`, `destinations/schemas.py`.
   - `JobDispatcher.dispatch_job` (×3), `JobManager.get_job_states`, `close_db`,
     `ExecutionStatus.is_complete`, `KafkaJobConsumer.stop()`.
   - `AbstractPipeline.destination` / `.transformations` (always raised), and the
     `ImportError` guards around required dependencies.

   Kept on purpose:
   - Public API: `source_registry`, `destination_registry` (in `reflowfy.__all__`),
     `SourceJob`, `get_required_parameters`, `resolve_for_id`.
   - Test entry points with real value: `apply_transformations_iteratively`
     (sync), `RateLimiter.can_dispatch`, `SourceFactory.list_types` /
     `is_registered`, `get_log_context`.
   - `TransformationRegistry`: never read, but it rejects duplicate transformation
     names at class definition. It goes in F8, with the registry primitive.

   Net effect for the branch so far, bug fixes included: `reflowfy/` +289 / −1,609 lines,
   `tests/` +451 / −1,533; mypy errors 114 → 105.
4. **F8 + F7 + F13:** ✅ done.
   - **Layering (F8).** `core` no longer imports anything above it at runtime:
     `chunk` → `core/planning.py`; record concurrency and skip counting →
     `core/record_steps.py`; `SourceError`, `DestinationError`,
     `PermanentDestinationError`, `TransformationError`, `StepOutputError` →
     `core/exceptions.py` (re-exported from their old modules);
     `step_output_type` → `transformations/output_type.py`; `Transformations` →
     beside `BaseTransformation`; `redact_row` → `reflow_manager/redaction.py`; the
     source-type registry → `sources/base.py`, which removes the sources↔factories
     cycle. `tests/unit/test_layering.py` fails on any new upward import. One listed
     exception is left for F2: `id_based_pipeline` wraps lists in `StaticSource`.
   - **Settings (F7).** `core/kafka_settings.py` is the one builder of aiokafka
     connection arguments, and `KafkaSettings.from_env()` reads `KAFKA_*` once. This
     fixed a real bug: the worker's consumer and the manager's job producer dropped
     `SSL` without SASL (plaintext) and crashed on `SASL_SSL` (no `ssl_context`).
     There is one `DATABASE_URL` default (`db_settings.database_url()`). `reflowfy
     init`'s example destination no longer reads `KAFKA_BOOTSTRAP_SERVERS`, which
     the scaffold pins to `ignored:9092`.
   - **Names (F13).** The `JobOutcome` enum and `metrics.count_job` hold every
     `jobs_processed_total` status; `StepMode` holds every `AppliedStep.mode`;
     `step_name()` replaces 11 inconsistent name reads.
   - Deferred to F3: the worker heartbeat vs the manager's running lease (one
     store will own claim, heartbeat and lease).
   - One compatibility note: `job_retry` writes exception import paths into job
     messages. New ones say `reflowfy.core.exceptions.X`; old messages still load.
     During a mixed-version rollout, an older worker would not retry those errors.
5. **F3:** ✅ done.
   - `reflowfy/store/` holds `models.py` and `db_settings.py` (moved from
     `reflow_manager/`), `states.py` and `leases.py`.
   - `JobState`/`ExecutionState` replace the bare state strings in every
     `Job.state`/`Execution.state` comparison and in the state writes of the job
     manager, execution manager, drive loop and worker. `jobs_dispatched(counts)`
     is the one formula for "dispatched".
   - `ExecutionManager.store_job_counts` / `set_total_jobs` are the only writers
     of the execution's count columns (`pipeline_runner` used to set them itself).
   - `JobLeases` reads the heartbeat and the three lease settings together; the
     manager and the worker warn at startup (`job_lease_too_short`) when the
     running lease leaves room for fewer than 3 heartbeats.
   - `schema.sql` and `dlq_schema.sql` (never loaded) are deleted.
   - `test_layering.py` now also forbids `store` → connectors/services and
     `execution`/`worker` → `reflow_manager`.
   - Not done, moved into F1: the `JobStore` port and the record-claims context
     manager (the worker's three release sites go away with the `JobRun` that
     owns the job's exit paths). The count aggregations in the stats/schedule
     routes stay per route: each groups by something different.
6. **F2:** ✅ done.
   - `resolve()` returns `ResolvedPlan(params, jobs, execution_only)` and stores
     nothing; `fork()`, `_source`, `_resolved_params` and the `source` property
     are gone.
   - Every job is a `PlannedJob(source, params)`: `plan_slices` yields them,
     `job()` returns one, and `IdBasedPipeline.plan_batch` builds one per ID
     batch. Nothing sets `job_params` on a source any more.
   - `ResolvedPlan.job_params(job)` is the one merge, and
     `execution_only_params = ("input_ids",)` replaces the manager's
     `isinstance(IdBasedPipeline)` strip.
   - `reflowfy test` plans ID batches with `plan_batch` and `chunk` instead of
     its own copy.
   - `resolve_source` returns a list as-is (`plan_slices` wraps it), so `core`
     no longer imports `sources`: `test_layering.py` has no exceptions left.
   - Removed: `resolve_for_ids`, `resolve_for_id`, `job_runtime_params`,
     `get_required_parameters`.
7. **F1:** ✅ done, in the scope that still applies.
   - With `LocalExecutor` gone there are two drivers (the worker and
     `reflowfy test`), and both already run the shared `run_job_records_async`.
   - The skip log lines are now one pair of helpers in `job_runner`
     (`log_job_dropped`, `log_skipped_records`). Each driver passes its own
     logger, so the log source stays the service's.
   - The outcome is classified in one place: `JobStats.outcome` (metrics label)
     beside `JobStats.stored_state` (the job row). `record_job_metrics(pipeline,
     stats)` replaces the 11 unpacked arguments, and `_pipeline_name` is
     `pipeline_name` again.
   - The sync `apply_transformations_iteratively` (tests only) is deleted.
   - Kept, on purpose:
     - The worker's `isinstance(RecordDestination)` checks. Moving
       `destinations_for` onto every destination broke duck-typed destinations,
       which the worker accepts today.
     - `SkipJob`'s reason on `runtime_params["skip_reason"]`. It is a documented
       reserved key that user code can read.
     - The record-claim release sites. Each exit path (unhealthy destination,
       error, shutdown) needs its own ordering against the job row write.
8. **F4:** ✅ done.
   - `reflow_manager/start.py` holds the two shared pieces:
     - `check_runnable` is the one "registered and valid?" check, raising
       `PipelineNotFoundError` / `InvalidParametersError` (both `ValueError`;
       `/run` maps them to 404/422).
     - `drive_execution` is the one background body: run the plan, mark the
       execution `failed` on any error (building the runner included), close the
       session.
   - All four start paths use them: `/run`, `lifecycle.run_pipeline`, the cron
     scheduler and `PipelineRunner.start_pipeline` (DLQ).
   - The scheduler now validates before it creates an execution (it used to
     create one that failed in the background). Its failures log the shared
     `dispatch_failed` (was `schedule_dispatch_error`); the Kibana dashboard and
     `docs/logging.md` (regenerated from the source, it was stale) follow.
   - Not done:
     - Splitting `pipeline_runner.py`. It is down to planning plus the drive
       loop, and tests pin its module constants.
     - The DLQ's `_dlq_source`/`_dlq_job_ids` stay. They are declared
       `RuntimeParams` keys a replayed run can read.
9. **F5 + F6:** ✅ done.
   - `public_row(row)` is the one way a route returns a stored row, and a test
     fails on any other `.to_dict()` in the manager package.
   - `redact_row` now also masks URL passwords in `error_message` / `hook_error`,
     and the stats routes mask `error_message`. Those were handed out raw.
   - The API has one `DistributedExecutor`, and each call names its `mode`.
     There used to be two instances that differed only in mode, chosen by
     `_run_body(pipeline, local_executor, distributed_executor, …)`.
   - `execution/base.py` is deleted: `BaseExecutor` had one implementation, and
     the `(str, Enum)` `ExecutionState` logged as `ExecutionState.FAILED` on
     Python 3.11+. `ExecutionStatus` lives beside the client and uses the
     `store.states` strings.
10. **F9 + F10:** ✅ done for the parts with a defect or a copy behind them.
   - The S3 and ID-based API `split()` rebuild each job from the whole config
     (`from_config({**config, …})`) instead of a hand-written key list. S3's
     list dropped the extra boto3 client kwargs from every job; a regression
     test now covers it.
   - Elasticsearch has one `_body(response)` in place of nine
     `raw.body if hasattr(raw, "body") else raw` copies.
   - `RecordDestination.fixed_targets()` (`None` by default, the targets for
     `fan_out`) replaces `record.py` importing and type-checking its own
     subclass `FanOut`.
   - Not done:
     - A config-spec dataclass per connector.
     - A pooled-connection base, `core/per_record.py`, a shared HTTP helper and
       the ES pager rewrite.
     - Why: each would rewrite the Kafka, API or Elasticsearch connectors'
       behaviour-defining code (header value rules differ between Kafka bytes
       and HTTP strings; the API source and destination probe health
       differently on purpose). That needs the E2E suites as a net per
       connector, not a cleanup pass.
     - SQL's `split()` keeps its explicit list: its slices change the query and
       turn templating off, so they are not a copy of the parent config.
11. **F11:** ✅ done.
   - `reflow_manager/periodic.py` `PeriodicTask` is the one start/stop/loop. The
     DLQ scheduler, the cron scheduler and the retention sweeper only define
     `tick()` and their interval. A failing sweep is still logged as
     `sweeper_error`, now by the loop.
   - A scheduler that fails to start is listed in `/health`
     (`"status": "degraded"`, `failed_components`). It answers 200, because a
     restart would not fix it, but it no longer reports healthy.
   - Swallowed exceptions, decided one by one:
     - The six connector health probes that returned `False` silently (SQL, S3,
       Elasticsearch and API sources; Kafka and API destinations) now log
       `health_check_failed` with the exception type and message, URL passwords
       masked (`core/health.py`).
     - An unknown `auth_type` raises instead of sending requests with no auth
       (behaviour change), and the check is case-insensitive.
     - Optional tracing packages are caught as `ImportError`; any other
       instrumentation failure is logged (`tracing_error`).
     - The Elasticsearch log handler uses logging's `handleError` when a record
       will not format.
     - `get_transformation_names` logs at debug why names are not listable.
     - The rest carry a comment naming their reason (rollback on a dead
       connection, version lookup during import).
   - The scheduler's own swallowing dispatch thread went with F4.
12. **F12:** ✅ done.
   - `cli/project.py` holds what `build`, `deploy`, `check` and `init` share:
     `IMAGES` (service → image), `RELEASE`, `image_repository()`,
     `require_registry()` (the one "Registry is required" exit) and
     `template_path()` (the bundled template, else the source tree's, which
     was copied six times in `init`).
   - Not done:
     - `new` still inlines its scaffolds. Its output is user-facing text that
       tests pin line by line.
     - python-on-whales vs subprocess, and `kubectl` vs `oc`. Each is a CLI
       behaviour change for users, not a cleanup.
13. **Comments:** ✅ done.
   - 37 comments that only restated the next line are removed (`# Create
     FastAPI app`, `# Mark as successful`, `# Load pipeline from registry`, …).
     Section labels in long functions stay.
   - Three false comments are corrected:
     - The consumer's "Don't commit - will retry": the next commit advances the
       partition past the message.
     - `BaseDestination`'s "Metrics reporting".
     - `TransformationRegistry`'s "Workers use this registry". Nothing reads it
       at runtime.
   - Two "Import here to avoid circular dependency" comments were false (both
     registries import nothing from reflowfy). The imports are now at module
     top.
14. **Repeat:** audit again for special cases, hidden links, needless links and
    surplus until nothing worth fixing is left. Then run the unit tests and the
    local and distributed E2E suites.

Every step keeps `reflowfy`'s top-level exports, the v2 Kafka job message and the
database contents compatible. If a step cannot, it stops and asks first.
