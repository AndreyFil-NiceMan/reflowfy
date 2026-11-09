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

Left for a decision:

- **DLQ runs block the queue.** The DLQ scheduler drives each execution to the
  end inside its poll, holding `FOR UPDATE` row locks. This is deliberate: the
  DLQ's retry and archive logic depends on the run raising synchronously.
  Moving the run to a thread changes what a DLQ retry means. Committing
  `processing` first releases the locks, but a crash then strands rows. See F4.
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
2. **Bugs above:** ✅ done (see status). The DLQ decision is still open.
3. **Surplus** (dead code from F2, F4, F6, F8, F10): shrinks everything that
   follows.
4. **F8 + F7 + F13:** layering, settings and names. These are mostly moves, with
   re-exports kept.
5. **F3:** `store/`, the enums and a single schema definition.
6. **F2 → F1:** the plan as a value, then one job pipeline. This is the biggest
   change.
7. **F4 + F5 + F6:** one start path, one serializer and a thin API.
8. **F9 + F10:** connector specs and shared building blocks.
9. **F11, F12, then comments.**
10. **Repeat:** audit again for special cases, hidden links, needless links and
    surplus until nothing worth fixing is left. Then run the unit tests and the
    local and distributed E2E suites.

Every step keeps `reflowfy`'s top-level exports, the v2 Kafka job message and the
database contents compatible. If a step cannot, it stops and asks first.
