# Test coverage plan

Status: **phases 0–5 done**, phase 6 partly (see *Outcome*) · Baseline measured
2026-10-20 on `claude/project-test-coverage-uhpgmp`

## Outcome

| | Before | After |
|---|---:|---:|
| Unit tests | 435 | 1,337 |
| Unit coverage (line+branch) | 58.7% | 99.5% |
| Coverage gate (`fail_under`) | none | 99 |
| E2E (local mode, full Docker stack) | not run in CI | 346 passed, 0 skipped |
| CI on pull requests | none | ruff + pyright + unit tests with the gate |

Writing the tests found **21 bugs**, every one fixed on this branch with a regression
test (each was first pinned as a strict xfail):

- **Kafka destination:** `security_protocol="SSL"` without SASL was dropped
  (plaintext); SSL had no `ssl_context`; `**producer_config` never reached the
  producer (now forwarded when aiokafka accepts it, warned about once otherwise).
- **Kafka consumer:** non-UTF-8 or non-object payloads were never committed and came
  back after every rebalance; they are committed and counted as `status="invalid"`.
- **Content dedup:** an `X-API-Key` header stayed in the destination identity, so
  rotating it made written content look new (previous-release claims are honored).
- **`DestinationFactory.create`** could not rebuild any built-in destination.
- **`ConsoleDestination`** crashed on datetimes with the default `pretty_print`.
- **`SqlSource`** ignored its documented `**engine_kwargs`.
- **API run route:** an unreachable manager answered 422 (now 502); optional
  list/dict params still required a JSON body.
- **`reflowfy deploy`** printed Kafka/Postgres/Elastic passwords in the helm command.
- **`resume_execution`** finalized an already-cancelled execution as `failed`.
- **DLQ:** jobs due together ran with the first job's params; dispatching an unknown
  job answered 500 instead of 404; scheduling an unregistered pipeline was accepted;
  `/stats/dlq` `avg_retry_count` was the last status row's, not the pipeline's.
- **E2E infra:** the Elasticsearch test node refused shards on a host past the 90%
  disk watermark, so every Elastic E2E test was skipped.
- **Helm:** the standalone `reflowfy-api` / `reflowfy-worker` charts rendered invalid
  YAML with their default `env: []` (caught by CI's `helm lint`).
- **Elastic log handler:** the first shipping-failure warning was swallowed on a host
  up for under a minute (`time.monotonic()` counts from boot).

Still open from phase 6: distributed E2E in CI (it runs locally via
`scripts/e2e-distributed.override.yml`), the kind/k3d chaos suite, a real-Postgres
integration suite, Helm snapshot tests and benchmarks.

## Baseline

Unit suite: **435 passed, 62.1% line coverage** (3,860 of 6,214 statements). E2E and
distributed suites were not measured: they need the Docker stack or a deployment.

```bash
uv sync --all-extras          # pytest/pytest-cov live in the `dev` extra, not the dev group
uv run pytest tests/unit --cov=reflowfy --cov-report=term-missing
```

| Package | Statements | Unit coverage | Target |
|---|---:|---:|---:|
| `core/` | 603 | 84% | 95% |
| `observability/` | 226 | 84% | 90% |
| `factories/` | 73 | 78% | 95% |
| `execution/` | 372 | 77% | 90% |
| `sources/` | 981 | 75% | 90% |
| `transformations/` | 89 | 72% | 95% |
| `worker/` | 379 | 62% | 90% |
| `reflow_manager/` | 2,086 | 58% | 85% |
| `destinations/` | 282 | 54% | 90% |
| `api/` | 194 | 51% | 85% |
| `cli/` | 799 | 36% | 75% |
| `templates/`, `kafka_config.py` | 91 | 0% | 80% |
| **Total** | **6,214** | **62%** | **85%** |

There are four structural gaps:

1. **No CI runs tests.** `.github/workflows/` holds only `release.yml`. Coverage that
   nobody checks just goes down over time.
2. **No coverage configuration.** There is no `[tool.coverage]`, branch coverage is
   off, and nothing enforces a minimum.
3. **The production path is not covered end to end.** E2E runs in local mode only, so
   manager → Kafka → `KafkaJobConsumer` → worker is only exercised by unit tests and the
   opt-in `tests/e2e_distributed/` suite.
4. **The HTTP surface is thin.** `stats_routes.py` (12%), `dlq_routes.py` (30%),
   `schedule_routes.py` (29%) and `app.py` (39%) are mostly reached only through E2E.

## Principles

- **Test at the cheapest layer that can catch the bug.** Unit tests use fakes and
  SQLite. E2E checks wiring, packaging and real services. The distributed suite checks
  delivery guarantees.
- **Reuse the patterns already in the repo.** These include SQLite and `aiosqlite`
  engines over `tmp_path` (`test_cancel_execution.py`, `test_dispatch_recovery_fixes.py`)
  and `fastapi.testclient.TestClient` against `reflow_manager.app`. They also include the
  fake ES used in `test_elastic_range_windows.py`, and the autouse `_no_worker_claim_db`
  stub in `tests/unit/conftest.py`.
- **Cover behaviour, not lines.** Every new terminal state or short-circuit gets a test
  that asserts all three observability channels (log line, `JobStats` field and
  `reflowfy_jobs_processed_total` label), per `CLAUDE.md` → *Observability*.
- **Never chase a number with trivial tests.** Excluding code is fine
  (`if __name__ == "__main__"`, `TYPE_CHECKING` blocks, `main()` uvicorn launchers) if it
  is done in config and reviewed.

## Phase 0: Infrastructure (do first)

| # | Item | Detail |
|---|---|---|
| 0.1 | Coverage config | Add `[tool.coverage.run]` with `source = ["reflowfy"]`, `branch = true`, and `omit = ["reflowfy/helm/*"]`. Add `[tool.coverage.report]` with `exclude_also` for `if TYPE_CHECKING:`, `if __name__ == .__main__.:` and `raise NotImplementedError`, plus `show_missing = true`. |
| 0.2 | CI workflow | Add `.github/workflows/tests.yml`. On PR: `uv sync --all-extras`, then ruff, pyright and `pytest tests/unit --cov --cov-fail-under=<ratchet>`. Upload `coverage.xml` as an artifact. |
| 0.3 | Ratchet | Start `fail_under` at the measured baseline (62). Raise it at the end of each phase, and never lower it. |
| 0.4 | E2E in CI | A nightly or manual job runs `./scripts/run_e2e_tests.sh` with `COVERAGE_PROCESS_START` set inside the containers. Combine those `.coverage.*` files with the unit run to get one framework-wide figure. |
| 0.5 | Shared fixtures | Move the copies of `sqlite_engine`, `manager_app_client`, `fake_kafka_producer`, `fake_es` and `make_pipeline(name, params_type)` out of individual test modules into `tests/unit/conftest.py`. |
| 0.6 | Markers | Register `unit`, `integration`, `e2e`, `distributed` and `chaos`, and fail on unknown markers (`--strict-markers`). |

Exit criterion: CI is green on every PR, and the coverage gate is enforced at 62%.

## Phase 1: Manager HTTP surface (largest gap, about +9% total)

The tests run against `TestClient(reflow_manager.app.app)` with a SQLite database and the
dispatcher stubbed.

| Module | Now | Tests to add (`tests/unit/…`) |
|---|---:|---|
| `stats_routes.py` | 12% | `test_stats_routes.py`: `get_overview_stats`, `get_pipeline_stats`, `get_pipeline_detail`, `get_failure_stats` and `get_dlq_stats`, each on an empty DB, a mixed-state DB (pending/running/completed/failed/deduplicated/cancelled) and a filter by pipeline. Assert that the counts add up and that `jobs_cancelled` is present. |
| `dlq_routes.py` | 30% | `test_dlq_routes.py`: schedule (valid, past date, unknown pipeline → 404, invalid params → 422), list with pagination and filters, get (404), cancel (twice → 409), dispatch one, and dispatch all for a pipeline. |
| `schedule_routes.py` | 29% | `test_schedule_routes.py`: `get_schedule` for scheduled and unscheduled pipelines, `get_schedule_stats` aggregation, and archived job list and detail including 404. |
| `app.py` | 39% | `test_manager_app_routes.py`: `/health` with the DB up and down. `run_pipeline` covering an unknown pipeline, param validation errors, the `rate_limit` override persisted, `enable_duplicate_jobs=false` setting `dedup_check`, and a schedule reset. `pause`/`resume` on every state (409 matrix), `retry-failed` with and without `error_contains`, and `jobs` and `errors` pagination. |
| `app.py` startup | — | `test_manager_startup.py`: `_startup` runs migrations, frees its own-host leases and calls `_recover_interrupted_executions`. That should resume a running execution, fail a planning execution with a stale lease, and leave a fresh lease alone. `_shutdown` stops all three schedulers. |
| `manager.py` | 58% | Extend `test_manager.py` with `get_execution_stats` and `_get_batch_checkpoint_stats` across multiple batches. |
| `execution.py` | 74% | `update_job_counts`, `pause`/`resume` transitions, and `update_deduplicated_count`. |

## Phase 2: Kafka path and worker (production-critical)

These tests use a fake `AIOKafkaProducer` and `AIOKafkaConsumer` (simple async classes
recording calls). No broker is needed.

| Module | Now | Tests to add |
|---|---:|---|
| `destinations/kafka.py` | 30% | `test_kafka_destination.py` (extend): `_build_connection_kwargs` for PLAINTEXT, SASL and SSL, and `send` covering batching, serialisation of non-JSON values and a producer error → `DestinationError`. `health_check` covers a healthy broker, an unreachable broker and lag over the threshold. `close` must be idempotent. |
| `reflow_manager/dispatcher.py` | 61% | `test_kafka_dispatcher.py`: lazy producer creation happens once per execution, `dispatch_jobs_batch` stops at the first failed send and reports what was sent (the "rest stay `pending`" guarantee), and `close`. |
| `worker/consumer.py` | 49% | `test_consumer_lifecycle.py`: `start` subscribes and processes messages, `schema_version != 2` is rejected and committed, `request_stop` drains the in-flight job before leaving the group, and a second signal exits immediately. Malformed JSON is committed and not retried forever. |
| `worker/executor.py` | 82% | `_execute_job_inner` branches: destination failure → `failed` with the error stored, transformation failure, cancelled claim, redelivered claim, heartbeat refresh on a long job. `_claim_job` against a real SQLite DB (`@pytest.mark.real_claim`): every from-state, `attempts` increments, and fail-open on a DB error. `_update_job_in_db` covers stats JSON persistence. |
| `worker/main.py` | 0% | `test_worker_main.py`: env parsing, pipeline discovery being called, and the signal handler wired to `request_stop`. |
| `kafka_config.py` | 0% | `test_kafka_config.py`: `get_kafka_config` / `get_kafka_env_vars` for every security protocol and missing env vars. |
| `execution/distributed_executor.py` | 21% | `test_distributed_executor.py`: `execute` with the HTTP client mocked, covering success, a 4xx/5xx from the manager, a timeout and polling to a terminal state. |
| `reflow_manager/local_dispatcher.py` | 25% | `test_local_dispatcher.py`: `dispatch_job` and `dispatch_jobs_batch` run in-process, and a job exception marks the job failed without stopping the batch. |

Exit criterion: every job-state transition in *Dispatch loop, leases and redelivery*
(`CLAUDE.md`) has at least one unit test on the Kafka path.

## Phase 3: Orchestration internals

| Module | Now | Tests to add |
|---|---:|---|
| `pipeline_runner.py` | 80% | `resume_execution` from paused, cancelled mid-loop, and replaced by another driver (the loop stops). `_wait_for_driver_lease` for a fresh lease, a stale lease and the same host. `_drive_step` covers the in-flight window top-up, a DB error with backoff, and the no-progress warning after 10 min (frozen clock). Also `_finalize_execution_state` with all jobs completed, some failed or all cancelled. |
| `rate_limiter.py` | 79% | State created on first use, refill over elapsed time, the burst cap, and two managers sharing a bucket. |
| `pipeline_scheduler.py` | 65% | `_poll_and_trigger` with due, not-due and disabled schedules. `_trigger_pipeline` when a run is already active, and `_dispatch_async` failure recording. Start and stop must be idempotent. |
| `dlq_scheduler.py` | 19% | `test_dlq_scheduler.py`: `_poll_and_process` picks only due jobs, `_handle_job_failure` retries then archives, `process_job_immediately` / `process_pipeline_immediately`, and the stop loop. |
| `content_dedup_scheduler.py` | 47% | Loop start and stop, a sweep is called once per interval, and a sweep exception does not kill the loop. |
| `job_manager.py`, `database.py` | 87% / 69% | Session error paths, and `get_db` closing on exception. |

## Phase 4: Sources, destinations and transformations

| Module | Now | Tests to add |
|---|---:|---|
| `sources/sql.py` | 43% | `test_sql_source.py` (unit, SQLite): `fetch` with params, `split_jobs` by ID range (gaps, single row, empty table) and by offset, `health_check` up and down, and the `SourceFactory` round-trip. |
| `sources/api.py` | 69% | `_fetch_by_id` / `_fetch_batch` with `httpx.MockTransport`: pagination, a 429 retry, auth headers, `split_jobs` and `health_check`. |
| `sources/s3.py` | 77% | Use `botocore.stub.Stubber`: prefix listing across pages, non-JSON objects as raw text, and an empty bucket. |
| `sources/elastic.py` | 84% | Snapshot mode when the PIT is lost (`No search context found` surfaces as a job failure), `max_slices` capping, and an empty index. |
| `sources/mock.py`, `decorators.py` | 56% / 64% | Decorator registration, a duplicate name and a wrong signature. |
| `destinations/console.py` | 23% | `send` output format, plus large and non-JSON records (`capsys`). |
| `destinations/api.py`, `base.py` | 87% / 77% | Retry exhaustion, `destination_identity()` stripping credentials, and the default `health_check`. |
| `transformations/registry.py` | 65% | Register, lookup, an unknown name and re-registration with the same name. |
| `factories/destination_factory.py` | 67% | Round-trip for every built-in destination, and an unknown type. |

## Phase 5: Front-door API and CLI

| Module | Now | Tests to add |
|---|---:|---|
| `api/routes.py` | 59% | `_create_run_route` generating an OpenAPI schema per typed-params pipeline (defaults, `Literal` enums, required vs optional), `_run_body` validation, and forwarding to the manager (mocked `httpx`). |
| `api/execution.py` | 33% | `update_job_completion` for every terminal state. |
| `api/app.py` | 51% | Startup discovery, `/health`, and the manager being unreachable → 502/503. |
| `cli/commands/test.py` | 31% | Use `typer.testing.CliRunner`: `resolve_pipeline` (by name, by path, unknown name with a suggestion), `collect_params` (prompt vs `--no-input`, `-p k=v` coercion), `--json` output shape, `--dry-run` sending nothing, ID batches and `render_failure` naming the step. |
| `cli/commands/deploy.py` | 12% | Use `subprocess.run` mocked: the helm command line for each chart and values file, a missing kubeconfig and dry-run. |
| `cli/commands/build.py`, `run.py`, `new_cmd.py` | 20–26% | Mock `docker compose`, assert argv for `--build` and `-d`, and check that `new` writes a valid pipeline file and refuses to overwrite. |
| `templates/pipeline_template.py` | 0% | Render the template, import the output and assert that it registers and defines a params type (so the deprecation warning does not fire). |
| `observability/tracing.py` | 45% | `init_tracing` with OTel enabled and disabled, and `instrument_fastapi` being a no-op without OTel. |

## Phase 6: Integration, E2E and non-functional

These don't add to the unit figure, but they cover what unit tests cannot.

| Area | Plan |
|---|---|
| Distributed in CI | A nightly job that applies `scripts/e2e-distributed.override.yml` and runs `tests/e2e/test_typed_params.py`, `test_define_jobs.py`, `test_skip_job.py` and `test_worker_content_dedup.py` over Kafka. This closes gap 3. |
| `tests/e2e_distributed/` | Run against a kind/k3d cluster weekly, with the `-m chaos` worker-kill and manager-restart scenarios. Assert exactly-once delivery from the sink ledger. |
| Real Postgres | A testcontainers-based `tests/integration/` suite for what SQLite cannot reproduce: the driver lease under concurrency, `lock_timeout` on migrations, TCP keepalive settings applied, and the `processed_content` claim race between two workers. |
| Helm | Run `helm lint` and `helm template` for each chart with default and production values in CI, plus a snapshot test of the rendered manifests (KEDA scaler, `terminationGracePeriodSeconds: 120`). |
| Packaging | Build the wheel, install it into a clean venv, then `import reflowfy`, check that `py.typed` is present, run `reflowfy --help` and `reflowfy init` in a temp dir. |
| Type contracts | `tests/typecheck/` holds files that must pass and files that must fail pyright (for example a wrong-arity `@transformation`, or a misspelled TypedDict key), run with `pyright --outputjson`. |
| Property tests | Hypothesis tests for `split()` invariants (the slices cover the source exactly once with no overlap) for static, SQL ID-range and ES range windows, and for `chunk()` sizing. |
| Performance | A benchmark (`pytest-benchmark`, not gating) for planning 1M jobs, bulk insert throughput and content hash cost per 10k records. |

## Sequencing and milestones

| Milestone | Phases | Expected unit coverage | Gate |
|---|---|---:|---:|
| M0 | 0 | 62% line / 58.7% line+branch | 58 |
| M1 | 1 | ~71% | 70 |
| M2 | 2 | ~77% | 76 |
| M3 | 3, 4 | ~83% | 82 |
| M4 | 5 | ~87% | 85 |
| M5 | 6 | — (adds E2E-combined report) | 85 unit / 90 combined |

The ordering follows risk: the HTTP surface and the Kafka path are what production
depends on and what currently has the least coverage. CLI work is last because E2E
already exercises most of it.

## Keeping it there

- PRs touching `reflowfy/` must not lower total coverage. CI comments the diff
  coverage, and new code needs at least 90% diff coverage.
- Every bug fix lands with a regression test named after the behaviour (the existing
  style: `test_planning_crash.py`, `test_concurrent_executions.py`).
- Re-measure each quarter, and update the baseline table above when a milestone closes.
