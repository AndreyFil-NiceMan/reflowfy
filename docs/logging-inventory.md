# Reflowfy logging inventory

A complete map of every log/print site in `reflowfy/`, with the level, the
mechanism (structured `logger` vs raw `print`), and what's wrong with it.

## Why the logs are "bad" (cross-cutting issues)

1. **Two parallel systems.** `reflow_manager/*`, `worker/executor.py`, and
   `observability/*` use the structured `logger` (ECS JSON, levels, context
   fields). Everything else — `api/`, `core/`, `execution/`, `destinations/`,
   `sources/`, `worker/consumer.py`, `worker/main.py` — uses raw `print()`
   (~90 sites). Prints bypass log levels, JSON formatting, and the
   `execution_id`/`job_id`/`pipeline_name`/`batch_id` context fields entirely.
   They go to stdout only, never to Elastic.
2. **Context fields almost never populated.** `ECSJSONFormatter` promotes
   `execution_id`, `job_id`, `pipeline_name`, `batch_id` to top-level fields
   (`logging.py:17-26`), but virtually no call passes `extra={...}`, so these
   columns are empty even in the structured logs. IDs are baked into message
   strings instead → not queryable.
3. **f-string vs lazy `%`-args, inconsistently.** `dispatcher`,
   `dlq_scheduler`, `pipeline_scheduler`, `local_dispatcher`,
   `content_dedup_scheduler`, `database`, `manager` use `logger.info(f"...")`;
   `app`, `pipeline_runner`, `executor` use `logger.info("... %s", x)`. Pick one
   (lazy `%`-args, so disabled levels cost nothing).
4. **Emoji decoration everywhere** (🚀 ✅ ❌ ⚠️ 📦 🔧 ⏰). Noise in JSON,
   not grep-friendly, no semantic value. Level already conveys severity.
5. **Wrong levels.** `content_dedup_scheduler.py:79` logs an exception at
   `info`. `api/routes.py:66` prints an execution failure with no level at all.
   Route-registration and per-request lines are `print` at effectively-info.
6. **Startup banners as prints** (`= * 60` boxes in `api/app.py`,
   `worker/main.py`) — cosmetic, unstructured, unfilterable.

---

## Structured logger sites (`logger.*`)

### `reflow_manager/app.py` — service lifecycle & /run
| Line | Level | Message | Note |
|------|-------|---------|------|
| 358 | exception | Failed to start pipeline %s | good |
| 405 | exception | Background job dispatch failed for %s | good |
| 414 | exception | Failed to mark execution %s as failed | good |
| 442 | exception | Error fetching execution stats for %s | good |
| 452 | info | Starting ReflowManager service (version %s) | |
| 456/459 | info | Initializing database… / Database initialized | |
| 466-468 | info | Kafka / Topic / Rate limit config dump | 3 lines → collapse to 1 |
| 471 | info | Checking for interrupted executions… | |
| 477/479 | info/exception | DLQ Scheduler initialized / failed | |
| 489/493 | info/exception | Pipeline Scheduler initialized / failed | |
| 498/500 | info/exception | Content Dedup Sweeper initialized / failed | |
| 515/518/522/530 | info | interrupted-execution recovery progress | no execution_id in `extra` |
| 543 | exception | Failed to resume execution %s | |
| 550/554 | info | Shutting down / Shutdown complete | |
| 565 | info | ReflowManager service starting on http://%s:%d | |

### `reflow_manager/pipeline_runner.py` — the core run/split/dispatch path
Uses `%`-args consistently (the good example to copy). ~35 sites.
| Line | Level | Purpose |
|------|-------|---------|
| 150 | info | Running pipeline: %s (execution %s) |
| 173 | exception | Failed to mark execution %s as failed |
| 208 | warning | Execution %s not found, skipping resume |
| 216 | warning | (resume path) |
| 232 | info | Execution %s has no incomplete batches, syncing state |
| 239 | info | Resuming execution %s from batch %d |
| 260 | warning | Local mode: resetting %d orphaned dispatched jobs to pending |
| 301 | info | (batch dispatch) |
| 315 | info | Dispatching batch %d (%d jobs)… |
| 334 / 727 | info | Batch %d: %d completed, %d failed |
| 340 | info | (batch summary) |
| 405 | info | Job dispatch starting: %s (execution %s) |
| 426 | info | Splitting source data into jobs (rate: %s/sec)… |
| 429 | info | Phase 1: Saving jobs to database… |
| 464 | info | Saved %d jobs to database in %d batches |
| 470 | info | (phase transition) |
| 516/519 | info | Processing %d IDs (batch_size=%d): %s — **dumps full ID list** |
| 538/548 | info | Processing ID batch: %s — **dumps IDs** |
| 588 | info | Saved %d jobs … (from %d IDs) |
| 599 | info | IdBasedPipeline %s: %d dispatched, %d completed, %d failed |
| 614 | warning | Could not back-fill total_batches for %s; continuing |
| 660 | info | Waiting for batch completion… |
| 687/689 | info | Phase 2: Executing/Dispatching batches |
| 702 | info | (batch loop) |
| 759 | warning | Could not sync counts for %s; using in-memory totals |
| 772 | warning | (count sync) |
| 875 | warning | Batch timeout after %ss, some jobs may not have completed |

### `reflow_manager/dispatcher.py` (Kafka) — f-strings + emoji
| Line | Level | Message |
|------|-------|---------|
| 96 | info | 🔄 Detected event loop change, resetting Kafka producer |
| 150 | error | ❌ Kafka error: {e} |
| 166 | warning | ⚠️ Rate limit timeout after 60s, stopping dispatch after {dispatched} jobs |
| 180 | error | ❌ Kafka error: {e} |

### `reflow_manager/local_dispatcher.py` — f-strings + emoji
| Line | Level | Message |
|------|-------|---------|
| 44 | warning | ⚠️ Rate limit timeout, skipping job |
| 52 | error | ❌ Local dispatch failed: {e} |
| 75 | warning | ⚠️ Rate limit timeout after 60s, stopping dispatch after {dispatched} jobs |
| 84 | error | ❌ Local job execution failed: {e} |

### `reflow_manager/manager.py`
| Line | Level | Message |
|------|-------|---------|
| 75 | info | 🔧 ReflowManager initialized in LOCAL mode (in-process dispatch) |
| 78 | info | 🔧 ReflowManager initialized in DISTRIBUTED mode (Kafka: {…}) |

### `reflow_manager/database.py`
| Line | Level | Message |
|------|-------|---------|
| 48 | info | DB init attempt {n}/{max} failed ({exc}), retrying in {d}s… — **retry failure at info; should be warning** |

### `reflow_manager/dlq_scheduler.py` — f-strings + emoji
| Line | Level | Message |
|------|-------|---------|
| 57 | warning | ⚠️ DLQ Scheduler already running |
| 64 | info | ✅ DLQ Scheduler started (polling every {n}s) |
| 71/79 | info | 🛑 Stopping / ✅ stopped |
| 87 | error | ❌ DLQ Scheduler error: {e} |
| 111 | info | 📋 DLQ Scheduler found {n} due jobs |
| 113 | debug | (per-job) |
| 130 | error | ❌ DLQ poll error: {e} |
| 141 | info | 🚀 Processing {n} DLQ jobs for pipeline: {name} |
| 159 | info | ✅ DLQ jobs dispatched to execution: {id} |
| 163 | error | (dispatch failed) |
| 214 | info | 📦 DLQ job {id} archived after {n} retries |
| 222 | info | (archive) |

### `reflow_manager/pipeline_scheduler.py` — f-strings + emoji
| Line | Level | Message |
|------|-------|---------|
| 48 | warning | ⚠️ Pipeline Scheduler already running |
| 55/62/70 | info | ✅ started / 🛑 stopping / ✅ stopped |
| 78 | error | ❌ Pipeline Scheduler error: {e} |
| 100 | info | ⏰ Pipeline Scheduler found {n} due pipeline(s) |
| 109 | error | ❌ Pipeline Scheduler poll error: {e} |
| 127 | info | 🚀 Triggering scheduled pipeline: {name} (execution={id}) |
| 137 | info | ✅ Scheduled execution created and dispatched: {id} |
| 140 | error | (create/dispatch failed) |
| 186 | error | ❌ Scheduled dispatch failed for {id}: {e} (exc_info) |
| 246/256/267 | info | ✓ Registered/Updated/… schedule for '{name}' |
| 263 | info | ✓ Disabled schedule for '{name}' (no longer scheduled) |

### `reflow_manager/content_dedup_scheduler.py`
| Line | Level | Message |
|------|-------|---------|
| 55 | info | Content Dedup Sweeper started (every {n}s, retain {h}h) |
| 76 | info | Content Dedup Sweeper purged {n} expired hash(es) |
| 79 | info | Content Dedup Sweeper error: {e} — **exception logged at info, no exc_info** |

### `worker/executor.py` — `%`-args (good), needs `extra={job_id}`
| Line | Level | Message |
|------|-------|---------|
| 201 | info | Job %s: no records to process |
| 208 | info | Processing job %s: %d records |
| 211 | debug | Applied transformation %s (%.3fs) |
| 224 | info | Job %s: content already processed — deduplicated |
| 235 | error | Job %s: destination health check failed |
| 245 | debug | Job %s: sending %d records to destination |
| 256 | info | Job %s completed (%.2fs, %d records) |
| 270 | error | Job %s failed: %s (exc_info) |
| 298 | warning | Job %s: failed to close destination: %s |
| 344 | warning | Job %s not found in database |
| 349 | debug | Job %s: status updated in database |
| 352 | error | Job %s: failed to update database: %s (exc_info) |

### `observability/logging.py`
| Line | Level | Message |
|------|-------|---------|
| 110 | warning | LOG_DESTINATION=%s requested but ELASTIC_LOG_URL empty; falling back to stdout |
| 116 | warning | Unrecognized LOG_DESTINATION=%r; defaulting to stdout |

### `observability/elastic_handler.py`
| Line | Mechanism | Note |
|------|-----------|------|
| 149 | `print(..., file=sys.stderr)` | intentional — handler's own diagnostics can't use `logger` (recursion). Leave as-is. |

---

## Raw `print()` sites — should become `logger.*`

### `worker/main.py` (startup + crash) — lines 19, 32-38, 51, 67-77, 92, 99, 101
Banner box + config dump + "Worker crashed: {e}" (line 101). Crash print has no
traceback and no error level.

### `worker/consumer.py` (hot loop) — lines 98, 103-109, 130, 138, 142, 157, 161, 166
Every consumed job (`📦 Received job`, line 142), schema rejects (138),
failures (157, 161, 166) go through `print`. This is the per-message path — the
most important place to have structured `job_id`/level, and it has neither.

### `execution/local_executor.py` — lines 108-263 (~18 sites)
Full fetch/transform/send trace + per-ID trace via `print`, including
`❌ Execution failed: {e}` (144, 263) with no traceback.

### `execution/distributed_executor.py` — lines 85-137 (~11 sites)
Includes literal `print(f"DEBUG: …")` (108-109) and `❌ {error_msg}` (127, 137).

### `api/app.py` — lines 34-37, 120, 123, 151, 163-166, 178-179
Startup banners + route-setup progress + "No pipelines registered" (120,
should be warning).

### `api/routes.py` — lines 52-72, 201, 245
Per-run banner (52-57), `Execution raised: {e}` (66, no level/traceback),
completion (72), and route-registration lines (201, 245).

### `destinations/console.py` — lines 46-75
Intentional: this is a *destination that prints records to console* by design.
**Leave as-is** — not diagnostic logging.

### `destinations/kafka.py` — lines 245, 249
Lag-check fail-open warnings via `print`. Should be `logger.warning`.

### Registration decorators (fire at import/discovery)
- `destinations/decorators.py:33`, `sources/decorators.py:34`,
  `transformations/registry.py:58`, `core/registry.py:56` — `✓ Registered …`
- `core/pipeline_discovery.py:63, 66, 105, 116, 118` — discovery progress +
  `Failed to load {label} {display}: {e}` (66, a real error at print level).

These run before `setup_logging()` in some entrypoints, which is likely why
they're prints. Worth routing through `logger` at `debug`/`info`, with 66 at
`warning`/`error`.

---

## Suggested target state (for a cleanup pass)

- One mechanism: `get_logger(__name__)` everywhere; delete `print` except
  `console.py` destination and `elastic_handler.py` self-diagnostics.
- Lazy `%`-args, no f-strings in log calls.
- Pass IDs via `extra={"execution_id": …, "job_id": …, "pipeline_name": …}`
  instead of embedding in the message, so the ECS fields populate.
- Drop emoji.
- Fix levels: `database.py:48` → warning; `content_dedup_scheduler.py:79` →
  error+exc_info; `pipeline_discovery.py:66` → warning; `api` failure prints →
  error with `exc_info`.
- Collapse startup banners/config dumps to single structured lines.
- Guard debug-level ID dumps (`pipeline_runner.py:519, 548`) behind `debug`.
