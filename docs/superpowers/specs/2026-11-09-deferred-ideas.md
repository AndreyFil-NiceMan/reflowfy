# Deferred ideas (2026-11-09)

Found in the framework-gaps audit, agreed as worth doing, **not scheduled**. Plan for the
scheduled items: `docs/superpowers/plans/2026-11-09-framework-gaps.md`.

## #12 Per-record retries in `@record_transformation`

- `@record_transformation(retries=3, retry_on=(httpx.TransportError, TimeoutError),
  backoff=1.0, max_backoff=30)`.
- Default `retry_on`: the step's timeout + connection errors; never `KeyError`/`TypeError`.
- Exponential backoff with jitter; each attempt gets its own `timeout`; `SkipRecord` /
  `SkipJob` never retried; after the last attempt `on_error` (#11) decides.
- Observability: `AppliedStep.retries`, `JobStats.record_retries`, debug log. No metric
  (a retry is not a terminal outcome).

## #13 Shared client in the decorator form

- `@record_transformation("enrich", resource=lambda params: httpx.AsyncClient(...))`,
  function `async def enrich(record, params, client)`.
- `resource` returns an async context manager, opened once per step (per job), passed
  as the third argument, closed even on failure. Per job because local mode and
  `reflowfy test` use a new loop per job; could later draw from the #22 worker pool.

## #20 Async sources — done (2026-11-13)

`AsyncSource.fetch_async`, awaited by `job_runner.fetch_records`. Converting the built-in
sources (notes below) is still open.

- Today `fetch` already runs in a thread, so the loop is not blocked; async adds
  concurrency inside one fetch and fewer threads.
- `AsyncSource` with `async def fetch_async`, awaited by the job runner (mirrors
  `AsyncTransformation`); sync `fetch()` kept. Planning (`split`) stays sync.
- Worth converting: `IDBasedAPISource` per-ID mode (sequential today; 100 IDs × 50 ms
  ≈ 5 s → ~0.3 s with bounded concurrency). Elastic: `AsyncElasticsearch`, small gain.
  SQL / S3: keep in threads (per-dialect async drivers, `aioboto3` dependency).
- Streaming pages inside one job is a separate, larger change (transformations and
  `define_destination` see the whole job's records).

## #24 Per-record send failures

- Kafka (per-message ack) and per-record `api_destination`: `send()` reports failed
  positions; `send_with_retry` retries only those (same backoff, #8 rules, Retry-After).
- After the last attempt, destination `on_error`: `"fail"` (default → job fails → #23),
  `"skip"` (existing skip path, reason = exception type), or
  `dead_letter=kafka_destination(topic="…-dlq")` with an envelope (original record, error
  type, HTTP status, destination, attempts, execution/job id).
- Dedup: claims of unsent records released; dead-lettered ones kept.
- Job-level `api_destination` (one request per job) keeps job-level behaviour.
- Observability: `records_failed` / `record_failure_reasons` in stats,
  `reflowfy_records_dead_lettered_total{pipeline, destination_type}`, log line.
