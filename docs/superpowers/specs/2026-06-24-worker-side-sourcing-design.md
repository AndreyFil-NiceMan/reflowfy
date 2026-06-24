# Worker-Side Sourcing — Design

**Date:** 2026-06-24
**Status:** Approved (design)

## Problem

Today the **ReflowManager** does almost all of a job's real work before a worker ever
sees it. In `pipeline_runner._run_pipeline_jobs` (`:456-521`) and
`_run_id_based_pipeline_jobs` (`:746-810`) the manager, per job:

1. **fetches** the records — `source.split_jobs()` pulls the data into the manager,
2. **applies every transformation** to build a `transformed_preview`
   (`:468-470` / `:757-759`) — real transform work, on the manager,
3. uses that preview to **resolve the destination** (`define_destination`),
4. ships the **raw, untransformed** records in the job payload (`:516` / `:804`),
5. and the **worker transforms them again** (`executor.py:154`).

Consequences:

- **The manager holds all the data.** A pipeline whose source returns ~1M records
  materializes them in the manager's memory and stuffs them into Kafka messages.
- **Transformations run twice** — once on the manager (only to pick a destination),
  once on the worker (for real).
- **The manager is a throughput bottleneck / SPOF** — all sourcing funnels through it.
- **The job message is stale and asymmetric.** `source` is not a first-class field;
  only leftover `source_metadata` survives, buried inside a doubly-nested `metadata`
  block. Half the payload (`rate_limit`, `transformation_specs`, the `destination`
  preview) is dead/fallback data in the common path, and there is no schema version,
  so producer/consumer drift is silent.

## Goal

Make the manager a **pure planner** and the worker the **executor of the whole
pipeline**:

- The manager never fetches bulk data, never transforms, never resolves a destination
  from records. It only **plans** how to divide the work and ships small descriptors.
- Each worker pulls its own slice of data and runs the full
  `source → transform → destination` flow.
- The job message is self-describing for what the worker cannot re-derive (the planned
  source slice) and lets the worker resolve the rest dynamically.

## Core idea: "a slice is a smaller source"

A slice is **not** metadata bolted onto a source — a slice **is** a narrowed source.
The manager takes one source and divides it into many sources, each already scoped to
its slice. Each job's `source` field is then fully self-contained, and the worker just
reconstructs it and calls `fetch()`. No separate `slice` field, no `fetch_slice`, no
special-casing.

### New method on `BaseSource`

```python
def split(self, runtime_params: Dict[str, Any]) -> Iterator["BaseSource"]:
    """Manager-side planning. Yield one narrowed source per job — cheap,
    metadata-only, NO bulk data fetch. Default yields [self] (single job)."""
    yield self
```

Per built-in source, `split` divides the work **without** pulling rows:

| Source | `split()` strategy (manager, metadata-only) | What the slice bakes into `config` |
|---|---|---|
| Elastic | Open a PIT; yield N sources, one per sliced-scroll slice | `pit_id`, `slice: {id, max}` |
| SQL | `min/max(id)` or `count(*)`; yield N range windows | bounded `where id BETWEEN lo AND hi` (or `offset/limit`) |
| S3 | `list_objects` (metadata only); yield N key-batches | `keys: [...]` subset |
| API (`IDBasedAPISource`) | Group ids / page ranges | `ids` subset or page/offset range |
| StaticSource | yield `[self]` | `records` already in config (id/passthrough case) |
| Any source without an override | default `yield self` → one job | unchanged config |

The worker fetches each narrowed source with the **existing** `fetch()` — the slice is
already encoded in `config`, so no new worker-side fetch verb is needed.

## Job message schema (v2)

This is the entire message sent to the worker. `source` is the only authoritative data
descriptor; transformations and the destination are resolved dynamically on the worker.

```jsonc
{
  "schema_version": 2,                    // int    — drift guard; worker branches/rejects on mismatch
  "execution_id":   "exec-abc123",        // string
  "job_id":         "uuid-or-sha256",     // string
  "pipeline_name":  "user_sync",          // string — drives DYNAMIC transform + destination resolution

  "source": {                             // object — AUTHORITATIVE; manager-planned, narrowed to this slice
    "type":   "ElasticSource",            //   string — SourceFactory registry key
    "config": { /* conn, query, slice baked in */ }   // dict — all the worker needs to fetch
  },

  "metadata": {                           // object — execution context, NO records
    "batch_id":      "uuid",
    "created_at":    "2026-06-24T10:00:00",
    "batch_number":  1,
    "total_batches": 16,
    "retry_count":   0,
    "is_retry":      false,
    "runtime_params": { /* per-job enriched user params */ },
    "current_ids":   [101, 102],          // present only for IdBasedPipeline jobs
    "source_metadata": { /* per-slice info, or null */ }
  }
}
```

**Removed from today's payload:** `records`, `transformations`, `transformation_specs`,
the `destination` preview, `rate_limit`, and the doubly-nested `metadata.metadata`.

### Why transformations / destination are NOT on the wire

They are resolved **dynamically** against the real fetched records — only the worker,
after fetching, knows the records. Freezing them in the message would break
content-dependent resolution (and would reintroduce stale/dead data). The worker derives
them from `pipeline_name` + the fetched records, exactly as `executor.py:154,177` already
does when the pipeline is in the registry.

## Manager flow (unified, both pipeline types)

The manager produces `source` descriptors and dispatches them. It never fetches bulk
data, never transforms, never resolves a destination.

1. **Gather base source(s):**
   - **AbstractPipeline:** one source from `define_source(params)`.
   - **IdBasedPipeline:** one source per id-batch from `define_source(batch_params)`;
     lists are wrapped in `StaticSource` (reuse `IdBasedPipeline.resolve_source`,
     `id_based_pipeline.py:384`).
2. **Plan slices:** `for sub in base_source.split(params):` → one job per `sub`.
3. **Build payload:** `source = {"type": registry_type(sub), "config": sub.config}`.
4. **Persist + dispatch** exactly as today (jobs table, batch numbers, checkpoint
   batching, rate limiting). Only the payload contents change.

A 1M-row Elastic query → `split()` yields N narrowed sources → N jobs → N workers fetch
in parallel, with the manager holding none of the data. An id pipeline → one source per
id-batch, each usually `[self]` → one job per id-batch.

## Worker flow (one code path)

```python
src      = SourceFactory.create(source["type"], source["config"])
records  = src.fetch(runtime_params)                                 # 1. fetch the planned slice

pipeline = pipeline_registry.get(pipeline_name)                      # 2. dynamic, content-aware
records  = apply_transformations_iteratively(pipeline, records, runtime_params)
dest     = pipeline.define_destination(records, runtime_params)

await dest.health_check()                                            # 3. lag gate, worker-side
await dest.send_with_retry(records, runtime_params)                  # 4. write
```

- The worker now **requires** the pipeline in its registry (auto-discovered on startup).
- The **frozen-transformation fallback** (`executor._apply_frozen_transformations`,
  `:226`) is **deleted** — it only existed for the "pipeline not discoverable" case,
  which can no longer source.
- The pre-dispatch **lag health check is removed from the manager**
  (`pipeline_runner._check_destination_lag_health`, `:947`). The worker already calls
  `destination.health_check()` (`executor.py:183`), which is the same method; a
  lag-exceeded job simply isn't committed and retries via Kafka. (DLQ-reschedule
  semantics can be reintroduced worker-side later if needed.)

## Supporting change: standardize source (de)serialization

This is the load-bearing change everything depends on. Today reconstruction is
inconsistent: `SourceFactory.create` (`source_factory.py:53`) does `Class(config)` and
only registers elastic/sql/mock, while `IDBasedAPISource`/`StaticSource` take explicit
kwargs and are **not** registered.

Establish one round-trip contract for **every** source:

- Each source exposes a stable **registry type name** and a `config` dict that fully
  reconstructs it via `SourceFactory.create(type_name, config)`.
- Register **all** built-ins (elastic, sql, mock, api, static, s3) in `SourceFactory`.
- Reconcile the two constructor styles (config-dict vs kwargs) behind a single
  `create()` path (e.g. a `from_config` classmethod, or normalize all sources to a
  uniform constructor). The serialized `config` must be exactly what reconstruction
  consumes.
- User-defined `BaseSource` subclasses must be registrable so custom sources round-trip.

## Deterministic job IDs

`generate_job_id` (`pipeline_runner.py:36`) can no longer hash `records`. It hashes
`{pipeline_name, source.type, source.config, current_ids?}` with the existing volatile
date-key stripping (`_DATE_KEY_PATTERNS`). Because the narrowed `config` encodes the
slice, identical slices dedupe; for `StaticSource` the records live in `config`, so the
hash is equivalent to today's record-hash. One code path for both pipeline types.

## Trade-offs and limitations

- **Worker must have the pipeline + connector classes** (source/transformation/
  destination) importable. Consistent with the existing auto-discovery model; the
  fallback path goes away.
- **Per-source planning is real work.** Each built-in needs a correct, data-free
  `split()`. Elastic relies on PIT + sliced scroll; SQL on id-range/offset windows; S3
  on key listing; API on id/page grouping. Sources that cannot cheaply pre-plan fall
  back to the default `split` → `[self]` (one job, worker fetches all), preserving
  correctness at the cost of parallelism for that source.
- **Lag backpressure** changes from manager-side DLQ-reschedule to worker-side
  health-check + Kafka retry. Acceptable now; revisit if DLQ semantics are required.

## Testing

- **Unit — source round-trip:** for every built-in, `(type, config) → instance` via
  `SourceFactory.create` reproduces an equivalent source.
- **Unit — `split()`:** each built-in yields the expected narrowed sources from
  metadata-only inputs (mock the count/list/PIT calls); assert no bulk fetch occurs.
- **Unit — worker:** `execute_job` rebuilds a real source and a `StaticSource`, fetches,
  and runs the dynamic transform → destination chain; frozen-fallback removed.
- **Unit — manager:** produces a fetch-free, records-free payload matching the v2 schema;
  job-id hashing is stable across runs for identical slices.
- **E2E:** id-based worker-sourcing flow end-to-end; Elastic AbstractPipeline split into
  N parallel jobs; passthrough/id (`StaticSource`) case.

## Out of scope

- Source self-split beyond the per-source strategies above (e.g. adaptive re-slicing).
- Reintroducing DLQ-reschedule backpressure on the worker.
- Changing the rate limiter, checkpoint batching, or execution state machine.
