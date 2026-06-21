# Dynamic (Iterative) Transformation Resolution — Design

**Date:** 2026-06-09
**Status:** Approved (design)

## Problem

A pipeline's `define_transformations(records, runtime_params)` is evaluated **once, up
front**, to build the full transformation list *before any transformation runs*. The
transformations then execute and may mutate the shared `runtime_params` dict in place.

This makes the following common pattern silently fail:

```python
def define_transformations(self, records, runtime_params):
    trans = [Transformation1()]                 # Transformation1 sets params["should_add_2"]
    if runtime_params.get("should_add_2"):      # evaluated against ORIGINAL params → False
        trans.append(Transformation2())
    return trans
```

Because the list is frozen before `Transformation1` runs, the `if` check never sees the
param that `Transformation1` produces, so `Transformation2` is never added or applied.

### Affected call sites

The "resolve once, then loop apply" shape is duplicated in five places:

- `reflowfy/execution/local_executor.py:113` — regular local execution
- `reflowfy/execution/local_executor.py:247` — id-based local execution
- `reflowfy/worker/executor.py:148` — distributed worker (replays a frozen
  `transformation_names` / `transformation_specs` list; never calls `define_transformations`)
- `reflowfy/cli/commands/test.py:340` — `reflowfy test`, regular path
- `reflowfy/cli/commands/test.py:239` — `reflowfy test`, id-based path

The `reflowfy test` command runs a pipeline locally without Docker and is the tool authors
use to verify a pipeline **before** deploying. It must therefore reproduce the exact same
dynamic-resolution behavior as real execution; otherwise an author would test a pipeline,
see a conditionally-appended transformation not applied, and be misled about production
behavior. Both of its apply-loops must route through the shared helper.

The producer side that builds the frozen list for job payloads:

- `reflowfy/reflow_manager/pipeline_runner.py:465` and `:754`

## Chosen approach

**Dynamic (iterative) resolution.** The user's pipeline code stays exactly as written.
Instead of resolving the whole list once, the framework resolves one transformation,
applies it (allowing it to mutate `runtime_params`), then re-resolves to discover any
newly-revealed transformations — repeating until the list stops growing.

(The alternative, wiring in the existing-but-never-called `should_apply_transformation`
hook, was rejected because it would require authors to rewrite pipelines to move the
gating condition out of `define_transformations`.)

## Design

### 1. Shared helper

Extract a single helper used by all three apply-loops so the logic lives in one place
rather than being copy-pasted (and drifting) across three call sites.

Proposed location: a small module under `reflowfy/execution/` (e.g.
`transformation_runner.py`). It must be importable by all four consumers —
`reflowfy/execution/local_executor.py`, `reflowfy/worker/executor.py`, and
`reflowfy/cli/commands/test.py` (which has two call sites) — without creating a circular
import. Final location decided during planning, but `reflowfy/execution/` is a safe,
dependency-light home that the CLI and worker can both import.

```python
def apply_transformations_iteratively(pipeline, original_records, runtime_params):
    transformed = original_records
    applied_count = 0
    applied_names = []
    MAX_STEPS = 1000  # guard against a pipeline that appends forever

    while True:
        current = list(pipeline.define_transformations(original_records, runtime_params))
        if len(current) <= applied_count:
            break                                   # no new transformation → done
        t = current[applied_count]                  # next, newly-revealed transformation
        t.validate_input(transformed)
        transformed = t.apply(transformed, runtime_params)   # may mutate runtime_params
        t.validate_output(transformed)
        applied_names.append(t.name)
        applied_count += 1
        if applied_count >= MAX_STEPS:
            raise TransformationError(...)          # runaway loop
    return transformed, applied_names
```

The helper must also surface whatever the call sites need for stats (per-transformation
timing keyed by name, as today in `worker/executor.py`). Exact return shape decided in
planning; behavior of the existing stats dict (name-keyed, duplicate names overwrite) is
preserved as-is.

### 2. Contract rules

- **`define_transformations` always receives the original, pre-transformation records.**
  Only `runtime_params` changes between re-resolutions. This matches the existing
  docstring ("records: ... before transformations") and keeps behavior predictable — a
  filtering transformation does not change what `define_transformations` observes.
- **Append-only.** Re-resolution may only *grow* the list. Transformations are applied
  strictly by position beyond `applied_count`; already-applied steps are never re-applied
  or un-applied. A change to an already-applied slot on a later pass is ignored. Adding
  params can append transformations, not rewrite earlier ones. This constraint is
  documented in the `define_transformations` docstring.

### 3. Paths that always have the pipeline instance

The two local-executor loops and the two `reflowfy test` loops already hold the concrete
pipeline instance, so they call the shared helper directly — no registry lookup or
fallback is involved. Only the distributed worker needs the lookup/fallback described
below.

### 4. Distributed / worker path

- The worker looks up `pipeline = pipeline_registry.get(payload["pipeline_name"])`. This
  works because the worker calls `discover_and_load_pipelines` on startup
  (`reflowfy/worker/main.py:40`).
- **If the pipeline is found** → use the shared helper. Resolving instances directly from
  the pipeline also sidesteps the registry name-collision problem that the frozen
  `transformation_specs` were originally working around.
- **If the pipeline is not found** (e.g. it failed to construct, so registration was
  silently skipped) → fall back to today's frozen-spec replay. Graceful degradation with
  no dynamic tail.
- The producer (`pipeline_runner.py`) continues to compute the initial transformation
  list for the job payload. It is still used for `job_id` generation / dedup and for
  stats. The initial resolution is deterministic (original params), so **job IDs and
  idempotency are unaffected** — dynamically-appended transformations never enter the
  hash.

### 5. Error handling

- `validate_input` / `validate_output` failures and `apply` exceptions are wrapped in
  `TransformationError` exactly as the current local executor does
  (`local_executor.py:131`).
- Exceeding `MAX_STEPS` raises `TransformationError` with a clear message naming the
  pipeline, indicating a runaway `define_transformations` that keeps appending.

### 6. Testing

- **Unit** (`tests/unit/`):
  - no param mutation → identical behavior to the old single-resolve path.
  - `Transformation1` sets a flag → `Transformation2` is appended and applied.
  - 3-deep chain: step 2 produces a param that gates step 3.
  - runaway append hits `MAX_STEPS` → raises `TransformationError`.
  - a change to an already-applied prefix slot on a later pass is ignored (append-only).
  - worker fallback: when the pipeline is absent from the registry, the frozen-spec
    replay path is used.
- **E2E** (`tests/e2e/test_runtime_params_flow.py`): extend the existing
  `TestTransformationChainEnrichment` with a mid-chain param that triggers an additional
  transformation, exercised in both local and distributed execution modes.
- **CLI `reflowfy test`** (`dx` E2E suite): verify that `reflowfy test` applies a
  conditionally-appended transformation, so the author-facing test tool matches production
  semantics. Since `reflowfy test` is interactive (prompts for params), coverage may lean
  on the shared-helper unit tests plus a scripted/non-interactive invocation in the `dx`
  suite; exact mechanics decided during planning.

## Trade-offs

- **Cost:** for *n* transformations, `define_transformations` is called ~*n+1* times and
  instances are re-constructed each pass — O(n²). Negligible for normal chains; bounded by
  `MAX_STEPS` in the worst case.
- `MAX_STEPS` is a safety net, not an expected code path.

## Out of scope

- Wiring in `should_apply_transformation` (the rejected Approach 1).
- Any change to destination resolution (still resolved once after all transformations).
- Any unrelated refactoring of the three executors beyond extracting the shared helper.
