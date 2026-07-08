# Elastic source: count-derived slicing for `split()`

**Date:** 2026-07-08
**Status:** Approved (design)

## Problem

`ElasticSource.split()` only shards into multiple jobs when the user manually
sets `num_slices` in the source config. With the default (`num_slices=1`) any
query — no matter how many documents it matches — becomes a **single job**.
Users expect a query to fan out into multiple worker jobs based on how much data
it returns (as every other source already does):

| Source | `split()` strategy |
|--------|--------------------|
| SQL | id-range windows → one job per window |
| API | ids grouped into `batch_size` chunks → one job each |
| S3 | list keys → one job per page of keys |
| **Elastic** | **only `num_slices`, manual, hash-based** — the outlier |

Reported symptom: `scroll="2m", size=1` against an index with 10 matching docs
produced 1 job, not 10. Root cause: `size` is the scroll **page** size (batching
inside one job), not a job-count control; `num_slices` is the only job-count
control and defaults to 1.

## Constraint that shapes the design

Result sets can exceed Elastic's `max_result_window` (default 10,000), so:

- `from`/`size` offset windows are **out** (capped at 10k).
- Pre-computing `search_after` cursors per window is **out** — it requires
  fetching every doc's sort key during planning, violating the metadata-only
  `split()` contract in `sources/base.py`.
- **Sliced scroll** is the only mechanism that partitions a query independently
  at any scale: each worker opens the PIT and requests "slice i of N"; Elastic
  hash-routes docs to slices. This path already exists in `ElasticSource`
  (PIT + slice in `split()`, PIT + `search_after` in `fetch()`).

## Design

Auto-derive the slice count from the matched-document count in `split()`,
reusing the existing slice/PIT/fetch machinery unchanged.

### Config (new optional knobs on `ElasticSource` / `elastic_source`)

- `docs_per_job: int | None = None` — target documents per job. When set, drives
  the split. When unset, behavior is unchanged from today.
- `max_slices: int = 1024` — hard cap on derived slice count (aligns with
  Elastic's `max_slices_per_scroll` default), prevents slice explosion on huge
  result sets.
- `size` is unchanged: the scroll **page** size *inside* each job. Independent of
  job count. `docs_per_job` controls parallelism across the worker pool; `size`
  controls round-trip batching within a single job. Setting `docs_per_job == size`
  yields "roughly one page's worth of docs per job."

### `split()` logic (replaces the `num_slices` line at elastic.py:171)

```
resolved = resolve_parameters(...) or config          # unchanged
count = self._count_documents(client, resolved)        # already exists
if count == 0: return                                  # already handled
docs_per_job = resolved.get("docs_per_job")
if docs_per_job:
    max_slices = int(resolved.get("max_slices", 1024))
    num_slices = min(ceil(count / int(docs_per_job)), max_slices)
else:
    num_slices = int(resolved.get("num_slices", 1))    # unchanged
if num_slices <= 1:
    yield self
    return
# open PIT, yield num_slices slice sub-sources          # unchanged (elastic.py:176-190)
```

### Backward compatibility

- Neither `docs_per_job` nor `num_slices` set → `num_slices` defaults to 1 →
  one job, exactly as today.
- `num_slices` set explicitly, `docs_per_job` unset → honored as today.
- Only *new* behavior is triggered by setting `docs_per_job`.

### Worker fetch

Unchanged. The slice/PIT/`search_after` path (elastic.py:89-114) already handles
each slice job.

## Trade-off (accepted)

Elastic distributes docs across slices by **hash**, so each job receives
*approximately* `docs_per_job` documents, not exactly. The job **count** is
deterministic; the per-job document count is not. Exact-N-per-job is not
achievable at >10k scale without violating the metadata-only planning contract,
and was explicitly deemed unnecessary.

## Testing

Unit tests in `tests/unit/test_source_split.py` (alongside existing elastic split
tests):

- `docs_per_job=1`, `_count_documents` mocked to 10 → `split()` yields 10
  sub-sources, each carrying a distinct `slice.id` (`0..9`) and `slice.max=10`,
  all sharing one `pit_id`.
- `docs_per_job=100`, count mocked to 1000 → 10 sub-sources.
- `docs_per_job` set, count mocked to 0 → yields nothing (no PIT opened).
- `docs_per_job` large enough that `ceil(count/docs_per_job) == 1` → yields
  `self`, no PIT opened.
- `max_slices` cap: `docs_per_job=1`, count=5000, `max_slices=100` → 100
  sub-sources, not 5000.
- Backward compat: neither knob set → yields `self` (single job).

`open_point_in_time` and `_count_documents` are mocked; no live Elasticsearch.
```
