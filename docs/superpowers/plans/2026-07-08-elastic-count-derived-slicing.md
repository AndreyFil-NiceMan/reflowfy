# Elastic Count-Derived Slicing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an Elastic query fan out into N parallel worker jobs based on how many documents it matches, via a new `docs_per_job` config knob.

**Architecture:** `ElasticSource.split()` already counts matched docs and (when `num_slices > 1`) opens a PIT and yields one sliced-scroll sub-source per slice. This change adds: when `docs_per_job` is set, derive `num_slices = min(ceil(count / docs_per_job), max_slices)` before the existing slice-yielding path runs. No worker/fetch changes — the existing slice/PIT/`search_after` fetch path handles each job.

**Tech Stack:** Python, `elasticsearch` client, pytest (`asyncio_mode=auto`), uv.

**Spec:** `docs/superpowers/specs/2026-07-08-elastic-count-derived-slicing-design.md`

---

### Task 1: Count-derived slice count in `ElasticSource.split()`

**Files:**
- Modify: `reflowfy/sources/elastic.py` (the `split()` method, currently lines 159-190)
- Test: `tests/unit/test_source_split.py` (add tests alongside existing elastic split tests, after line ~305)

- [ ] **Step 1: Write the failing tests**

Add these to `tests/unit/test_source_split.py`:

```python
def test_elastic_split_docs_per_job_derives_slice_count(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(
        url="http://es:9200",
        index="logs-*",
        base_query={"query": {"match_all": {}}},
        size=1000,
        docs_per_job=1,
    )

    class _Client:
        def count(self, **k):
            return {"count": 10}

        def open_point_in_time(self, **k):
            return {"id": "PIT123"}

    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    subs = list(src.split({}))
    assert len(subs) == 10
    assert all(s.config["pit_id"] == "PIT123" for s in subs)
    assert [s.config["slice"] for s in subs] == [{"id": i, "max": 10} for i in range(10)]


def test_elastic_split_docs_per_job_rounds_up(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(
        url="http://es:9200",
        index="logs-*",
        base_query={"query": {"match_all": {}}},
        size=1000,
        docs_per_job=100,
    )

    class _Client:
        def count(self, **k):
            return {"count": 950}  # ceil(950/100) == 10

        def open_point_in_time(self, **k):
            return {"id": "PIT"}

    monkeypatch.setattr(src, "_get_client", lambda: _Client())
    assert len(list(src.split({}))) == 10


def test_elastic_split_docs_per_job_capped_by_max_slices(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(
        url="http://es:9200",
        index="logs-*",
        base_query={"query": {"match_all": {}}},
        size=1000,
        docs_per_job=1,
        max_slices=100,
    )

    class _Client:
        def count(self, **k):
            return {"count": 5000}

        def open_point_in_time(self, **k):
            return {"id": "PIT"}

    monkeypatch.setattr(src, "_get_client", lambda: _Client())
    assert len(list(src.split({}))) == 100


def test_elastic_split_docs_per_job_single_slice_yields_self(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(
        url="http://es:9200",
        index="logs-*",
        base_query={"query": {"match_all": {}}},
        size=1000,
        docs_per_job=1000,
    )

    class _Client:
        def count(self, **k):
            return {"count": 5}  # ceil(5/1000) == 1

        def open_point_in_time(self, **k):
            raise AssertionError("must not open a PIT for a single-slice split")

    monkeypatch.setattr(src, "_get_client", lambda: _Client())
    subs = list(src.split({}))
    assert len(subs) == 1
    assert subs[0] is src


def test_elastic_split_docs_per_job_empty_yields_nothing(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(
        url="http://es:9200",
        index="logs-*",
        base_query={"query": {"match_all": {}}},
        size=1000,
        docs_per_job=1,
    )

    class _Client:
        def count(self, **k):
            return {"count": 0}

        def open_point_in_time(self, **k):
            raise AssertionError("must not open a PIT when the query matches no documents")

    monkeypatch.setattr(src, "_get_client", lambda: _Client())
    assert list(src.split({})) == []
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/unit/test_source_split.py -k docs_per_job -v`
Expected: FAIL — with `docs_per_job` unused, count=10/`docs_per_job=1` still yields 1 job (`num_slices` defaults to 1), so `test_elastic_split_docs_per_job_derives_slice_count` fails on `len(subs) == 10`.

- [ ] **Step 3: Implement the count-derived slice logic**

In `reflowfy/sources/elastic.py`, add `from math import ceil` to the imports at the top (after the existing `from typing import ...` line).

Then replace this block in `split()` (currently lines 168-174):

```python
        if self._count_documents(client, resolved) == 0:
            return

        num_slices = int(resolved.get("num_slices", 1))
        if num_slices <= 1:
            yield self
            return
```

with:

```python
        count = self._count_documents(client, resolved)
        if count == 0:
            return

        docs_per_job = resolved.get("docs_per_job")
        if docs_per_job:
            max_slices = int(resolved.get("max_slices", 1024))
            num_slices = min(ceil(count / int(docs_per_job)), max_slices)
        else:
            num_slices = int(resolved.get("num_slices", 1))
        if num_slices <= 1:
            yield self
            return
```

Also update the `split()` docstring (lines 160-165) to mention `docs_per_job`:

```python
        """Open a PIT and yield one source per sliced-scroll slice.

        Job count is driven by ``docs_per_job`` (config) when set:
        ``num_slices = min(ceil(count / docs_per_job), max_slices)`` (default
        ``max_slices`` 1024). When ``docs_per_job`` is unset, ``num_slices``
        (config, default 1) controls parallelism. With 1 slice this is a single
        job. No documents are fetched here — the query is counted first, so a
        query matching no documents yields no jobs.
        """
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/unit/test_source_split.py -k docs_per_job -v`
Expected: PASS (5 passed)

- [ ] **Step 5: Run the full split suite to confirm no regressions**

Run: `uv run pytest tests/unit/test_source_split.py -v`
Expected: PASS — all existing elastic/sql/s3/api/static split tests plus the 5 new ones.

- [ ] **Step 6: Lint and type-check the changed file**

Run: `uv run ruff check reflowfy/sources/elastic.py && uv run black --check reflowfy/sources/elastic.py && uv run mypy reflowfy/sources/elastic.py`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add reflowfy/sources/elastic.py tests/unit/test_source_split.py
git commit -m "feat(elastic): derive job count from docs_per_job in split()"
```

---

### Task 2: Document the knob on the `elastic_source` factory

**Files:**
- Modify: `reflowfy/sources/elastic.py` (the `elastic_source` factory docstring, currently lines 282-322)

`docs_per_job` and `max_slices` already reach config through `**kwargs` (same path `num_slices` uses), so no signature change is needed — this task only makes the knob discoverable.

- [ ] **Step 1: Add a `docs_per_job` example to the factory docstring**

In `reflowfy/sources/elastic.py`, extend the `elastic_source` docstring `Example:` block (ends around line 311) with a note after the existing example:

```python
        To split the query across the worker pool, set ``docs_per_job`` — the
        manager counts matches and dispatches ``ceil(count / docs_per_job)``
        parallel slice-jobs (capped by ``max_slices``, default 1024):

        >>> source = elastic_source(
        ...     url="https://elastic:9200",
        ...     index="logs-*",
        ...     base_query={"query": {"match_all": {}}},
        ...     docs_per_job=1000,
        ... )
    """
```

- [ ] **Step 2: Lint the changed file**

Run: `uv run ruff check reflowfy/sources/elastic.py && uv run black --check reflowfy/sources/elastic.py`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add reflowfy/sources/elastic.py
git commit -m "docs(elastic): document docs_per_job on elastic_source factory"
```

---

### Task 3: Keep the knowledge graph current

**Files:** none (tooling only)

- [ ] **Step 1: Update the graphify graph**

Run: `graphify update .`
Expected: AST-only re-extraction of the changed `elastic.py`; no API cost.

- [ ] **Step 2: Commit if graph output changed**

```bash
git add graphify-out/
git diff --cached --quiet || git commit -m "chore: refresh graphify graph after elastic split change"
```
```
