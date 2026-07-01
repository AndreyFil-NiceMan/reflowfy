# `self.load_query()` — Frictionless Query Loading

**Date:** 2026-07-02
**Status:** Approved (pending implementation plan)

## Problem

Every pipeline that uses a query template from the scaffolded `queries/` folder repeats
boilerplate just to read the file off disk:

```python
from pathlib import Path
import json

QUERIES_DIR = Path(__file__).parent / "queries"
SQL_QUERY = (QUERIES_DIR / "events_by_date.sql").read_text()
ELASTIC_QUERY = json.loads((QUERIES_DIR / "events_by_timestamp.json").read_text())
```

This is pure ceremony: two imports, a module-level constant, manual path juggling, and a
manual `json.loads`. It is duplicated across every pipeline and is the first thing a new
user hits when following the scaffold. The `queries/` folder is a first-class concept in
the project layout (created by `reflowfy init`), but the framework gives no help using it.

## Goal

Collapse the boilerplate to a single, discoverable call:

```python
def define_source(self, runtime_params):
    return e2e_sql(query=self.load_query("events_by_date.sql"), id_column="id")
```

No `pathlib` import, no `json` import, no module-level constant.

## Design

### Public API — methods on `AbstractPipeline`

```python
def load_query(self, filename: str) -> str | dict | list:
    """Load a query template from the project's queries/ folder.

    .json files are parsed to a dict/list; all other extensions return raw text.
    The queries/ folder is discovered automatically and searched recursively by
    filename.
    """

def load_query_text(self, filename: str) -> str:
    """Escape hatch: return the raw file text without extension-based parsing."""
```

Both live on `AbstractPipeline` so they are available inside `define_source`,
`define_destination`, `define_transformations`, and any helper method, and are
discoverable via autocomplete on the base class.

### Folder resolution — upward search

The `queries/` directory is located relative to the **subclass's own module file**, not
the base class or the current working directory. Resolution:

1. Determine the defining module file: `sys.modules[type(self).__module__].__file__`.
2. Starting from that file's directory, walk upward through parents. At each level, check
   for a `queries/` subdirectory. The **first** one found wins.
   - e2e layout: `tests/e2e/test_pipelines/queries/` (sibling of the pipeline file) is
     found immediately.
   - scaffold layout: pipeline at `pipelines/foo.py`, `queries/` at the project root, is
     found one level up.
3. The walk is bounded — stop at the filesystem root.
4. Fallback: if `__file__` is unavailable (e.g. namespace packages) or no `queries/` dir is
   found by the walk, fall back to `Path.cwd() / "queries"`.

The resolved `queries/` directory is cached on the instance after first resolution.

### Subfolder support — recursive by filename

`queries/` may be organized into subfolders for readability. A query is addressed by
**filename only**; the resolver searches the entire tree under `queries/` recursively
(`rglob(filename)`):

- **0 matches** → `FileNotFoundError` naming the requested filename and the absolute
  `queries/` directory that was searched.
- **1 match** → load it.
- **2+ matches** (same filename in different subfolders) → `ValueError` listing every
  matching path relative to `queries/`, instructing the user to make filenames unique.

Consequence: **query filenames must be unique across all subfolders.** The ambiguity error
enforces this rather than silently guessing. This is the accepted trade-off for the
shortest possible call site (`load_query("events_by_date.sql")` regardless of nesting).

### Return type — auto-parse by extension

- `.json` → parsed via `json.loads` → `dict` or `list`.
- `.sql`, `.txt`, or any other extension → raw `str`.
- `load_query_text()` always returns raw `str`, bypassing parsing (used when a user stores
  JSON but wants the text, or for a format we do not special-case).

Loaded/parsed results are cached per resolved file path on the instance, so repeated calls
within a run do not re-read the file or re-glob the tree.

### Explicitly out of scope (YAGNI)

- No `queries_dir` class-attribute override. The upward search covers both known layouts;
  an override can be added later if a real non-standard layout appears.
- No new env var. Resolution is convention-based.
- No support for `../` / absolute paths in `filename` — filename only; recursive search
  handles organization.

## Changes

- `reflowfy/core/abstract_pipeline.py` — add `load_query`, `load_query_text`, and a private
  resolver/cache helper.
- Migrate existing e2e pipelines to the new API (validates the feature under the existing
  e2e run):
  - `tests/e2e/test_pipelines/sql_source_test_pipeline.py`
  - `tests/e2e/test_pipelines/elastic_source_test_pipeline.py`
  - `tests/e2e/test_pipelines/elastic_routed_destinations_pipeline.py`
- `reflowfy/cli/commands/init_cmd.py` and/or the scaffolded example pipeline template — the
  generated example should demonstrate `self.load_query(...)` rather than hand-rolled path
  juggling.

## Testing

Unit tests (`tests/unit/`) for the resolver and API:

- Resolution — file-adjacent `queries/` layout (e2e style).
- Resolution — project-root `queries/` layout via upward walk (scaffold style).
- Subfolder recursion — query nested in `queries/sql/foo.sql` found by `load_query("foo.sql")`.
- Ambiguity — same filename in two subfolders raises `ValueError` listing both paths.
- Missing file — raises `FileNotFoundError` naming the searched directory.
- `.json` returns a parsed dict; `.sql` returns raw text.
- `load_query_text()` on a `.json` file returns raw text.
- Caching — the file is read once across repeated calls (assert via patched read or mtime).

The three migrated e2e pipelines provide end-to-end coverage through the existing
`sources` / `destinations` suites in `scripts/run_e2e_tests.sh`.
