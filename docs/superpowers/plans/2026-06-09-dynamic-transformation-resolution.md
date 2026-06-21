# Dynamic (Iterative) Transformation Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Re-evaluate `define_transformations` after each transformation runs, so runtime params produced mid-chain can reveal later transformations — across all five execution paths (local, id-based local, distributed worker, and both `reflowfy test` paths).

**Architecture:** Extract one shared helper, `apply_transformations_iteratively`, into `reflowfy/execution/transformation_runner.py`. It resolves one transformation, applies it (letting it mutate the shared `runtime_params`), then re-resolves until the list stops growing. All execution paths call it. The distributed worker looks the pipeline up in the registry and falls back to today's frozen-spec replay when the pipeline is not discoverable.

**Tech Stack:** Python 3.14, uv, pytest (`asyncio_mode=auto`), typer (CLI), existing `BaseTransformation` / `TransformationError` / `pipeline_registry`.

**Spec:** `docs/superpowers/specs/2026-06-09-dynamic-transformation-resolution-design.md`

---

## File Structure

- **Create:** `reflowfy/execution/transformation_runner.py` — the shared iterative helper. Sole responsibility: resolve-and-apply loop + error wrapping. Depends only on `reflowfy.transformations.base` and stdlib, so the CLI and worker can both import it without cycles (`reflowfy/execution/__init__.py` is empty).
- **Create:** `tests/unit/test_transformation_runner.py` — unit tests for the helper.
- **Modify:** `reflowfy/execution/local_executor.py` — regular (`:113`) and id-based (`:247`) loops call the helper.
- **Modify:** `reflowfy/worker/executor.py` — registry lookup + helper; extract today's loop into a `_apply_frozen_transformations` fallback.
- **Modify:** `reflowfy/cli/commands/test.py` — regular (`:340`) and id-based (`:239`) loops call the helper.
- **Modify:** `reflowfy/core/abstract_pipeline.py` — document the append-only contract in the `define_transformations` docstring.
- **Modify:** `tests/e2e/test_runtime_params_flow.py` — add a mid-chain-reveal E2E case.

### Intentional behavior change (call out in review)

Today only the local executor calls `validate_input` / `validate_output`; the worker and CLI `test` do not. The shared helper **always** validates. Since the base `validate_*` methods are no-ops by default, this only affects transformations that override them, and it makes behavior consistent across all paths. This is intended.

---

## Task 1: Shared iterative helper

**Files:**
- Create: `reflowfy/execution/transformation_runner.py`
- Test: `tests/unit/test_transformation_runner.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_transformation_runner.py`:

```python
"""Unit tests for the iterative transformation runner."""

import pytest

from reflowfy.execution.transformation_runner import apply_transformations_iteratively
from reflowfy.transformations.base import BaseTransformation, TransformationError


class AppendMarker(BaseTransformation):
    """Appends its own name to the record list (so we can see ordering)."""

    name = "append_marker"

    def apply(self, records, runtime_params):
        return records + [self.name]


class SetFlag(BaseTransformation):
    name = "set_flag"

    def apply(self, records, runtime_params):
        runtime_params["should_add_2"] = True
        return records + [self.name]


class Marker2(BaseTransformation):
    name = "marker2"

    def apply(self, records, runtime_params):
        return records + [self.name]


class SetK2(BaseTransformation):
    name = "set_k2"

    def apply(self, records, runtime_params):
        runtime_params["k2"] = True
        return records + [self.name]


class Marker3(BaseTransformation):
    name = "marker3"

    def apply(self, records, runtime_params):
        return records + [self.name]


class BoomOnApply(BaseTransformation):
    name = "boom"

    def apply(self, records, runtime_params):
        raise ValueError("kaboom")


class FakePipeline:
    """Minimal stand-in: the helper only needs `name` + `define_transformations`."""

    name = "fake_pipeline"

    def __init__(self, fn):
        self._fn = fn

    def define_transformations(self, records, runtime_params):
        return self._fn(records, runtime_params)


def test_static_list_applies_each_once():
    pipeline = FakePipeline(lambda records, params: [AppendMarker(), Marker2()])
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    assert result == ["append_marker", "marker2"]
    assert [name for name, _ in applied] == ["append_marker", "marker2"]


def test_midchain_param_reveals_next_transformation():
    def define(records, params):
        trans = [SetFlag()]
        if params.get("should_add_2"):
            trans.append(Marker2())
        return trans

    pipeline = FakePipeline(define)
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    assert result == ["set_flag", "marker2"]
    assert [name for name, _ in applied] == ["set_flag", "marker2"]


def test_three_deep_chain():
    def define(records, params):
        trans = [SetFlag()]
        if params.get("should_add_2"):
            trans.append(SetK2())
        if params.get("k2"):
            trans.append(Marker3())
        return trans

    pipeline = FakePipeline(define)
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    assert [name for name, _ in applied] == ["set_flag", "set_k2", "marker3"]


def test_runaway_append_raises():
    # Each pass appends one more transformation than the records grew by, so the
    # list always stays one ahead of applied_count — an unbounded append.
    def grow(records, params):
        return [AppendMarker() for _ in range(len(records) + 1)]

    pipeline = FakePipeline(grow)
    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, [], {}, max_steps=5)
    assert "max_steps" in str(exc.value)


def test_prefix_change_is_ignored():
    # On the second pass the element at index 0 differs, but index 0 is already
    # applied, so it must not be re-applied; only the appended tail runs.
    calls = {"n": 0}

    def define(records, params):
        calls["n"] += 1
        if calls["n"] == 1:
            return [AppendMarker()]
        return [Marker2(), Marker3()]  # index 0 changed AppendMarker->Marker2

    pipeline = FakePipeline(define)
    result, applied = apply_transformations_iteratively(pipeline, [], {})
    # First applied is the original index-0 (append_marker); then the new tail (marker3).
    assert [name for name, _ in applied] == ["append_marker", "marker3"]


def test_apply_error_is_wrapped():
    pipeline = FakePipeline(lambda records, params: [BoomOnApply()])
    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, [], {})
    assert exc.value.transformation_name == "boom"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_transformation_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'reflowfy.execution.transformation_runner'`

- [ ] **Step 3: Implement the helper**

Create `reflowfy/execution/transformation_runner.py`:

```python
"""Iterative transformation resolution and application.

`define_transformations` is re-evaluated after every applied transformation so
that runtime_params mutated by one transformation can reveal transformations
that should run later. See
docs/superpowers/specs/2026-06-09-dynamic-transformation-resolution-design.md.
"""

import time
from typing import Any, Dict, List, Tuple

from reflowfy.transformations.base import TransformationError

DEFAULT_MAX_STEPS = 1000


def apply_transformations_iteratively(
    pipeline: Any,
    original_records: List[Any],
    runtime_params: Dict[str, Any],
    max_steps: int = DEFAULT_MAX_STEPS,
) -> Tuple[List[Any], List[Tuple[str, float]]]:
    """Apply a pipeline's transformations, re-resolving the list after each step.

    ``define_transformations`` is always called with the ORIGINAL pre-transformation
    records; only ``runtime_params`` changes between re-resolutions. The list must
    be append-only: re-resolution may only grow it, and already-applied steps
    (positions < applied_count) are never re-applied or un-applied.

    Args:
        pipeline: A resolved pipeline exposing ``name`` and ``define_transformations``.
        original_records: The pre-transformation records for this job/batch.
        runtime_params: The shared, mutable runtime params dict. Mutations made by a
            transformation's ``apply`` are visible to the next re-resolution.
        max_steps: Safety cap on how many transformations may be applied; protects
            against a ``define_transformations`` that appends without bound.

    Returns:
        ``(transformed_records, applied)`` where ``applied`` is a list of
        ``(transformation_name, duration_seconds)`` in application order.

    Raises:
        TransformationError: If a transformation fails validation/apply, or if
            ``max_steps`` is exceeded.
    """
    transformed = original_records
    applied: List[Tuple[str, float]] = []
    applied_count = 0

    while True:
        current = list(pipeline.define_transformations(original_records, runtime_params))
        if len(current) <= applied_count:
            break

        if applied_count >= max_steps:
            raise TransformationError(
                transformation_name=getattr(pipeline, "name", "<unknown>"),
                message=(
                    f"Exceeded max_steps={max_steps} while resolving transformations; "
                    "define_transformations appears to append without bound."
                ),
            )

        transformation = current[applied_count]
        start = time.time()
        try:
            transformation.validate_input(transformed)
            transformed = transformation.apply(transformed, runtime_params)
            transformation.validate_output(transformed)
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(
                transformation_name=getattr(transformation, "name", "<unknown>"),
                message=str(exc),
                original_error=exc,
            )
        applied.append((transformation.name, time.time() - start))
        applied_count += 1

    return transformed, applied
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_transformation_runner.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Lint / type-check the new file**

Run: `uv run ruff check reflowfy/execution/transformation_runner.py && uv run black --check reflowfy/execution/transformation_runner.py && uv run mypy reflowfy/execution/transformation_runner.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add reflowfy/execution/transformation_runner.py tests/unit/test_transformation_runner.py
git commit -m "feat: add iterative transformation runner helper"
```

---

## Task 2: Wire helper into local executor (regular path)

**Files:**
- Modify: `reflowfy/execution/local_executor.py:113-136`

- [ ] **Step 1: Add the import**

At the top of `reflowfy/execution/local_executor.py`, alongside the existing `from reflowfy.execution... import build_flat_runtime_params` import block, add:

```python
from reflowfy.execution.transformation_runner import apply_transformations_iteratively
```

(If `build_flat_runtime_params` is imported from a sibling module, place this import next to it; confirm there is no circular import by running the test in Step 3.)

- [ ] **Step 2: Replace the resolve-once + apply loop**

In `_execute`, replace this block (currently around lines 112-136):

```python
            # 2. Resolve transformations from current records/runtime params
            transformations = list(pipeline.define_transformations(records, flat_runtime_params))

            # 3. Apply transformations
            transformed_records = records

            for transformation in transformations:
                print(f"🔄 Applying transformation: {transformation.name}")

                try:
                    transformation.validate_input(transformed_records)
                    transformed_records = transformation.apply(
                        transformed_records,
                        flat_runtime_params,
                    )
                    transformation.validate_output(transformed_records)

                    print(f"✓ Transformation complete: {len(transformed_records)} records")

                except Exception as e:
                    raise TransformationError(
                        transformation_name=transformation.name,
                        message=str(e),
                        original_error=e,
                    )
```

with:

```python
            # 2 + 3. Resolve and apply transformations iteratively so that params
            # mutated mid-chain can reveal later transformations.
            transformed_records, applied = apply_transformations_iteratively(
                pipeline, records, flat_runtime_params
            )
            for name, _duration in applied:
                print(f"✓ Applied transformation: {name}")
```

- [ ] **Step 3: Verify no circular import and existing tests still pass**

Run: `uv run python -c "import reflowfy.execution.local_executor"`
Expected: no error

Run: `uv run pytest tests/unit/ -v`
Expected: PASS (all existing unit tests + Task 1 tests)

- [ ] **Step 4: Remove the now-unused import if applicable**

If `TransformationError` is no longer referenced anywhere else in `local_executor.py`, remove its import. Verify:

Run: `uv run ruff check reflowfy/execution/local_executor.py`
Expected: no unused-import (F401) errors

- [ ] **Step 5: Commit**

```bash
git add reflowfy/execution/local_executor.py
git commit -m "feat: use iterative transformation runner in local executor"
```

---

## Task 3: Wire helper into local executor (id-based path)

**Files:**
- Modify: `reflowfy/execution/local_executor.py:247-256`

- [ ] **Step 1: Replace the id-based resolve + apply loop**

In `_execute_id_based`, replace (currently around lines 246-256):

```python
                # Resolve transformations from current records/runtime params.
                transformations = list(pipeline.define_transformations(records, flat_id_params))

                for transformation in transformations:
                    print(f"  🔄 Applying: {transformation.name}")
                    transformation.validate_input(transformed_records)
                    transformed_records = transformation.apply(
                        transformed_records,
                        flat_id_params,
                    )
                    transformation.validate_output(transformed_records)
```

with:

```python
                # Resolve and apply transformations iteratively for this ID's chain.
                transformed_records, applied = apply_transformations_iteratively(
                    pipeline, records, flat_id_params
                )
                for name, _duration in applied:
                    print(f"  ✓ Applied: {name}")
```

Note: `transformed_records` is initialized to `records` just above this block in the existing code; the helper reassigns it, so leave that initialization as-is (it is harmless).

- [ ] **Step 2: Run unit tests**

Run: `uv run pytest tests/unit/ -v`
Expected: PASS

- [ ] **Step 3: Lint**

Run: `uv run ruff check reflowfy/execution/local_executor.py`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add reflowfy/execution/local_executor.py
git commit -m "feat: use iterative transformation runner in id-based local executor"
```

---

## Task 4: Wire helper into distributed worker with fallback

**Files:**
- Modify: `reflowfy/worker/executor.py:1-18` (imports), `:145-182` (apply loop)

- [ ] **Step 1: Add imports**

In `reflowfy/worker/executor.py`, add to the import block:

```python
from reflowfy.core.registry import pipeline_registry
from reflowfy.execution.transformation_runner import apply_transformations_iteratively
```

- [ ] **Step 2: Replace the apply loop with lookup + helper, keeping a fallback**

Replace this block (currently around lines 145-179):

```python
            # Load and apply transformations (CPU-bound, stays sync)
            transformed_records = records

            for idx, transformation_name in enumerate(transformation_names):
                print(f"  🔄 Applying: {transformation_name}")

                # Track transformation start time
                transform_start = time.time()

                # Prefer explicit transformation spec from producer process to
                # avoid name-collision ambiguity in registry for duplicated names.
                transformation = None
                if idx < len(transformation_specs):
                    spec = transformation_specs[idx] or {}
                    spec_name = spec.get("name")
                    module_name = spec.get("module")
                    class_name = spec.get("class_name")
                    if spec_name == transformation_name and module_name and class_name:
                        module = importlib.import_module(module_name)
                        cls = getattr(module, class_name)
                        transformation = cls()

                if transformation is None:
                    transformation = transformation_registry.create_instance(transformation_name)

                # Apply transformation — pass the shared mutable runtime_params
                transformed_records = transformation.apply(transformed_records, runtime_params)

                # Track transformation time
                transform_duration = time.time() - transform_start
                stats.transformation_times[transformation_name] = round(transform_duration, 3)

                print(
                    f"  ✓ {transformation_name}: {len(transformed_records)} records ({transform_duration:.2f}s)"
                )
```

with:

```python
            # Load and apply transformations (CPU-bound, stays sync).
            pipeline_name = job_payload.get("pipeline_name")
            pipeline = pipeline_registry.get(pipeline_name) if pipeline_name else None

            if pipeline is not None:
                # Dynamic resolution: re-resolve after each step so params mutated
                # mid-chain can reveal later transformations (matches local/test).
                transformed_records, applied = apply_transformations_iteratively(
                    pipeline, records, runtime_params
                )
                for name, duration in applied:
                    stats.transformation_times[name] = round(duration, 3)
                    print(f"  ✓ {name}")
            else:
                # Fallback: pipeline not discoverable in this worker process — replay
                # the frozen transformation list from the producer (no dynamic tail).
                transformed_records = self._apply_frozen_transformations(
                    records, runtime_params, transformation_names, transformation_specs, stats
                )
```

- [ ] **Step 3: Add the `_apply_frozen_transformations` method**

Add this method to the executor class (same class that contains the loop above), preserving today's exact behavior:

```python
    def _apply_frozen_transformations(
        self, records, runtime_params, transformation_names, transformation_specs, stats
    ):
        """Replay a frozen transformation list (fallback when the pipeline is not
        discoverable in this worker process). No dynamic re-resolution."""
        transformed_records = records
        for idx, transformation_name in enumerate(transformation_names):
            print(f"  🔄 Applying: {transformation_name}")
            transform_start = time.time()

            # Prefer explicit transformation spec from producer process to
            # avoid name-collision ambiguity in registry for duplicated names.
            transformation = None
            if idx < len(transformation_specs):
                spec = transformation_specs[idx] or {}
                spec_name = spec.get("name")
                module_name = spec.get("module")
                class_name = spec.get("class_name")
                if spec_name == transformation_name and module_name and class_name:
                    module = importlib.import_module(module_name)
                    cls = getattr(module, class_name)
                    transformation = cls()

            if transformation is None:
                transformation = transformation_registry.create_instance(transformation_name)

            transformed_records = transformation.apply(transformed_records, runtime_params)

            transform_duration = time.time() - transform_start
            stats.transformation_times[transformation_name] = round(transform_duration, 3)
            print(
                f"  ✓ {transformation_name}: {len(transformed_records)} records "
                f"({transform_duration:.2f}s)"
            )
        return transformed_records
```

- [ ] **Step 4: Verify no circular import**

Run: `uv run python -c "import reflowfy.worker.executor"`
Expected: no error

- [ ] **Step 5: Run unit tests + lint**

Run: `uv run pytest tests/unit/ -v && uv run ruff check reflowfy/worker/executor.py`
Expected: PASS, no lint errors

- [ ] **Step 6: Commit**

```bash
git add reflowfy/worker/executor.py
git commit -m "feat: dynamic transformation resolution in worker with frozen-list fallback"
```

---

## Task 5: Wire helper into `reflowfy test` (both paths)

**Files:**
- Modify: `reflowfy/cli/commands/test.py` (imports; id-based loop `:239`; regular loop `:340`)

- [ ] **Step 1: Add the import**

Near the top imports of `reflowfy/cli/commands/test.py`, add:

```python
from reflowfy.execution.transformation_runner import apply_transformations_iteratively
from reflowfy.transformations.base import TransformationError
```

- [ ] **Step 2: Replace the id-based loop**

Replace the id-based block (currently around lines 239-256):

```python
                transformations = list(pipeline.define_transformations(records, meta))
                console.print(
                    f"  [bold]🔄 Transformations:[/bold] {[t.__class__.__name__ for t in transformations]}"
                )

                for t in transformations:
                    console.print(f"    [cyan]Applying {t.__class__.__name__}...[/cyan]")
                    try:
                        transformed = t.apply(transformed, meta)
                        console.print(
                            f"    [green]✓ {t.__class__.__name__}: {len(transformed)} records[/green]"
                        )
                    except Exception as e:
                        console.print(f"    [red]❌ {t.__class__.__name__} failed: {e}[/red]")
                        traceback.print_exc()
                        break
```

with:

```python
                try:
                    transformed, applied = apply_transformations_iteratively(
                        pipeline, records, meta
                    )
                    for name, _duration in applied:
                        console.print(f"    [green]✓ {name}: {len(transformed)} records[/green]")
                except TransformationError as e:
                    console.print(f"    [red]❌ {e.transformation_name} failed: {e}[/red]")
                    traceback.print_exc()
```

- [ ] **Step 3: Replace the regular loop**

Replace the regular block (currently around lines 340-352, through the end of that loop):

```python
        # Resolve and apply transformations
        transformations = list(pipeline.define_transformations(records, flat_test_params))
        console.print(
            f"[bold]🔄 Transformations:[/bold] {[t.__class__.__name__ for t in transformations]}"
        )

        transformed = records
        for t in transformations:
            console.print(f"  [cyan]Applying {t.__class__.__name__}...[/cyan]")
            try:
                transformed = t.apply(transformed, flat_test_params)
                console.print(
                    f"  [green]✓ {t.__class__.__name__}: {len(transformed)} records[/green]"
                )
            except Exception as e:
                console.print(f"  [red]❌ {t.__class__.__name__} failed: {e}[/red]")
                traceback.print_exc()
                break
```

with:

```python
        # Resolve and apply transformations iteratively (matches production semantics).
        transformed = records
        try:
            transformed, applied = apply_transformations_iteratively(
                pipeline, records, flat_test_params
            )
            for name, _duration in applied:
                console.print(f"  [green]✓ {name}: {len(transformed)} records[/green]")
        except TransformationError as e:
            console.print(f"  [red]❌ {e.transformation_name} failed: {e}[/red]")
            traceback.print_exc()
```

Note: confirm the exact original lines of the regular loop (especially the trailing `break` and any code after it) before replacing, since line numbers may have shifted. Preserve any code that runs after the loop (sample output, destination send).

- [ ] **Step 4: Verify the CLI imports cleanly**

Run: `uv run python -c "import reflowfy.cli.commands.test"`
Expected: no error

- [ ] **Step 5: Lint**

Run: `uv run ruff check reflowfy/cli/commands/test.py`
Expected: no errors (remove the now-unused `time` import only if it is genuinely unused elsewhere in the file)

- [ ] **Step 6: Commit**

```bash
git add reflowfy/cli/commands/test.py
git commit -m "feat: dynamic transformation resolution in reflowfy test command"
```

---

## Task 6: Document the append-only contract

**Files:**
- Modify: `reflowfy/core/abstract_pipeline.py:358-381` (the `define_transformations` docstring)

- [ ] **Step 1: Extend the docstring**

In the `define_transformations` abstract method docstring, after the existing `Example:` block, add a `Note:` paragraph:

```python
        Note:
            This method is re-evaluated after each transformation is applied, so a
            transformation that adds a key to ``runtime_params`` can cause a later
            transformation to be appended to the returned list on the next pass.
            The list must be **append-only** with respect to growing
            ``runtime_params``: re-resolution may only grow the list. The
            ``records`` argument is always the original pre-transformation records
            (it does not change between passes); only ``runtime_params`` changes.
            Already-applied transformations are never re-applied, and changes to
            earlier (already-applied) positions on a later pass are ignored.
```

- [ ] **Step 2: Verify docstring builds / file imports**

Run: `uv run python -c "import reflowfy.core.abstract_pipeline"`
Expected: no error

- [ ] **Step 3: Commit**

```bash
git add reflowfy/core/abstract_pipeline.py
git commit -m "docs: document append-only contract for define_transformations"
```

---

## Task 7: E2E mid-chain-reveal test

**Files:**
- Modify: `tests/e2e/test_runtime_params_flow.py` (add a test under `TestTransformationChainEnrichment`)
- Possibly modify: `tests/e2e/test_pipelines/` — add a pipeline + transformations that reveal a step mid-chain (follow the existing test-pipeline pattern in that directory).

- [ ] **Step 1: Inspect the existing chain-enrichment test + its pipeline**

Read `tests/e2e/test_runtime_params_flow.py` (focus on the `TestTransformationChainEnrichment` class and its helper methods), then read the pipeline(s) it references under `tests/e2e/test_pipelines/` to copy the established pattern: source, transformations, destination, and how records are asserted via the mock destination.

- [ ] **Step 2: Add a pipeline that reveals a transformation mid-chain**

In the appropriate file under `tests/e2e/test_pipelines/` (mirror the existing chain-enrichment pipeline), add a pipeline whose `define_transformations` appends a second transformation only when the first set a flag:

```python
class RevealMidChainPipeline(AbstractPipeline):
    name = "reveal_midchain"

    # define_source / define_destination / define_parameters: copy the pattern
    # used by the existing TestTransformationChainEnrichment pipeline in this dir.

    def define_transformations(self, records, runtime_params):
        trans = [SetShouldAddSecond()]            # sets runtime_params["should_add_second"] = True
        if runtime_params.get("should_add_second"):
            trans.append(StampSecondApplied())    # stamps each record so the test can assert it ran
        return trans
```

Define the two transformations following the existing `@transformation` / `BaseTransformation` pattern in that directory: `SetShouldAddSecond` sets `runtime_params["should_add_second"] = True` and returns records unchanged; `StampSecondApplied` adds a field (e.g. `record["second_applied"] = True`) to every record.

- [ ] **Step 3: Add the E2E assertion**

In `tests/e2e/test_runtime_params_flow.py`, under `TestTransformationChainEnrichment`, add:

```python
    def test_midchain_param_reveals_second_transformation(self):
        """A param set by the first transformation must cause the second to run."""
        self.clear_received_records()
        execution_id = self.trigger_pipeline("reveal_midchain", params={})
        self.wait_for_execution(execution_id)
        records = self.get_received_records()
        assert records, "expected records at the destination"
        assert all(r.get("second_applied") is True for r in records), (
            "second transformation was not applied — mid-chain param was not honored"
        )
```

Use the same helper-method names already present in this test file (`clear_received_records`, `trigger_pipeline`, `wait_for_execution`, `get_received_records`) — adjust signatures to match their existing definitions.

- [ ] **Step 4: Run the relevant E2E suite (both modes)**

Run: `./scripts/run_e2e_tests.sh --test-file tests/e2e/test_runtime_params_flow.py`
Expected: PASS, including the new test. (This rebuilds the wheel and spins up the stack — required so source edits are exercised.)

If the suite runs a single execution mode by default, also confirm the distributed path: ensure `EXECUTION_MODE=distributed` is exercised per the existing test harness conventions in `scripts/run_e2e_tests.sh`.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/test_runtime_params_flow.py tests/e2e/test_pipelines/
git commit -m "test: e2e coverage for mid-chain transformation reveal"
```

---

## Task 8: Full verification sweep

- [ ] **Step 1: Unit + lint + types**

Run: `uv run pytest tests/unit/ -v && uv run ruff check reflowfy/ && uv run black --check reflowfy/ && uv run mypy reflowfy/`
Expected: all PASS / no errors

- [ ] **Step 2: Full E2E (dx suite covers the CLI `test` command)**

Run: `./scripts/run_e2e_tests.sh dx` then `./scripts/run_e2e_tests.sh destinations`
Expected: PASS. The `dx` suite exercises CLI scaffolding/commands; confirm `reflowfy test` still runs end-to-end with the new helper. If the `dx` suite does not currently assert a mid-chain reveal via `reflowfy test`, rely on the Task 1 unit tests for that semantic (the CLI path uses the same helper).

- [ ] **Step 3: Refresh the graphify graph**

Run: `graphify update .`
Expected: graph updated (AST-only, no API cost), per `AGENTS.md`.

- [ ] **Step 4: Final commit (if graph or any docs changed)**

```bash
git add -A
git commit -m "chore: refresh graphify graph after dynamic transformation resolution"
```

---

## Self-Review Notes

- **Spec coverage:** §1 helper → Task 1; §2 contract rules → helper (original records, append-only by position) + Task 6 docs; §3 local/test always have pipeline → Tasks 2,3,5; §4 worker lookup+fallback → Task 4; §5 error handling (wrap + max_steps) → Task 1 helper; §6 testing → Tasks 1 (unit) + 7 (e2e) + 8 (dx). Affected call sites (5) → Tasks 2,3,4,5.
- **Behavior change:** worker + CLI now run `validate_input`/`validate_output` (previously local-only). Documented at top under File Structure; defaults are no-ops.
- **Type consistency:** helper returns `(transformed_records, applied)` with `applied: List[Tuple[str, float]]`; every call site unpacks the same way and iterates `for name, duration/_duration in applied`.
