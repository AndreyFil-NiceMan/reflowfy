"""Per-step record counts and failure diagnostics from the transformation runner.

These back the `reflowfy test` reporting: what each step did to the batch, and —
when a step raises — which step it was and which record caused it.
"""

import pytest

from reflowfy.execution.transformation_runner import (
    apply_transformations_iteratively,
    find_culprit_record,
)
from reflowfy.transformations.base import BaseTransformation, TransformationError


class FakePipeline:
    """Minimal stand-in: the runner only needs `name` + `define_transformations`."""

    name = "fake_pipeline"

    def __init__(self, transformations):
        self._transformations = transformations

    def define_transformations(self, records, runtime_params):
        return self._transformations


class KeepEvens(BaseTransformation):
    name = "diag_keep_evens"

    def apply(self, records, runtime_params):
        return [r for r in records if r["id"] % 2 == 0]


class DropAll(BaseTransformation):
    name = "diag_drop_all"

    def apply(self, records, runtime_params):
        return []


class NeedsLastName(BaseTransformation):
    name = "diag_needs_last_name"

    def apply(self, records, runtime_params):
        return [{**r, "last_name": r["last_name"].upper()} for r in records]


class Boom(BaseTransformation):
    name = "diag_boom"

    def apply(self, records, runtime_params):
        raise ValueError("kaboom")


# ---------------------------------------------------------------------------
# Per-step record counts
# ---------------------------------------------------------------------------


def test_applied_step_reports_real_in_out_counts():
    # The regression this guards: the CLI used to print the FINAL record count
    # next to every step, so a filter that dropped 5 of 10 looked like a no-op.
    pipeline = FakePipeline([KeepEvens()])
    records = [{"id": i} for i in range(10)]

    result, applied = apply_transformations_iteratively(pipeline, records, {})

    assert len(result) == 5
    step = applied[0]
    assert step.name == "diag_keep_evens"
    assert step.records_in == 10
    assert step.records_out == 5
    assert step.delta == -5


def test_applied_step_counts_are_per_step_not_final():
    pipeline = FakePipeline([KeepEvens(), DropAll()])
    records = [{"id": i} for i in range(10)]

    _result, applied = apply_transformations_iteratively(pipeline, records, {})

    assert [(s.records_in, s.records_out) for s in applied] == [(10, 5), (5, 0)]


def test_applied_step_records_duration():
    pipeline = FakePipeline([KeepEvens()])
    _result, applied = apply_transformations_iteratively(pipeline, [{"id": 2}], {})
    assert applied[0].duration >= 0.0


# ---------------------------------------------------------------------------
# Failure diagnostics
# ---------------------------------------------------------------------------


def test_failure_names_the_exception_type():
    # str(KeyError) is bare ("'last_name'"); without the type the reader cannot
    # tell a missing key from a bad value.
    pipeline = FakePipeline([NeedsLastName()])
    records = [{"id": 0, "last_name": "a"}, {"id": 1}]

    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, records, {})

    assert "KeyError" in str(exc.value)
    assert exc.value.transformation_name == "diag_needs_last_name"


def test_failure_points_at_the_offending_record():
    pipeline = FakePipeline([NeedsLastName()])
    records = [{"id": i, "last_name": f"L{i}"} for i in range(10)]
    del records[7]["last_name"]

    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, records, {})

    assert exc.value.culprit_index == 7
    assert exc.value.culprit_record == {"id": 7}
    assert exc.value.records_in == 10


def test_failure_reports_position_in_the_chain():
    pipeline = FakePipeline([KeepEvens(), NeedsLastName()])
    records = [{"id": i} for i in range(4)]

    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, records, {})

    # keep_evens ran first (index 0), so the failure is at step index 1, and it
    # received only the 2 records that survived the filter.
    assert exc.value.step_index == 1
    assert exc.value.records_in == 2


def test_non_keyerror_failure_identifies_no_culprit():
    # We refuse to guess: only KeyError names the thing it wanted.
    pipeline = FakePipeline([Boom()])

    with pytest.raises(TransformationError) as exc:
        apply_transformations_iteratively(pipeline, [{"id": 1}], {})

    assert exc.value.culprit_index is None
    assert exc.value.culprit_record is None
    assert "ValueError" in str(exc.value)


# ---------------------------------------------------------------------------
# find_culprit_record
# ---------------------------------------------------------------------------


def test_find_culprit_ignores_non_dict_records():
    # A source may yield scalars or raw text; those cannot cause a missing key.
    index, record = find_culprit_record(KeyError("k"), ["raw text", 42])
    assert (index, record) == (None, None)


def test_find_culprit_returns_first_match():
    records = [{"k": 1}, {}, {}]
    assert find_culprit_record(KeyError("k"), records) == (1, {})


def test_find_culprit_respects_scan_limit():
    # The offending record is past the limit, so we decline rather than scan on.
    records = [{"k": 1}] * 50 + [{}]
    assert find_culprit_record(KeyError("k"), records, scan_limit=10) == (None, None)
