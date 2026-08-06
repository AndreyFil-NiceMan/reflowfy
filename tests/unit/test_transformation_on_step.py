"""The optional on_step observer feeds `reflowfy test`'s per-step view.

It reports what each transformation consumed and produced, which is how a
record-dropping step becomes visible. Omitting it must leave the worker's
behaviour and return value untouched.
"""

from reflowfy.execution.transformation_runner import apply_transformations_iteratively


class _Filter:
    """Keeps only records where keep is True."""

    name = "drop_unkept"

    def apply(self, records, runtime_params):
        return [r for r in records if r["keep"]]


class _InPlace:
    """Mutates the list it was given instead of returning a new one."""

    name = "mutate_in_place"

    def apply(self, records, runtime_params):
        records.pop()
        return records


class _Pipeline:
    name = "observed"

    def __init__(self, transformations):
        self._transformations = transformations

    def define_transformations(self, records, runtime_params):
        return self._transformations

    def define_destination(self, records, runtime_params):
        raise AssertionError("not used in these tests")


RECORDS = [{"id": 1, "keep": True}, {"id": 2, "keep": False}, {"id": 3, "keep": True}]


def test_on_step_reports_records_before_and_after_each_step():
    seen = []

    transformed, applied = apply_transformations_iteratively(
        _Pipeline([_Filter()]),
        list(RECORDS),
        {},
        on_step=lambda name, before, after: seen.append((name, list(before), list(after))),
    )

    assert len(seen) == 1
    name, before, after = seen[0]
    assert name == "drop_unkept"
    assert len(before) == 3
    assert len(after) == 2
    assert after == transformed
    assert [n for n, _ in applied] == ["drop_unkept"]


def test_in_place_mutation_still_shows_a_distinct_before():
    """Without a snapshot, before and after would be the same list object."""
    seen = []

    apply_transformations_iteratively(
        _Pipeline([_InPlace()]),
        list(RECORDS),
        {},
        on_step=lambda name, before, after: seen.append((len(before), len(after))),
    )

    assert seen == [(3, 2)]


def test_without_on_step_behaviour_is_unchanged():
    transformed, applied = apply_transformations_iteratively(
        _Pipeline([_Filter()]), list(RECORDS), {}
    )

    assert transformed == [{"id": 1, "keep": True}, {"id": 3, "keep": True}]
    assert len(applied) == 1
    name, duration = applied[0]
    assert name == "drop_unkept"
    assert duration >= 0
