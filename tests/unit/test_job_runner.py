"""run_job_records must not invoke transformations on an empty slice.

Elastic sliced-scroll hash-partitions unevenly, so a slice can fetch zero docs;
those empty jobs must skip transformation + destination resolution instead of
calling the pipeline's transformation on [].
"""

from reflowfy.execution.job_runner import run_job_records


class _EmptySource:
    def fetch(self, runtime_params, limit=None):
        return []


class _ExplodingPipeline:
    name = "explodes-on-empty"

    def define_transformations(self, records, runtime_params):
        raise AssertionError("transformations must not run on an empty slice")

    def define_destination(self, records, runtime_params):
        raise AssertionError("destination must not resolve on an empty slice")


def test_empty_slice_skips_transformation_and_destination():
    records, transformed, applied, destination = run_job_records(
        _EmptySource(), _ExplodingPipeline(), {}
    )
    assert records == []
    assert transformed == []
    assert applied == []
    assert destination is None
