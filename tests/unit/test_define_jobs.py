"""Unit tests for the define_jobs hook and chunk() job sizing."""

import pytest

from reflowfy import chunk
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.execution.job_runner import plan_slices
from reflowfy.sources.mock import MockSource
from reflowfy.sources.static import StaticSource


class TestChunk:
    def test_size_one_gives_one_chunk_per_record(self):
        assert chunk([1, 2, 3], size=1) == [[1], [2], [3]]

    def test_default_size_is_one(self):
        assert chunk([1, 2, 3]) == [[1], [2], [3]]

    def test_remainder_lands_in_last_chunk(self):
        assert chunk([1, 2, 3], size=2) == [[1, 2], [3]]

    def test_size_larger_than_input_gives_single_chunk(self):
        assert chunk([1, 2, 3], size=10) == [[1, 2, 3]]

    def test_empty_records_gives_no_chunks(self):
        assert chunk([], size=5) == []

    def test_size_below_one_rejected(self):
        with pytest.raises(ValueError, match="must be >= 1"):
            chunk([1, 2, 3], size=0)


class TestPlanSlicesJobPlan:
    def test_list_of_chunks_becomes_one_static_source_per_chunk(self):
        slices = list(plan_slices(chunk([1, 2, 3], size=1), {}))

        assert len(slices) == 3
        assert all(isinstance(s, StaticSource) for s in slices)
        assert [s.fetch({}) for s in slices] == [[1], [2], [3]]

    def test_empty_chunks_produce_no_jobs(self):
        slices = list(plan_slices([[], [{"id": 1}], []], {}))

        assert len(slices) == 1
        assert slices[0].fetch({}) == [{"id": 1}]

    def test_empty_plan_produces_no_jobs(self):
        assert list(plan_slices([], {})) == []

    def test_source_element_splits_itself(self):
        # MockSource.split() chunks its data by batch_size -> 2 jobs
        plan = [[{"id": 0}], MockSource(data=[{"id": 1}, {"id": 2}], batch_size=1)]

        slices = list(plan_slices(plan, {}))

        assert len(slices) == 3
        assert [s.fetch({}) for s in slices] == [[{"id": 0}], [{"id": 1}], [{"id": 2}]]

    def test_flat_record_list_is_rejected_with_a_hint(self):
        with pytest.raises(TypeError, match="chunk"):
            list(plan_slices([{"id": 1}, {"id": 2}], {}))

    def test_non_list_source_still_uses_split(self):
        source = MockSource(data=[{"id": 1}, {"id": 2}], batch_size=2)

        slices = list(plan_slices(source, {}))

        assert len(slices) == 1


class _DefineJobsPipeline(AbstractPipeline):
    name = "unit_define_jobs_pipeline"

    def define_jobs(self, runtime_params):
        return chunk([{"n": i} for i in range(3)], size=runtime_params["job_size"])

    def define_destination(self, records, runtime_params):
        return None

    def define_transformations(self, records, runtime_params):
        return []


class _DefineSourcePipeline(AbstractPipeline):
    name = "unit_define_source_pipeline"

    def define_source(self, runtime_params):
        return MockSource(data=[{"id": 1}], batch_size=1)

    def define_destination(self, records, runtime_params):
        return None

    def define_transformations(self, records, runtime_params):
        return []


class _NoHookPipeline(AbstractPipeline):
    name = "unit_no_hook_pipeline"

    def define_destination(self, records, runtime_params):
        return None

    def define_transformations(self, records, runtime_params):
        return []


class TestDefineJobsHook:
    def test_resolve_uses_define_jobs_plan(self):
        pipeline = _DefineJobsPipeline()

        pipeline.resolve({"job_size": 1})

        assert len(list(plan_slices(pipeline.source, {}))) == 3

    def test_job_size_controls_job_count(self):
        pipeline = _DefineJobsPipeline()

        pipeline.resolve({"job_size": 2})

        assert len(list(plan_slices(pipeline.source, {}))) == 2

    def test_define_jobs_defaults_to_define_source(self):
        pipeline = _DefineSourcePipeline()

        pipeline.resolve({})

        assert isinstance(pipeline.source, MockSource)

    def test_neither_hook_implemented_raises(self):
        with pytest.raises(NotImplementedError, match="define_source\\(\\) or define_jobs\\(\\)"):
            _NoHookPipeline().resolve({})
