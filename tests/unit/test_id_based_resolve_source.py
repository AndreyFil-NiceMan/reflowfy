"""Unit tests for IdBasedPipeline.resolve_source coercion."""

import pytest

from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.sources.base import BaseSource
from reflowfy.sources.mock import MockSource
from reflowfy.sources.static import StaticSource


class _ListSourcePipeline(IdBasedPipeline):
    """Returns the current batch's ids directly as records (no fetch)."""

    name = "list_source_test"

    def define_source(self, params):
        return params["current_ids"]

    def define_transformations(self, records, params):
        return []

    def define_destination(self, records, params):
        return MockSource([])


class _RealSourcePipeline(IdBasedPipeline):
    """Returns a real BaseSource (existing behaviour)."""

    name = "real_source_test"

    def define_source(self, params):
        return MockSource([{"id": params["current_id"]}])

    def define_transformations(self, records, params):
        return []

    def define_destination(self, records, params):
        return MockSource([])


class _BadSourcePipeline(IdBasedPipeline):
    name = "bad_source_test"

    def define_source(self, params):
        return 42  # not a BaseSource, not a list

    def define_transformations(self, records, params):
        return []

    def define_destination(self, records, params):
        return MockSource([])


class TestResolveSource:
    def test_list_return_is_wrapped_in_static_source(self):
        pipeline = _ListSourcePipeline()
        batch_params = {"current_ids": [101, 102], "current_id": 101}
        source = pipeline.resolve_source(batch_params)
        assert isinstance(source, StaticSource)

    def test_list_records_are_raw_ids(self):
        pipeline = _ListSourcePipeline()
        batch_params = {"current_ids": [101, 102], "current_id": 101}
        source = pipeline.resolve_source(batch_params)
        jobs = list(source.split_jobs(batch_params))
        assert jobs[0].records == [101, 102]

    def test_base_source_is_passed_through(self):
        pipeline = _RealSourcePipeline()
        batch_params = {"current_ids": [7], "current_id": 7}
        source = pipeline.resolve_source(batch_params)
        assert isinstance(source, MockSource)
        assert isinstance(source, BaseSource)

    def test_invalid_return_raises_type_error(self):
        pipeline = _BadSourcePipeline()
        batch_params = {"current_ids": [1], "current_id": 1}
        with pytest.raises(TypeError):
            pipeline.resolve_source(batch_params)


class TestResolveForIdsUsesCoercion:
    def test_resolve_for_ids_returns_static_source_for_list(self):
        pipeline = _ListSourcePipeline()
        resolved = pipeline.resolve_for_ids({}, [101, 102])
        assert isinstance(resolved["source"], StaticSource)
        jobs = list(resolved["source"].split_jobs(resolved["batch_params"]))
        assert jobs[0].records == [101, 102]
