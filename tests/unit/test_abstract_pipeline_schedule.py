"""Unit tests for AbstractPipeline schedule attribute."""

from unittest.mock import MagicMock

import pytest

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.registry import pipeline_registry


class _ScheduledPipeline(AbstractPipeline):
    name = "test_scheduled_pipeline_unit"
    schedule = "*/5 * * * *"

    def define_source(self, runtime_params):
        return MagicMock()

    def define_destination(self, records, runtime_params):
        return MagicMock()

    def define_transformations(self, records, runtime_params):
        return []


class _UnscheduledPipeline(AbstractPipeline):
    name = "test_unscheduled_pipeline_unit"

    def define_source(self, runtime_params):
        return MagicMock()

    def define_destination(self, records, runtime_params):
        return MagicMock()

    def define_transformations(self, records, runtime_params):
        return []


def test_schedule_none_by_default():
    p = pipeline_registry.get("test_unscheduled_pipeline_unit")
    assert p.schedule is None
    assert p.is_scheduled is False


def test_is_scheduled_true_when_schedule_set():
    p = pipeline_registry.get("test_scheduled_pipeline_unit")
    assert p.schedule == "*/5 * * * *"
    assert p.is_scheduled is True


def test_invalid_cron_raises_at_class_definition():
    # The metaclass validates the cron expression at class-definition time,
    # so the ValueError fires before instantiation ever happens.
    with pytest.raises(ValueError, match="invalid cron"):

        class _BadCronPipeline(AbstractPipeline):
            name = "bad_cron_pipeline_unit"
            schedule = "not-a-valid-cron"

            def define_source(self, runtime_params):
                return MagicMock()

            def define_destination(self, records, runtime_params):
                return MagicMock()

            def define_transformations(self, records, runtime_params):
                return []


def test_schedule_in_to_dict():
    p = pipeline_registry.get("test_scheduled_pipeline_unit")
    d = p.to_dict()
    assert "schedule" in d
    assert "is_scheduled" in d
    assert d["schedule"] == "*/5 * * * *"
    assert d["is_scheduled"] is True


def test_schedule_none_in_to_dict_for_unscheduled():
    p = pipeline_registry.get("test_unscheduled_pipeline_unit")
    d = p.to_dict()
    assert d["schedule"] is None
    assert d["is_scheduled"] is False
