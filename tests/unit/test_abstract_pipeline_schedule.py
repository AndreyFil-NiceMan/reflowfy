"""Unit tests for AbstractPipeline schedules attribute."""

from unittest.mock import MagicMock

import pytest

from reflowfy.core.abstract_pipeline import AbstractPipeline, ScheduledRun
from reflowfy.core.registry import pipeline_registry


class _ScheduledPipeline(AbstractPipeline):
    name = "test_scheduled_pipeline_unit"
    schedules = [ScheduledRun(name="default", cron="*/5 * * * *")]

    def define_source(self, runtime_params):
        return MagicMock()

    def define_destination(self, records, runtime_params):
        return MagicMock()

    def define_transformations(self, records, runtime_params):
        return []


class _MultiScheduledPipeline(AbstractPipeline):
    name = "test_multi_scheduled_pipeline_unit"
    schedules = [
        ScheduledRun(name="morning", cron="0 9 * * *", params={"mode": "fast"}),
        ScheduledRun(name="evening", cron="0 17 * * *", params={"mode": "full"}),
    ]

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


def test_schedules_empty_by_default():
    p = pipeline_registry.get("test_unscheduled_pipeline_unit")
    assert p.schedules == []
    assert p.is_scheduled is False


def test_is_scheduled_true_when_schedules_set():
    p = pipeline_registry.get("test_scheduled_pipeline_unit")
    assert p.schedules == [ScheduledRun(name="default", cron="*/5 * * * *")]
    assert p.is_scheduled is True


def test_multiple_named_schedules_with_distinct_params():
    p = pipeline_registry.get("test_multi_scheduled_pipeline_unit")
    assert p.is_scheduled is True
    by_name = {r.name: r for r in p.schedules}
    assert by_name["morning"].cron == "0 9 * * *"
    assert by_name["morning"].params == {"mode": "fast"}
    assert by_name["evening"].cron == "0 17 * * *"
    assert by_name["evening"].params == {"mode": "full"}


def test_invalid_cron_raises_at_class_definition():
    # The metaclass validates each schedule's cron at class-definition time,
    # so the ValueError fires before instantiation ever happens.
    with pytest.raises(ValueError, match="invalid cron"):

        class _BadCronPipeline(AbstractPipeline):
            name = "bad_cron_pipeline_unit"
            schedules = [ScheduledRun(name="default", cron="not-a-valid-cron")]

            def define_source(self, runtime_params):
                return MagicMock()

            def define_destination(self, records, runtime_params):
                return MagicMock()

            def define_transformations(self, records, runtime_params):
                return []


def test_duplicate_schedule_name_raises_at_class_definition():
    with pytest.raises(ValueError, match="duplicate schedule name"):

        class _DuplicateNamePipeline(AbstractPipeline):
            name = "duplicate_schedule_name_pipeline_unit"
            schedules = [
                ScheduledRun(name="daily", cron="0 0 * * *"),
                ScheduledRun(name="daily", cron="0 12 * * *"),
            ]

            def define_source(self, runtime_params):
                return MagicMock()

            def define_destination(self, records, runtime_params):
                return MagicMock()

            def define_transformations(self, records, runtime_params):
                return []


def test_schedules_in_to_dict():
    p = pipeline_registry.get("test_scheduled_pipeline_unit")
    d = p.to_dict()
    assert "schedules" in d
    assert "is_scheduled" in d
    assert d["schedules"] == [{"name": "default", "cron": "*/5 * * * *", "params": {}}]
    assert d["is_scheduled"] is True


def test_schedules_empty_in_to_dict_for_unscheduled():
    p = pipeline_registry.get("test_unscheduled_pipeline_unit")
    d = p.to_dict()
    assert d["schedules"] == []
    assert d["is_scheduled"] is False


@pytest.mark.parametrize(
    "legacy",
    ["0 * * * *", ScheduledRun(name="default", cron="0 * * * *"), ["0 * * * *"]],
)
def test_singular_schedule_attribute_raises_at_class_definition(legacy):
    """`schedule = ...` used to be silently ignored: the pipeline ran, never scheduled."""
    with pytest.raises(TypeError, match=r"sets `schedule`, which is not read.*schedules = "):

        class _LegacyScheduled(AbstractPipeline):
            name = "test_legacy_schedule_attr_unit"
            schedule = legacy

            def define_source(self, runtime_params):
                return MagicMock()

            def define_destination(self, records, runtime_params):
                return MagicMock()

            def define_transformations(self, records, runtime_params):
                return []

    assert pipeline_registry.get("test_legacy_schedule_attr_unit") is None


def test_schedule_method_is_not_mistaken_for_the_old_attribute():
    class _HasScheduleMethod(AbstractPipeline):
        name = "test_schedule_method_unit"

        def schedule(self):
            return "unrelated helper"

        def define_source(self, runtime_params):
            return MagicMock()

        def define_destination(self, records, runtime_params):
            return MagicMock()

        def define_transformations(self, records, runtime_params):
            return []

    assert pipeline_registry.get("test_schedule_method_unit") is not None
