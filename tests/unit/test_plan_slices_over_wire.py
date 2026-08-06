"""`reflowfy test` must fail on jobs that cannot reach a worker.

The manager serializes each planned slice, JSON-encodes it onto Kafka, and the
worker rebuilds it with ``SourceFactory.create``. ``plan_slices_over_wire``
performs that same hop locally, so a source that cannot survive the trip fails
during `reflowfy test` instead of after deploy.
"""

from datetime import datetime

import pytest

from reflowfy.execution.job_runner import (
    JobNotSerializableError,
    chunk,
    plan_slices,
    plan_slices_over_wire,
)
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.base import BaseSource
from reflowfy.sources.static import StaticSource


class _BadConfigSource(BaseSource):
    """config is NOT this class's constructor kwargs, so cls(**config) fails."""

    def __init__(self, rows):
        super().__init__(config={"not_a_constructor_kwarg": rows})

    def fetch(self, runtime_params, limit=None):
        return []

    def split_jobs(self, runtime_params, batch_size=1000):
        return iter(())

    def health_check(self):
        return True


class _UnregisteredSource(BaseSource):
    def __init__(self, rows=None):
        super().__init__(config={"rows": rows or []})

    def fetch(self, runtime_params, limit=None):
        return []

    def split_jobs(self, runtime_params, batch_size=1000):
        return iter(())

    def health_check(self):
        return True


def test_wellformed_plan_round_trips_with_payload_size():
    plan = chunk([{"id": 1}, {"id": 2}, {"id": 3}], size=2)

    jobs = list(plan_slices_over_wire(plan, {}))

    assert len(jobs) == 2
    first, first_bytes = jobs[0]
    assert isinstance(first, StaticSource)
    # A *reconstructed* source, not the original object handed back.
    assert first.fetch({}) == [{"id": 1}, {"id": 2}]
    assert first_bytes > 0
    assert jobs[1][0].fetch({}) == [{"id": 3}]


def test_non_json_records_fail_here_not_at_dispatch():
    """A datetime in a define_jobs plan kills json.dumps in the manager."""
    plan = chunk([{"seen_at": datetime(2026, 8, 6, 12, 0, 0)}], size=1)

    # plan_slices alone is perfectly happy — which is exactly why `reflowfy test`
    # used to pass and the deploy then failed.
    assert len(list(plan_slices(plan, {}))) == 1

    with pytest.raises(JobNotSerializableError) as exc:
        list(plan_slices_over_wire(plan, {}))

    message = str(exc.value)
    assert "not JSON-serializable" in message
    assert "define_jobs" in message


def test_config_that_is_not_constructor_kwargs_is_caught():
    SourceFactory.register("_BadConfigSource", _BadConfigSource)
    try:
        with pytest.raises(JobNotSerializableError) as exc:
            list(plan_slices_over_wire([_BadConfigSource([{"id": 1}])], {}))
    finally:
        SourceFactory._registry.pop("_BadConfigSource", None)

    message = str(exc.value)
    assert "constructor kwargs" in message
    assert "not_a_constructor_kwarg" in message


def test_unregistered_source_type_is_caught():
    with pytest.raises(ValueError, match="Unknown source type"):
        list(plan_slices_over_wire([_UnregisteredSource()], {}))
