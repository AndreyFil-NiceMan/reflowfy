"""Unit tests for PipelineScheduler."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from reflowfy.core.abstract_pipeline import ScheduledRun
from reflowfy.reflow_manager.pipeline_scheduler import PipelineScheduler
from reflowfy.store.models import PipelineSchedule


# ---------------------------------------------------------------------------
# _compute_next_run
# ---------------------------------------------------------------------------

def test_compute_next_run_every_5_minutes():
    after = datetime(2026, 4, 19, 12, 0, 0)
    result = PipelineScheduler._compute_next_run("*/5 * * * *", after)
    assert result == datetime(2026, 4, 19, 12, 5, 0)


def test_compute_next_run_daily_midnight():
    after = datetime(2026, 4, 19, 12, 0, 0)
    result = PipelineScheduler._compute_next_run("0 0 * * *", after)
    assert result == datetime(2026, 4, 20, 0, 0, 0)


def test_compute_next_run_returns_datetime():
    result = PipelineScheduler._compute_next_run("*/1 * * * *", datetime.now(timezone.utc).replace(tzinfo=None))
    assert isinstance(result, datetime)


# ---------------------------------------------------------------------------
# sync_schedules_from_registry
# ---------------------------------------------------------------------------

def _make_pipeline_mock(name: str, schedules: list[ScheduledRun]) -> MagicMock:
    p = MagicMock()
    p.name = name
    p.schedules = schedules
    return p


def test_sync_inserts_new_schedule():
    scheduler = PipelineScheduler()
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None  # no existing row
    db.query.return_value.filter.return_value.all.return_value = []      # no enabled rows to disable

    pipeline = _make_pipeline_mock(
        "my_pipeline", [ScheduledRun(name="default", cron="*/5 * * * *", params={"env": "prod"})]
    )

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = [pipeline]
        scheduler.sync_schedules_from_registry(db)

    db.add.assert_called_once()
    added: PipelineSchedule = db.add.call_args[0][0]
    assert added.pipeline_name == "my_pipeline"
    assert added.schedule_name == "default"
    assert added.cron_expression == "*/5 * * * *"
    assert added.runtime_params == {"env": "prod"}
    assert added.enabled == "true"
    assert added.next_run_at is not None


def test_sync_inserts_one_row_per_named_schedule():
    scheduler = PipelineScheduler()
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    db.query.return_value.filter.return_value.all.return_value = []

    pipeline = _make_pipeline_mock(
        "my_pipeline",
        [
            ScheduledRun(name="morning", cron="0 9 * * *", params={"mode": "fast"}),
            ScheduledRun(name="evening", cron="0 17 * * *", params={"mode": "full"}),
        ],
    )

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = [pipeline]
        scheduler.sync_schedules_from_registry(db)

    assert db.add.call_count == 2
    added_rows = [call.args[0] for call in db.add.call_args_list]
    by_name = {row.schedule_name: row for row in added_rows}
    assert by_name["morning"].cron_expression == "0 9 * * *"
    assert by_name["morning"].runtime_params == {"mode": "fast"}
    assert by_name["evening"].cron_expression == "0 17 * * *"
    assert by_name["evening"].runtime_params == {"mode": "full"}


def test_sync_preserves_existing_next_run():
    scheduler = PipelineScheduler()
    db = MagicMock()

    future = datetime(2099, 1, 1, 0, 0, 0)
    existing = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="default",
        cron_expression="*/5 * * * *",
        runtime_params={},
        next_run_at=future,
        enabled="true",
    )
    db.query.return_value.filter.return_value.first.return_value = existing
    db.query.return_value.filter.return_value.all.return_value = [existing]

    pipeline = _make_pipeline_mock(
        "my_pipeline", [ScheduledRun(name="default", cron="*/5 * * * *")]
    )

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = [pipeline]
        scheduler.sync_schedules_from_registry(db)

    # next_run_at must not change when cron expression is the same
    assert existing.next_run_at == future
    db.add.assert_not_called()


def test_sync_updates_changed_cron_expression():
    scheduler = PipelineScheduler()
    db = MagicMock()

    existing = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="default",
        cron_expression="0 * * * *",  # old: hourly
        runtime_params={},
        next_run_at=datetime(2099, 1, 1),
        enabled="true",
    )
    db.query.return_value.filter.return_value.first.return_value = existing
    db.query.return_value.filter.return_value.all.return_value = [existing]

    pipeline = _make_pipeline_mock(
        "my_pipeline", [ScheduledRun(name="default", cron="*/5 * * * *")]  # new: every 5 min
    )

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = [pipeline]
        scheduler.sync_schedules_from_registry(db)

    assert existing.cron_expression == "*/5 * * * *"
    # next_run_at recalculated — should not be the old far-future value
    assert existing.next_run_at != datetime(2099, 1, 1)


def test_sync_updates_changed_params_without_touching_next_run():
    scheduler = PipelineScheduler()
    db = MagicMock()

    future = datetime(2099, 1, 1, 0, 0, 0)
    existing = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="default",
        cron_expression="*/5 * * * *",
        runtime_params={"mode": "fast"},
        next_run_at=future,
        enabled="true",
    )
    db.query.return_value.filter.return_value.first.return_value = existing
    db.query.return_value.filter.return_value.all.return_value = [existing]

    pipeline = _make_pipeline_mock(
        "my_pipeline",
        [ScheduledRun(name="default", cron="*/5 * * * *", params={"mode": "full"})],
    )

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = [pipeline]
        scheduler.sync_schedules_from_registry(db)

    assert existing.runtime_params == {"mode": "full"}
    # cron unchanged → next_run_at preserved
    assert existing.next_run_at == future


def test_sync_disables_unscheduled_pipeline():
    scheduler = PipelineScheduler()
    db = MagicMock()

    orphan_row = PipelineSchedule(
        pipeline_name="old_pipeline",
        schedule_name="default",
        cron_expression="*/5 * * * *",
        runtime_params={},
        next_run_at=datetime(2099, 1, 1),
        enabled="true",
    )
    # No matching pipeline in registry
    db.query.return_value.filter.return_value.all.return_value = [orphan_row]

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = []  # no scheduled pipelines
        scheduler.sync_schedules_from_registry(db)

    assert orphan_row.enabled == "false"


def test_sync_disables_removed_named_schedule_but_keeps_the_other():
    scheduler = PipelineScheduler()
    db = MagicMock()

    morning_row = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="morning",
        cron_expression="0 9 * * *",
        runtime_params={},
        next_run_at=datetime(2099, 1, 1),
        enabled="true",
    )
    evening_row = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="evening",
        cron_expression="0 17 * * *",
        runtime_params={},
        next_run_at=datetime(2099, 1, 1),
        enabled="true",
    )
    db.query.return_value.filter.return_value.first.return_value = morning_row
    db.query.return_value.filter.return_value.all.return_value = [morning_row, evening_row]

    # Pipeline now only declares "morning" — "evening" was removed from code
    pipeline = _make_pipeline_mock(
        "my_pipeline", [ScheduledRun(name="morning", cron="0 9 * * *")]
    )

    with patch("reflowfy.core.registry.pipeline_registry") as reg:
        reg.list_all.return_value = [pipeline]
        scheduler.sync_schedules_from_registry(db)

    assert morning_row.enabled == "true"
    assert evening_row.enabled == "false"


# ---------------------------------------------------------------------------
# reset_schedule
# ---------------------------------------------------------------------------

def test_reset_schedule_recalculates_next_run():
    scheduler = PipelineScheduler()
    db = MagicMock()

    triggered_at = datetime(2026, 4, 19, 12, 0, 0)
    row = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="default",
        cron_expression="*/5 * * * *",
        runtime_params={},
        next_run_at=datetime(2026, 4, 19, 11, 55, 0),
        enabled="true",
    )
    db.query.return_value.filter.return_value.all.return_value = [row]

    scheduler.reset_schedule(db, "my_pipeline", triggered_at)

    assert row.last_triggered_at == triggered_at
    assert row.next_run_at == datetime(2026, 4, 19, 12, 5, 0)


def test_reset_schedule_resets_every_named_schedule_of_the_pipeline():
    scheduler = PipelineScheduler()
    db = MagicMock()

    triggered_at = datetime(2026, 4, 19, 12, 0, 0)
    morning_row = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="morning",
        cron_expression="0 9 * * *",
        runtime_params={},
        next_run_at=datetime(2026, 4, 19, 11, 55, 0),
        enabled="true",
    )
    evening_row = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="evening",
        cron_expression="0 17 * * *",
        runtime_params={},
        next_run_at=datetime(2026, 4, 19, 11, 55, 0),
        enabled="true",
    )
    db.query.return_value.filter.return_value.all.return_value = [morning_row, evening_row]

    scheduler.reset_schedule(db, "my_pipeline", triggered_at)

    assert morning_row.last_triggered_at == triggered_at
    assert evening_row.last_triggered_at == triggered_at


def test_reset_schedule_noop_when_no_row():
    scheduler = PipelineScheduler()
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = []

    # Should not raise
    scheduler.reset_schedule(db, "nonexistent", datetime.now(timezone.utc).replace(tzinfo=None))


def test_reset_schedule_noop_when_disabled():
    scheduler = PipelineScheduler()
    db = MagicMock()

    triggered_at = datetime(2026, 4, 19, 12, 0, 0)
    original_next = datetime(2026, 4, 19, 12, 30, 0)
    row = PipelineSchedule(
        pipeline_name="my_pipeline",
        schedule_name="default",
        cron_expression="*/5 * * * *",
        runtime_params={},
        next_run_at=original_next,
        enabled="false",
    )
    # Disabled row is excluded by the enabled == "true" filter, so the mocked
    # query returns no rows for it.
    db.query.return_value.filter.return_value.all.return_value = []

    scheduler.reset_schedule(db, "my_pipeline", triggered_at)

    # Disabled row should not be modified
    assert row.next_run_at == original_next
    assert row.last_triggered_at is None
