"""
Scheduled Test Pipeline.

Pipeline that fires on a cron schedule — used for E2E schedule tests.
A very frequent cron (every minute) lets tests observe auto-triggering
without waiting long.

These write to the **console**, never to the shared mock HTTP server, and that
is load-bearing: the scheduler fires them on its own clock, independently of
whichever test happens to be running. Pointing them at `e2e_http` puts
untransformed records into the record store that other suites assert over —
those tests reset the store, run their pipeline, then assert every record
carries their marker, so a scheduled fire landing inside that window fails them
(`test_error_tolerant_pipeline_completes` did, once per full-suite run). The
schedule tests only read `/schedules` and execution records, so they never
needed the HTTP sink.
"""

import uuid
from typing import Literal

from typing_extensions import NotRequired

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Records,
    RuntimeParams,
    ScheduledRun,
    Transformations,
)
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_console


class MultiScheduleParams(RuntimeParams, total=False):
    """The one key a named schedule's ``params`` can set: how many records
    to fetch. Different per named schedule, so the fired execution's
    total_jobs count proves which schedule's params actually reached the run.
    """

    speed: NotRequired[Literal["fast", "full"]]

# Unique per service startup so stale hashes from previous runs never block run 1,
# but stable within a single service lifetime so run 2 sees run 1's hashes.
_SERVICE_RUN_ID = str(uuid.uuid4())

_NO_DUP_FIXED_DATA = [
    {"id": i, "name": f"record-{i}", "value": i * 10, "_run": _SERVICE_RUN_ID} for i in range(1, 6)
]


class E2EScheduledTestPipeline(AbstractPipeline[RuntimeParams]):
    """E2E scheduled pipeline — fires every minute."""

    name = "e2e_scheduled_test"
    schedules = [ScheduledRun(name="default", cron="* * * * *")]

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=5)

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []


class E2EScheduledSlowPipeline(AbstractPipeline[RuntimeParams]):
    """E2E scheduled pipeline with a less frequent schedule."""

    name = "e2e_scheduled_slow_test"
    schedules = [ScheduledRun(name="default", cron="0 * * * *")]

    def define_source(self, runtime_params):
        return e2e_mock(count=5, batch_size=5)

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []


class E2EScheduledNoDuplicatesPipeline(AbstractPipeline[RuntimeParams]):
    """E2E scheduled pipeline with duplicate jobs disabled."""

    name = "e2e_scheduled_no_duplicates_test"
    # once a year — never auto-fires during tests
    schedules = [ScheduledRun(name="default", cron="0 0 1 1 *")]
    enable_duplicate_jobs = False

    def define_source(self, runtime_params):
        return e2e_mock(data=_NO_DUP_FIXED_DATA, batch_size=5)

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        return []


class E2EMultiSchedulePipeline(AbstractPipeline[MultiScheduleParams]):
    """E2E pipeline with two named schedules, each with its own params.

    Verifies /schedules registers both named schedules with their own cron
    and params. Never auto-fires during a normal test run (both crons are
    "once a year", on different days) — firing + param-threading is covered
    separately by E2EMultiScheduleFrequentPipeline below.
    """

    name = "e2e_multi_schedule_test"
    schedules = [
        ScheduledRun(name="morning", cron="0 9 1 1 *", params={"speed": "fast"}),
        ScheduledRun(name="evening", cron="0 17 2 1 *", params={"speed": "full"}),
    ]

    def define_source(self, runtime_params: MultiScheduleParams):
        count = 2 if runtime_params.get("speed") == "fast" else 5
        return e2e_mock(count=count, batch_size=1)

    def define_destination(
        self, records: Records, runtime_params: MultiScheduleParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: Records, runtime_params: MultiScheduleParams
    ) -> Transformations:
        return []


class E2EMultiScheduleFrequentPipeline(AbstractPipeline[MultiScheduleParams]):
    """E2E pipeline with two named schedules on the same every-minute cron.

    Both fire on their own clock, but with different params — "fast" fetches
    2 records (2 jobs, batch_size=1) and "full" fetches 5 (5 jobs). Each
    PipelineSchedule row tracks its own `last_execution_id`, so a test can
    read the schedule-specific execution and assert its `total_jobs` matches
    that schedule's params — proving the stored runtime_params actually
    reached the fired run, not just the /schedules listing.
    """

    name = "e2e_multi_schedule_frequent_test"
    schedules = [
        ScheduledRun(name="fast", cron="* * * * *", params={"speed": "fast"}),
        ScheduledRun(name="full", cron="* * * * *", params={"speed": "full"}),
    ]

    def define_source(self, runtime_params: MultiScheduleParams):
        count = 2 if runtime_params.get("speed") == "fast" else 5
        return e2e_mock(count=count, batch_size=1)

    def define_destination(
        self, records: Records, runtime_params: MultiScheduleParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: Records, runtime_params: MultiScheduleParams
    ) -> Transformations:
        return []
