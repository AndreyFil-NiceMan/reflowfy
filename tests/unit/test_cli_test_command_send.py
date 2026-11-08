"""Regression test: `reflowfy test` must send per job, not merge and send once.

A pipeline with docs_per_job=1 (here: MockSource(batch_size=1)) splits into
one job per record. The destination must receive one send per job — the
same as production (PipelineRunner -> LocalDispatcher -> WorkerExecutor) —
not one send for the whole batch merged together.
"""

from unittest.mock import AsyncMock

from reflowfy.cli.commands.test import TestOptions, run_single
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.sources.mock import MockSource


class _SplittingPipeline(AbstractPipeline):
    """3 records, one job each -- destination.send_with_retry must fire 3x."""

    name = "test_cli_send_per_job_pipeline"

    def __init__(self, destination: AsyncMock):
        super().__init__()
        self._destination = destination

    def define_source(self, runtime_params):
        return MockSource(
            data=[{"id": 1}, {"id": 2}, {"id": 3}],
            batch_size=1,
        )

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return self._destination


def _make_destination() -> AsyncMock:
    destination = AsyncMock()
    destination.health_check.return_value = True
    return destination


def test_run_single_sends_once_per_job_not_once_for_the_batch():
    destination = _make_destination()
    pipeline = _SplittingPipeline(destination)
    opts = TestOptions(limit=100, dry_run=False)

    report = run_single(pipeline, {}, opts)

    assert report.ok
    assert report.fetched == 3
    assert report.sent_count == 3
    assert destination.send_with_retry.call_count == 3
    sent_record_batches = [call.args[0] for call in destination.send_with_retry.call_args_list]
    assert all(len(batch) == 1 for batch in sent_record_batches)


def test_run_single_dry_run_sends_nothing():
    destination = _make_destination()
    pipeline = _SplittingPipeline(destination)
    opts = TestOptions(limit=100, dry_run=True)

    report = run_single(pipeline, {}, opts)

    assert report.ok
    assert report.fetched == 3
    assert report.sent_count == 0
    destination.send_with_retry.assert_not_called()


def test_each_jobs_destination_is_closed_after_its_send():
    destination = _make_destination()
    report = run_single(_SplittingPipeline(destination), {}, TestOptions(limit=100, dry_run=False))
    assert report.ok
    assert destination.close.await_count == 3


class _NoClose:
    """A duck-typed destination with no close(): still sent to, nothing to close."""

    def __init__(self) -> None:
        self.sent = 0

    async def health_check(self) -> bool:
        return True

    async def send_with_retry(self, records, metadata):
        self.sent += 1


class _SyncClose(_NoClose):
    closed = 0

    def close(self) -> None:
        _SyncClose.closed += 1


def test_a_destination_without_close_is_still_sent():
    destination = _NoClose()
    report = run_single(_SplittingPipeline(destination), {}, TestOptions(limit=100, dry_run=False))
    assert report.ok and destination.sent == 3


def test_a_synchronous_close_is_called():
    destination = _SyncClose()
    report = run_single(_SplittingPipeline(destination), {}, TestOptions(limit=100, dry_run=False))
    assert report.ok and _SyncClose.closed == 3
