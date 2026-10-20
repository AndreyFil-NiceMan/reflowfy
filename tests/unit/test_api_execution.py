"""ExecutionTracker: the API's in-memory view of executions it started."""

import threading

import pytest

from reflowfy.api.execution import ExecutionTracker
from reflowfy.execution.base import ExecutionState, ExecutionStatus


def _status(execution_id: str = "e1", total_jobs: int = 2) -> ExecutionStatus:
    return ExecutionStatus(
        execution_id=execution_id,
        pipeline_name="p",
        state=ExecutionState.RUNNING,
        total_jobs=total_jobs,
    )


@pytest.fixture
def tracker() -> ExecutionTracker:
    return ExecutionTracker()


class TestTrackAndLookup:
    def test_get_status_unknown_is_none(self, tracker):
        assert tracker.get_status("missing") is None

    def test_track_then_get_returns_same_object(self, tracker):
        status = _status()
        tracker.track(status)
        assert tracker.get_status("e1") is status

    def test_track_replaces_previous_status_for_same_id(self, tracker):
        tracker.track(_status(total_jobs=1))
        newer = _status(total_jobs=9)
        tracker.track(newer)
        assert tracker.get_status("e1") is newer
        assert len(tracker.list_all()) == 1

    def test_list_all_and_clear(self, tracker):
        tracker.track(_status("a"))
        tracker.track(_status("b"))
        assert sorted(s.execution_id for s in tracker.list_all()) == ["a", "b"]
        tracker.clear()
        assert tracker.list_all() == []
        assert tracker.get_status("a") is None


class TestUpdateJobCompletion:
    def test_partial_progress_stays_running(self, tracker):
        tracker.track(_status(total_jobs=3))
        tracker.update_job_completion("e1", success=True)
        status = tracker.get_status("e1")
        assert status.completed_jobs == 1
        assert status.state == ExecutionState.RUNNING

    def test_all_succeeded_is_completed(self, tracker):
        tracker.track(_status(total_jobs=2))
        tracker.update_job_completion("e1", success=True)
        tracker.update_job_completion("e1", success=True)
        status = tracker.get_status("e1")
        assert status.state == ExecutionState.COMPLETED
        assert status.failed_jobs == 0
        assert status.error_message is None

    def test_all_failed_is_failed_and_keeps_last_error(self, tracker):
        tracker.track(_status(total_jobs=2))
        tracker.update_job_completion("e1", success=False, error_message="first")
        tracker.update_job_completion("e1", success=False, error_message="second")
        status = tracker.get_status("e1")
        assert status.state == ExecutionState.FAILED
        assert status.failed_jobs == 2
        assert status.error_message == "second"

    def test_mixed_outcome_is_partially_failed(self, tracker):
        tracker.track(_status(total_jobs=2))
        tracker.update_job_completion("e1", success=True)
        tracker.update_job_completion("e1", success=False, error_message="boom")
        status = tracker.get_status("e1")
        assert status.state == ExecutionState.PARTIALLY_FAILED
        assert (status.completed_jobs, status.failed_jobs) == (1, 1)
        assert status.error_message == "boom"

    def test_failure_without_message_keeps_previous_error(self, tracker):
        tracker.track(_status(total_jobs=3))
        tracker.update_job_completion("e1", success=False, error_message="kept")
        tracker.update_job_completion("e1", success=False)
        assert tracker.get_status("e1").error_message == "kept"

    @pytest.mark.parametrize(
        "success, expected",
        [(True, ExecutionState.COMPLETED), (False, ExecutionState.FAILED)],
    )
    def test_unknown_execution_is_created_as_single_job(self, tracker, success, expected):
        tracker.update_job_completion("new", success=success, error_message="err")
        status = tracker.get_status("new")
        assert status is not None
        assert status.pipeline_name == "unknown"
        assert status.total_jobs == 1
        assert status.state == expected
        assert status.error_message == (None if success else "err")

    def test_concurrent_updates_are_not_lost(self, tracker):
        tracker.track(_status(total_jobs=400))

        def worker(ok: bool) -> None:
            for _ in range(100):
                tracker.update_job_completion("e1", success=ok)

        threads = [threading.Thread(target=worker, args=(i % 2 == 0,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        status = tracker.get_status("e1")
        assert status.completed_jobs == 200
        assert status.failed_jobs == 200
        assert status.state == ExecutionState.PARTIALLY_FAILED
