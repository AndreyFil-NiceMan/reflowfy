"""LocalExecutor must run the same plan the manager dispatches.

Its per-ID branch resolves define_source once per ID, which silently ignores a
pipeline's own define_jobs — so the same pipeline produced different work
depending on which runner picked it up.
"""

from reflowfy import job
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.execution.base import ExecutionState
from reflowfy.execution.local_executor import LocalExecutor

seen: list[tuple[list, object]] = []


class _RecordingDestination:
    def send_with_retry(self, records, metadata):
        seen.append((records, metadata.get("current_id")))

    def health_check(self):
        return True


class _FanOutIdPipeline(IdBasedPipeline):
    """define_jobs owns the splitting: one ID -> two jobs, and no define_source."""

    name = "local_exec_fanout_pipeline"

    def define_jobs(self, runtime_params):
        for parent_id in runtime_params["ids"]:
            for child in range(2):
                yield job([{"child_id": f"{parent_id}-{child}"}], current_id=parent_id)

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return _RecordingDestination()


class _BuiltinIdPipeline(IdBasedPipeline):
    """No define_jobs override — must keep using the per-ID branch."""

    name = "local_exec_builtin_pipeline"

    def define_source(self, runtime_params):
        return [{"id": i} for i in runtime_params["current_ids"]]

    def define_transformations(self, records, runtime_params):
        return []

    def define_destination(self, records, runtime_params):
        return _RecordingDestination()


def test_define_jobs_plan_is_used_instead_of_per_id_define_source():
    seen.clear()

    status = LocalExecutor().execute(_FanOutIdPipeline(), {"ids": [101, 102]})

    assert status.state is ExecutionState.COMPLETED
    # 2 IDs x 2 children = 4 jobs. The per-ID branch would have produced 2.
    assert [records[0]["child_id"] for records, _ in seen] == [
        "101-0",
        "101-1",
        "102-0",
        "102-1",
    ]


def test_each_job_sees_its_own_params():
    seen.clear()

    LocalExecutor().execute(_FanOutIdPipeline(), {"ids": [101, 102]})

    assert [current_id for _, current_id in seen] == [101, 101, 102, 102]


def test_pipelines_without_an_override_still_take_the_per_id_path():
    seen.clear()

    status = LocalExecutor().execute(_BuiltinIdPipeline(), {"ids": [7, 8]})

    assert status.state is ExecutionState.COMPLETED
    assert status.metadata["ids_processed"] == 2
    assert [records[0]["id"] for records, _ in seen] == [7, 8]
