"""What was removed after 1.1.6 stays removed.

- ``callback_url`` (#119): gone from ``/run``, the API client, ``run_pipeline`` and the
  executions table.
- The old decorator spellings (#123): see ``test_decorator_api.TestRemovedSpellings``.
- Backward-compatibility aliases and re-exports (#123): ``JobDispatcher``,
  ``Checkpoint``, ``execution.job_runner.chunk``, and the errors' old homes no longer
  export them (``__all__``); their home is ``reflowfy.core``.
- The ``MAX_JOBS_PER_MINUTE`` startup warning and the ``kafka_destination(concurrency=)``
  guard (#123): see ``test_lifecycle_hooks`` and the last test here.
"""

import inspect

import pytest

import reflowfy
import reflowfy.reflow_manager as reflow_manager
from reflowfy.core import exceptions, planning, record_steps
from reflowfy.destinations import base as destinations_base
from reflowfy.execution import job_runner
from reflowfy.execution.distributed_executor import DistributedExecutor
from reflowfy.reflow_manager import dispatcher, lifecycle
from reflowfy.reflow_manager.schemas import RunPipelineRequest
from reflowfy.store import models


class TestCallbackUrlIsGone:
    def test_run_request_has_no_callback_url(self) -> None:
        assert "callback_url" not in RunPipelineRequest.model_fields
        # Sent anyway, it is not part of the request the manager acts on.
        request = RunPipelineRequest(pipeline_name="p", callback_url="https://x")  # type: ignore[call-arg]
        assert "callback_url" not in request.model_dump()

    @pytest.mark.parametrize(
        "function",
        [DistributedExecutor.execute, lifecycle.run_pipeline],
        ids=["executor", "run_pipeline"],
    )
    def test_no_entry_point_takes_it(self, function) -> None:
        assert "callback_url" not in inspect.signature(function).parameters

    def test_executions_have_no_callback_column(self) -> None:
        assert "callback_url" not in models.Execution.__table__.columns

    def test_the_callback_poster_is_gone(self) -> None:
        for name in ("_post_callback", "callback_timeout_seconds"):
            assert not hasattr(lifecycle, name)


class TestAliasesAndReexportsAreGone:
    @pytest.mark.parametrize(
        "module, name",
        [
            (dispatcher, "JobDispatcher"),
            (models, "Checkpoint"),
            (job_runner, "chunk"),
        ],
        ids=["JobDispatcher", "Checkpoint", "job_runner.chunk"],
    )
    def test_alias_is_gone(self, module, name) -> None:
        assert not hasattr(module, name)

    def test_reflow_manager_exports_job_not_checkpoint(self) -> None:
        assert "Job" in reflow_manager.__all__
        assert "Checkpoint" not in reflow_manager.__all__
        assert reflow_manager.Job is models.Job

    def test_destinations_base_no_longer_reexports_the_errors(self) -> None:
        assert "DestinationError" not in destinations_base.__all__
        assert "PermanentDestinationError" not in destinations_base.__all__

    @pytest.mark.parametrize(
        "name, home",
        [
            ("SourceError", exceptions),
            ("DestinationError", exceptions),
            ("PermanentDestinationError", exceptions),
            ("TransformationError", exceptions),
            ("StepOutputError", exceptions),
            ("chunk", planning),
        ],
    )
    def test_the_public_names_come_from_core(self, name, home) -> None:
        assert getattr(reflowfy, name) is getattr(home, name)

    def test_skip_reason_helpers_live_in_core(self) -> None:
        for name in ("count_skip_reason", "top_skip_reasons", "default_record_concurrency"):
            assert callable(getattr(record_steps, name))


def test_kafka_concurrency_is_dropped_like_any_unknown_producer_setting() -> None:
    from test_worker_executor_paths import capture_logs  # type: ignore[import-not-found]

    from reflowfy.destinations import kafka

    kafka._warned_producer_keys.discard("concurrency")
    destination = reflowfy.kafka_destination(bootstrap_servers="k:9092", topic="t", concurrency=4)
    with capture_logs(kafka.logger.name) as messages:
        kwargs = destination._producer_kwargs()
    assert "concurrency" not in kwargs
    assert any("ignoring producer setting 'concurrency'" in m for m in messages)
