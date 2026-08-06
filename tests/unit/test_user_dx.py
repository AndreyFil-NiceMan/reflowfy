"""User-facing DX: a logger reachable without `self`, and errors that name the step."""

import logging

import pytest

from reflowfy import (
    AbstractPipeline,
    DestinationError,
    PipelineError,
    SourceError,
    TransformationError,
    get_logger,
    pipeline_registry,
)
from reflowfy.execution.job_runner import run_job_records
from reflowfy.observability.context import log_context


@pytest.fixture
def reflowfy_logs():
    """Capture records off the 'reflowfy' logger itself.

    caplog can't be used here: setup_logging() sets propagate=False on 'reflowfy',
    so records never reach the root handler caplog installs. Whether that has run
    depends on test ordering, which made this flaky.
    """
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("reflowfy")
    handler = _Collector(level=logging.DEBUG)
    previous_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        yield records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)


class _StaticSource:
    """Minimal duck-typed source: no split(), so plan_slices yields it whole."""

    def __init__(self, records, error=None):
        self._records = records
        self._error = error

    def fetch(self, runtime_params):
        if self._error:
            raise self._error
        return self._records


def test_public_surface_is_importable():
    """Everything a user needs is on the package root."""
    assert get_logger("x").name == "reflowfy.x"
    assert get_logger().name == "reflowfy"
    for exc in (PipelineError, SourceError, DestinationError, TransformationError):
        assert issubclass(exc, Exception)

    err = PipelineError("boom")
    assert err.original_error is None


def test_logging_from_a_free_function_carries_job_context(reflowfy_logs):
    """The 'no self' case: a module-level logger still gets execution_id/job_id."""
    logger = get_logger("tests.free_function")

    with log_context(execution_id="e1", job_id="j1", pipeline_name="p1"):
        logger.info("hello from a plain function")

    record = next(r for r in reflowfy_logs if r.getMessage() == "hello from a plain function")
    assert record.execution_id == "e1"
    assert record.job_id == "j1"
    assert record.pipeline_name == "p1"


def test_registration_failure_is_logged_and_explains_the_later_lookup(reflowfy_logs):
    """A pipeline whose __init__ raises must not vanish silently."""

    class BrokenPipeline(AbstractPipeline):
        name = "broken_dx_pipeline"

        def __init__(self):
            raise KeyError("ES_URL")

        def define_source(self, runtime_params):
            return []

        def define_destination(self, records, runtime_params):
            return None

        def define_transformations(self, records, runtime_params):
            return []

    try:
        assert pipeline_registry.get("broken_dx_pipeline") is None

        errors = [r for r in reflowfy_logs if r.levelno == logging.ERROR]
        assert any("broken_dx_pipeline" in r.getMessage() for r in errors)
        assert any(r.exc_info for r in errors), "traceback must be logged"

        reason = pipeline_registry.describe_missing("broken_dx_pipeline")
        assert "KeyError" in reason and "ES_URL" in reason
        # A name nobody ever tried gets no explanation.
        assert pipeline_registry.describe_missing("never_seen") == ""
    finally:
        pipeline_registry._failures.pop("broken_dx_pipeline", None)


@pytest.mark.parametrize(
    "failing_hook, step", [("destination", "define_destination"), ("source", "source fetch")]
)
def test_hook_errors_name_the_failing_step(failing_hook, step):
    """A raise from a hook is wrapped so the message says WHICH step failed."""
    boom = ValueError("boom")

    class _Pipeline:
        name = "step_naming_pipeline"

        def define_transformations(self, records, runtime_params):
            return []

        def define_destination(self, records, runtime_params):
            if failing_hook == "destination":
                raise boom
            return None

    source = _StaticSource([{"id": 1}], error=boom if failing_hook == "source" else None)

    with pytest.raises(PipelineError) as exc_info:
        run_job_records(source, _Pipeline(), {})

    message = str(exc_info.value)
    assert step in message
    assert "step_naming_pipeline" in message
    assert "ValueError: boom" in message
    assert exc_info.value.original_error is boom


def test_deliberate_pipeline_error_passes_through_unwrapped():
    """A user's own PipelineError is not re-wrapped or renamed."""

    class _Pipeline:
        name = "deliberate_pipeline"

        def define_transformations(self, records, runtime_params):
            return []

        def define_destination(self, records, runtime_params):
            raise PipelineError("nothing worth sending")

    with pytest.raises(PipelineError) as exc_info:
        run_job_records(_StaticSource([{"id": 1}]), _Pipeline(), {})

    assert str(exc_info.value) == "nothing worth sending"
    assert exc_info.value.original_error is None
