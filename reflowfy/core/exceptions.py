"""Public exception types for user pipeline code.

Import from the package root::

    from reflowfy import PipelineError

    def define_destination(self, records, runtime_params):
        if not records:
            raise PipelineError("nothing to send")
"""

from contextlib import contextmanager
from typing import Generator, Optional


class PipelineError(Exception):
    """Raised by a pipeline step.

    Raise this from any ``define_*`` hook to fail the job deliberately. The
    framework also wraps unexpected errors escaping a hook in one of these, with
    ``original_error`` set to the underlying cause.

    Attributes:
        original_error: The wrapped exception, or None if raised directly.
    """

    def __init__(self, message: str, original_error: Optional[Exception] = None) -> None:
        self.original_error = original_error
        super().__init__(message)


@contextmanager
def pipeline_step(step: str, pipeline_name: str) -> Generator[None, None, None]:
    """Re-raise an error from a user hook as a :class:`PipelineError` naming the step.

    Errors that already identify their own step (``PipelineError``,
    ``TransformationError``) pass through untouched, as does
    ``NotImplementedError`` — an unimplemented hook is a framework contract
    error, not a failure of the user's code.
    """
    # Imported lazily so this module stays dependency-free and cannot cycle.
    from reflowfy.transformations.base import TransformationError

    try:
        yield
    except (PipelineError, TransformationError, NotImplementedError):
        raise
    except Exception as exc:
        raise PipelineError(
            f"{step} of pipeline '{pipeline_name}' raised {type(exc).__name__}: {exc}",
            original_error=exc,
        ) from exc
