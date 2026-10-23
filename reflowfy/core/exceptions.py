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


class SkipJob(PipelineError):
    """Raise from any hook (or a transformation) to drop the current job.

    The job stops before its destination write and is recorded as completed
    with zero records — nothing is sent, nothing fails::

        def define_destination(self, records, runtime_params):
            if not [r for r in records if r["status"] == "ready"]:
                raise SkipJob("nothing ready yet")
    """


class SkipRecord(PipelineError):
    """Raise from a per-record transformation to drop that one record.

    The rest of the job carries on. Unlike returning ``None`` (a silent drop, like
    a filter), the reason is kept: logged, stored on the job's ``stats``
    (``records_skipped`` / ``record_skip_reasons``) and counted as
    ``reflowfy_records_skipped_total``::

        @record_transformation("enrich_user")
        async def enrich_user(record, runtime_params):
            user = await fetch_user(record["user_id"])
            if not user["active"]:
                raise SkipRecord("user inactive")
            return {**record, "user": user}

    Keep the reason a short, fixed phrase rather than one built from the record:
    reasons are counted, so ``"user inactive"`` groups and ``f"user {id} inactive"``
    does not. Only a per-record transformation can skip a record; anywhere else
    there is no single record to drop, and it fails the job saying so.
    """
