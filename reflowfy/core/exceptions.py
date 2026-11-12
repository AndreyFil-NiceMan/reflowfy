"""Public exception types for user pipeline code.

Import from the package root::

    from reflowfy import PipelineError

    def define_destination(self, records, runtime_params):
        if not records:
            raise PipelineError("nothing to send")
"""

from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional


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


class RecordValidationError(PipelineError):
    """Fetched records do not match the pipeline's ``record_type``.

    Raised right after ``source.fetch`` when ``on_invalid_record = "fail"`` (the
    default), so bad source data fails the job before any transformation runs.
    The message names positions and fields, never values::

        record 3 of 50 does not match TypedRecord[Order]: data.amount: missing

    Attributes:
        invalid: 0-based record index -> what is wrong with it (``"data.amount: missing"``).
        total: How many records were checked.
    """

    def __init__(self, message: str, invalid: Dict[int, str], total: int) -> None:
        self.invalid = invalid
        self.total = total
        super().__init__(message)


# ----- connector and step errors ---------------------------------------------------
# Defined here, at the bottom of the package, so core (pipeline_step, job_retry's
# defaults) can name them without importing the connectors. The module each used to
# live in re-exports it.


class SourceError(Exception):
    """Raised when a source operation fails."""

    def __init__(self, source_type: str, message: str, original_error: Exception | None):
        self.source_type = source_type
        self.original_error = original_error
        super().__init__(f"Source '{source_type}' error: {message}")


class DestinationError(Exception):
    """Raised when a destination operation fails. ``send_with_retry`` retries it.

    ``retry_after``: seconds the server asked to wait before the next attempt
    (HTTP ``Retry-After``), or None.
    """

    def __init__(
        self,
        destination_type: str,
        message: str,
        original_error: Optional[Exception] = None,
        retry_after: Optional[float] = None,
    ):
        self.destination_type = destination_type
        self.original_error = original_error
        self.retry_after = retry_after
        self.reason = message
        super().__init__(f"Destination '{destination_type}' error: {message}")


class PermanentDestinationError(DestinationError):
    """A destination failure that sending again cannot fix, so it is not retried.

    For example an HTTP 4xx other than 408/425/429: the request itself is wrong
    (bad URL, rejected payload, missing permission). The job fails at once instead
    of after every retry.
    """


class TransformationError(Exception):
    """Raised when a transformation fails.

    Carries enough context to say *where* in the chain the failure happened and
    *which* record caused it, so callers can report it without re-running any
    user code.

    Attributes:
        transformation_name: The transformation that raised.
        original_error: The underlying exception, if any.
        step_index: 0-based position in the applied chain, or None if unknown.
        records_in: How many records this step received, or None if unknown.
        culprit_index: Index of the implicated record, or None if not identified.
        culprit_record: The implicated record itself, or None.
        applied_steps: The steps that succeeded before this one, in order.
    """

    def __init__(
        self,
        transformation_name: str,
        message: str,
        original_error: Optional[Exception] = None,
        step_index: Optional[int] = None,
        records_in: Optional[int] = None,
        culprit_index: Optional[int] = None,
        culprit_record: Optional[Any] = None,
        applied_steps: Optional[List[Any]] = None,
    ):
        self.transformation_name = transformation_name
        self.original_error = original_error
        self.step_index = step_index
        self.records_in = records_in
        self.culprit_index = culprit_index
        self.culprit_record = culprit_record
        self.applied_steps: List[Any] = applied_steps or []
        super().__init__(f"Transformation '{transformation_name}' failed: {message}")


class StepOutputError(TransformationError):
    """A transformation returned records that do not match its own return type.

    Raised after the step when the pipeline sets ``validate_steps = True``. It is a
    bug in that step, not bad input, so it always fails the job — whatever
    ``on_invalid_record`` says — and is counted under its own ``error_type``.
    """
