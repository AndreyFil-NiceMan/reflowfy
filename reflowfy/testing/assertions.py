"""Assertions about a :class:`PipelineResult` whose failures say what happened."""

from typing import Any, List, Optional, Sequence

from reflowfy.core.record_validation import describe_invalid, find_invalid_records
from reflowfy.core.serialization import to_json_safe
from reflowfy.testing.diff import diff_values, summarize, without_fields
from reflowfy.testing.result import PipelineResult


def assert_ok(result: PipelineResult) -> None:
    """Fail with the pipeline's own error if the run did not succeed."""
    if result.error is not None:
        raise AssertionError(
            f"pipeline '{result.pipeline}' failed: "
            f"{type(result.error).__name__}: {result.error}"
        ) from result.error


def _sort_key(record: Any) -> str:
    return repr(to_json_safe(record))


def assert_sent(
    result: PipelineResult,
    expected: List[Any],
    *,
    ordered: bool = True,
    ignore: Sequence[str] = (),
) -> None:
    """Assert the records sent to the destination, with a per-field diff on failure.

    ``ordered=False`` compares them as a bag (a step with ``concurrency`` above 1
    may deliver out of order). ``ignore`` names fields left out of the comparison,
    such as a timestamp a step stamps: a key, or a dotted path, matched at any depth.
    """
    assert_ok(result)
    want: Any = without_fields(to_json_safe(expected), ignore)
    got: Any = without_fields(to_json_safe(result.sent), ignore)
    if not ordered:
        want, got = sorted(want, key=_sort_key), sorted(got, key=_sort_key)
    lines = diff_values(want, got, "sent")
    if lines:
        raise AssertionError(
            f"pipeline '{result.pipeline}' did not send what was expected:\n{summarize(lines)}"
        )


def assert_skipped(
    result: PipelineResult, reason: Optional[str] = None, count: Optional[int] = None
) -> None:
    """Assert records were dropped with a reason (``SkipRecord``, or ``record_type``).

    With ``reason``, counts only that reason. With ``count``, the number must
    match exactly; without it, at least one record must have been skipped.
    """
    reasons = result.skip_reasons
    actual = reasons.get(reason, 0) if reason is not None else result.records_skipped
    what = f"with reason {reason!r}" if reason is not None else "with a reason"
    if count is None and actual > 0:
        return
    if count is not None and actual == count:
        return
    wanted = "at least 1" if count is None else str(count)
    raise AssertionError(
        f"expected {wanted} record(s) skipped {what}, got {actual}. "
        f"Skip reasons: {reasons or 'none'}"
    )


def assert_job_dropped(result: PipelineResult, reason: Optional[str] = None) -> None:
    """Assert the pipeline dropped a job with ``SkipJob`` (for ``reason``, if given)."""
    dropped = result.jobs_dropped
    if dropped and (reason is None or reason in dropped):
        return
    what = f"for {reason!r}" if reason is not None else "(SkipJob)"
    raise AssertionError(
        f"expected a job dropped {what}; dropped jobs: {dropped or 'none'}"
    )


def assert_valid_records(record_type: Any, records: List[Any]) -> None:
    """Assert every record matches ``record_type``, as ``record_type =`` on a pipeline checks.

    ``record_type`` is the type itself (``TypedRecord[Order]``) or a pipeline that
    declares one.
    """
    declared = getattr(record_type, "record_type", None)
    checked = record_type if declared is None else declared
    invalid = find_invalid_records(checked, records)
    if invalid:
        raise AssertionError(describe_invalid(invalid, len(records), checked))
