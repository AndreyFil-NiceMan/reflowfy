"""Testing one transformation on its own, and building its inputs."""

from typing import Any, Dict, List, NamedTuple, Optional, Tuple

from reflowfy.core.types import Record, Records, wrap_as_record
from reflowfy.execution.transformation_runner import (
    AppliedStep,
    apply_transformations_iteratively,
)


class _OneStep:
    """The least a pipeline has to be for the transformation runner: one step."""

    validate_steps = False

    def __init__(self, step: Any) -> None:
        self.name = str(getattr(step, "name", "") or "test")
        self._step = step

    def define_transformations(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        return [self._step]


def run_step(
    step: Any, records: List[Any], runtime_params: Dict[str, Any]
) -> Tuple[List[Any], AppliedStep]:
    """Run one transformation instance exactly as a job would run it."""
    out, applied = apply_transformations_iteratively(_OneStep(step), records, runtime_params)
    return out, applied[0]


class StepResult(NamedTuple):
    """What one transformation returned. Unpacks as ``records, skip_reasons``."""

    records: List[Any]
    skip_reasons: Dict[str, int]
    """``SkipRecord`` reason -> records dropped for it (per-record steps only)."""


def apply_transformation(
    transformation: Any, records: List[Any], params: Optional[Dict[str, Any]] = None
) -> StepResult:
    """Run one transformation the way the worker runs it, and return its output.

    ``transformation`` is an instance, or the class a ``@transformation`` /
    ``@record_transformation`` decorator returns (it is instantiated for you). Sync,
    async and per-record steps all work: a per-record step runs with its real
    concurrency, ``None`` drops a record and ``SkipRecord`` drops it with a reason.

    A failure raises ``TransformationError``, as it does in a job, with the cause
    on ``.original_error`` and the record on ``.culprit_record``. ``SkipJob`` is
    raised as it is.

    Example:
        >>> out, skips = apply_transformation(add_tax, make_records([{"amount": 10.0}]),
        ...                                   params={"region": "eu"})
        >>> assert out[0]["tax"] == 2.0
    """
    step = transformation() if isinstance(transformation, type) else transformation
    out, applied = run_step(step, records, dict(params or {}))
    return StepResult(out, dict(applied.skip_reasons or {}))


def record(data: Any, **extra: Any) -> Record:
    """One record shaped the way built-in sources return it: ``{"data": data}``.

    Keywords become top-level keys beside ``data``, where transformations usually
    add theirs: ``record({"id": 1}, tax=2.0)`` is ``{"data": {"id": 1}, "tax": 2.0}``.
    """
    wrapped = wrap_as_record(data)
    wrapped.update(extra)
    return wrapped


def make_records(items: List[Any]) -> Records:
    """Wrap each item as ``{"data": item}``, as a source would."""
    return [wrap_as_record(item) for item in items]
