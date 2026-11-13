"""Running a whole pipeline in a test, with nothing real on either end.

``run_pipeline`` goes through the same per-job core the worker runs
(``reflowfy.execution.job_runner``): fetch, ``record_type`` check, transformations
re-resolved after every step, ``define_destination``. What differs is the edges:
the source can be replaced by records the test supplies, the destination is an
in-memory capture, and the network is answered by mocks or a recorded fixture.
"""

import copy
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.serialization import to_json_safe
from reflowfy.core.planning import PlannedJob, ResolvedPlan
from reflowfy.destinations.base import BaseDestination
from reflowfy.destinations.pool import loop_pool_scope
from reflowfy.execution.job_runner import (
    log_job_dropped,
    log_skipped_records,
    plan_slices,
    preview_runtime_params,
    run_job_records,
)
from reflowfy.execution.transformation_runner import AppliedStep
from reflowfy.observability.context import log_context
from reflowfy.observability.logging import get_logger
from reflowfy.sources.base import BaseSource
from reflowfy.sources.mock import MockSource
from reflowfy.testing.cassette import (
    FixtureError,
    Session,
    UnmockedCallError,
    activate,
    current_session,
    patch_sources,
)
from reflowfy.testing.destinations import CaptureDestination
from reflowfy.testing.result import JobResult, PipelineResult
from reflowfy.testing.transforms import run_step
from reflowfy.transformations.base import BaseTransformation, TransformationError

logger = get_logger("testing")

PipelineRef = Union[str, type, "AbstractPipeline[Any]"]


def resolve_pipeline(pipeline: PipelineRef) -> "AbstractPipeline[Any]":
    """The registered pipeline for a name, a class or an instance.

    A name is looked up in the registry, running discovery (``PIPELINE_MODULE``,
    default ``pipelines/``) the first time it is not there.
    """
    from reflowfy.core.registry import pipeline_registry

    if isinstance(pipeline, str):
        found = pipeline_registry.get(pipeline)
        if found is None:
            from reflowfy.core.pipeline_discovery import discover_and_load_pipelines

            discover_and_load_pipelines()
            found = pipeline_registry.get(pipeline)
        if found is None:
            known = ", ".join(sorted(pipeline_registry.list_names())) or "none"
            raise LookupError(
                f"No pipeline named '{pipeline}'{pipeline_registry.describe_missing(pipeline)}. "
                f"Registered pipelines: {known}"
            )
        return found
    if isinstance(pipeline, type):
        registered = pipeline_registry.get(str(getattr(pipeline, "name", "")))
        # Compared outside the `if`, so the type checkers keep `registered` as it is.
        same_class = registered.__class__ is pipeline
        if registered is not None and same_class:
            return registered
        instance: "AbstractPipeline[Any]" = pipeline()
        return instance
    return pipeline


def private_copy(pipeline: "AbstractPipeline[Any]") -> "AbstractPipeline[Any]":
    """A shallow copy of ``pipeline`` for one test run.

    A run instruments the copy's ``define_transformations`` (to know which step is
    running, and to freeze or record steps), so the registered instance, shared by
    every run in the process, is never touched.
    """
    return copy.copy(pipeline)


def make_params(pipeline: PipelineRef, **overrides: Any) -> Dict[str, Any]:
    """The runtime_params a job of ``pipeline`` would see: defaults applied, your
    overrides on top, and the reserved keys (``execution_id`` …) filled in.

    For calling a hook or a transformation directly in a test.
    """
    resolved = resolve_pipeline(pipeline)
    return preview_runtime_params(resolved.name, dict(resolved.apply_defaults(dict(overrides))))


# ---------------------------------------------------------------------------
# Steps replaced during a fixture run
# ---------------------------------------------------------------------------


class _RecordingStep(BaseTransformation):
    """Runs the real step and keeps what it returned (``--freeze-step``, recording)."""

    def __init__(self, inner: Any, session: Session, keep: Callable[[List[Any]], None]) -> None:
        self.name = str(getattr(inner, "name", ""))
        self._inner = inner
        self._session = session
        self._keep = keep

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        self._session.paused = True
        try:
            out, _ = run_step(self._inner, records, runtime_params)
        finally:
            self._session.paused = False
        self._keep(to_json_safe(out))
        return out


class _FrozenStep(BaseTransformation):
    """Returns a step's recorded output instead of running it (replay)."""

    def __init__(self, name: str, output: Any) -> None:
        self.name = name
        self._output = output

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        if self._output is None:
            raise FixtureError(
                f"step '{self.name}' is frozen but the fixture holds no output of it for this "
                f"job. Record the fixture again with --freeze-step {self.name}."
            )
        return copy.deepcopy(self._output)


@dataclass
class StepHooks:
    """How a fixture run treats individual steps."""

    frozen: Dict[str, List[Any]] = field(default_factory=lambda: {})
    """Replay: step name -> its recorded output, one entry per job."""
    capture: Set[str] = field(default_factory=lambda: set())
    """Record: the steps whose output to keep."""
    captured: Dict[str, Dict[int, Any]] = field(default_factory=lambda: {})
    """Filled while recording: step name -> job index -> output."""


@dataclass
class _Progress:
    job: int = 0
    resolutions: int = 0


def _instrument(
    fork: "AbstractPipeline[Any]", session: Session, hooks: StepHooks, progress: _Progress
) -> None:
    """Tell the session which step is running, and swap in frozen/recording steps.

    The runner re-resolves ``define_transformations`` before every step and then
    applies the step at the position it has reached, so the n-th resolution of a
    job comes right before step n. That is all the bookkeeping this needs, and it
    leaves the step objects themselves (and their ``isinstance``) alone.
    """
    original = fork.define_transformations

    def swap(resolved: Any) -> List[Any]:
        steps: List[Any] = list(resolved)
        position = progress.resolutions
        progress.resolutions += 1
        if position < len(steps):
            step = steps[position]
            name = str(getattr(step, "name", "<unknown>"))
            if name in hooks.frozen:
                outputs = hooks.frozen[name]
                steps[position] = _FrozenStep(
                    name, outputs[progress.job] if progress.job < len(outputs) else None
                )
            elif name in hooks.capture:
                job_index = progress.job
                steps[position] = _RecordingStep(
                    step, session, lambda out: hooks.captured.setdefault(name, {}).update({job_index: out})
                )
            session.current_step = name
        return steps

    def start() -> None:
        session.in_job = True
        session.current_step = None

    define_transformations: Callable[..., Any]
    if inspect.iscoroutinefunction(original):
        # An async hook stays async, so the runner awaits it on the job's loop.

        async def define_transformations_async(records: List[Any], runtime_params: Any) -> List[Any]:
            start()
            return swap(await original(records, runtime_params))

        define_transformations = define_transformations_async
    else:

        def define_transformations_sync(records: List[Any], runtime_params: Any) -> List[Any]:
            start()
            return swap(original(records, runtime_params))

        define_transformations = define_transformations_sync

    setattr(fork, "define_transformations", define_transformations)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _setup_problem(exc: Optional[BaseException]) -> Optional[BaseException]:
    """The test-setup error inside a failure, if that is what it is.

    An unanswered request is a problem with the test, not a result of the
    pipeline, so it is raised to the test instead of landing in ``result.error``.
    """
    seen: Set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, (UnmockedCallError, FixtureError)):
            return exc
        exc = getattr(exc, "original_error", None) or exc.__cause__ or exc.__context__
    return None


def _log_job(job: JobResult, logger: Any = logger) -> None:
    """The worker's ``job_dropped`` / ``records_skipped`` lines, so ``caplog`` sees them."""
    if job.skip_reason:
        log_job_dropped(logger, str(job.skip_reason))
    for step in job.steps:
        log_skipped_records(logger, step)


async def _send(target: BaseDestination, records: List[Any], params: Dict[str, Any]) -> None:
    # Sent and closed in one loop, with this job's connection pool: a Kafka producer
    # belongs to its loop (as in ``reflowfy test`` and the worker).
    async with loop_pool_scope():
        try:
            await target.send_with_retry(records, params)
        finally:
            await target.close()


def execute(
    fork: "AbstractPipeline[Any]",
    flat: Dict[str, Any],
    plan: Callable[[], Any],
    session: Session,
    *,
    destination: Union[str, BaseDestination] = "capture",
    send: bool = True,
    limit: Optional[int] = None,
    hooks: Optional[StepHooks] = None,
    sender: Optional[Callable[[Any, List[Any], Dict[str, Any]], None]] = None,
    job_logger: Any = None,
) -> PipelineResult:
    """Plan and run every job of a private copy of a pipeline inside ``session``.

    The one loop behind ``run_pipeline``, the fixture recorder/replayer and
    ``reflowfy test``. Stops at the first job that fails; the failure is on
    ``result.error``.

    ``sender(destination, records, job_params)`` replaces how a job's records are
    sent (``reflowfy test`` prints and health-checks); it raises to fail the job.
    ``job_logger`` is where the skip/drop lines go (default ``reflowfy.testing``).
    """
    result = PipelineResult(pipeline=fork.name, params=flat)
    progress = _Progress()
    _instrument(fork, session, hooks or StepHooks(), progress)
    capture = CaptureDestination()
    # The manager's merge, so a job sees what it would in production (an ID-based
    # job gets its `current_ids`, not the whole `input_ids` list).
    merge = ResolvedPlan(params=flat, jobs=None, execution_only=fork.execution_only_params)
    session.in_job = False
    session.current_step = None
    try:
        for planned in plan_slices(plan(), flat):
            remaining = None if limit is None else limit - result.fetched
            if remaining is not None and remaining <= 0:
                break
            sub = planned.source
            job_params = merge.job_params(planned)
            job = JobResult(
                index=len(result.jobs),
                params=job_params,
                job_params=dict(planned.params),
            )
            result.jobs.append(job)
            progress.job, progress.resolutions = job.index, 0
            session.main_sources.add(id(sub))
            patch_sources()
            try:
                with log_context(
                    execution_id=job_params.get("execution_id"),
                    pipeline_name=job_params.get("pipeline_name"),
                ):
                    fetched, job.records, job.steps, job.destination = run_job_records(
                        sub, fork, job_params, limit=remaining
                    )
                    job.skip_reason = job_params.get("skip_reason")
                    # A dropped job returns no records at all; what it fetched is
                    # still the job's input, and a fixture has to hold it.
                    raw = session.main_fetches.get(id(sub))
                    job.fetched = fetched if raw is None else to_json_safe(raw)[:remaining]
                    _log_job(job, job_logger or logger)
                    if job.records and send and sender is not None:
                        sender(job.destination, job.records, job_params)
                        job.sent = True
                    elif job.records and send:
                        if isinstance(destination, BaseDestination):
                            target = destination
                        else:
                            target = job.destination if destination == "real" else capture
                        run_coroutine_sync(_send(target, job.records, job_params))
                        job.sent = True
            except Exception as exc:  # noqa: BLE001 - reported on the result
                job.error = exc
                raw = session.main_fetches.get(id(sub))
                if raw is not None:
                    job.fetched = to_json_safe(raw)[:remaining]
                if isinstance(exc, TransformationError):
                    job.steps = [s for s in exc.applied_steps if isinstance(s, AppliedStep)]
                result.error = exc
                break
            finally:
                session.in_job = False
                session.current_step = None
                session.main_sources.discard(id(sub))
                session.main_fetches.pop(id(sub), None)
    except Exception as exc:  # noqa: BLE001 - a failure while planning
        result.error = exc
    problem = _setup_problem(result.error)
    if problem is not None:
        raise problem
    return result


def run_pipeline(
    pipeline: PipelineRef,
    *,
    params: Optional[Dict[str, Any]] = None,
    records: Optional[List[Any]] = None,
    jobs: Optional[List[Any]] = None,
    source: Any = None,
    wrap: bool = True,
    destination: Union[str, BaseDestination] = "capture",
    send: bool = True,
    limit: Optional[int] = None,
    allow_network: bool = False,
    source_stubs: Optional[Dict[Union[type, str], List[Any]]] = None,
) -> PipelineResult:
    """Run a pipeline in a test and return what it did.

    Args:
        pipeline: A pipeline name, class or instance. The registered instance is
            never touched: the run uses a private copy.
        params: The user's parameters. Defaults are applied and the values
            validated as ``/run`` would; invalid ones raise ``ValueError``.
        records: Input for one job, replacing the pipeline's source.
        jobs: Input for several jobs: a list of record lists, or of
            ``reflowfy.job(records, **params)`` for per-job params. Replaces the
            pipeline's source and job plan.
        source: A source to use instead of the pipeline's own.
        wrap: ``records``/``jobs`` are wrapped as ``{"data": record}``, the shape
            built-in sources return. Pass ``False`` to use them as they are.
        destination: ``"capture"`` (default) sends to memory. ``"real"`` sends to
            what ``define_destination`` returned. An instance sends there.
        send: ``False`` stops after ``define_destination``.
        limit: Stop after this many fetched records.
        allow_network: By default an HTTP request that no mock answers raises
            ``UnmockedCallError``. ``True`` lets it through.
        source_stubs: Records for sources a transformation fetches from itself,
            by source class or type name.

    With none of ``records``/``jobs``/``source``, the pipeline's own
    ``define_jobs``/``define_source`` plans the run.

    Returns:
        A :class:`PipelineResult`. A failing pipeline does not raise: check
        ``result.ok`` (or call ``result.raise_for_error()``).

    Example:
        >>> result = run_pipeline("orders", params={"region": "eu"},
        ...                       records=[{"id": 1, "amount": 10.0}])
        >>> assert result.ok
        >>> assert result.sent == [{"data": {"id": 1, "amount": 10.0}, "tax": 2.0}]
    """
    given = [name for name, value in (("records", records), ("jobs", jobs), ("source", source))
             if value is not None]
    if len(given) > 1:
        raise ValueError(f"pass only one of records, jobs and source (got {', '.join(given)})")

    fork = private_copy(resolve_pipeline(pipeline))
    user_params = dict(fork.apply_defaults(dict(params or {})))
    checked = user_params
    if given and isinstance(fork, IdBasedPipeline) and "input_ids" not in user_params:
        # The test supplies the jobs, so there are no IDs to plan them from.
        checked = {**user_params, "input_ids": ["<supplied by the test>"]}
    errors = fork.validate_parameters(checked)
    if errors:
        raise ValueError(f"Invalid parameters: {'; '.join(errors)}")
    flat = preview_runtime_params(fork.name, user_params)

    def one_job(items: Any) -> Any:
        # A source, or a `reflowfy.job(...)`, is used as it is.
        if isinstance(items, (BaseSource, PlannedJob)) or not wrap:
            return items
        return MockSource(data=list(items), batch_size=max(len(items), 1))

    def plan() -> Any:
        if records is not None:
            return [one_job(records)]
        if jobs is not None:
            return [one_job(items) for items in jobs]
        if source is not None:
            return source
        return fork.define_jobs(flat)

    def go(session: Session) -> PipelineResult:
        for source_type, stubbed in (source_stubs or {}).items():
            session.sources(source_type, stubbed)
        return execute(
            fork, flat, plan, session, destination=destination, send=send, limit=limit
        )

    active = current_session()
    if active is not None:
        return go(active)
    with activate(Session("passthrough" if allow_network else "replay")) as session:
        return go(session)
