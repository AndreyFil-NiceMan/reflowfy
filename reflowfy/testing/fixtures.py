"""Fixtures: one real run of a pipeline, saved to a file a test replays offline.

``reflowfy test <pipeline> --save-fixture tests/fixtures/x.json`` runs the pipeline
against its real source and writes down three things:

- **the input**: the params, and the records each job fetched;
- **what the outside world answered** while the steps ran: HTTP responses, the
  records of sources a step fetched from itself, and the output of any frozen step;
- **the expected output**: what would be sent, what was skipped and why.

Replaying it (``run_fixture``, the pytest plugin, ``reflowfy test --fixture``) feeds
the saved input back through the pipeline with the saved answers and compares.
Nothing is fetched and nothing is sent, so the test keeps passing after the source
has dropped the data. Only recording ever touches a real system.

The expected output is not taken from the recording run: it is computed by
replaying the fixture as it will be saved (after ``--redact``), so the input, the
answers and the expectation always agree with each other.
"""

import copy
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import reflowfy
from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.content_dedup import destination_identity
from reflowfy.execution.job_runner import job, preview_runtime_params
from reflowfy.testing.cassette import FixtureError, Session, UnmockedCallError, activate
from reflowfy.testing.diff import diff_values, redact, summarize, without_fields
from reflowfy.testing.result import PipelineResult
from reflowfy.testing.runner import (
    PipelineRef,
    StepHooks,
    execute,
    private_copy,
    resolve_pipeline,
)

FIXTURE_VERSION = 1

# Written by the framework per run; a recorded job must not bring its old ones back.
_RESERVED_PARAMS = {"execution_id", "batch_id", "pipeline_name", "created_at", "skip_reason"}
# The ones that differ from run to run. A fixture keeps the recorded values and a
# replay runs with them, so a step that stamps `execution_id` into its records
# produces the same records every time.
_CONTEXT_KEYS = ("execution_id", "batch_id", "created_at")


@dataclass
class PipelineFixture:
    """A recorded run. See the module docstring for what each part is for."""

    pipeline: str
    params: Dict[str, Any] = field(default_factory=lambda: {})
    context: Dict[str, Any] = field(default_factory=lambda: {})
    """``execution_id``, ``batch_id`` and ``created_at`` of the recorded run; a
    replay runs with the same ones."""
    jobs: List[List[Any]] = field(default_factory=lambda: [])
    """The records each job fetched, before any transformation."""
    job_params: List[Dict[str, Any]] = field(default_factory=lambda: [])
    """The params the plan attached to each job (``job(...)``, ``current_ids``)."""
    http: List[Dict[str, Any]] = field(default_factory=lambda: [])
    source_calls: List[Dict[str, Any]] = field(default_factory=lambda: [])
    step_outputs: Dict[str, List[Any]] = field(default_factory=lambda: {})
    """Frozen step -> its output per job (``None`` where it did not run)."""
    frozen_steps: List[str] = field(default_factory=lambda: [])
    expected: Dict[str, Any] = field(default_factory=lambda: {})
    ignore: List[str] = field(default_factory=lambda: [])
    redacted: List[str] = field(default_factory=lambda: [])
    recorded_at: str = ""
    reflowfy_version: str = ""
    path: Optional[Path] = None

    def to_dict(self) -> Dict[str, Any]:
        """The file's content, in the order a person reads it."""
        return {
            "reflowfy_fixture": FIXTURE_VERSION,
            "pipeline": self.pipeline,
            "reflowfy_version": self.reflowfy_version,
            "recorded_at": self.recorded_at,
            "params": self.params,
            "context": self.context,
            "jobs": self.jobs,
            "job_params": self.job_params,
            "http": self.http,
            "source_calls": self.source_calls,
            "step_outputs": self.step_outputs,
            "frozen_steps": self.frozen_steps,
            "expected": self.expected,
            "ignore": self.ignore,
            "redacted": self.redacted,
        }

    def save(self, path: Union[str, Path, None] = None) -> Path:
        """Write the fixture as JSON (to ``path``, or back to where it was loaded from)."""
        target = Path(path) if path is not None else self.path
        if target is None:
            raise FixtureError("this fixture has no path yet: pass one to save()")
        target.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(self.to_dict(), indent=2, ensure_ascii=False, default=str)
        target.write_text(text + "\n", encoding="utf-8")
        self.path = target
        return target


def load_fixture(path: Union[str, Path]) -> PipelineFixture:
    """Read a fixture file written by ``--save-fixture``."""
    source = Path(path)
    try:
        raw: Any = json.loads(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise FixtureError(f"cannot read fixture {source}: {exc}") from exc
    except ValueError as exc:
        raise FixtureError(f"fixture {source} is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict) or "reflowfy_fixture" not in raw or "pipeline" not in raw:
        raise FixtureError(f"{source} is not a reflowfy fixture (no 'reflowfy_fixture' key)")
    data: Dict[str, Any] = raw  # pyright: ignore[reportUnknownVariableType]
    version = data["reflowfy_fixture"]
    if not isinstance(version, int) or version > FIXTURE_VERSION:
        raise FixtureError(
            f"{source} is fixture format {version!r}; this reflowfy reads up to "
            f"{FIXTURE_VERSION}. Upgrade reflowfy, or record the fixture again."
        )
    jobs: List[List[Any]] = list(data.get("jobs") or [])
    job_params: List[Dict[str, Any]] = list(data.get("job_params") or [])
    return PipelineFixture(
        pipeline=str(data["pipeline"]),
        params=dict(data.get("params") or {}),
        context=dict(data.get("context") or {}),
        jobs=jobs,
        job_params=job_params + [{} for _ in range(len(jobs) - len(job_params))],
        http=list(data.get("http") or []),
        source_calls=list(data.get("source_calls") or []),
        step_outputs=dict(data.get("step_outputs") or {}),
        frozen_steps=list(data.get("frozen_steps") or []),
        expected=dict(data.get("expected") or {}),
        ignore=list(data.get("ignore") or []),
        redacted=list(data.get("redacted") or []),
        recorded_at=str(data.get("recorded_at") or ""),
        reflowfy_version=str(data.get("reflowfy_version") or ""),
        path=source,
    )


def expected_from(result: PipelineResult) -> Dict[str, Any]:
    """The part of a run a fixture pins down, in JSON form."""
    return {
        "sent": to_json_safe([job.records if job.sent else [] for job in result.jobs]),
        "records_skipped": result.records_skipped,
        "skip_reasons": dict(result.skip_reasons),
        "jobs_dropped": list(result.jobs_dropped),
        # Class and config without credentials: what the dedup hash already uses.
        "destinations": to_json_safe(
            [
                destination_identity(job.destination) if job.destination is not None else None
                for job in result.jobs
            ]
        ),
    }


def _prepare(pipeline: PipelineRef, params: Dict[str, Any]) -> Tuple[Any, Dict[str, Any]]:
    """A private copy of the pipeline and the flat params a run of it starts from."""
    fork = private_copy(resolve_pipeline(pipeline))
    user_params = dict(fork.apply_defaults(dict(params)))
    return fork, preview_runtime_params(fork.name, user_params)


def replay(
    fixture: PipelineFixture, *, freeze_steps: Optional[Sequence[str]] = None
) -> PipelineResult:
    """Run the fixture's input through the pipeline with its recorded answers.

    Offline: the pipeline's source is never built, a request the fixture does not
    hold raises :class:`UnmockedCallError`, and nothing is sent.
    """
    fork, flat = _prepare(fixture.pipeline, fixture.params)
    flat.update({key: fixture.context[key] for key in _CONTEXT_KEYS if key in fixture.context})
    frozen: List[str] = []
    for name in [*fixture.frozen_steps, *(freeze_steps or [])]:
        if name not in frozen:
            frozen.append(name)
    missing = [name for name in frozen if name not in fixture.step_outputs]
    if missing:
        raise FixtureError(
            f"the fixture holds no output for step(s) {', '.join(missing)}. A step can only "
            "be frozen on replay if it was frozen when the fixture was recorded: record it "
            f"again with --freeze-step {missing[0]}."
        )
    plan = [
        job(copy.deepcopy(records), **params)
        for records, params in zip(fixture.jobs, fixture.job_params)
    ]
    session = Session(
        "replay",
        http=copy.deepcopy(fixture.http),
        source_calls=copy.deepcopy(fixture.source_calls),
        strict_sources=True,
    )
    hooks = StepHooks(frozen={name: fixture.step_outputs[name] for name in frozen})
    with activate(session):
        return execute(fork, flat, lambda: plan, session, hooks=hooks)


def diff_fixture(result: PipelineResult, fixture: PipelineFixture) -> List[str]:
    """What a run did differently from what the fixture expects; empty when it matches."""
    if result.error is not None:
        return [f"the pipeline failed: {type(result.error).__name__}: {result.error}"]
    expected: Dict[str, Any] = without_fields(fixture.expected, fixture.ignore)
    actual: Dict[str, Any] = without_fields(expected_from(result), fixture.ignore)
    lines: List[str] = []
    sent_before: List[Any] = list(expected.get("sent") or [])
    sent_after: List[Any] = list(actual["sent"])
    if len(sent_before) != len(sent_after):
        lines.append(f"jobs: {len(sent_before)} → {len(sent_after)}")
    for index, (before, after) in enumerate(zip(sent_before, sent_after)):
        if len(before) != len(after):
            lines.append(f"job {index}: {len(before)} record(s) sent → {len(after)}")
        for position, (old, new) in enumerate(zip(before, after)):
            lines.extend(
                f"job {index}, record {position}: {line}" for line in diff_values(old, new)
            )
    for key in ("records_skipped", "skip_reasons", "jobs_dropped", "destinations"):
        lines.extend(diff_values(expected.get(key), actual[key], key))
    return lines


def assert_matches_fixture(result: PipelineResult, fixture: PipelineFixture) -> None:
    """Fail, listing every difference, unless ``result`` is what the fixture expects."""
    lines = diff_fixture(result, fixture)
    if lines:
        where = f" ({fixture.path})" if fixture.path else ""
        raise AssertionError(
            f"pipeline '{fixture.pipeline}' no longer matches its fixture{where}:\n"
            f"{summarize(lines)}\n"
            "If the change is intended, update the expected output:\n"
            f"  reflowfy test {fixture.pipeline} --fixture "
            f"{fixture.path or '<file>'} --update-fixture"
        )


def run_fixture(
    fixture: Union[str, Path, PipelineFixture],
    *,
    freeze_steps: Optional[Sequence[str]] = None,
    check: bool = True,
) -> PipelineResult:
    """Replay a fixture and assert the pipeline still does what it recorded.

    Example:
        >>> def test_orders_eu():
        ...     run_fixture("tests/fixtures/orders_eu.json")

    ``check=False`` only replays, for a test that asserts something of its own on
    the returned result.
    """
    loaded = fixture if isinstance(fixture, PipelineFixture) else load_fixture(fixture)
    result = replay(loaded, freeze_steps=freeze_steps)
    if check:
        assert_matches_fixture(result, loaded)
    return result


def update_fixture(fixture: Union[str, Path, PipelineFixture]) -> Tuple[PipelineFixture, List[str]]:
    """Recompute a fixture's expected output from its saved input, and save it.

    No real system is involved. Returns the fixture and what changed, so the
    caller can show it; review the file's diff before committing.
    """
    loaded = fixture if isinstance(fixture, PipelineFixture) else load_fixture(fixture)
    result = replay(loaded)
    if result.error is not None:
        raise FixtureError(
            f"the pipeline fails on this fixture, so there is no output to save: "
            f"{type(result.error).__name__}: {result.error}"
        ) from result.error
    changes = diff_fixture(result, loaded)
    loaded.expected = expected_from(result)
    loaded.reflowfy_version = reflowfy.__version__
    if loaded.path is not None:
        loaded.save()
    return loaded, changes


def _redact_exchange(exchange: Dict[str, Any], fields: Sequence[str]) -> Dict[str, Any]:
    response = dict(exchange["response"])
    if "body_encoding" not in response:  # a JSON body; text and binary are left as they are
        response["body"] = redact(response.get("body"), fields)
    return {**exchange, "response": response}


def record_fixture(
    pipeline: PipelineRef,
    params: Optional[Dict[str, Any]] = None,
    *,
    limit: Optional[int] = None,
    redact_fields: Sequence[str] = (),
    ignore: Sequence[str] = (),
    freeze_steps: Sequence[str] = (),
    max_body_kb: int = 256,
) -> PipelineFixture:
    """Run the pipeline against its real source and return the run as a fixture.

    The only function here that touches real systems: it fetches from the source
    and lets the steps make their calls. It never sends to the destination.

    Args:
        pipeline: Name, class or instance.
        params: The user's parameters (validated as for any run).
        limit: Stop after this many fetched records.
        redact_fields: Fields to mask (see :func:`reflowfy.testing.diff.redact`), in
            the input, in recorded JSON responses and source records, and so in
            the expected output.
        ignore: Fields left out when a replay is compared, e.g. a timestamp.
        freeze_steps: Steps whose output is saved and replayed instead of run, for
            a step whose calls cannot be recorded (aiohttp, a database driver).
        max_body_kb: Largest HTTP response body stored.

    Raises:
        ValueError: Invalid parameters.
        FixtureError: The pipeline failed, fetched nothing, or the fixture does not
            replay (a step's request depends on a redacted field).
    """
    fork, flat = _prepare(pipeline, dict(params or {}))
    user_params = {k: v for k, v in flat.items() if k not in _RESERVED_PARAMS}
    errors = fork.validate_parameters(user_params)
    if errors:
        raise ValueError(f"Invalid parameters: {'; '.join(errors)}")

    hooks = StepHooks(capture=set(freeze_steps))
    session = Session("record", max_body_bytes=max_body_kb * 1024)
    with activate(session):
        result = execute(
            fork, flat, lambda: fork.define_jobs(flat), session, send=False, limit=limit,
            hooks=hooks,
        )
    if result.error is not None:
        raise FixtureError(
            "the pipeline failed while recording, so there is nothing to save: "
            f"{type(result.error).__name__}: {result.error}"
        ) from result.error
    kept = [recorded for recorded in result.jobs if recorded.fetched]
    if not kept:
        raise FixtureError(
            "the pipeline fetched no records, so there is nothing to save. Check the "
            "parameters, or the source."
        )
    never_ran = [name for name in freeze_steps if name not in hooks.captured]
    if never_ran:
        ran = sorted({step.name for step in result.steps}) or ["none"]
        raise FixtureError(
            f"--freeze-step {never_ran[0]}: no step of that name ran. Steps that ran: "
            f"{', '.join(ran)}"
        )

    fixture = PipelineFixture(
        pipeline=fork.name,
        params=to_json_safe(dict(fork.apply_defaults(dict(params or {})))),
        context={key: flat[key] for key in _CONTEXT_KEYS},
        jobs=[redact(recorded.fetched, redact_fields) for recorded in kept],
        job_params=[
            to_json_safe(
                {
                    key: value
                    for key, value in recorded.job_params.items()
                    if key not in _RESERVED_PARAMS and flat.get(key, _RESERVED_PARAMS) != value
                }
            )
            for recorded in kept
        ],
        http=[_redact_exchange(exchange, redact_fields) for exchange in session.recorded_http],
        source_calls=[
            {**call, "records": redact(call["records"], redact_fields)}
            for call in session.recorded_source_calls
        ],
        step_outputs={
            name: [redact(outputs.get(recorded.index), redact_fields) for recorded in kept]
            for name, outputs in hooks.captured.items()
        },
        frozen_steps=list(freeze_steps),
        ignore=list(ignore),
        redacted=list(redact_fields),
        recorded_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        reflowfy_version=reflowfy.__version__,
    )

    try:
        replayed = replay(fixture)
    except UnmockedCallError as exc:
        raise FixtureError(
            f"recorded, but the fixture does not replay: {exc} A step that builds a "
            "request from a redacted field, or from something that changes on every run "
            "(a timestamp, a random id), cannot be replayed: freeze it with --freeze-step."
        ) from exc
    if replayed.error is not None:
        raise FixtureError(
            "recorded, but the pipeline fails when the fixture is replayed: "
            f"{type(replayed.error).__name__}: {replayed.error}"
        ) from replayed.error
    fixture.expected = expected_from(replayed)

    # A second replay must give the same output, or the fixture can never pass.
    unstable = diff_fixture(replay(fixture), fixture)
    if unstable:
        fields = sorted({_field_of(line) for line in unstable})
        raise FixtureError(
            "the pipeline's output changes from one run to the next, so this fixture "
            f"would fail every time:\n{summarize(unstable, limit=5)}\n"
            f"Leave what changes out of the comparison (--ignore {','.join(fields)}), "
            "or freeze the step that produces it (--freeze-step)."
        )
    return fixture


def _field_of(line: str) -> str:
    """The field a difference line is about: ``job 0, record 1: a.b[2].c: 1 → 2`` -> ``c``."""
    path = line.split(": ")[1] if line.startswith("job ") else line.split(":")[0]
    return path.split(".")[-1].split("[")[0]
