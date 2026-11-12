"""Test a pipeline locally without Docker.

One execution path serves both plain and ID-based pipelines: each "batch" (a
single run, or one ID batch) goes through :func:`run_batch`, and everything
else is presentation. That matters because the reporting below is the whole
point of the command, and it used to be written twice.

Planning, fetching, transforming and sending go through the same loop
``reflowfy.testing`` uses (``reflowfy.testing.runner.execute``), which drives the
worker's per-job core (``reflowfy.execution.job_runner``). What you see here is
what a test asserts and what production does; this module only presents it.
"""

import importlib.util
import json
import logging
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, TypeGuard, cast

import typer

import reflowfy

if TYPE_CHECKING:  # Runtime imports stay lazy — these are only for annotations.
    from reflowfy.core.abstract_pipeline import AbstractPipeline
    from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.cli.utils import console
from reflowfy.core.exceptions import PipelineError
from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.execution.job_runner import preview_runtime_params
from reflowfy.execution.transformation_runner import AppliedStep
from reflowfy.observability.logging import get_logger, setup_logging
from reflowfy.transformations.base import TransformationError, top_skip_reasons

logger = get_logger("cli.test")

# Repr length beyond which a source/destination is truncated at verbosity 0.
# A mock source's repr embeds every record it holds; dumping that before the
# run buries the output that matters.
REPR_BUDGET = 160

# Records shown in the sample block, by verbosity level.
SAMPLE_SIZE = {0: 3, 1: 10}

# Where the framework lives, so its frames can be dropped from tracebacks.
FRAMEWORK_ROOT = str(Path(reflowfy.__file__).resolve().parent)


@dataclass
class TestOptions:
    """Everything the flags control, so the renderers take one argument."""

    limit: int = 100
    dry_run: bool = False
    verbose: int = 0
    fail_fast: bool = False
    as_json: bool = False


@dataclass
class BatchReport:
    """What one batch did. The unit both pipeline shapes report in."""

    label: Optional[str] = None
    source_repr: str = ""
    fetched: int = 0
    transformed: List[Any] = field(default_factory=lambda: [])
    applied: List[AppliedStep] = field(default_factory=lambda: [])
    error: Optional[BaseException] = None
    sent: bool = False
    sent_count: int = 0
    jobs_dropped: List[str] = field(default_factory=lambda: [])
    """``SkipJob`` reasons, one per job the pipeline dropped."""

    @property
    def ok(self) -> bool:
        return self.error is None

    @property
    def skipped(self) -> int:
        """Records the batch's per-record steps dropped with ``SkipRecord``."""
        return sum(s.records_skipped for s in self.applied)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "label": self.label,
            "records_fetched": self.fetched,
            "records_out": len(self.transformed),
            "records_skipped": self.skipped,
            "sent": self.sent,
            "sent_count": self.sent_count,
            "jobs_dropped": list(self.jobs_dropped),
            "steps": [
                {
                    "name": s.name,
                    "records_in": s.records_in,
                    "records_out": s.records_out,
                    "duration_ms": round(s.duration * 1000, 3),
                    "records_skipped": s.records_skipped,
                    "skip_reasons": dict(s.skip_reasons or {}),
                }
                for s in self.applied
            ],
            "ok": self.ok,
        }
        if self.error is not None:
            out["error"] = describe_error(self.error)
        return out


# ---------------------------------------------------------------------------
# Error description
# ---------------------------------------------------------------------------


def describe_error(exc: BaseException) -> Dict[str, Any]:
    """Structured view of a failure, shared by the rich card and ``--json``."""
    info: Dict[str, Any] = {
        "type": type(exc).__name__,
        "message": str(exc),
    }
    if isinstance(exc, TransformationError):
        info["transformation"] = exc.transformation_name
        info["step_index"] = exc.step_index
        info["records_in"] = exc.records_in
        info["culprit_index"] = exc.culprit_index
        info["culprit_record"] = exc.culprit_record
    if isinstance(exc, PipelineError) and exc.original_error is not None:
        info["cause"] = f"{type(exc.original_error).__name__}: {exc.original_error}"
    return info


def root_cause(exc: BaseException) -> BaseException:
    """The user's actual exception, unwrapping the framework's wrapper."""
    original = getattr(exc, "original_error", None)
    return original if isinstance(original, BaseException) else exc


def user_frames(exc: BaseException) -> List[traceback.FrameSummary]:
    """Traceback frames belonging to the user's code, framework frames dropped.

    The framework sits between the CLI and the user's ``apply``, so an
    unfiltered traceback is mostly reflowfy internals. If filtering leaves
    nothing (the failure really was inside the framework), return everything
    rather than an empty trace.
    """
    tb = root_cause(exc).__traceback__ or exc.__traceback__
    if tb is None:
        return []
    frames = traceback.extract_tb(tb)
    user = [f for f in frames if not str(Path(f.filename).resolve()).startswith(FRAMEWORK_ROOT)]
    return user or list(frames)


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def summarize(value: Any, verbose: int) -> str:
    """Repr of a source/destination, truncated unless the user asked for more."""
    text = repr(value)
    if verbose >= 1 or len(text) <= REPR_BUDGET:
        return text
    return f"{text[:REPR_BUDGET]}… [dim](+{len(text) - REPR_BUDGET} chars, -v for full)[/dim]"


def render_steps(report: BatchReport, opts: TestOptions, indent: str = "") -> None:
    """One line per transformation: real in/out counts, and timing under -v."""
    for step in report.applied:
        delta = step.delta
        if delta < 0:
            change = f"[yellow]{delta}[/yellow]"
        elif delta > 0:
            change = f"[cyan]+{delta}[/cyan]"
        else:
            change = "[dim]±0[/dim]"

        timing = ""
        if opts.verbose >= 1:
            mode = step.mode if step.concurrency is None else f"{step.mode} ×{step.concurrency}"
            timing = f"  [dim]{step.duration * 1000:.1f}ms, {mode}[/dim]"
        marker = "[green]✓[/green]"
        line = (
            f"{indent}  {marker} [bold]{step.name}[/bold]  "
            f"{step.records_in} → {step.records_out} records  ({change}){timing}"
        )
        console.print(line)
        for reason, times in sorted((step.skip_reasons or {}).items(), key=lambda kv: -kv[1]):
            console.print(f"{indent}    [dim]skipped {times}: {reason}[/dim]")

        # An empty batch after a step is the most common silent bug: everything
        # downstream is a no-op and the run still "succeeds".
        if step.records_out == 0 and step.records_in > 0:
            console.print(
                f"{indent}    [yellow]⚠️  this step emptied the batch — "
                f"nothing downstream will run[/yellow]"
            )
    # The step that broke the chain, in the same shape as the ones that worked.
    error = report.error
    if isinstance(error, TransformationError):
        received = f"{error.records_in} records in" if error.records_in is not None else ""
        console.print(
            f"{indent}  [red]✗[/red] [bold red]{error.transformation_name}[/bold red]  "
            f"[red]{received} → failed[/red]"
        )

    if opts.verbose >= 1 and report.applied:
        total = sum(s.duration for s in report.applied)
        console.print(f"{indent}  [dim]total transform time: {total * 1000:.1f}ms[/dim]")


def render_failure(exc: BaseException, opts: TestOptions, indent: str = "") -> None:
    """The failure card: which step, which record, which line of user code."""
    cause = root_cause(exc)

    if isinstance(exc, TransformationError):
        position = f"#{exc.step_index + 1} " if exc.step_index is not None else ""
        console.print(
            f"\n{indent}[bold red]❌ Transformation {position}failed:[/bold red] "
            f"[bold]{exc.transformation_name}[/bold]"
        )
    elif isinstance(exc, PipelineError):
        console.print(f"\n{indent}[bold red]❌ Pipeline step failed[/bold red]")
    else:
        console.print(f"\n{indent}[bold red]❌ Failed[/bold red]")

    console.print(f"{indent}   [red]{type(cause).__name__}: {cause}[/red]")

    if isinstance(exc, TransformationError):
        if exc.records_in is not None:
            console.print(f"{indent}   [dim]received {exc.records_in} records[/dim]")

        if exc.culprit_record is not None:
            where = f"index {exc.culprit_index}"
            if exc.records_in:
                where += f" of {exc.records_in}"
            console.print(f"\n{indent}   [bold]Failing record[/bold] [dim]({where})[/dim]:")
            rendered = json.dumps(exc.culprit_record, default=str, indent=2)
            if opts.verbose == 0 and len(rendered) > 600:
                rendered = rendered[:600] + "\n… (-v for the full record)"
            for line in rendered.splitlines():
                console.print(f"{indent}     [dim]{line}[/dim]")
        elif isinstance(cause, KeyError):
            console.print(
                f"{indent}   [dim]no single record identified — the key may be "
                f"missing from a nested value[/dim]"
            )

    frames = user_frames(exc)
    if frames:
        console.print(f"\n{indent}   [bold]Where[/bold]:")
        shown = frames if opts.verbose >= 1 else frames[-3:]
        for frame in shown:
            console.print(
                f"{indent}     [cyan]{frame.filename}:{frame.lineno}[/cyan] in {frame.name}"
            )
            if frame.line:
                console.print(f"{indent}       [dim]{frame.line.strip()}[/dim]")
        if opts.verbose == 0 and len(frames) > len(shown):
            console.print(f"{indent}     [dim](-v for the full traceback)[/dim]")

    if opts.verbose >= 2:
        console.print(f"\n{indent}   [dim]--- full traceback ---[/dim]")
        console.print(
            "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            style="dim",
        )


def render_records(report: BatchReport, opts: TestOptions, indent: str = "") -> None:
    """Sample of the transformed records."""
    if not report.transformed:
        return
    count = SAMPLE_SIZE.get(opts.verbose, len(report.transformed))
    shown = report.transformed[: min(count, len(report.transformed))]
    console.print(
        f"\n{indent}[bold]📋 Sample output ({len(shown)} of {len(report.transformed)} "
        f"records):[/bold]"
    )
    budget = 500 if opts.verbose == 0 else 5000
    for i, record in enumerate(shown, 1):
        rendered = json.dumps(record, default=str, indent=2)[:budget]
        console.print(f"{indent}  [dim]Record {i}:[/dim]")
        for line in rendered.splitlines():
            console.print(f"{indent}    {line}")


def render_params(params: Dict[str, Any], opts: TestOptions, indent: str = "") -> None:
    """Under -v, show the flat runtime_params the chain actually ran with."""
    if opts.verbose < 1:
        return
    console.print(f"\n{indent}[bold]🔧 runtime_params:[/bold]")
    for key in sorted(params):
        console.print(f"{indent}  [dim]{key}[/dim] = {params[key]!r}")


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _send_job(
    destination: Any,
    records: List[Any],
    job_params: Dict[str, Any],
    opts: TestOptions,
    report: BatchReport,
    indent: str = "",
) -> None:
    """Send one job's records to its own resolved destination.

    Mirrors the worker (``reflowfy/worker/executor.py``): each job's
    destination — already resolved against that job's transformed records —
    health-checks and sends independently. Raises on failure, which fails the
    job and stops the batch.
    """
    console.print(f"{indent}[bold]📤 Destination:[/bold] {summarize(destination, opts.verbose)}")

    async def _send() -> None:
        if not await destination.health_check():
            raise PipelineError("destination health check failed")
        await destination.send_with_retry(records, job_params)

    run_coroutine_sync(_send())
    report.sent = True
    report.sent_count += 1
    console.print(f"{indent}[green]✓ Sent {len(records)} records[/green]")


def run_batch(
    pipeline: "AbstractPipeline[Any]",
    source: Any,
    flat_params: Dict[str, Any],
    opts: TestOptions,
    label: Optional[str] = None,
    indent: str = "",
) -> BatchReport:
    """Fetch + transform + send each job, through the same loop ``reflowfy.testing`` uses.

    That loop (``reflowfy.testing.runner.execute``) drives the worker's per-job
    core, so what this command shows is what a test asserts and what production
    does. Only presentation lives here: how a job is sent (printed and
    health-checked) and which logger the skip lines go to.

    Sends per job — matching production, where each job resolves and sends
    its own destination — rather than merging every job's records into one
    request. A pipeline with ``docs_per_job=1`` splitting into 30 jobs makes
    30 destination sends here, the same as it would over Kafka.

    Never raises: the failure is captured on the report so the caller decides
    whether to stop or carry on to the next batch.
    """
    from reflowfy.testing.cassette import Session, activate, current_session
    from reflowfy.testing.runner import execute

    report = BatchReport(label=label, source_repr=repr(source))

    def send(destination: Any, records: List[Any], job_params: Dict[str, Any]) -> None:
        _send_job(destination, records, job_params, opts, report, indent)

    def go(session: Session) -> None:
        # A private copy: the run instruments define_transformations on it.
        result = execute(
            pipeline.fork(),
            flat_params,
            lambda: source,
            session,
            send=not opts.dry_run,
            limit=opts.limit,
            sender=send,
            job_logger=logger,
        )
        report.fetched = result.fetched
        report.transformed = result.records
        report.applied = result.steps
        report.jobs_dropped = result.jobs_dropped
        report.error = result.error

    # Nothing is mocked here: a preview talks to the real systems.
    active = current_session()
    if active is not None:
        go(active)
    else:
        with activate(Session("passthrough")) as session:
            go(session)

    exc = report.error
    if isinstance(exc, TransformationError) and report.fetched == 0:
        # A source that is not a BaseSource cannot be watched; the failing step
        # still knows how many records it was given.
        if report.applied:
            report.fetched = report.applied[0].records_in
        elif exc.records_in:
            report.fetched = exc.records_in
    return report


def report_batch(report: BatchReport, opts: TestOptions, indent: str = "") -> None:
    """Render one batch's outcome."""
    if report.error is not None and report.fetched == 0 and not report.applied:
        render_failure(report.error, opts, indent)
        return

    for reason in report.jobs_dropped:
        console.print(f"{indent}[yellow]⏭️  Job dropped by pipeline (SkipJob): {reason}[/yellow]")
    console.print(f"{indent}[green]✓ Fetched {report.fetched} records[/green]")
    if report.fetched == 0:
        if report.jobs_dropped:
            return
        console.print(f"{indent}[yellow]⚠️  No records returned from source[/yellow]")
        return

    render_steps(report, opts, indent)
    if report.error is not None:
        render_failure(report.error, opts, indent)
        return
    render_records(report, opts, indent)


def build_flat_params(pipeline: "AbstractPipeline[Any]", params: Dict[str, Any]) -> Dict[str, Any]:
    """The flat runtime_params a run starts from (mirrors the local executor)."""
    return preview_runtime_params(pipeline.name, params)


# ---------------------------------------------------------------------------
# Pipeline + parameter resolution
# ---------------------------------------------------------------------------


def load_from_path(path: Path) -> None:
    """Import a single pipeline file, triggering metaclass registration."""
    spec = importlib.util.spec_from_file_location("test_pipeline_module", str(path))
    if spec is None or spec.loader is None:
        console.print(f"[red]❌ Cannot load: {path}[/red]")
        raise typer.Exit(1)
    try:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as exc:
        console.print(f"[red]❌ Error loading pipeline file: {exc}[/red]")
        raise typer.Exit(1)


def resolve_pipeline(target: str, no_input: bool) -> "AbstractPipeline[Any]":
    """Find the pipeline named (or filed at) ``target``.

    A name is tried first, because every other part of reflowfy — the API, the
    scheduler, the job payload — keys on the pipeline name. A path still works.
    """
    from reflowfy.core.registry import pipeline_registry

    path = Path(target)
    if path.suffix == ".py" or path.exists():
        if not path.exists():
            console.print(f"[red]❌ File not found: {target}[/red]")
            raise typer.Exit(1)
        load_from_path(path.resolve())
    else:
        from reflowfy.core.pipeline_discovery import discover_and_load_pipelines

        discover_and_load_pipelines()
        pipeline = pipeline_registry.get(target)
        if pipeline is not None:
            return pipeline
        console.print(
            f"[red]❌ No pipeline named '{target}'"
            f"{pipeline_registry.describe_missing(target)}[/red]"
        )
        known = pipeline_registry.list_names()
        if known:
            console.print(f"[dim]Registered pipelines: {', '.join(sorted(known))}[/dim]")
        raise typer.Exit(1)

    pipelines = pipeline_registry.list_all()
    if not pipelines:
        console.print(
            "[red]❌ No pipelines found in the file. A pipeline registers itself when "
            "its class defines a `name`; if its __init__ raised, it was skipped.[/red]"
        )
        raise typer.Exit(1)

    if len(pipelines) == 1:
        return pipelines[0]

    if no_input:
        console.print(
            f"[red]❌ {len(pipelines)} pipelines in that file; name the one you want.[/red]"
        )
        console.print(f"[dim]Found: {', '.join(p.name for p in pipelines)}[/dim]")
        raise typer.Exit(1)

    from rich.prompt import Prompt

    console.print("\n[bold]Found multiple pipelines:[/bold]")
    for i, p in enumerate(pipelines, 1):
        console.print(f"  {i}. {p.name}")
    choice = Prompt.ask("Select pipeline number", default="1")
    try:
        return pipelines[int(choice) - 1]
    except (ValueError, IndexError):
        console.print("[red]Invalid choice[/red]")
        raise typer.Exit(1)


def parse_param_flags(values: Sequence[str]) -> Dict[str, str]:
    """Turn ``--param key=value`` repeats into a dict."""
    parsed: Dict[str, str] = {}
    for raw in values:
        key, sep, value = raw.partition("=")
        if not sep or not key:
            console.print(f"[red]❌ --param expects key=value, got '{raw}'[/red]")
            raise typer.Exit(2)
        parsed[key.strip()] = value
    return parsed


def collect_params(
    pipeline: "AbstractPipeline[Any]", supplied: Dict[str, str], no_input: bool
) -> Dict[str, Any]:
    """Resolve the pipeline's declared parameters from flags, defaults, prompts."""
    from rich.prompt import Confirm, Prompt

    params: Dict[str, Any] = {}
    parameters = pipeline.get_all_parameters()

    if not parameters:
        if supplied:
            console.print(
                f"[yellow]⚠️  Pipeline declares no parameters; passing "
                f"{', '.join(supplied)} through anyway[/yellow]"
            )
            params.update(supplied)
        else:
            console.print("\n[dim]No parameters needed for this pipeline.[/dim]")
        return params

    console.print("\n[bold]📝 Pipeline parameters:[/bold]")
    for param in parameters:
        # 1. Given on the command line.
        if param.name in supplied:
            params[param.name] = param.coerce(supplied[param.name])
            console.print(
                f"  [dim]{param.name}[/dim] = {params[param.name]!r} [dim](--param)[/dim]"
            )
            continue

        hints = [param.type_name]
        if param.choices:
            hints.append(f"choices: {param.choices}")
        if param.required:
            hints.append("required")
        label = f"  {param.name}"
        if param.description:
            label += f" [dim]({param.description})[/dim]"

        # 2. Non-interactive: default, or fail.
        if no_input:
            if param.required and param.default is None:
                console.print(
                    f"[red]❌ Required parameter '{param.name}' not supplied "
                    f"(--param {param.name}=...) and --no-input is set[/red]"
                )
                raise typer.Exit(2)
            if param.default is not None:
                params[param.name] = param.default
                console.print(f"  [dim]{param.name}[/dim] = {param.default!r} [dim](default)[/dim]")
            continue

        # 3. Prompt.
        console.print(f"{label}  [dim]{', '.join(hints)}[/dim]")
        default_val = param.default
        if param.param_type is bool:
            value = (
                Confirm.ask(f"    → {param.name}", default=bool(default_val))
                if default_val is not None
                else Confirm.ask(f"    → {param.name}")
            )
        else:
            if default_val is not None:
                default_str: Any = str(default_val)
            elif not param.required:
                default_str = ""
            else:
                default_str = ...
            raw = Prompt.ask(f"    → {param.name}", default=default_str)
            value = param.coerce(raw)

        if param.choices and value not in param.choices:
            console.print(
                f"[yellow]⚠️  '{value}' not in choices {param.choices}, using anyway[/yellow]"
            )
        params[param.name] = value

    return params


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def register(app: typer.Typer):
    """Register the test command."""

    @app.command()
    def test(
        pipeline: str = typer.Argument(
            ...,
            help="Pipeline name (e.g. my_pipeline) or file path (pipelines/my_pipeline.py)",
        ),
        limit: int = typer.Option(
            100, "--limit", "-l", help="Maximum number of records to process"
        ),
        dry_run: bool = typer.Option(
            False, "--dry-run", help="Print records instead of sending to destination"
        ),
        verbose: int = typer.Option(
            0,
            "--verbose",
            "-v",
            count=True,
            help="Show more: -v adds timings, params and full tracebacks; -vv adds all records",
        ),
        param: List[str] = typer.Option(
            [], "--param", "-p", help="Set a parameter: -p key=value (repeatable)"
        ),
        no_input: bool = typer.Option(
            False, "--no-input", help="Never prompt; fail if a required parameter is missing"
        ),
        fail_fast: bool = typer.Option(
            False,
            "--fail-fast",
            help="For ID pipelines, stop at the first failed batch and exit non-zero",
        ),
        as_json: bool = typer.Option(
            False, "--json", help="Emit a machine-readable report on stdout"
        ),
        save_fixture: Optional[Path] = typer.Option(
            None,
            "--save-fixture",
            help="Record this run (input, what the steps' calls answered, expected output) "
            "to a JSON file your tests replay offline. Never sends to the destination.",
        ),
        fixture: Optional[Path] = typer.Option(
            None,
            "--fixture",
            help="Replay a recorded fixture offline and compare with its expected output",
        ),
        update_fixture: bool = typer.Option(
            False,
            "--update-fixture",
            help="With --fixture: rewrite the expected output from the saved input",
        ),
        redact: List[str] = typer.Option(
            [],
            "--redact",
            help="With --save-fixture: mask these fields in the file (key or dotted path, "
            "comma-separated or repeated)",
        ),
        ignore: List[str] = typer.Option(
            [],
            "--ignore",
            help="With --save-fixture: fields left out of the comparison, e.g. a timestamp",
        ),
        freeze_step: List[str] = typer.Option(
            [],
            "--freeze-step",
            help="With --save-fixture: save this step's output and replay it instead of "
            "running the step (for calls that cannot be recorded)",
        ),
        max_body_kb: int = typer.Option(
            256, "--max-body-kb", help="With --save-fixture: largest HTTP response stored"
        ),
    ):
        """
        Test a pipeline locally without Docker.

        Resolves the pipeline by name (or file path), fetches a capped sample
        through the same execution core the worker uses, applies the
        transformations, and reports what each step did.

        Exits non-zero if any batch fails, so it can gate CI.
        """
        from rich.panel import Panel

        opts = TestOptions(
            limit=limit,
            dry_run=dry_run,
            verbose=verbose,
            fail_fast=fail_fast,
            as_json=as_json,
        )

        # Wire up logging BEFORE importing the pipeline file, so the user sees both
        # their own get_logger() output and any pipeline that fails to register.
        # Plain text, not JSON — JSON lines are unreadable in a terminal preview.
        os.environ.setdefault("LOG_JSON", "false")
        if opts.verbose >= 1:
            os.environ["LOG_LEVEL"] = "DEBUG"
        setup_logging(service_name="cli")

        # --json is a data channel; rich output would corrupt it, and so would log
        # lines on stdout (skips are logged), so those move to stderr.
        if opts.as_json:
            console.quiet = True
            for handler in logging.getLogger("reflowfy").handlers:
                if getattr(handler, "stream", None) is sys.stdout:
                    cast("logging.StreamHandler[Any]", handler).setStream(sys.stderr)

        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)

        resolved = resolve_pipeline(pipeline, no_input=no_input)
        console.print(Panel(f"🧪 Testing [bold]{resolved.name}[/bold]", style="cyan"))

        if update_fixture and fixture is None:
            console.print("[red]❌ --update-fixture needs --fixture <file>[/red]")
            raise typer.Exit(2)
        if fixture is not None:
            replay_fixture_file(resolved, fixture, update=update_fixture)
            return

        params = collect_params(resolved, parse_param_flags(param), no_input=no_input)
        if save_fixture is not None:
            record_fixture_file(
                resolved,
                params,
                save_fixture,
                limit=opts.limit,
                redact=split_fields(redact),
                ignore=split_fields(ignore),
                freeze_steps=split_fields(freeze_step),
                max_body_kb=max_body_kb,
            )
            return
        console.print(f"\n[bold]⚙️  Params:[/bold] {params}")
        console.print(f"[bold]📊 Record limit:[/bold] {opts.limit}")

        reports = run_pipeline_test(resolved, params, opts)

        failed = [r for r in reports if not r.ok]
        total_records = sum(len(r.transformed) for r in reports)
        total_skipped = sum(r.skipped for r in reports)
        # A single-batch run has nothing to be partial about, so any failure
        # fails the run. Per-batch ID failures are tolerated unless asked not.
        strict = opts.fail_fast or not runs_id_batches(resolved)

        if opts.as_json:
            console.quiet = False
            payload = {
                "pipeline": resolved.name,
                "params": {k: v for k, v in params.items()},
                "limit": opts.limit,
                "dry_run": opts.dry_run,
                "batches": [r.to_dict() for r in reports],
                "records_out": total_records,
                "records_skipped": total_skipped,
                "ok": not failed,
            }
            typer.echo(json.dumps(payload, default=str, indent=2))
            raise typer.Exit(1 if failed and strict else 0)

        if opts.dry_run:
            console.print("\n[yellow]🏜️  Dry run — skipped destination send[/yellow]")

        sent = any(r.sent for r in reports)
        outcome = "records sent successfully" if sent else "records processed"
        if total_skipped:
            outcome += f", {total_skipped} skipped by SkipRecord"

        if failed:
            console.print(
                f"\n[bold red]❌ Test failed: {len(failed)} of {len(reports)} "
                f"batch(es) errored[/bold red]"
            )
            # An ID pipeline previews many independent batches, and one bad ID
            # should not condemn the rest — a partial failure is reported but
            # does not fail the run. --fail-fast opts into the strict reading.
            if strict:
                raise typer.Exit(1)
            console.print(
                f"[yellow]⚠️  Finished with failures: {total_records} {outcome} "
                f"from {len(reports) - len(failed)} of {len(reports)} batch(es). "
                f"Use --fail-fast to exit non-zero.[/yellow]"
            )
            return

        console.print(f"\n[bold green]✅ Test complete: {total_records} {outcome}[/bold green]")


# ---------------------------------------------------------------------------
# Fixtures (reflowfy.testing)
# ---------------------------------------------------------------------------


def split_fields(values: Sequence[str]) -> List[str]:
    """``--redact a,b --redact c`` -> ``["a", "b", "c"]``."""
    return [name.strip() for value in values for name in value.split(",") if name.strip()]


def record_fixture_file(
    pipeline: "AbstractPipeline[Any]",
    params: Dict[str, Any],
    path: Path,
    *,
    limit: int,
    redact: List[str],
    ignore: List[str],
    freeze_steps: List[str],
    max_body_kb: int,
) -> None:
    """``--save-fixture``: run against the real source and write the run to ``path``."""
    from reflowfy.testing.cassette import FixtureError
    from reflowfy.testing.fixtures import record_fixture

    console.print(f"\n[cyan]Recording (limit={limit}); nothing is sent...[/cyan]")
    try:
        recorded = record_fixture(
            pipeline,
            params,
            limit=limit,
            redact_fields=redact,
            ignore=ignore,
            freeze_steps=freeze_steps,
            max_body_kb=max_body_kb,
        )
    except (FixtureError, ValueError) as exc:
        console.print(f"[red]❌ {exc}[/red]")
        raise typer.Exit(1)
    recorded.save(path)

    expected = recorded.expected
    fetched = sum(len(records) for records in recorded.jobs)
    would_send = sum(len(records) for records in expected["sent"])
    console.print(f"[green]✓ Fetched {fetched} records in {len(recorded.jobs)} job(s)[/green]")
    if recorded.http:
        console.print(f"[green]✓ Recorded {len(recorded.http)} HTTP call(s)[/green]")
    if recorded.source_calls:
        console.print(f"[green]✓ Recorded {len(recorded.source_calls)} source call(s)[/green]")
    if recorded.frozen_steps:
        console.print(f"[green]✓ Frozen step(s): {', '.join(recorded.frozen_steps)}[/green]")
    skipped = expected["records_skipped"]
    outcome = f"{would_send} would be sent"
    if skipped:
        outcome += f", {skipped} skipped ({top_skip_reasons(expected['skip_reasons'])})"
    console.print(f"[green]✓ {outcome}[/green]")
    console.print(f"[bold green]✅ Fixture written: {path}[/bold green]")
    console.print(
        "[yellow]⚠️  It contains real data. Review it before committing "
        "(--redact masks fields).[/yellow]"
    )
    console.print(f"[dim]Replay it offline: pytest, or reflowfy test {pipeline.name} --fixture {path}[/dim]")


def replay_fixture_file(pipeline: "AbstractPipeline[Any]", path: Path, update: bool) -> None:
    """``--fixture``: replay offline and compare; ``--update-fixture`` rewrites the expectation."""
    from reflowfy.testing.cassette import FixtureError, UnmockedCallError
    from reflowfy.testing.diff import summarize
    from reflowfy.testing.fixtures import diff_fixture, load_fixture, replay
    from reflowfy.testing.fixtures import update_fixture as rewrite_expected

    try:
        loaded = load_fixture(path)
        if loaded.pipeline != pipeline.name:
            raise FixtureError(
                f"{path} was recorded for pipeline '{loaded.pipeline}', not '{pipeline.name}'"
            )
        if update:
            _, changes = rewrite_expected(loaded)
        else:
            changes = diff_fixture(replay(loaded), loaded)
    except (FixtureError, UnmockedCallError) as exc:
        console.print(f"[red]❌ {exc}[/red]")
        raise typer.Exit(1)

    if update:
        if changes:
            console.print(f"[bold]Expected output updated ({len(changes)} change(s)):[/bold]")
            console.print(summarize(changes), markup=False)
            console.print(f"[bold green]✅ Fixture updated: {path}[/bold green]")
        else:
            console.print("[green]✅ Nothing changed: the fixture already matches[/green]")
        return
    if changes:
        console.print(f"[bold red]❌ {len(changes)} difference(s) from {path}:[/bold red]")
        console.print(summarize(changes), markup=False)
        console.print(
            "[dim]If the change is intended, add --update-fixture and review the file's diff.[/dim]"
        )
        raise typer.Exit(1)
    console.print(f"[bold green]✅ Matches {path}[/bold green]")


def run_pipeline_test(
    pipeline: "AbstractPipeline[Any]", params: Dict[str, Any], opts: TestOptions
) -> List[BatchReport]:
    """Run the pipeline, as one batch or one per ID batch, and report each."""
    if runs_id_batches(pipeline):
        return run_id_batches(pipeline, params, opts)
    return [run_single(pipeline, params, opts)]


def runs_id_batches(pipeline: "AbstractPipeline[Any]") -> TypeGuard["IdBasedPipeline[Any]"]:
    """True if this pipeline is previewed one batch of IDs at a time.

    A pipeline that overrides ``define_jobs`` owns its own splitting, so it
    takes the standard path even when it is ID-based — the per-ID loop calls
    ``define_source``, which such a pipeline need not implement.
    """
    from reflowfy.core.id_based_pipeline import IdBasedPipeline

    if not isinstance(pipeline, IdBasedPipeline):
        return False
    return type(pipeline).define_jobs is IdBasedPipeline.define_jobs


def run_single(
    pipeline: "AbstractPipeline[Any]", params: Dict[str, Any], opts: TestOptions
) -> BatchReport:
    """The standard path: one source (or job plan), one report."""
    flat = build_flat_params(pipeline, dict(params))

    try:
        source = pipeline.define_jobs(flat)
    except Exception as exc:  # noqa: BLE001 — reported, not swallowed
        report = BatchReport(error=exc)
        render_failure(exc, opts)
        return report

    console.print(f"\n[bold]🔌 Source:[/bold] {summarize(source, opts.verbose)}")
    render_params(flat, opts)
    console.print(f"\n[cyan]Fetching records (limit={opts.limit})...[/cyan]")

    report = run_batch(pipeline, source, flat, opts)
    report_batch(report, opts)
    return report


def run_id_batches(
    pipeline: "IdBasedPipeline[Any]", params: Dict[str, Any], opts: TestOptions
) -> List[BatchReport]:
    """The ID-based path: one report per batch of IDs."""
    ids: List[Any] = list(params.get("input_ids") or [])
    if not ids:
        console.print("[red]❌ No IDs provided. The 'input_ids' parameter is required.[/red]")
        raise typer.Exit(1)

    batch_size: int = pipeline.ids_batch_size
    id_batches: List[List[Any]] = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]
    console.print(
        f"\n[bold]🔑 IdBasedPipeline: {len(ids)} ID(s) in {len(id_batches)} batch(es) "
        f"(ids_batch_size={batch_size})[/bold]"
    )

    flat_base = build_flat_params(pipeline, dict(params))
    reports: List[BatchReport] = []

    for number, id_batch in enumerate(id_batches, 1):
        label = f"Batch {number}/{len(id_batches)}: {id_batch}"
        console.print(f"\n[bold cyan]━━━ {label} ━━━[/bold cyan]")

        # Fresh per-batch copy so define_source enrichments don't accumulate.
        flat = dict(flat_base)
        flat["current_ids"] = list(id_batch)
        flat["current_id"] = id_batch[0] if id_batch else None

        try:
            source = pipeline.resolve_source(flat)
        except Exception as exc:  # noqa: BLE001 — reported, not swallowed
            report = BatchReport(label=label, error=exc)
            reports.append(report)
            render_failure(exc, opts, indent="  ")
            if opts.fail_fast:
                break
            continue

        console.print(f"  [bold]🔌 Source:[/bold] {summarize(source, opts.verbose)}")
        render_params(flat, opts, indent="  ")

        report = run_batch(pipeline, source, flat, opts, label=label, indent="  ")
        reports.append(report)
        report_batch(report, opts, indent="  ")

        if not report.ok and opts.fail_fast:
            console.print("  [dim]stopping — --fail-fast is set[/dim]")
            break

    return reports
