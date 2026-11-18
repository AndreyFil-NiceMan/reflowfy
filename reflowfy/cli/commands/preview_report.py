"""What ``reflowfy test`` prints: options, the per-batch report and its rendering.

Everything here is presentation; running the batch is ``commands.test``.
"""

import json
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional


import reflowfy

if TYPE_CHECKING:  # Runtime imports stay lazy — these are only for annotations.
    pass
from reflowfy.cli.utils import console
from reflowfy.core.exceptions import PipelineError
from reflowfy.execution.transformation_runner import SKIPPED_MODE, AppliedStep
from reflowfy.core.exceptions import TransformationError

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
                    "mode": s.mode,
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


def summarize(value: Any, verbose: int) -> str:
    """Repr of a source/destination, truncated unless the user asked for more."""
    text = repr(value)
    if verbose >= 1 or len(text) <= REPR_BUDGET:
        return text
    return f"{text[:REPR_BUDGET]}… [dim](+{len(text) - REPR_BUDGET} chars, -v for full)[/dim]"


def render_steps(report: BatchReport, opts: TestOptions, indent: str = "") -> None:
    """One line per transformation: real in/out counts, and timing under -v."""
    for step in report.applied:
        if step.mode == SKIPPED_MODE:
            # Shown whatever the verbosity: "why didn't my step run?" is the question.
            console.print(
                f"{indent}  [dim]– {step.name}  not applied "
                f"(should_apply_transformation declined it)[/dim]"
            )
            continue
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
