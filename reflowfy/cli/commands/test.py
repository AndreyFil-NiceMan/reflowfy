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

import inspect
import json
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypeGuard, cast

import typer

if TYPE_CHECKING:  # Runtime imports stay lazy — these are only for annotations.
    from reflowfy.core.abstract_pipeline import AbstractPipeline
    from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.cli.utils import console
from reflowfy.core.exceptions import PipelineError
from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.core.planning import chunk
from reflowfy.destinations.pool import loop_pool_scope
from reflowfy.sources.static import StaticSource
from reflowfy.observability.logging import get_logger, setup_logging
from reflowfy.core.exceptions import TransformationError
from reflowfy.core.record_steps import top_skip_reasons

from reflowfy.cli.commands.preview_inputs import (
    build_flat_params,
    collect_params,
    parse_param_flags,
    resolve_pipeline,
    split_fields,
)
from reflowfy.cli.commands.preview_report import (
    BatchReport,
    TestOptions,
    render_failure,
    render_params,
    report_batch,
    summarize,
)

logger = get_logger("cli.test")


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
        # Closed in the same loop that sent: a Kafka producer belongs to its loop,
        # and an unclosed one leaks a connection per job. So the connection pool
        # is this job's too, closed when the scope ends.
        async with loop_pool_scope():
            try:
                if not await destination.health_check():
                    raise PipelineError("destination health check failed")
                await destination.send_with_retry(records, job_params)
            finally:
                # A duck-typed destination may have no close().
                close = getattr(destination, "close", None)
                if close is not None:
                    closed = close()
                    if inspect.isawaitable(closed):
                        await closed

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
    from reflowfy.testing.runner import execute, private_copy

    report = BatchReport(label=label)

    def send(destination: Any, records: List[Any], job_params: Dict[str, Any]) -> None:
        _send_job(destination, records, job_params, opts, report, indent)

    def go(session: Session) -> None:
        # A private copy: the run instruments define_transformations on it.
        result = execute(
            private_copy(pipeline),
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


def register(app: typer.Typer) -> None:
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
    ) -> None:
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
    console.print(
        f"[dim]Replay it offline: pytest, or reflowfy test {pipeline.name} --fixture {path}[/dim]"
    )


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
    id_batches = chunk(ids, batch_size)
    console.print(
        f"\n[bold]🔑 IdBasedPipeline: {len(ids)} ID(s) in {len(id_batches)} batch(es) "
        f"(ids_batch_size={batch_size})[/bold]"
    )

    flat_base = build_flat_params(pipeline, dict(params))
    reports: List[BatchReport] = []

    for number, id_batch in enumerate(id_batches, 1):
        label = f"Batch {number}/{len(id_batches)}: {id_batch}"
        console.print(f"\n[bold cyan]━━━ {label} ━━━[/bold cyan]")

        try:
            # The manager's own batch planner, so the preview cannot drift from it.
            planned = pipeline.plan_batch(flat_base, id_batch)
        except Exception as exc:  # noqa: BLE001 — reported, not swallowed
            report = BatchReport(label=label, error=exc)
            reports.append(report)
            render_failure(exc, opts, indent="  ")
            if opts.fail_fast:
                break
            continue

        source = planned.source
        if isinstance(source, list):
            source = StaticSource(cast(List[Any], source))
        flat = {**flat_base, **planned.params}
        console.print(f"  [bold]🔌 Source:[/bold] {summarize(source, opts.verbose)}")
        render_params(flat, opts, indent="  ")

        report = run_batch(pipeline, source, flat, opts, label=label, indent="  ")
        reports.append(report)
        report_batch(report, opts, indent="  ")

        if not report.ok and opts.fail_fast:
            console.print("  [dim]stopping — --fail-fast is set[/dim]")
            break

    return reports
