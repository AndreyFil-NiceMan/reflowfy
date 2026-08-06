"""Test a pipeline locally without Docker."""

from typing import Any, Dict, List, Optional, Tuple
import asyncio
import datetime
import importlib.util
import json
import os
import sys
import traceback
import uuid
from pathlib import Path

import typer

from reflowfy.cli.utils import console
from reflowfy.core.execution_context import ExecutionContext
from reflowfy.execution.job_runner import (
    JobNotSerializableError,
    plan_slices_over_wire,
    run_job_records,
)
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.transformations.base import TransformationError

# Kafka's default max message size. Records ride inside the job payload (see the
# ponytail note in job_runner.plan_slices), so a too-large chunk() only fails at
# dispatch — warn well before the cliff.
KAFKA_DEFAULT_MAX_BYTES = 1_048_576
PAYLOAD_WARN_BYTES = 900_000


def _fmt_size(n: int) -> str:
    """Human-readable byte count."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def _dump(record: Any) -> str:
    """Pretty JSON for one record, tolerating non-JSON values."""
    return json.dumps(record, default=str, indent=2)


def _print_records(indent: str, records: List[Any], verbose: bool, sample: int) -> None:
    """Print records: all of them in full when verbose, else a truncated sample."""
    shown = records if verbose else records[:sample]
    for i, record in enumerate(shown):
        text = _dump(record) if verbose else _dump(record)[:400]
        console.print(f"{indent}[dim][{i}][/dim] {text}")
    if not verbose and len(records) > sample:
        console.print(f"{indent}[dim]… {len(records) - sample} more (-v to see all)[/dim]")


def _run_plan(
    source: Any,
    pipeline: Any,
    meta: Dict[str, Any],
    limit: int,
    verbose: bool,
    indent: str,
    jobs_log: List[Dict[str, Any]],
) -> Tuple[List[Any], int]:
    """Plan the jobs and run each through the shared v2 core.

    Slices are planned with :func:`plan_slices_over_wire`, so every job makes the
    same serialize → JSON → reconstruct hop the manager and worker perform; a
    source that cannot travel fails here rather than after deploy.

    Prints the records entering and leaving every step, and appends one entry per
    job to ``jobs_log`` for ``--out``. Returns ``(transformed_records, fetched)``.
    """
    transformed_all: List[Any] = []
    fetched = 0

    for idx, (sub, payload_bytes) in enumerate(plan_slices_over_wire(source, meta), 1):
        remaining = limit - fetched
        if remaining <= 0:
            break

        job: Dict[str, Any] = {
            "index": idx,
            "source": SourceFactory.serialize(sub),
            "payload_bytes": payload_bytes,
            "steps": [],
        }
        jobs_log.append(job)

        console.print(
            f"\n{indent}[bold cyan]━━━ Job {idx} · {type(sub).__name__} · "
            f"payload {_fmt_size(payload_bytes)} ━━━[/bold cyan]"
        )
        if payload_bytes > PAYLOAD_WARN_BYTES:
            console.print(
                f"{indent}[yellow]⚠️  payload {_fmt_size(payload_bytes)} is near or over "
                f"Kafka's {_fmt_size(KAFKA_DEFAULT_MAX_BYTES)} default — "
                f"use a smaller chunk() size.[/yellow]"
            )

        # The first step's `before` IS the source output, so print it from there
        # and fall back to `records` when a pipeline has no transformations.
        source_shown = [False]

        def show_source(records: List[Any]) -> None:
            if source_shown[0]:
                return
            source_shown[0] = True
            console.print(f"{indent}[green]📥 SOURCE → {len(records)} records[/green]")
            _print_records(indent + "  ", records, verbose, 2)

        def on_step(
            name: str, before: List[Any], after: List[Any], _job: Dict[str, Any] = job
        ) -> None:
            show_source(before)
            _job["steps"].append(
                {"name": name, "in": len(before), "out": len(after), "records": after}
            )
            dropped = len(before) - len(after)
            note = f" [yellow]({dropped} dropped)[/yellow]" if dropped > 0 else ""
            console.print(
                f"{indent}[green]🔄 {name}: {len(before)} → {len(after)} records[/green]{note}"
            )
            _print_records(indent + "  ", after, verbose, 2)

        records, t_records, t_applied, dest = run_job_records(
            sub, pipeline, meta, limit=remaining, on_step=on_step
        )
        show_source(records)

        # `applied` and the observed steps are the same steps in the same order.
        for step, (_name, duration) in zip(job["steps"], t_applied):
            step["duration_ms"] = round(duration * 1000, 3)
        job["records"] = records
        job["destination"] = repr(dest) if dest is not None else None

        fetched += len(records)
        transformed_all.extend(t_records)

    return transformed_all, fetched


def register(app: typer.Typer):
    """Register the test command."""

    @app.command()
    def test(
        pipeline_file: str = typer.Argument(
            ..., help="Path to the pipeline file (e.g., pipelines/my_pipeline.py)"
        ),
        limit: int = typer.Option(
            100, "--limit", "-l", help="Maximum number of records to process"
        ),
        dry_run: bool = typer.Option(
            False, "--dry-run", help="Print records instead of sending to destination"
        ),
        verbose: bool = typer.Option(
            False,
            "--verbose",
            "-v",
            help=(
                "Print every record in full at every step (source and after each "
                "transformation) instead of a truncated sample. Bounded by --limit; "
                "pipe to a pager for large runs."
            ),
        ),
        out: Optional[Path] = typer.Option(
            None,
            "--out",
            help=(
                "Write the whole run to a JSON file: params, per-job source "
                "descriptor and payload size, records at every step, and errors. "
                "Written even if the run fails."
            ),
        ),
    ):
        """
        Test a pipeline locally without Docker.

        Loads the pipeline file, prompts for parameters, and runs
        with a record limit (default 100).

        Every planned job is serialized and reconstructed exactly as the manager
        and worker do it, so a source that cannot survive the trip fails here
        rather than after deploy.
        """
        from rich.panel import Panel
        from rich.prompt import Confirm, Prompt

        pipeline_path = Path(pipeline_file).resolve()
        if not pipeline_path.exists():
            console.print(f"[red]❌ File not found: {pipeline_file}[/red]")
            raise typer.Exit(1)

        # Load the pipeline file
        console.print(
            Panel(f"🧪 Testing pipeline from: [bold]{pipeline_path.name}[/bold]", style="cyan")
        )

        # Ensure cwd is in sys.path for imports
        cwd = os.getcwd()
        if cwd not in sys.path:
            sys.path.insert(0, cwd)

        # Import the pipeline file
        spec = importlib.util.spec_from_file_location("test_pipeline_module", str(pipeline_path))
        if spec is None or spec.loader is None:
            console.print(f"[red]❌ Cannot load: {pipeline_file}[/red]")
            raise typer.Exit(1)

        try:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            console.print(f"[red]❌ Error loading pipeline file: {e}[/red]")
            raise typer.Exit(1)

        # Find registered pipelines
        from reflowfy.core.registry import pipeline_registry

        pipelines = pipeline_registry.list_all()

        if not pipelines:
            console.print(
                "[red]❌ No pipelines found in the file. Make sure your pipeline is registered.[/red]"
            )
            raise typer.Exit(1)

        # If multiple pipelines, let user choose
        if len(pipelines) > 1:
            console.print("\n[bold]Found multiple pipelines:[/bold]")
            for i, p in enumerate(pipelines, 1):
                console.print(f"  {i}. {p.name}")
            choice = Prompt.ask("Select pipeline number", default="1")
            try:
                pipeline = pipelines[int(choice) - 1]
            except (ValueError, IndexError):
                console.print("[red]Invalid choice[/red]")
                raise typer.Exit(1)
        else:
            pipeline = pipelines[0]

        console.print(f"\n[bold green]📦 Pipeline:[/bold green] {pipeline.name}")

        # Detect pipeline type
        from reflowfy.core.id_based_pipeline import IdBasedPipeline

        is_id_based = isinstance(pipeline, IdBasedPipeline)

        if is_id_based:
            batch_size = pipeline.ids_batch_size
            console.print(
                f"[bold cyan]🔑 Pipeline type: IdBasedPipeline (ids_batch_size={batch_size})[/bold cyan]"
            )

        # Prompt for parameters
        # IdBasedPipeline uses get_all_parameters() which includes built-in 'ids'
        params = {}
        parameters = pipeline.get_all_parameters() if is_id_based else pipeline.define_parameters()

        if parameters:
            console.print("\n[bold]📝 Pipeline parameters:[/bold]")
            for param in parameters:
                # Build prompt label
                label = f"  {param.name}"
                if param.description:
                    label += f" [dim]({param.description})[/dim]"

                # Show type info
                type_name = param._TYPE_NAMES.get(  # pyright: ignore[reportPrivateUsage]
                    param.param_type, str(param.param_type)
                )
                hints = [type_name]
                if param.choices:
                    hints.append(f"choices: {param.choices}")
                if param.required:
                    hints.append("required")
                console.print(f"{label}  [dim]{', '.join(hints)}[/dim]")

                # Determine default
                default_val = param.default
                if default_val is not None:
                    default_str = str(default_val)
                elif not param.required:
                    default_str = ""
                else:
                    default_str = None

                # Prompt
                if param.param_type is bool:
                    if default_val is not None:
                        value = Confirm.ask(f"    → {param.name}", default=bool(default_val))
                    else:
                        value = Confirm.ask(f"    → {param.name}")
                else:
                    raw = Prompt.ask(
                        f"    → {param.name}",
                        default=default_str if default_str is not None else ...,
                    )
                    value = param.coerce(raw)

                # Validate choices
                if param.choices and value not in param.choices:
                    console.print(
                        f"[yellow]⚠️  '{value}' not in choices {param.choices}, using anyway[/yellow]"
                    )

                params[param.name] = value
        else:
            console.print("\n[dim]No parameters needed for this pipeline.[/dim]")

        console.print(f"\n[bold]⚙️  Running with params:[/bold] {params}")
        console.print(f"[bold]📊 Record limit:[/bold] {limit}")

        # Everything the run saw, exported by --out. Populated as the run
        # proceeds and written in a finally block, so a crashed run still
        # leaves a log — which is exactly when it is wanted.
        run_log: Dict[str, Any] = {
            "pipeline": pipeline.name,
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "params": params,
            "limit": limit,
            "dry_run": dry_run,
            "jobs": [],
            "batches": [],
            "errors": [],
        }

        def write_out() -> None:
            """Write the run log to --out, if one was requested."""
            if out is None:
                return
            run_log["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            try:
                out.parent.mkdir(parents=True, exist_ok=True)
                with out.open("w", encoding="utf-8") as fh:
                    json.dump(run_log, fh, default=str, indent=2)
                console.print(f"\n[bold]📝 Run log:[/bold] {out}")
            except OSError as e:
                # Never let logging failure mask the run's own result.
                console.print(f"[yellow]⚠️  Could not write run log to {out}: {e}[/yellow]")

        try:
            _run(
                pipeline=pipeline,
                is_id_based=is_id_based,
                params=params,
                limit=limit,
                dry_run=dry_run,
                verbose=verbose,
                run_log=run_log,
            )
        finally:
            write_out()

    def _run(
        pipeline: Any,
        is_id_based: bool,
        params: Dict[str, Any],
        limit: int,
        dry_run: bool,
        verbose: bool,
        run_log: Dict[str, Any],
    ) -> None:
        """Execute the pipeline and fill ``run_log``. Raises typer.Exit on failure."""
        # ================================================================
        # IdBasedPipeline: per-ID execution
        # ================================================================
        if is_id_based:
            ids = params.get("ids", [])
            if not ids:
                console.print("[red]❌ No IDs provided. 'ids' parameter is required.[/red]")
                raise typer.Exit(1)

            # Chunk IDs into batches according to ids_batch_size
            batch_size = pipeline.ids_batch_size
            ids_batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]
            console.print(
                f"\n[bold]🔑 Processing {len(ids)} IDs "
                f"in {len(ids_batches)} batch(es) (batch_size={batch_size}):[/bold] {ids}"
            )

            total_records = 0

            # Build execution context (mirrors local_executor behavior)
            context = ExecutionContext(
                execution_id=str(uuid.uuid4()),
                pipeline_name=pipeline.name,
                runtime_params=params,
            )

            for batch_num, ids_batch in enumerate(ids_batches, 1):
                batch_label = f"Batch {batch_num}/{len(ids_batches)}: {ids_batch}"
                console.print(f"\n[bold cyan]━━━ {batch_label} ━━━[/bold cyan]")

                # Use a fresh per-batch copy so define_source enrichments don't
                # accumulate across batches.
                batch_params = dict(params)
                batch_params["current_ids"] = list(ids_batch)
                batch_params["current_id"] = ids_batch[0] if ids_batch else None

                try:
                    source = pipeline.resolve_source(batch_params)
                except Exception as e:
                    console.print(f"[red]❌ Setup failed for batch {ids_batch}: {e}[/red]")
                    traceback.print_exc()
                    continue

                console.print(f"  [bold]🔌 Source:[/bold] {source}")

                # Build flat mutable runtime_params for this batch's chain.
                meta = dict(batch_params)
                meta.update(
                    {
                        "execution_id": context.execution_id,
                        "batch_id": context.batch_id,
                        "pipeline_name": context.pipeline_name,
                        "created_at": context.created_at.isoformat(),
                        "current_ids": ids_batch,
                        "current_id": ids_batch[0] if ids_batch else None,
                    }
                )

                # Plan slices over the wire and run each through the shared v2
                # core, capped at `limit` across slices.
                console.print(f"  [cyan]Fetching records (limit={limit})...[/cyan]")
                batch_log: List[Dict[str, Any]] = []
                run_log["batches"].append({"ids": list(ids_batch), "jobs": batch_log})
                try:
                    transformed, batch_fetched = _run_plan(
                        source, pipeline, meta, limit, verbose, "  ", batch_log
                    )
                except TransformationError as e:
                    console.print(f"    [red]❌ {e.transformation_name} failed: {e}[/red]")
                    run_log["errors"].append(f"batch {ids_batch}: {e.transformation_name}: {e}")
                    traceback.print_exc()
                    continue
                except JobNotSerializableError as e:
                    # The message names the cause and the fix; a traceback would bury it.
                    console.print(f"  [red]❌ This job cannot reach a worker:[/red] {e}")
                    run_log["errors"].append(f"batch {ids_batch}: {e}")
                    continue
                except Exception as e:
                    console.print(
                        f"  [red]❌ Source fetch/transform failed for batch {ids_batch}: {e}[/red]"
                    )
                    run_log["errors"].append(f"batch {ids_batch}: {e}")
                    traceback.print_exc()
                    continue

                console.print(f"  [green]✓ Fetched {batch_fetched} records[/green]")
                if batch_fetched == 0:
                    console.print(f"  [yellow]⚠️ No records for batch: {ids_batch}[/yellow]")
                    continue

                # Send to destination or dry-run
                if not dry_run:
                    try:
                        destination = pipeline.define_destination(transformed, meta)
                        console.print(f"  [bold]📤 Destination:[/bold] {destination}")

                        async def _send_batch(
                            recs: Any = transformed, m: Any = meta, dest: Any = destination
                        ):
                            await dest.send_with_retry(recs, m)

                        asyncio.run(_send_batch())
                        run_log["sent"] = run_log.get("sent", 0) + len(transformed)
                        console.print(f"  [green]✓ Sent {len(transformed)} records[/green]")
                    except Exception as e:
                        console.print(f"  [red]❌ Send failed for batch {ids_batch}: {e}[/red]")
                        run_log["errors"].append(f"batch {ids_batch} send: {e}")

                total_records += len(transformed)

            if dry_run:
                console.print("\n[yellow]🏜️  Dry run — skipping destination send[/yellow]")
            console.print(
                f"\n[bold green]✅ Test complete: {len(ids)} IDs "
                f"({len(ids_batches)} batches), {total_records} records processed[/bold green]"
            )
            return

        # ================================================================
        # AbstractPipeline: standard execution (existing flow)
        # ================================================================

        # Use a per-test copy so define_source enrichments are captured.
        test_params = dict(params)

        # Initialize source (or the job plan, if the pipeline overrides define_jobs)
        try:
            source = pipeline.define_jobs(test_params)
        except Exception as e:
            console.print(f"[red]❌ Pipeline setup failed: {e}[/red]")
            traceback.print_exc()
            raise typer.Exit(1)

        console.print(f"\n[bold]🔌 Source:[/bold] {source}")

        # Build execution context (mirrors local_executor behavior)
        context = ExecutionContext(
            execution_id=str(uuid.uuid4()),
            pipeline_name=pipeline.name,
            runtime_params=test_params,
        )

        # Build flat mutable runtime_params for the transformation chain.
        flat_test_params = dict(test_params)
        flat_test_params.update(
            {
                "execution_id": context.execution_id,
                "batch_id": context.batch_id,
                "pipeline_name": context.pipeline_name,
                "created_at": context.created_at.isoformat(),
            }
        )

        # Plan the jobs the way the manager does — serialized and reconstructed
        # through the wire — and run each through the same v2 core the worker
        # runs (fetch → normalize → transform → resolve destination), capped at
        # `limit` across jobs.
        console.print(f"\n[cyan]Fetching records (limit={limit})...[/cyan]")
        try:
            transformed, total_fetched = _run_plan(
                source, pipeline, flat_test_params, limit, verbose, "", run_log["jobs"]
            )
        except TransformationError as e:
            console.print(f"  [red]❌ {e.transformation_name} failed: {e}[/red]")
            run_log["errors"].append(f"{e.transformation_name}: {e}")
            traceback.print_exc()
            raise typer.Exit(1)
        except JobNotSerializableError as e:
            # The message names the cause and the fix; a traceback would bury it.
            console.print(f"\n[red]❌ This job cannot reach a worker:[/red] {e}")
            run_log["errors"].append(str(e))
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]❌ Source fetch/transform failed: {e}[/red]")
            run_log["errors"].append(str(e))
            traceback.print_exc()
            raise typer.Exit(1)

        console.print(f"\n[green]✓ Fetched {total_fetched} records[/green]")
        if total_fetched == 0:
            console.print("[yellow]⚠️  No records returned from source[/yellow]")
            raise typer.Exit(0)

        # Send to destination or dry-run
        if dry_run:
            console.print("\n[yellow]🏜️  Dry run — skipping destination send[/yellow]")
            console.print(
                f"\n[bold green]✅ Test complete: {len(transformed)} records processed[/bold green]"
            )
        else:
            console.print(f"\n[cyan]Sending {len(transformed)} records to destination...[/cyan]")
            try:
                destination = pipeline.define_destination(transformed, flat_test_params)
                console.print(f"[bold]📤 Destination:[/bold] {destination}")

                async def _send():
                    if not await destination.health_check():
                        console.print("[red]❌ Destination health check failed[/red]")
                        raise typer.Exit(1)
                    await destination.send_with_retry(transformed, flat_test_params)

                asyncio.run(_send())
                run_log["sent"] = len(transformed)
                console.print(
                    f"[bold green]✅ Test complete: {len(transformed)} records sent successfully[/bold green]"
                )
            except Exception as e:
                console.print(f"[red]❌ Destination send failed: {e}[/red]")
                run_log["errors"].append(f"destination send: {e}")
                traceback.print_exc()
                raise typer.Exit(1)
