"""Test a pipeline locally without Docker."""

import asyncio
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
from reflowfy.execution.job_runner import plan_slices, run_job_records
from reflowfy.transformations.base import TransformationError


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
    ):
        """
        Test a pipeline locally without Docker.

        Loads the pipeline file, prompts for parameters, and runs
        with a record limit (default 100).
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
                type_name = param._TYPE_NAMES.get(param.param_type, str(param.param_type))
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

                # Plan slices and run each through the shared v2 core, capped at
                # `limit` across slices.
                console.print(f"  [cyan]Fetching records (limit={limit})...[/cyan]")
                transformed = []
                applied = []
                batch_fetched = 0
                try:
                    for sub in plan_slices(source, meta):
                        remaining = limit - batch_fetched
                        if remaining <= 0:
                            break
                        records, t_records, t_applied, _dest = run_job_records(
                            sub, pipeline, meta, limit=remaining
                        )
                        batch_fetched += len(records)
                        transformed.extend(t_records)
                        applied.extend(t_applied)
                except TransformationError as e:
                    console.print(f"    [red]❌ {e.transformation_name} failed: {e}[/red]")
                    traceback.print_exc()
                    continue
                except Exception as e:
                    console.print(
                        f"  [red]❌ Source fetch/transform failed for batch {ids_batch}: {e}[/red]"
                    )
                    traceback.print_exc()
                    continue

                console.print(f"  [green]✓ Fetched {batch_fetched} records[/green]")
                if batch_fetched == 0:
                    console.print(f"  [yellow]⚠️ No records for batch: {ids_batch}[/yellow]")
                    continue

                for name, _duration in applied:
                    console.print(f"    [green]✓ {name}: {len(transformed)} records[/green]")

                # Show sample output
                console.print(
                    f"  [bold]📋 Sample ({min(2, len(transformed))} of {len(transformed)}):[/bold]"
                )
                for i, record in enumerate(transformed[:2]):
                    console.print(
                        f"    [dim]Record {i + 1}:[/dim] {json.dumps(record, default=str, indent=2)[:400]}"
                    )

                # Send to destination or dry-run
                if not dry_run:
                    try:
                        destination = pipeline.define_destination(transformed, meta)
                        console.print(f"  [bold]📤 Destination:[/bold] {destination}")

                        async def _send_batch(recs=transformed, m=meta, dest=destination):
                            await dest.send_with_retry(recs, m)

                        asyncio.run(_send_batch())
                        console.print(f"  [green]✓ Sent {len(transformed)} records[/green]")
                    except Exception as e:
                        console.print(f"  [red]❌ Send failed for batch {ids_batch}: {e}[/red]")

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

        # Initialize source
        try:
            source = pipeline.define_source(test_params)
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

        # Plan slices and run each through the same v2 core the worker runs
        # (fetch → normalize → transform → resolve destination), capped at
        # `limit` across slices. The wire round-trip exercises serialization.
        console.print(f"\n[cyan]Fetching records (limit={limit})...[/cyan]")
        transformed = []
        applied = []
        total_fetched = 0
        try:
            for sub in plan_slices(source, flat_test_params):
                remaining = limit - total_fetched
                if remaining <= 0:
                    break
                records, t_records, t_applied, _dest = run_job_records(
                    sub, pipeline, flat_test_params, limit=remaining
                )
                total_fetched += len(records)
                transformed.extend(t_records)
                applied.extend(t_applied)
        except TransformationError as e:
            console.print(f"  [red]❌ {e.transformation_name} failed: {e}[/red]")
            traceback.print_exc()
            raise typer.Exit(1)
        except Exception as e:
            console.print(f"[red]❌ Source fetch/transform failed: {e}[/red]")
            traceback.print_exc()
            raise typer.Exit(1)

        console.print(f"[green]✓ Fetched {total_fetched} records[/green]")
        if total_fetched == 0:
            console.print("[yellow]⚠️  No records returned from source[/yellow]")
            raise typer.Exit(0)

        for name, _duration in applied:
            console.print(f"  [green]✓ {name}: {len(transformed)} records[/green]")

        # Show sample output
        console.print(
            f"\n[bold]📋 Sample output ({min(3, len(transformed))} of {len(transformed)} records):[/bold]"
        )
        for i, record in enumerate(transformed[:3]):
            console.print(
                f"  [dim]Record {i + 1}:[/dim] {json.dumps(record, default=str, indent=2)[:500]}"
            )

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
                console.print(
                    f"[bold green]✅ Test complete: {len(transformed)} records sent successfully[/bold green]"
                )
            except Exception as e:
                console.print(f"[red]❌ Destination send failed: {e}[/red]")
                traceback.print_exc()
                raise typer.Exit(1)
