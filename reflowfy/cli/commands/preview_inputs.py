"""``reflowfy test`` inputs: finding the pipeline and collecting its parameters."""

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Sequence

import typer

if TYPE_CHECKING:  # Runtime imports stay lazy — these are only for annotations.
    from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.cli.utils import console
from reflowfy.execution.job_runner import preview_runtime_params


def build_flat_params(pipeline: "AbstractPipeline[Any]", params: Dict[str, Any]) -> Dict[str, Any]:
    """The flat runtime_params a run starts from (mirrors the local executor)."""
    return preview_runtime_params(pipeline.name, params)


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


def split_fields(values: Sequence[str]) -> List[str]:
    """``--redact a,b --redact c`` -> ``["a", "b", "c"]``."""
    return [name.strip() for value in values for name in value.split(",") if name.strip()]
