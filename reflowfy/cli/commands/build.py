"""Build and push Docker images to a private registry."""

import typer
import os
from pathlib import Path
from typing import Optional
from python_on_whales import DockerClient

from reflowfy.cli.utils import console, get_dockerfiles_path

docker = DockerClient()


def _build_images(
    registry: str,
    project: str,
    push: bool,
    no_cache: bool = False,
    custom_tag: Optional[str] = None,
) -> None:
    """Helper to build and optionally push images."""
    from rich.panel import Panel

    cache_msg = " (no-cache)" if no_cache else ""
    console.print(
        Panel(f"Building images for registry: [bold cyan]{registry}[/bold cyan]{cache_msg}")
    )

    image_map = {
        "reflowfy-api": "api",
        "reflowfy-reflow-manager": "reflow-manager",
        "reflowfy-worker": "worker",
    }

    build_context = Path(".")

    if not (build_context / "pipelines").exists():
        console.print("⚠️  No 'pipelines/' folder found in current directory.", style="yellow")
        console.print(
            "   Make sure you're in your project root or run 'reflowfy init' first.", style="yellow"
        )

    for image_name, dockerfile_name in image_map.items():
        base_image = f"{registry}/{project}/{image_name}"
        if custom_tag:
            tags = [f"{base_image}:{custom_tag}"]
        else:
            tags = [f"{base_image}:latest"]
            try:
                import reflowfy

                version = reflowfy.__version__
                tags.append(f"{base_image}:{version}")
            except ImportError:
                console.print(
                    "⚠️  Could not import reflowfy to get version for tagging.", style="yellow"
                )

        dockerfile = f"Dockerfile.{dockerfile_name}"
        dockerfiles_path = get_dockerfiles_path()
        dockerfile_full = dockerfiles_path / dockerfile

        reflowfy_base = os.getenv("REFLOWFY_BASE_IMAGE", "reflowfy-base:latest")
        console.print(
            f"📦 Building [bold]{image_name}[/bold] (Dockerfile: {dockerfile_full}, Base: {reflowfy_base})...",
            style="yellow",
        )

        try:
            # load=True guarantees the built image (with all tags) is loaded into
            # the local Docker daemon regardless of the active buildx driver. Without
            # it, the default "docker" driver happens to load the image but the
            # "docker-container" driver does not, leaving the subsequent push loop
            # with no local image to push.
            docker.build(
                str(build_context),
                file=str(dockerfile_full),
                tags=tags,
                cache=not no_cache,
                load=True,
                build_args={"REFLOWFY_BASE_IMAGE": reflowfy_base},
            )
            console.print(f"✅ Built [bold]{', '.join(tags)}[/bold]", style="green")

            if push:
                for tag in tags:
                    console.print(f"🚀 Pushing [bold]{tag}[/bold]...", style="blue")
                    docker.push(tag)
                    console.print(f"✅ Pushed [bold]{tag}[/bold]", style="green")
        except Exception as e:
            console.print(f"❌ Failed to build/push {image_name}: {e}", style="red")
            raise typer.Exit(code=1)

    console.print(Panel("🎉 Build & Push Complete!", style="bold green"))


def register(app: typer.Typer):
    """Register the build command."""

    @app.command()
    def build(
        registry: Optional[str] = typer.Option(
            None, envvar="REGISTRY", help="Private registry URL (e.g. registry.lab.local)"
        ),
        project: Optional[str] = typer.Option(
            None, envvar="PROJECT", help="Project/Namespace name"
        ),
        push: bool = typer.Option(True, help="Push images to registry after building"),
        no_cache: bool = typer.Option(False, "--no-cache", help="Build images without using cache"),
        tag: Optional[str] = typer.Option(
            None,
            "--tag",
            "-t",
            help="Specific tag for the image (overrides default version tagging)",
        ),
    ):
        """
        Build and push Reflowfy images to a private registry (OpenShift Ready).
        """
        project = project or "reflowfy"

        if not registry:
            console.print(
                "❌ Registry is required. Set --registry or REGISTRY in .env", style="red"
            )
            if not Path(".env").exists():
                console.print("⚠️  No .env file found in current directory.", style="yellow")
            raise typer.Exit(code=1)
        _build_images(registry, project, push, no_cache=no_cache, custom_tag=tag)
