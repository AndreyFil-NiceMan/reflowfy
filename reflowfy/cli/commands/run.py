"""Run Reflowfy locally using Docker Compose."""

import subprocess
import typer

from reflowfy.cli.utils import console


def register(app: typer.Typer):
    """Register the run command."""

    @app.command()
    def run(
        build: bool = typer.Option(False, "--build", "-b", help="Rebuild images before starting"),
        detach: bool = typer.Option(False, "--detach", "-d", help="Run in background"),
    ):
        """
        Run locally using Docker Compose (Dev Mode).
        """
        from rich.panel import Panel

        console.print(Panel("🚀 Starting Local Development Stack"))
        
        if build:
            console.print("🔨 Building images with --no-cache...", style="yellow")
            try:
                subprocess.run(["docker", "compose", "build", "--no-cache"], check=True)
                console.print("✅ Build complete", style="green")
            except subprocess.CalledProcessError:
                console.print("❌ Build failed", style="red")
                raise typer.Exit(code=1)

        cmd = ["docker", "compose", "up"]
        if detach:
            cmd.append("-d")
            
        try:
            subprocess.run(cmd)
        except KeyboardInterrupt:
            console.print("\n👋 Stopping...")
