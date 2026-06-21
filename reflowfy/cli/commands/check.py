"""Verify deployment health."""

import subprocess
import typer

from reflowfy.cli.utils import console


def register(app: typer.Typer):
    """Register the check command."""

    @app.command()
    def check():
        """
        Verify deployment health.
        """
        console.print("🔍 Checking Pod Status...")
        subprocess.run(["kubectl", "get", "pods", "-l", "app.kubernetes.io/instance=reflowfy"])
