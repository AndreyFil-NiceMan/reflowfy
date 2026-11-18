"""Verify deployment health."""

import subprocess
import typer

from reflowfy.cli.project import RELEASE
from reflowfy.cli.utils import console


def register(app: typer.Typer) -> None:
    """Register the check command."""

    @app.command()
    def check() -> None:
        """
        Verify deployment health.
        """
        console.print("🔍 Checking Pod Status...")
        subprocess.run(["kubectl", "get", "pods", "-l", f"app.kubernetes.io/instance={RELEASE}"])
