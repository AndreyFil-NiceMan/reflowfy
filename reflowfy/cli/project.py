"""What `build`, `deploy`, `check` and `init` agree on about a Reflowfy project.

The image names, the Helm release, the registry requirement and where a bundled
template is found used to be written into each command separately.
"""

from pathlib import Path
from typing import Dict, Optional

import typer

from reflowfy.cli.utils import console, get_package_path

#: The Helm release every command installs or inspects.
RELEASE = "reflowfy"
DEFAULT_PROJECT = "reflowfy"

#: Image name per service, and the Dockerfile.<service> it is built from.
IMAGES: Dict[str, str] = {
    "api": "reflowfy-api",
    "reflow-manager": "reflowfy-reflow-manager",
    "worker": "reflowfy-worker",
}


def image_repository(registry: str, project: str, service: str) -> str:
    """``<registry>/<project>/<image>`` for one service (no tag)."""
    return f"{registry}/{project}/{IMAGES[service]}"


def require_registry(registry: Optional[str]) -> str:
    """The registry, or exit with the message both `build` and `deploy` give."""
    if registry:
        return registry
    console.print("❌ Registry is required. Set --registry or REGISTRY in .env", style="red")
    if not Path(".env").exists():
        console.print("⚠️  No .env file found in current directory.", style="yellow")
    raise typer.Exit(code=1)


def template_path(name: str) -> Path:
    """A file under ``reflowfy/templates``: the installed package's, else the source
    tree's (running from a checkout). May not exist; callers check."""
    bundled = get_package_path() / "templates" / name
    return bundled if bundled.exists() else Path("reflowfy/templates") / name
