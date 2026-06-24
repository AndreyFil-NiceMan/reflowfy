"""Shared utilities for CLI commands."""

from pathlib import Path
from rich.console import Console

console = Console()


def get_package_path() -> Path:
    """Get the path to the installed reflowfy package."""
    import reflowfy

    return Path(reflowfy.__file__).parent


def get_helm_chart_path() -> Path:
    """Get path to the bundled Helm chart. Falls back to local ./helm/reflowfy if available."""
    # Priority 1: Local development (./helm/reflowfy)
    local_chart = Path("./helm/reflowfy")
    if local_chart.exists() and (local_chart / "Chart.yaml").exists():
        return local_chart

    # Priority 1.5: Local development from root (./reflowfy/helm/reflowfy)
    root_chart = Path("reflowfy/helm/reflowfy")
    if root_chart.exists() and (root_chart / "Chart.yaml").exists():
        return root_chart

    # Priority 2: Bundled in package
    bundled_chart = get_package_path() / "helm" / "reflowfy"
    if bundled_chart.exists():
        return bundled_chart

    raise FileNotFoundError(
        "Helm chart not found. Ensure you're in the project root or have installed reflowfy with bundled charts."
    )


def get_dockerfiles_path() -> Path:
    """Get path to Dockerfiles (templates for init or dev source)."""
    # Priority 1: Current directory (User project or Development mode)
    if Path("Dockerfile.api").exists() or Path("Dockerfile.reflow-manager").exists():
        return Path(".")

    # Priority 2: Templates bundled in package (Production/Init mode)
    return get_package_path() / "templates"
