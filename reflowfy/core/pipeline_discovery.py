"""Global pipeline discovery utility.

This module provides centralized pipeline auto-discovery and loading,
used by the API, Worker, and ReflowManager services.

It recursively scans the following user directories (including any nested
subdirectories, with or without ``__init__.py``):
- pipelines/          → Pipeline definitions (auto-registered via metaclass)
- sources/            → Reusable source configurations (@source decorator)
- destinations/       → Reusable destination configurations (@destination decorator)
- transformations/    → Shared transformations (@transformation decorator or BaseTransformation subclass)
"""

import importlib
import os
import sys
from pathlib import Path


def _scan_directory(module_name: str, label: str) -> int:
    """
    Recursively scan and import all Python modules under a directory.

    Walks the package tree so modules in nested subdirectories are loaded too.
    Modules are imported by dotted name (e.g. ``pipelines.group_a.sub.deep``),
    which works even when intermediate directories lack ``__init__.py`` thanks
    to Python's implicit namespace packages, while still honouring relative
    imports in subtrees that are regular packages.

    Args:
        module_name: Dotted module path (e.g., 'pipelines', 'transformations')
        label: Human-readable label for logging

    Returns:
        Number of modules loaded
    """
    loaded_count = 0

    try:
        package = importlib.import_module(module_name)
    except ImportError:
        # Directory doesn't exist or isn't importable — that's fine
        return 0

    # __path__ exists for both regular and namespace packages (unlike __file__,
    # which is None for namespace packages).
    for base in getattr(package, "__path__", []):
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d != "__pycache__"]

            rel_dir = Path(dirpath).relative_to(base)
            for filename in sorted(filenames):
                if not filename.endswith(".py") or filename == "__init__.py":
                    continue

                rel_parts = [*rel_dir.parts, filename[: -len(".py")]]
                suffix = ".".join(rel_parts)
                full_module = f"{module_name}.{suffix}"
                display = rel_dir / filename
                try:
                    importlib.import_module(full_module)
                    print(f"  Loaded {label}: {display}")
                    loaded_count += 1
                except Exception as e:
                    print(f"  Failed to load {label} {display}: {e}")

    return loaded_count


def discover_and_load_pipelines(module_name: str = "pipelines") -> int:
    """
    Auto-discover and import all pipeline modules and reusable components.

    Scans the following directories (recursively, including nested
    subdirectories) relative to the pipeline module:
    - The pipeline module itself (e.g., 'pipelines/')
    - 'sources/' — reusable source configs
    - 'destinations/' — reusable destination configs
    - 'transformations/' — shared transformations

    Pipelines are auto-registered via metaclass when their class is defined.
    Sources/destinations are registered via @source/@destination decorators.
    Transformations are registered via metaclass or @transformation decorator.

    Args:
        module_name: Name of the module/directory containing pipelines

    Returns:
        Number of pipeline files loaded
    """
    # Ensure current directory is in sys.path
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    total_loaded = 0

    print("Discovering components...")

    # Scan reusable components first (so pipelines can reference them)
    total_loaded += _scan_directory("sources", "source")
    total_loaded += _scan_directory("destinations", "destination")
    total_loaded += _scan_directory("transformations", "transformation")

    # Scan pipelines last (they may import from sources/destinations/transformations)
    total_loaded += _scan_directory(module_name, "pipeline")

    if total_loaded == 0:
        print("  No component files found")
    else:
        print(f"  Loaded {total_loaded} component file(s) total")

    return total_loaded
