"""Global pipeline discovery utility.

This module provides centralized pipeline auto-discovery and loading,
used by the API, Worker, and ReflowManager services.

It scans the following user directories:
- pipelines/          → Pipeline definitions (auto-registered via metaclass)
- sources/            → Reusable source configurations (@source decorator)
- destinations/       → Reusable destination configurations (@destination decorator)
- transformations/    → Shared transformations (@transformation decorator or BaseTransformation subclass)
"""

import os
import sys
import importlib
import pkgutil
from pathlib import Path


def _scan_directory(module_name: str, label: str) -> int:
    """
    Scan and import all Python modules in a directory.
    
    Args:
        module_name: Dotted module path (e.g., 'pipelines', 'transformations')
        label: Human-readable label for logging
        
    Returns:
        Number of modules loaded
    """
    loaded_count = 0
    
    try:
        package = importlib.import_module(module_name)
        package_path = Path(package.__file__).parent
        
        for _, mod_name, is_pkg in pkgutil.iter_modules([str(package_path)]):
            if not is_pkg:  # Only import .py files, not subdirectories
                try:
                    full_module = f"{module_name}.{mod_name}"
                    importlib.import_module(full_module)
                    print(f"  Loaded {label}: {mod_name}.py")
                    loaded_count += 1
                except Exception as e:
                    print(f"  Failed to load {label} {mod_name}.py: {e}")
    except ImportError:
        # Directory doesn't exist or isn't a package — that's fine
        pass
    
    return loaded_count


def discover_and_load_pipelines(module_name: str = "pipelines") -> int:
    """
    Auto-discover and import all pipeline modules and reusable components.
    
    Scans the following directories relative to the pipeline module:
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
    
    print(f"Discovering components...")
    
    # Scan reusable components first (so pipelines can reference them)
    total_loaded += _scan_directory("sources", "source")
    total_loaded += _scan_directory("destinations", "destination")
    total_loaded += _scan_directory("transformations", "transformation")
    
    # Scan pipelines last (they may import from sources/destinations/transformations)
    total_loaded += _scan_directory(module_name, "pipeline")
    
    if total_loaded == 0:
        print(f"  No component files found")
    else:
        print(f"  Loaded {total_loaded} component file(s) total")
    
    return total_loaded
