"""Global pipeline discovery utility.

This module provides centralized pipeline auto-discovery and loading,
used by the API, Worker, and ReflowManager services.
"""

import os
import sys
import importlib
import pkgutil
from pathlib import Path


def discover_and_load_pipelines(module_name: str = "pipelines") -> int:
    """
    Auto-discover and import all pipeline modules from specified directory.
    This registers transformations so workers can use them.
    
    Args:
        module_name: Name of the module/directory containing pipelines
        
    Returns:
        Number of pipeline files loaded
    """
    loaded_count = 0
    
    # Ensure current directory is in sys.path
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    
    try:
        # Try to import the pipelines package
        pipelines_package = importlib.import_module(module_name)
        package_path = Path(pipelines_package.__file__).parent
        
        print(f"Discovering pipelines in '{module_name}'...")
        
        # Import all Python files in the pipelines directory
        for _, module_name_inner, is_pkg in pkgutil.iter_modules([str(package_path)]):
            if not is_pkg:  # Only import Python files, not subdirectories
                try:
                    full_module = f"{module_name}.{module_name_inner}"
                    importlib.import_module(full_module)
                    print(f"  Loaded {module_name_inner}.py")
                    loaded_count += 1
                except Exception as e:
                    print(f"  Failed to load {module_name_inner}.py: {e}")
        
        if loaded_count == 0:
            print(f"  No pipeline files found in '{module_name}'")
        else:
            print(f"  Loaded {loaded_count} pipeline file(s)")
            
    except ImportError as e:
        print(f"  Module '{module_name}' not found - no pipelines loaded: {e}")
    
    return loaded_count
