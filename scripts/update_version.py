#!/usr/bin/env python3
import re
import sys
from pathlib import Path

def update_version(new_version: str):
    # 1. Update reflowfy/__init__.py
    init_path = Path("reflowfy/__init__.py")
    if init_path.exists():
        content = init_path.read_text()
        # Regex to find __version__ = "..."
        new_content = re.sub(
            r'__version__\s*=\s*"[^"]+"',
            f'__version__ = "{new_version}"',
            content
        )
        init_path.write_text(new_content)
        print(f"✅ Updated {init_path} to {new_version}")
    else:
        print(f"❌ Could not find {init_path}")

    # 2. Update reflowfy/helm/reflowfy/Chart.yaml
    chart_path = Path("reflowfy/helm/reflowfy/Chart.yaml")
    if chart_path.exists():
        content = chart_path.read_text()
        # Update appVersion: "..."
        content = re.sub(
            r'appVersion:\s*"[^"]+"',
            f'appVersion: "{new_version}"',
            content
        )
        # Update version: ... (semantic version of the chart itself, often kept in sync)
        # For simplicity, we sync them here, but typically chart version might move independently.
        # However, "single source of truth for user" implies syncing them.
        # Let's check if the user wants to sync chart version too. 
        # Usually good practice to bump chart version on app change.
        content = re.sub(
            r'^version:\s*[0-9.]+',
            f'version: {new_version}',
            content,
            flags=re.MULTILINE
        )
        chart_path.write_text(content)
        print(f"✅ Updated {chart_path} to {new_version}")
    else:
        print(f"❌ Could not find {chart_path}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pythonscripts/update_version.py <new_version>")
        sys.exit(1)
    
    update_version(sys.argv[1])
