"""Unit tests for recursive component discovery."""

import sys
import textwrap
from pathlib import Path

import pytest

from reflowfy.core.pipeline_discovery import _scan_directory, discover_and_load_pipelines


def _write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(body))


@pytest.fixture
def project_on_path(tmp_path, monkeypatch):
    """Put a throwaway project dir on sys.path and clean up imported modules."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    # Drop any pre-cached copies of the component package roots (e.g. the test
    # suite's own ``tests/unit/sources`` package, which pytest imports as
    # ``sources``) so _scan_directory resolves fresh against tmp_path.
    _component_roots = ("pipelines", "sources", "transformations", "destinations")
    for name in list(sys.modules):
        if name in _component_roots or name.startswith(tuple(r + "." for r in _component_roots)):
            sys.modules.pop(name, None)
    before = set(sys.modules)
    yield tmp_path
    # Drop anything imported during the test so repeated runs stay isolated.
    for name in set(sys.modules) - before:
        sys.modules.pop(name, None)


def test_scans_top_level_modules(project_on_path):
    pkg = project_on_path / "pipelines"
    _write(pkg / "__init__.py", "")
    _write(pkg / "flat.py", "LOADED = True\n")

    count = _scan_directory("pipelines", "pipeline")

    assert count == 1
    assert sys.modules["pipelines.flat"].LOADED is True


def test_scans_nested_modules_with_init(project_on_path):
    pkg = project_on_path / "pipelines"
    _write(pkg / "__init__.py", "")
    _write(pkg / "group_a" / "__init__.py", "")
    _write(pkg / "group_a" / "sub" / "__init__.py", "")
    _write(pkg / "group_a" / "sub" / "deep.py", "LOADED = True\n")

    count = _scan_directory("pipelines", "pipeline")

    assert count == 1
    assert sys.modules["pipelines.group_a.sub.deep"].LOADED is True


def test_scans_nested_modules_without_init(project_on_path):
    """Nested dirs without __init__.py still load via namespace packages."""
    pkg = project_on_path / "pipelines"
    _write(pkg / "__init__.py", "")
    # No __init__.py in the intermediate directories.
    _write(pkg / "group_b" / "deep.py", "LOADED = True\n")

    count = _scan_directory("pipelines", "pipeline")

    assert count == 1
    assert sys.modules["pipelines.group_b.deep"].LOADED is True


def test_skips_init_and_pycache(project_on_path):
    pkg = project_on_path / "transformations"
    _write(pkg / "__init__.py", "")
    _write(pkg / "real.py", "LOADED = True\n")
    _write(pkg / "__pycache__" / "junk.py", "raise RuntimeError('should not import')\n")

    count = _scan_directory("transformations", "transformation")

    assert count == 1


def test_one_broken_module_does_not_abort_others(project_on_path):
    pkg = project_on_path / "sources"
    _write(pkg / "__init__.py", "")
    _write(pkg / "broken.py", "raise ImportError('boom')\n")
    _write(pkg / "good" / "ok.py", "LOADED = True\n")

    count = _scan_directory("sources", "source")

    assert count == 1
    assert sys.modules["sources.good.ok"].LOADED is True


def test_missing_package_returns_zero(project_on_path):
    assert _scan_directory("does_not_exist", "pipeline") == 0


def test_module_name_defaults_to_env(project_on_path, monkeypatch):
    """discover_and_load_pipelines() with no arg resolves PIPELINE_MODULE."""
    pkg = project_on_path / "my_pipes"
    _write(pkg / "__init__.py", "")
    _write(pkg / "p.py", "LOADED = True\n")

    monkeypatch.setenv("PIPELINE_MODULE", "my_pipes")
    assert discover_and_load_pipelines() >= 1
    assert sys.modules["my_pipes.p"].LOADED is True
