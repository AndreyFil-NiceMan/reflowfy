"""Packaging smoke tests: what the wheel must carry, checked from the source tree.

A stale or missing entry here does not fail any other unit test — it shows up as
a consumer's type checker ignoring the package (no ``py.typed``) or as
``reflowfy deploy`` finding no chart, after the release is out.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml
from typer.testing import CliRunner

import reflowfy

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = REPO_ROOT / "reflowfy"
HELM_DIR = PACKAGE_DIR / "helm"
CHART_DIRS = sorted(p for p in HELM_DIR.iterdir() if (p / "Chart.yaml").exists())
EXPECTED_CHARTS = {"reflowfy", "reflowfy-api", "reflowfy-reflow-manager", "reflowfy-worker"}
CLI_COMMANDS = ["init", "new", "run", "build", "check", "deploy", "test", "version"]


def _pyproject() -> Dict[str, Any]:
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover
        tomllib = pytest.importorskip("tomli")
    return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())


def _manifest_lines() -> List[str]:
    text = (REPO_ROOT / "MANIFEST.in").read_text()
    return [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]


# --- PEP 561 ---------------------------------------------------------------------


def test_py_typed_marker_exists():
    assert (PACKAGE_DIR / "py.typed").is_file()


def test_py_typed_is_in_package_data():
    package_data = _pyproject()["tool"]["setuptools"]["package-data"]
    assert "py.typed" in package_data["reflowfy"]


def test_py_typed_is_in_manifest():
    assert "include reflowfy/py.typed" in _manifest_lines()


def test_installed_package_resolves_to_the_marker():
    """The imported package (editable or not) ships the marker next to __init__."""
    assert (Path(reflowfy.__file__).parent / "py.typed").is_file()


# --- Helm charts -----------------------------------------------------------------


def test_every_expected_chart_is_present():
    assert {p.name for p in CHART_DIRS} == EXPECTED_CHARTS


@pytest.mark.parametrize("chart", CHART_DIRS, ids=lambda p: p.name)
def test_chart_has_metadata_values_and_templates(chart: Path):
    meta = yaml.safe_load((chart / "Chart.yaml").read_text())
    assert meta["name"] == chart.name
    assert meta["apiVersion"] == "v2"
    assert meta.get("version")
    assert isinstance(yaml.safe_load((chart / "values.yaml").read_text()), dict)
    assert any((chart / "templates").glob("*.yaml"))


def test_umbrella_chart_version_tracks_the_package_version():
    meta = yaml.safe_load((HELM_DIR / "reflowfy" / "Chart.yaml").read_text())
    assert str(meta["version"]) == reflowfy.__version__
    assert str(meta["appVersion"]) == reflowfy.__version__


def test_umbrella_chart_dependencies_are_vendored():
    """`reflowfy deploy` runs offline: every declared dependency ships as a .tgz."""
    chart = HELM_DIR / "reflowfy"
    meta = yaml.safe_load((chart / "Chart.yaml").read_text())
    for dep in meta.get("dependencies", []):
        assert (chart / "charts" / f"{dep['name']}-{dep['version']}.tgz").is_file(), dep


def test_helm_charts_are_in_package_data_and_manifest():
    package_data = _pyproject()["tool"]["setuptools"]["package-data"]
    assert "**/*" in package_data["reflowfy.helm"]
    assert "recursive-include reflowfy/helm *" in _manifest_lines()


_helm = shutil.which("helm")


@pytest.mark.skipif(_helm is None, reason="helm is not on PATH")
@pytest.mark.parametrize("chart", CHART_DIRS, ids=lambda p: p.name)
def test_helm_lint_and_template(chart: Path):
    assert _helm is not None
    lint = subprocess.run([_helm, "lint", str(chart)], capture_output=True, text=True, timeout=120)
    assert lint.returncode == 0, lint.stdout + lint.stderr
    # The umbrella chart refuses to render with SASL on (its default) and no password,
    # by design; give it one. Subcharts ignore the value.
    rendered = subprocess.run(
        [_helm, "template", "smoke", str(chart), "--set", "kafka.sasl.password=smoke"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert rendered.returncode == 0, rendered.stdout + rendered.stderr
    assert re.search(r"^kind: ", rendered.stdout, re.MULTILINE)


# --- CLI -------------------------------------------------------------------------


def test_cli_help_lists_every_command():
    from reflowfy.cli.main import app

    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0, result.output
    # Rich colours its output on CI (GITHUB_ACTIONS); "\x1b[1minit" has no \b before "init".
    plain = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
    for command in CLI_COMMANDS:
        assert re.search(rf"\b{command}\b", plain), command


@pytest.mark.parametrize("command", CLI_COMMANDS[:-1])
def test_cli_subcommand_help_works(command: str):
    from reflowfy.cli.main import app

    result = CliRunner().invoke(app, [command, "--help"])

    assert result.exit_code == 0, result.output


def test_cli_version_matches_package():
    from reflowfy.cli.main import app

    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.output.strip() == f"reflowfy {reflowfy.__version__}"


def test_console_script_points_at_the_typer_app():
    assert _pyproject()["project"]["scripts"]["reflowfy"] == "reflowfy.cli.main:app"
