"""Type contracts: tests/typecheck/pass_*.py must be clean, fail_*.py must error.

The public typing promises in CLAUDE.md (wrong-arity ``@transformation`` is
rejected, a misspelled params key is caught, reserved ``RuntimeParams`` keys must
be read with ``.get()``, ``AbstractPipeline[RuntimeParams]`` is the zero-cost
migration) are only promises until a checker enforces them. This runs pyright
once over the snippet directory, with the repo's own ``[tool.pyright]`` settings,
and checks each file against its declared expectation.

A fail_ file marks the line that must error with ``# expect: <rule>``.
"""

import json
import re
import shutil
import subprocess
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TYPECHECK_DIR = REPO_ROOT / "tests" / "typecheck"
EXPECT_RE = re.compile(r"#\s*expect:\s*(\w+)")

PASS_FILES = sorted(TYPECHECK_DIR.glob("pass_*.py"))
FAIL_FILES = sorted(TYPECHECK_DIR.glob("fail_*.py"))


def _pyright_cmd() -> Optional[List[str]]:
    exe = shutil.which("pyright", path=str(Path(sys.executable).parent)) or shutil.which("pyright")
    return [exe] if exe else None


@lru_cache(maxsize=1)
def _diagnostics() -> Dict[str, List[Dict[str, Any]]]:
    """Run pyright once over tests/typecheck and group diagnostics by file name."""
    cmd = _pyright_cmd()
    if cmd is None:
        pytest.skip("pyright is not installed")
    try:
        proc = subprocess.run(
            [*cmd, "--outputjson", "--pythonpath", sys.executable, str(TYPECHECK_DIR)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        report = json.loads(proc.stdout)
    except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
        pytest.skip(f"pyright is not runnable here: {exc!r}")
    by_file: Dict[str, List[Dict[str, Any]]] = {}
    for diag in report.get("generalDiagnostics", []):
        by_file.setdefault(Path(diag["file"]).name, []).append(diag)
    if report.get("summary", {}).get("filesAnalyzed", 0) < len(PASS_FILES) + len(FAIL_FILES):
        pytest.skip(f"pyright did not analyze the snippets: {proc.stderr[:500]}")
    return by_file


def _errors(path: Path) -> List[Tuple[int, Optional[str], str]]:
    return [
        (d["range"]["start"]["line"] + 1, d.get("rule"), d["message"])
        for d in _diagnostics().get(path.name, [])
        if d["severity"] == "error"
    ]


def _expectations(path: Path) -> List[Tuple[int, str]]:
    lines = path.read_text().splitlines()
    return [(n, m.group(1)) for n, line in enumerate(lines, 1) if (m := EXPECT_RE.search(line))]


def test_snippet_directory_is_populated():
    assert PASS_FILES and FAIL_FILES
    for path in FAIL_FILES:
        assert _expectations(path), f"{path.name} declares no '# expect: <rule>' line"


@pytest.mark.parametrize("path", PASS_FILES, ids=lambda p: p.name)
def test_pass_file_is_clean(path: Path):
    assert _errors(path) == []


@pytest.mark.parametrize("path", FAIL_FILES, ids=lambda p: p.name)
def test_fail_file_errors_with_expected_rule(path: Path):
    errors = _errors(path)
    for line, rule in _expectations(path):
        assert any(
            e_line == line and e_rule == rule for e_line, e_rule, _ in errors
        ), f"{path.name}:{line} expected {rule}, pyright reported {errors}"
    # Nothing else broke: an import typo would also "fail", for the wrong reason.
    expected = set(_expectations(path))
    assert {(e_line, e_rule) for e_line, e_rule, _ in errors} == expected


def test_main_pyright_config_does_not_cover_fail_snippets():
    """`uv run pyright` must stay at zero: its include list must not reach tests/typecheck."""
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover
        tomli = pytest.importorskip("tomli")
        tomllib = tomli
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["tool"]["pyright"]
    for entry in config.get("include", []):
        inc = (REPO_ROOT / entry).resolve()
        assert not (TYPECHECK_DIR == inc or inc in TYPECHECK_DIR.parents), entry
