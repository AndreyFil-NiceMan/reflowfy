"""`reflowfy init` must write a real POSTGRES_PASSWORD, not the blank template value."""

from pathlib import Path

from typer.testing import CliRunner

from reflowfy.cli.main import app


def _password_from(env_path: Path) -> str:
    for line in env_path.read_text().splitlines():
        if line.startswith("POSTGRES_PASSWORD="):
            return line.split("=", 1)[1].split("#", 1)[0].strip()
    raise AssertionError("POSTGRES_PASSWORD missing from generated .env")


def test_init_generates_a_unique_postgres_password(tmp_path: Path) -> None:
    runner = CliRunner()
    for project in ("a", "b"):
        result = runner.invoke(app, ["init", str(tmp_path / project)])
        assert result.exit_code == 0, result.output

    first = _password_from(tmp_path / "a" / ".env")
    second = _password_from(tmp_path / "b" / ".env")

    assert first and first != "reflowfy", "blank or default password would fail the chart guard"
    assert first != second, "a shared password means it is committed, not generated"


def test_init_scaffolds_under_src_and_dockerfiles_copy_it(tmp_path: Path) -> None:
    result = CliRunner().invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 0, result.output

    for folder in ("pipelines", "sources", "destinations", "transformations", "queries"):
        assert (tmp_path / "src" / folder).is_dir()
        assert not (tmp_path / folder).exists()
    for dockerfile in ("api", "reflow-manager", "worker"):
        text = (tmp_path / f"Dockerfile.{dockerfile}").read_text()
        assert "COPY src/ src/" in text and "PYTHONPATH=/app/src" in text


# --- uv project: pyproject.toml + uv.lock, no requirements.txt ----------------------

import subprocess  # noqa: E402
import sys  # noqa: E402
from typing import Any, List  # noqa: E402

import pytest  # noqa: E402

import reflowfy  # noqa: E402
from reflowfy.cli import utils  # noqa: E402
from reflowfy.cli.commands import init_cmd  # noqa: E402


# Captured at import, before conftest's autouse fixture replaces it.
_REAL_FIND_UV = init_cmd._find_uv


def test_find_uv_uses_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(init_cmd.shutil, "which", lambda name: f"/opt/{name}")
    assert _REAL_FIND_UV() == "/opt/uv"


@pytest.fixture(autouse=True)
def _wide_console(monkeypatch: pytest.MonkeyPatch) -> None:
    # Rich wraps at 80 columns under CliRunner; keep messages on one line.
    monkeypatch.setattr(utils.console, "_width", 300)


def test_init_scaffolds_a_uv_project_and_no_requirements_txt(tmp_path: Path) -> None:
    project = tmp_path / "My Orders"
    result = CliRunner().invoke(app, ["init", str(project)])
    assert result.exit_code == 0, result.output

    assert not (project / "requirements.txt").exists()
    text = (project / "pyproject.toml").read_text()
    assert 'name = "my-orders"' in text
    assert f'"reflowfy>={reflowfy.__version__}",' in text
    assert "[tool.uv]" in text and "package = false" in text
    if sys.version_info >= (3, 11):  # tomllib; 3.10 still checks the text above
        import tomllib

        parsed = tomllib.loads(text)
        assert parsed["project"]["dependencies"] == [f"reflowfy>={reflowfy.__version__}"]
    ignored = (project / ".dockerignore").read_text().splitlines()
    assert ".env" in ignored and ".venv/" in ignored
    assert "uv not found" in result.output


@pytest.mark.parametrize("dockerfile", ["api", "reflow-manager", "worker"])
def test_dockerfiles_install_only_from_uv_lock_as_non_root(tmp_path: Path, dockerfile: str) -> None:
    assert CliRunner().invoke(app, ["init", str(tmp_path)]).exit_code == 0
    text = (tmp_path / f"Dockerfile.{dockerfile}").read_text()
    assert "COPY pyproject.toml uv.lock ./" in text
    assert "uv sync --locked --no-dev --no-install-project --no-install-package reflowfy" in text
    assert "requirements" not in text and "pip install" not in text
    # Deps are installed before the source is copied, so code edits keep the layer cached.
    assert text.index("uv sync") < text.index("COPY src/ src/")
    assert "USER 10001" in text


def test_init_keeps_existing_pyproject_and_dockerignore(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("# mine\n")
    (tmp_path / ".dockerignore").write_text("# mine\n")
    assert CliRunner().invoke(app, ["init", str(tmp_path)]).exit_code == 0
    assert (tmp_path / "pyproject.toml").read_text() == "# mine\n"
    assert (tmp_path / ".dockerignore").read_text() == "# mine\n"


def _fake_uv(monkeypatch: pytest.MonkeyPatch, returncode: int, stderr: str = "") -> List[Any]:
    calls: List[Any] = []

    def run(cmd: List[str], **kwargs: Any) -> subprocess.CompletedProcess:
        calls.append((cmd, kwargs["cwd"]))
        (kwargs["cwd"] / "uv.lock").write_text("") if returncode == 0 else None
        return subprocess.CompletedProcess(cmd, returncode, stdout="", stderr=stderr)

    monkeypatch.setattr(init_cmd, "_find_uv", lambda: "/bin/uv")
    monkeypatch.setattr(init_cmd.subprocess, "run", run)
    return calls


def test_init_runs_uv_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _fake_uv(monkeypatch, 0)
    result = CliRunner().invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert calls == [(["/bin/uv", "lock"], tmp_path)]
    assert "Created uv.lock" in result.output

    # A re-run keeps the existing lock.
    assert CliRunner().invoke(app, ["init", str(tmp_path)]).exit_code == 0
    assert len(calls) == 1


@pytest.mark.parametrize(
    "stderr, shown", [("resolve\nNo solution found\n", "No solution found"), ("", "no output")]
)
def test_failed_uv_lock_warns_but_init_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stderr: str, shown: str
) -> None:
    _fake_uv(monkeypatch, 1, stderr)
    result = CliRunner().invoke(app, ["init", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert "uv lock` failed" in result.output and shown in result.output
    assert not (tmp_path / "uv.lock").exists()


def test_no_lock_skips_uv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _fake_uv(monkeypatch, 0)
    result = CliRunner().invoke(app, ["init", str(tmp_path), "--no-lock"])
    assert result.exit_code == 0, result.output
    assert calls == []


def test_project_name_fallback(tmp_path: Path) -> None:
    assert init_cmd._project_name(tmp_path / "__") == "reflowfy-project"


def test_lock_needs_a_pyproject(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _fake_uv(monkeypatch, 0)
    init_cmd._lock(tmp_path)
    assert calls == []


@pytest.mark.parametrize(
    "present, missing",
    [((), "pyproject.toml and uv.lock"), (("pyproject.toml",), "uv.lock")],
)
@pytest.mark.parametrize("command", [["build", "--registry", "r"], ["run", "--build"]])
def test_build_requires_a_locked_uv_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, present: Any, missing: str, command: List[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    for name in present:
        (tmp_path / name).write_text("")
    result = CliRunner().invoke(app, command)
    assert result.exit_code == 1
    assert f"Missing {missing}" in result.output


def test_check_uv_project_passes(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("")
    (tmp_path / "uv.lock").write_text("")
    utils.check_uv_project(tmp_path)
