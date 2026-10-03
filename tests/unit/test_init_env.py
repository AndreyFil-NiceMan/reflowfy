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
