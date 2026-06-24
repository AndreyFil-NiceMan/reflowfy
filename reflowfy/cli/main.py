"""Reflowfy CLI — main entry point.

All commands are organized in the `commands/` subpackage.
Shared utilities live in `utils.py`.
"""

import typer
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

from reflowfy.cli.commands import build, deploy, run, check, init_cmd, test, new_cmd
from reflowfy import __version__

# Create the main typer app
app = typer.Typer(help="Reflowfy CLI Tool for easy deployment and management.")


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"reflowfy {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "-v",
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    pass


@app.command("version")
def version_cmd() -> None:
    """Show the reflowfy version."""
    typer.echo(f"reflowfy {__version__}")


# Load .env file if it exists
env_path = Path(".") / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    load_dotenv()

# Register all commands
build.register(app)
deploy.register(app)
run.register(app)
check.register(app)
init_cmd.register(app)
test.register(app)
new_cmd.register(app)


if __name__ == "__main__":
    app()
