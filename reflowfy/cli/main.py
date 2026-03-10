"""Reflowfy CLI — main entry point.

All commands are organized in the `commands/` subpackage.
Shared utilities live in `utils.py`.
"""

import typer
from pathlib import Path
from dotenv import load_dotenv

from reflowfy.cli.commands import build, deploy, run, check, init_cmd, test, new_cmd

# Create the main typer app
app = typer.Typer(help="Reflowfy CLI Tool for easy deployment and management.")

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
