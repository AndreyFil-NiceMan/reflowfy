"""
E2E Tests for CLI 'reflowfy run' command.

Tests the run command's Docker Compose orchestration behavior
using mocked subprocess calls.
"""

import os
import pytest
import tempfile
import shutil
from unittest.mock import patch, call
from typer.testing import CliRunner

from reflowfy.cli.main import app

pytestmark = [pytest.mark.e2e]
runner = CliRunner()


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace for CLI tests."""
    temp_dir = tempfile.mkdtemp()
    original_cwd = os.getcwd()
    os.chdir(temp_dir)

    yield temp_dir

    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)


class TestRunCommand:
    """Tests for the 'reflowfy run' command."""

    @patch("reflowfy.cli.commands.run.subprocess")
    def test_run_calls_docker_compose_up(self, mock_subprocess, temp_workspace):
        """Run without flags should call 'docker compose up'."""
        mock_subprocess.run.return_value = None

        result = runner.invoke(app, ["run"])

        assert result.exit_code == 0
        mock_subprocess.run.assert_called_once_with(["docker", "compose", "up"])

    @patch("reflowfy.cli.commands.run.subprocess")
    def test_run_with_build_flag(self, mock_subprocess, temp_workspace):
        """Run with --build should build first then start."""
        mock_subprocess.run.return_value = None
        # CalledProcessError would need check=True to raise, mock returns None
        mock_subprocess.CalledProcessError = Exception

        result = runner.invoke(app, ["run", "--build"])

        assert result.exit_code == 0
        calls = mock_subprocess.run.call_args_list
        assert len(calls) == 2

        # First call: build
        assert calls[0] == call(
            ["docker", "compose", "build", "--no-cache"], check=True
        )
        # Second call: up
        assert calls[1] == call(["docker", "compose", "up"])

    @patch("reflowfy.cli.commands.run.subprocess")
    def test_run_with_detach_flag(self, mock_subprocess, temp_workspace):
        """Run with --detach should append -d flag."""
        mock_subprocess.run.return_value = None

        result = runner.invoke(app, ["run", "--detach"])

        assert result.exit_code == 0
        mock_subprocess.run.assert_called_once_with(["docker", "compose", "up", "-d"])

    @patch("reflowfy.cli.commands.run.subprocess")
    def test_run_with_build_and_detach(self, mock_subprocess, temp_workspace):
        """Run with --build --detach should build then start in detached mode."""
        mock_subprocess.run.return_value = None
        mock_subprocess.CalledProcessError = Exception

        result = runner.invoke(app, ["run", "--build", "--detach"])

        assert result.exit_code == 0
        calls = mock_subprocess.run.call_args_list
        assert len(calls) == 2

        # Build step
        assert calls[0] == call(
            ["docker", "compose", "build", "--no-cache"], check=True
        )
        # Up step with -d
        assert calls[1] == call(["docker", "compose", "up", "-d"])
