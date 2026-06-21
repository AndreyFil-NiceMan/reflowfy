"""
E2E Tests for CLI 'reflowfy check' command.

Tests the check command's kubectl invocation behavior
using mocked subprocess calls.
"""

import os
import pytest
import tempfile
import shutil
from unittest.mock import patch
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


class TestCheckCommand:
    """Tests for the 'reflowfy check' command."""

    @patch("reflowfy.cli.commands.check.subprocess")
    def test_check_calls_kubectl_get_pods(self, mock_subprocess, temp_workspace):
        """Check should call kubectl with correct label selector."""
        mock_subprocess.run.return_value = None

        result = runner.invoke(app, ["check"])

        assert result.exit_code == 0
        mock_subprocess.run.assert_called_once_with(
            ["kubectl", "get", "pods", "-l", "app.kubernetes.io/instance=reflowfy"]
        )

    @patch("reflowfy.cli.commands.check.subprocess")
    def test_check_outputs_status_message(self, mock_subprocess, temp_workspace):
        """Check should print a status checking message."""
        mock_subprocess.run.return_value = None

        result = runner.invoke(app, ["check"])

        assert result.exit_code == 0
        assert "Checking Pod Status" in result.stdout
