"""
E2E Tests for CLI 'reflowfy build' command.

Tests the build command's argument validation, image building behavior,
and flag handling using mocked Docker client.
"""

import os
import pytest
import tempfile
import shutil
from unittest.mock import patch, MagicMock
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


class TestBuildCommand:
    """Tests for the 'reflowfy build' command."""

    def test_missing_registry_exits_with_error(self, temp_workspace):
        """Build without --registry should fail with exit code 1."""
        # Unset REGISTRY env var if present
        env = {k: v for k, v in os.environ.items() if k != "REGISTRY"}
        result = runner.invoke(app, ["build"], env=env)

        assert result.exit_code == 1
        assert "Registry is required" in result.stdout

    def test_warns_when_no_pipelines_directory(self, temp_workspace):
        """Build should warn when pipelines/ directory is missing."""
        with patch("reflowfy.cli.commands.build.docker") as mock_docker:
            mock_docker.build = MagicMock()
            result = runner.invoke(
                app, ["build", "--registry", "registry.test.local", "--no-push"]
            )
            assert "No 'pipelines/' folder found" in result.stdout

    @patch("reflowfy.cli.commands.build.docker")
    def test_build_with_registry_targets_all_images(self, mock_docker, temp_workspace):
        """Build should build all 3 images (api, reflow-manager, worker)."""
        os.mkdir("pipelines")
        mock_docker.build = MagicMock()
        mock_docker.push = MagicMock()

        result = runner.invoke(
            app, ["build", "--registry", "registry.test.local", "--no-push"]
        )

        assert result.exit_code == 0
        assert mock_docker.build.call_count == 3

        # Verify all three image names appear in the built tags
        all_tags = []
        for call in mock_docker.build.call_args_list:
            all_tags.extend(call.kwargs.get("tags", []))

        tag_str = " ".join(all_tags)
        assert "reflowfy-api" in tag_str
        assert "reflowfy-reflow-manager" in tag_str
        assert "reflowfy-worker" in tag_str

    @patch("reflowfy.cli.commands.build.docker")
    def test_build_with_no_cache_flag(self, mock_docker, temp_workspace):
        """Build with --no-cache should pass cache=False to Docker."""
        os.mkdir("pipelines")
        mock_docker.build = MagicMock()

        result = runner.invoke(
            app,
            ["build", "--registry", "registry.test.local", "--no-push", "--no-cache"],
        )

        assert result.exit_code == 0
        for call in mock_docker.build.call_args_list:
            assert call.kwargs.get("cache") is False

    @patch("reflowfy.cli.commands.build.docker")
    def test_build_without_push(self, mock_docker, temp_workspace):
        """Build with --no-push should not call docker.push."""
        os.mkdir("pipelines")
        mock_docker.build = MagicMock()
        mock_docker.push = MagicMock()

        result = runner.invoke(
            app, ["build", "--registry", "registry.test.local", "--no-push"]
        )

        assert result.exit_code == 0
        mock_docker.push.assert_not_called()

    @patch("reflowfy.cli.commands.build.docker")
    def test_build_with_push(self, mock_docker, temp_workspace):
        """Build with push enabled should call docker.push for each image."""
        os.mkdir("pipelines")
        mock_docker.build = MagicMock()
        mock_docker.push = MagicMock()

        result = runner.invoke(
            app, ["build", "--registry", "registry.test.local", "--push"]
        )

        assert result.exit_code == 0
        assert mock_docker.push.call_count > 0

    @patch("reflowfy.cli.commands.build.docker")
    def test_build_uses_custom_project(self, mock_docker, temp_workspace):
        """Build with --project should use it in image tags."""
        os.mkdir("pipelines")
        mock_docker.build = MagicMock()

        result = runner.invoke(
            app,
            [
                "build",
                "--registry",
                "registry.test.local",
                "--project",
                "my-team",
                "--no-push",
            ],
        )

        assert result.exit_code == 0
        all_tags = []
        for call in mock_docker.build.call_args_list:
            all_tags.extend(call.kwargs.get("tags", []))

        tag_str = " ".join(all_tags)
        assert "registry.test.local/my-team/" in tag_str
