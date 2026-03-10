"""
E2E Tests for CLI 'reflowfy deploy' command.

Tests the deploy command's Helm chart deployment behavior,
argument validation, and flag handling using mocked subprocess calls.
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


def _strip_env(*keys):
    """Return a copy of os.environ without specific keys."""
    return {k: v for k, v in os.environ.items() if k not in keys}


class TestDeployCommand:
    """Tests for the 'reflowfy deploy' command."""

    def test_missing_registry_exits_with_error(self, temp_workspace):
        """Deploy without --registry should fail with exit code 1."""
        env = _strip_env("REGISTRY")
        result = runner.invoke(app, ["deploy"], env=env)

        assert result.exit_code == 1
        assert "Registry is required" in result.stdout

    def test_missing_kafka_exits_with_error(self, temp_workspace):
        """Deploy with --registry but without --kafka should fail."""
        env = _strip_env("KAFKA_BOOTSTRAP_SERVERS")
        result = runner.invoke(
            app, ["deploy", "--registry", "registry.test.local"], env=env
        )

        assert result.exit_code == 1
        assert "Kafka is required" in result.stdout

    @patch("reflowfy.cli.commands.deploy.subprocess")
    @patch("reflowfy.cli.commands.deploy.get_helm_chart_path")
    def test_deploy_constructs_correct_helm_command(
        self, mock_chart_path, mock_subprocess, temp_workspace
    ):
        """Deploy should construct a valid helm upgrade --install command."""
        mock_chart_path.return_value = "/fake/chart/path"
        mock_subprocess.run.return_value = MagicMock(returncode=0)
        mock_subprocess.CalledProcessError = Exception
        mock_subprocess.DEVNULL = -1

        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--namespace", "test-ns",
            ],
        )

        assert result.exit_code == 0

        # First call should be the helm command
        helm_call = mock_subprocess.run.call_args_list[0]
        helm_cmd = helm_call[0][0]

        assert "helm" in helm_cmd
        assert "upgrade" in helm_cmd
        assert "--install" in helm_cmd
        assert "reflowfy" in helm_cmd
        assert "--namespace" in helm_cmd
        assert "test-ns" in helm_cmd

        # Check image repos are set
        cmd_str = " ".join(helm_cmd)
        assert "registry.test.local" in cmd_str
        assert "kafka.test:9092" in cmd_str

    @patch("reflowfy.cli.commands.deploy.subprocess")
    @patch("reflowfy.cli.commands.deploy.get_helm_chart_path")
    def test_deploy_with_keda_enabled(
        self, mock_chart_path, mock_subprocess, temp_workspace
    ):
        """Deploy with --keda should include KEDA configuration."""
        mock_chart_path.return_value = "/fake/chart/path"
        mock_subprocess.run.return_value = MagicMock(returncode=0)
        mock_subprocess.CalledProcessError = Exception
        mock_subprocess.DEVNULL = -1

        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--keda",
                "--keda-min", "2",
                "--keda-max", "50",
            ],
        )

        assert result.exit_code == 0

        helm_call = mock_subprocess.run.call_args_list[0]
        cmd_str = " ".join(helm_call[0][0])

        assert "worker.keda.enabled=true" in cmd_str
        assert "worker.keda.minReplicaCount=2" in cmd_str
        assert "worker.keda.maxReplicaCount=50" in cmd_str

    @patch("reflowfy.cli.commands.deploy.subprocess")
    @patch("reflowfy.cli.commands.deploy.get_helm_chart_path")
    def test_deploy_without_keda(
        self, mock_chart_path, mock_subprocess, temp_workspace
    ):
        """Deploy without --keda should disable it and set worker replicas."""
        mock_chart_path.return_value = "/fake/chart/path"
        mock_subprocess.run.return_value = MagicMock(returncode=0)
        mock_subprocess.CalledProcessError = Exception
        mock_subprocess.DEVNULL = -1

        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--no-keda",
                "--workers", "3",
            ],
        )

        assert result.exit_code == 0

        helm_call = mock_subprocess.run.call_args_list[0]
        cmd_str = " ".join(helm_call[0][0])

        assert "worker.keda.enabled=false" in cmd_str
        assert "worker.replicaCount=3" in cmd_str

    @patch("reflowfy.cli.commands.deploy.subprocess")
    @patch("reflowfy.cli.commands.deploy.get_helm_chart_path")
    def test_deploy_with_external_postgres(
        self, mock_chart_path, mock_subprocess, temp_workspace
    ):
        """Deploy with --no-deploy-postgres should use external DB config."""
        mock_chart_path.return_value = "/fake/chart/path"
        mock_subprocess.run.return_value = MagicMock(returncode=0)
        mock_subprocess.CalledProcessError = Exception
        mock_subprocess.DEVNULL = -1

        env = {
            **os.environ,
            "DATABASE_URL": "postgresql://user:pass@db.host.local:5432/mydb",
        }

        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--no-deploy-postgres",
            ],
            env=env,
        )

        assert result.exit_code == 0

        helm_call = mock_subprocess.run.call_args_list[0]
        cmd_str = " ".join(helm_call[0][0])

        assert "postgresql.enabled=false" in cmd_str
        assert "postgresql.external.host=db.host.local" in cmd_str
        assert "postgresql.external.port=5432" in cmd_str
        assert "postgresql.external.database=mydb" in cmd_str

    @patch("reflowfy.cli.commands.deploy.subprocess")
    @patch("reflowfy.cli.commands.deploy.get_helm_chart_path")
    def test_deploy_with_custom_postgres_image(
        self, mock_chart_path, mock_subprocess, temp_workspace
    ):
        """Deploy with --postgres-image should set repo and tag."""
        mock_chart_path.return_value = "/fake/chart/path"
        mock_subprocess.run.return_value = MagicMock(returncode=0)
        mock_subprocess.CalledProcessError = Exception
        mock_subprocess.DEVNULL = -1

        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--postgres-image", "myrepo/postgres:14",
            ],
        )

        assert result.exit_code == 0

        helm_call = mock_subprocess.run.call_args_list[0]
        cmd_str = " ".join(helm_call[0][0])

        assert "postgresql.image.repository=myrepo/postgres" in cmd_str
        assert "postgresql.image.tag=14" in cmd_str

    def test_deploy_external_postgres_without_database_url(self, temp_workspace):
        """Deploy with --no-deploy-postgres but no DATABASE_URL should fail."""
        env = _strip_env("DATABASE_URL")
        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--no-deploy-postgres",
            ],
            env=env,
        )

        assert result.exit_code == 1
        assert "DATABASE_URL is required" in result.stdout

    @patch("reflowfy.cli.commands.deploy.subprocess")
    @patch("reflowfy.cli.commands.deploy.get_helm_chart_path")
    def test_deploy_with_image_pull_secret(
        self, mock_chart_path, mock_subprocess, temp_workspace
    ):
        """Deploy with --image-pull-secret should set the secret name."""
        mock_chart_path.return_value = "/fake/chart/path"
        mock_subprocess.run.return_value = MagicMock(returncode=0)
        mock_subprocess.CalledProcessError = Exception
        mock_subprocess.DEVNULL = -1

        result = runner.invoke(
            app,
            [
                "deploy",
                "--registry", "registry.test.local",
                "--kafka", "kafka.test:9092",
                "--image-pull-secret", "my-secret",
            ],
        )

        assert result.exit_code == 0

        helm_call = mock_subprocess.run.call_args_list[0]
        cmd_str = " ".join(helm_call[0][0])

        assert "global.imagePullSecrets[0].name=my-secret" in cmd_str
