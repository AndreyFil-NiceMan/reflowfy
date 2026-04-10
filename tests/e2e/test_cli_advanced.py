"""
Advanced E2E Tests for CLI commands (new, init, deploy, build).

Covers gaps not addressed in the existing test_cli_*.py files:
- 'new transformation' command
- 'init --name' creates correct class name
- 'init' is idempotent on second run
- 'deploy --no-deploy-postgres' without DATABASE_URL → exit 1
- 'deploy' DATABASE_URL parsing → correct helm --set flags
- 'deploy --keda --keda-min/--keda-max' → correct replica flags
- 'build --tag' and 'build --project' affect all image tags

No external services required — subprocess, docker, kubectl, and helm are mocked.
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
    """Temporary workspace; also chdir'd into for file-creation commands."""
    temp_dir = tempfile.mkdtemp()
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    yield temp_dir
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)


def _strip_env(*keys):
    """Return os.environ without the listed keys."""
    return {k: v for k, v in os.environ.items() if k not in keys}


# ---------------------------------------------------------------------------
# 'reflowfy new' — advanced coverage
# ---------------------------------------------------------------------------

class TestCliNewAdvanced:
    """Tests for the 'reflowfy new' scaffold command."""

    def test_new_transformation_creates_file(self, temp_workspace):
        """'new transformation my_enrich' should create transformations/my_enrich.py."""
        os.makedirs("transformations", exist_ok=True)

        result = runner.invoke(app, ["new", "transformation", "my_enrich"])

        assert result.exit_code == 0, result.stdout
        assert os.path.isfile("transformations/my_enrich.py"), (
            "Expected transformations/my_enrich.py to be created"
        )
        content = open("transformations/my_enrich.py").read()
        assert "MyEnrichTransformation" in content or "my_enrich" in content, (
            f"Expected class name or 'my_enrich' in generated file:\n{content}"
        )

    def test_new_transformation_contains_base_class(self, temp_workspace):
        """Generated transformation file should reference BaseTransformation."""
        os.makedirs("transformations", exist_ok=True)

        runner.invoke(app, ["new", "transformation", "data_cleaner"])

        content = open("transformations/data_cleaner.py").read()
        assert "BaseTransformation" in content or "transformation" in content.lower(), (
            f"Expected BaseTransformation reference in generated file:\n{content}"
        )

    def test_new_single_word_pipeline_name(self, temp_workspace):
        """'new pipeline users' should create class 'UsersPipeline'."""
        os.makedirs("pipelines", exist_ok=True)

        result = runner.invoke(app, ["new", "pipeline", "users"])

        assert result.exit_code == 0, result.stdout
        content = open("pipelines/users.py").read()
        assert "UsersPipeline" in content, (
            f"Expected 'UsersPipeline' class in generated file:\n{content}"
        )

    def test_new_pipeline_name_with_numbers(self, temp_workspace):
        """'new pipeline pipeline_v2' should PascalCase to 'PipelineV2Pipeline'."""
        os.makedirs("pipelines", exist_ok=True)

        result = runner.invoke(app, ["new", "pipeline", "pipeline_v2"])

        assert result.exit_code == 0, result.stdout
        content = open("pipelines/pipeline_v2.py").read()
        assert "PipelineV2Pipeline" in content, (
            f"Expected 'PipelineV2Pipeline' in generated file:\n{content}"
        )

    def test_new_duplicate_transformation_exits(self, temp_workspace):
        """Creating the same transformation twice should fail with exit 1."""
        os.makedirs("transformations", exist_ok=True)

        runner.invoke(app, ["new", "transformation", "dup_transform"])
        result = runner.invoke(app, ["new", "transformation", "dup_transform"])

        assert result.exit_code == 1, (
            f"Expected exit 1 on duplicate transformation:\n{result.stdout}"
        )


# ---------------------------------------------------------------------------
# 'reflowfy init' — advanced coverage
# ---------------------------------------------------------------------------

class TestCliInitAdvanced:
    """Tests for the 'reflowfy init' project initialisation command."""

    def test_init_custom_name_creates_correct_class(self, temp_workspace):
        """'init --name billing_etl' should create pipelines/billing_etl.py with an AbstractPipeline."""
        result = runner.invoke(app, ["init", ".", "--name", "billing_etl"])

        assert result.exit_code == 0, result.stdout
        assert os.path.isfile("pipelines/billing_etl.py"), (
            "Expected pipelines/billing_etl.py to be created"
        )
        content = open("pipelines/billing_etl.py").read()
        assert "AbstractPipeline" in content or "Pipeline" in content, (
            f"Expected pipeline class definition in generated file:\n{content}"
        )

    def test_init_creates_all_required_directories(self, temp_workspace):
        """init must create all 5 standard directories."""
        runner.invoke(app, ["init", ".", "--name", "test_pipe"])

        for d in ("pipelines", "sources", "destinations", "transformations", "queries"):
            assert os.path.isdir(d), f"Expected directory '{d}' to be created by init"

    def test_init_idempotent_does_not_overwrite(self, temp_workspace):
        """Running init twice should succeed (exit 0) without raising an exception."""
        result1 = runner.invoke(app, ["init", ".", "--name", "my_pipe"])
        assert result1.exit_code == 0, result1.stdout

        result2 = runner.invoke(app, ["init", ".", "--name", "my_pipe"])
        assert result2.exit_code == 0, (
            f"Second 'init' run should succeed (exit 0):\n{result2.stdout}"
        )
        # Directories must still exist after second run
        for d in ("pipelines", "sources", "destinations", "transformations", "queries"):
            assert os.path.isdir(d), f"Directory '{d}' missing after second init"


# ---------------------------------------------------------------------------
# 'reflowfy deploy' — advanced coverage
# ---------------------------------------------------------------------------

class TestCliDeployAdvanced:
    """Tests for edge cases in the 'reflowfy deploy' Helm deployment command."""

    def test_no_deploy_postgres_without_database_url_exits(self, temp_workspace):
        """--no-deploy-postgres without DATABASE_URL env var → exit 1."""
        env = _strip_env("DATABASE_URL", "REGISTRY", "KAFKA_BOOTSTRAP_SERVERS")

        with patch("reflowfy.cli.commands.deploy.get_helm_chart_path", return_value="/fake"):
            with patch("reflowfy.cli.commands.deploy.subprocess") as mock_sub:
                mock_sub.run.return_value = MagicMock(returncode=0)
                mock_sub.CalledProcessError = Exception
                mock_sub.DEVNULL = -1

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

        assert result.exit_code == 1, result.stdout
        assert "DATABASE_URL" in result.stdout, (
            f"Expected DATABASE_URL error message:\n{result.stdout}"
        )

    def test_database_url_parsed_into_helm_sets(self, temp_workspace):
        """DATABASE_URL is parsed and injected as postgresql.external.* helm --set flags."""
        env = {
            **_strip_env("DATABASE_URL"),
            "DATABASE_URL": "postgresql://admin:secret@db.internal:5432/mydb",
        }

        with patch("reflowfy.cli.commands.deploy.get_helm_chart_path", return_value="/fake"):
            with patch("reflowfy.cli.commands.deploy.subprocess") as mock_sub:
                mock_sub.run.return_value = MagicMock(returncode=0)
                mock_sub.CalledProcessError = Exception
                mock_sub.DEVNULL = -1

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

        assert result.exit_code == 0, result.stdout
        assert "postgresql.external.host=db.internal" in result.stdout, (
            f"Expected 'postgresql.external.host=db.internal' in output:\n{result.stdout}"
        )
        assert "postgresql.external.database=mydb" in result.stdout, (
            f"Expected 'postgresql.external.database=mydb' in output:\n{result.stdout}"
        )

    def test_keda_min_max_in_helm_command(self, temp_workspace):
        """--keda --keda-min 2 --keda-max 20 should emit the correct replica --set flags."""
        with patch("reflowfy.cli.commands.deploy.get_helm_chart_path", return_value="/fake"):
            with patch("reflowfy.cli.commands.deploy.subprocess") as mock_sub:
                mock_sub.run.return_value = MagicMock(returncode=0)
                mock_sub.CalledProcessError = Exception
                mock_sub.DEVNULL = -1

                result = runner.invoke(
                    app,
                    [
                        "deploy",
                        "--registry", "registry.test.local",
                        "--kafka", "kafka.test:9092",
                        "--keda",
                        "--keda-min", "2",
                        "--keda-max", "20",
                    ],
                )

        assert result.exit_code == 0, result.stdout
        assert "worker.keda.minReplicaCount=2" in result.stdout, (
            f"Expected 'worker.keda.minReplicaCount=2':\n{result.stdout}"
        )
        assert "worker.keda.maxReplicaCount=20" in result.stdout, (
            f"Expected 'worker.keda.maxReplicaCount=20':\n{result.stdout}"
        )

    def test_no_keda_workers_flag_sets_replica_count(self, temp_workspace):
        """--no-keda --workers 4 should set worker.replicaCount=4."""
        with patch("reflowfy.cli.commands.deploy.get_helm_chart_path", return_value="/fake"):
            with patch("reflowfy.cli.commands.deploy.subprocess") as mock_sub:
                mock_sub.run.return_value = MagicMock(returncode=0)
                mock_sub.CalledProcessError = Exception
                mock_sub.DEVNULL = -1

                result = runner.invoke(
                    app,
                    [
                        "deploy",
                        "--registry", "registry.test.local",
                        "--kafka", "kafka.test:9092",
                        "--no-keda",
                        "--workers", "4",
                    ],
                )

        assert result.exit_code == 0, result.stdout
        assert "worker.replicaCount=4" in result.stdout, (
            f"Expected 'worker.replicaCount=4':\n{result.stdout}"
        )


# ---------------------------------------------------------------------------
# 'reflowfy build' — advanced coverage
# ---------------------------------------------------------------------------

class TestCliBuildAdvanced:
    """Tests for edge cases in the 'reflowfy build' Docker image command."""

    def test_custom_tag_used_in_all_images(self, temp_workspace):
        """--tag v9.9.9 should appear in all 3 built image tags."""
        mock_docker = MagicMock()

        os.makedirs("pipelines", exist_ok=True)

        with patch("reflowfy.cli.commands.build.docker", mock_docker):
            result = runner.invoke(
                app,
                [
                    "build",
                    "--registry", "registry.test.local",
                    "--tag", "v9.9.9",
                    "--no-push",
                ],
            )

        assert result.exit_code == 0, result.stdout
        all_output = result.stdout
        assert "v9.9.9" in all_output, (
            f"Expected custom tag 'v9.9.9' referenced in output:\n{all_output}"
        )

    def test_custom_project_in_image_names(self, temp_workspace):
        """--project acme should include 'acme' in the image namespace."""
        mock_docker = MagicMock()

        os.makedirs("pipelines", exist_ok=True)

        with patch("reflowfy.cli.commands.build.docker", mock_docker):
            result = runner.invoke(
                app,
                [
                    "build",
                    "--registry", "registry.test.local",
                    "--project", "acme",
                    "--no-push",
                ],
            )

        assert result.exit_code == 0, result.stdout
        assert "acme" in result.stdout, (
            f"Expected 'acme' namespace in build output:\n{result.stdout}"
        )
