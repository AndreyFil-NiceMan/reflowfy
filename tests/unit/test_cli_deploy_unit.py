"""`reflowfy deploy`: the helm/oc command lines it builds. Nothing is executed."""

import subprocess
from pathlib import Path
from typing import Any, Dict, List

import pytest
from typer.testing import CliRunner

import reflowfy
from reflowfy.cli.commands import deploy as deploy_mod
from reflowfy.cli.main import app
from reflowfy.cli.utils import console

runner = CliRunner()

CHART = Path("/charts/reflowfy")

# Every envvar the command reads; a developer's shell or .env must not leak in.
DEPLOY_ENV = [
    "REGISTRY",
    "PROJECT",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_SASL_ENABLED",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SASL_EXISTING_SECRET",
    "BUSYBOX_IMAGE",
    "NAMESPACE",
    "IMAGE_PULL_SECRET",
    "DEPLOY_POSTGRES",
    "POSTGRES_IMAGE",
    "POSTGRES_ROUTE_ENABLED",
    "POSTGRES_PASSWORD",
    "KEDA_ENABLED",
    "KEDA_MIN_REPLICAS",
    "KEDA_MAX_REPLICAS",
    "KEDA_LAG_THRESHOLD",
    "KAFKA_TOPIC",
    "KAFKA_GROUP_ID",
    "WORKERS",
    "IMAGE_TAG",
    "LOG_DESTINATION",
    "ELASTIC_LOG_URL",
    "ELASTIC_LOG_USERNAME",
    "ELASTIC_LOG_PASSWORD",
    "DATABASE_URL",
]


@pytest.fixture
def calls(monkeypatch, tmp_path) -> List[Dict[str, Any]]:
    """Record subprocess.run calls; chart path fixed; env and cwd isolated."""
    for name in DEPLOY_ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(deploy_mod, "get_helm_chart_path", lambda: CHART)
    monkeypatch.setattr(console, "_width", 400)
    recorded: List[Dict[str, Any]] = []

    def fake_run(cmd: List[str], **kwargs: Any) -> subprocess.CompletedProcess:
        recorded.append({"cmd": list(cmd), **kwargs})
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(deploy_mod.subprocess, "run", fake_run)
    return recorded


BASE = ["--registry", "reg.local", "--kafka", "k1:9092,k2:9092", "--kafka-password", "kpw"]


def deploy(*args: str) -> Any:
    return runner.invoke(app, ["deploy", *args])


def sets(cmd: List[str]) -> List[str]:
    """The values passed with --set, in order."""
    return [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "--set"]


def set_map(cmd: List[str]) -> Dict[str, str]:
    return dict(s.split("=", 1) for s in sets(cmd))


class TestValidation:
    def test_registry_required(self, calls):
        result = deploy("--kafka", "k:9092", "--no-kafka-sasl")
        assert result.exit_code == 1
        assert "Registry is required" in result.output
        assert "No .env file found" in result.output
        assert calls == []

    def test_kafka_required(self, calls):
        result = deploy("--registry", "reg.local")
        assert result.exit_code == 1
        assert "Kafka is required" in result.output
        assert calls == []

    def test_sasl_needs_a_password_or_secret(self, calls):
        result = deploy("--registry", "reg.local", "--kafka", "k:9092")
        assert result.exit_code == 1
        assert "Kafka SASL is enabled but no password was provided" in result.output
        assert calls == []

    def test_pg_route_needs_a_password(self, calls):
        result = deploy(*BASE, "--pg-route")
        assert result.exit_code == 1
        assert "--pg-route requires --postgres-password" in result.output
        assert calls == []

    def test_external_db_needs_database_url(self, calls):
        result = deploy(*BASE, "--no-deploy-postgres")
        assert result.exit_code == 1
        assert "DATABASE_URL is required" in result.output
        assert calls == []

    def test_missing_chart(self, calls, monkeypatch):
        def missing() -> Path:
            raise FileNotFoundError("Helm chart not found.")

        monkeypatch.setattr(deploy_mod, "get_helm_chart_path", missing)
        result = deploy(*BASE)
        assert result.exit_code == 1
        assert "Helm chart not found." in result.output
        assert calls == []


class TestHelmCommand:
    def test_defaults(self, calls):
        result = deploy(*BASE)
        assert result.exit_code == 0, result.output

        helm, expose_api, expose_manager = calls
        cmd = helm["cmd"]
        assert cmd[:5] == ["helm", "upgrade", "--install", "reflowfy", str(CHART)]
        assert cmd[5:7] == ["--namespace", "reflowfy"]
        assert helm["check"] is True

        values = set_map(cmd)
        version = reflowfy.__version__
        for component in ("api", "reflow-manager", "worker"):
            key = {"api": "api", "reflow-manager": "reflowManager", "worker": "worker"}[component]
            assert values[f"{key}.image.repository"] == f"reg.local/reflowfy/reflowfy-{component}"
            assert values[f"{key}.image.tag"] == version
            assert values[f"{key}.image.pullPolicy"] == "Always"
        assert values["kafka.external.bootstrapServers"] == "k1:9092\\,k2:9092"
        assert values["kafka.topic"] == "reflow.jobs"
        assert values["kafka.groupId"] == "reflowfy-workers"
        assert values["kafka.sasl.enabled"] == "true"
        assert values["kafka.sasl.password"] == "kpw"
        assert values["busybox.image"] == "busybox:1.36"
        assert values["postgresql.enabled"] == "true"
        assert values["worker.keda.enabled"] == "false"
        assert values["worker.replicaCount"] == "1"
        assert "deployTimestamp" in values
        assert "global.imagePullSecrets[0].name" not in values
        assert not any(k.startswith("observability.") for k in values)

        assert expose_api["cmd"] == [
            "oc",
            "expose",
            "svc/reflowfy-api",
            "--name=reflowfy-api",
            "-n",
            "reflowfy",
        ]
        assert expose_manager["cmd"][:3] == ["oc", "expose", "svc/reflowfy-reflow-manager"]
        assert "Deployment Complete" in result.output

    def test_namespace_project_tag_and_pull_secret(self, calls):
        result = deploy(
            *BASE,
            "--namespace",
            "team-a",
            "--project",
            "proj",
            "--tag",
            "1.2.3",
            "--image-pull-secret",
            "pull",
        )
        assert result.exit_code == 0, result.output
        cmd = calls[0]["cmd"]
        assert cmd[5:7] == ["--namespace", "team-a"]
        values = set_map(cmd)
        assert values["api.image.repository"] == "reg.local/proj/reflowfy-api"
        assert values["worker.image.tag"] == "1.2.3"
        assert values["global.imagePullSecrets[0].name"] == "pull"
        assert calls[1]["cmd"][-2:] == ["-n", "team-a"]

    def test_project_defaults_to_namespace(self, calls):
        result = deploy(*BASE, "--namespace", "team-b")
        assert result.exit_code == 0, result.output
        assert set_map(calls[0]["cmd"])["api.image.repository"] == "reg.local/team-b/reflowfy-api"

    def test_existing_secret_wins_over_password(self, calls):
        result = deploy(
            "--registry",
            "reg.local",
            "--kafka",
            "k:9092",
            "--kafka-username",
            "svc",
            "--kafka-existing-secret",
            "kafka-creds",
        )
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["kafka.sasl.username"] == "svc"
        assert values["kafka.sasl.existingSecret"] == "kafka-creds"
        assert "kafka.sasl.password" not in values

    def test_no_sasl(self, calls):
        result = deploy("--registry", "reg.local", "--kafka", "k:9092", "--no-kafka-sasl")
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["kafka.sasl.enabled"] == "false"
        assert not any(k.startswith("kafka.sasl.") and k != "kafka.sasl.enabled" for k in values)

    @pytest.mark.parametrize(
        "image, repo, tag",
        [
            ("myrepo/postgres:14", "myrepo/postgres", "14"),
            ("myrepo/postgres", "myrepo/postgres", "latest"),
        ],
    )
    def test_custom_postgres_image(self, calls, image, repo, tag):
        result = deploy(*BASE, "--postgres-image", image)
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["postgresql.image.repository"] == repo
        assert values["postgresql.image.tag"] == tag
        assert values["postgresql.image.registry"] == ""
        assert values["global.security.allowInsecureImages"] == "true"

    def test_postgres_password_and_route(self, calls):
        result = deploy(*BASE, "--postgres-password", "pgpw", "--pg-route")
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["postgresql.auth.password"] == "pgpw"
        assert values["postgresql.auth.postgresPassword"] == "pgpw"
        assert values["postgresql.route.enabled"] == "true"

    def test_external_database(self, calls, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pw@db.example:6543/reflow")
        result = deploy(*BASE, "--no-deploy-postgres")
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["postgresql.enabled"] == "false"
        assert values["postgresql.external.host"] == "db.example"
        assert values["postgresql.external.port"] == "6543"
        assert values["postgresql.external.database"] == "reflow"
        assert values["postgresql.external.username"] == "user"
        assert values["postgresql.external.password"] == "pw"

    def test_external_database_default_port(self, calls, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user@db.example/reflow")
        result = deploy(*BASE, "--no-deploy-postgres")
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["postgresql.external.port"] == "5432"
        assert values["postgresql.external.password"] == ""

    def test_keda(self, calls):
        result = deploy(
            *BASE,
            "--keda",
            "--keda-min",
            "2",
            "--keda-max",
            "20",
            "--keda-lag-threshold",
            "50",
        )
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["worker.keda.enabled"] == "true"
        assert values["worker.keda.minReplicaCount"] == "2"
        assert values["worker.keda.maxReplicaCount"] == "20"
        assert values["worker.keda.lagThreshold"] == "50"
        assert "worker.replicaCount" not in values

    def test_workers_topic_group_and_observability(self, calls):
        result = deploy(
            *BASE,
            "--workers",
            "4",
            "--kafka-topic",
            "t",
            "--kafka-group-id",
            "g",
            "--log-destination",
            "both",
            "--elastic-log-url",
            "https://es:9200",
            "--elastic-log-username",
            "elastic",
            "--elastic-log-password",
            "espw",
        )
        assert result.exit_code == 0, result.output
        values = set_map(calls[0]["cmd"])
        assert values["worker.replicaCount"] == "4"
        assert values["kafka.topic"] == "t"
        assert values["kafka.groupId"] == "g"
        assert values["observability.logDestination"] == "both"
        assert values["observability.elasticLog.url"] == "https://es:9200"
        assert values["observability.elasticLog.username"] == "elastic"
        assert values["observability.elasticLog.password"] == "espw"

    def test_reads_options_from_environment(self, calls, monkeypatch):
        monkeypatch.setenv("REGISTRY", "env.reg")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "envk:9092")
        monkeypatch.setenv("KAFKA_SASL_ENABLED", "false")
        monkeypatch.setenv("NAMESPACE", "env-ns")
        monkeypatch.setenv("IMAGE_TAG", "9.9.9")
        result = deploy()
        assert result.exit_code == 0, result.output
        cmd = calls[0]["cmd"]
        assert cmd[5:7] == ["--namespace", "env-ns"]
        values = set_map(cmd)
        assert values["api.image.repository"] == "env.reg/env-ns/reflowfy-api"
        assert values["api.image.tag"] == "9.9.9"
        assert values["kafka.external.bootstrapServers"] == "envk:9092"


class TestExecution:
    def test_helm_failure_stops_before_routes(self, calls, monkeypatch):
        def failing_run(cmd: List[str], **kwargs: Any) -> None:
            calls.append({"cmd": list(cmd), **kwargs})
            raise subprocess.CalledProcessError(1, cmd)

        monkeypatch.setattr(deploy_mod.subprocess, "run", failing_run)
        result = deploy(*BASE)
        assert result.exit_code == 1
        assert "Deployment failed" in result.output
        assert [c["cmd"][0] for c in calls] == ["helm"]

    def test_missing_oc_is_a_warning(self, calls, monkeypatch):
        def run(cmd: List[str], **kwargs: Any) -> subprocess.CompletedProcess:
            calls.append({"cmd": list(cmd), **kwargs})
            if cmd[0] == "oc":
                raise FileNotFoundError("oc")
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(deploy_mod.subprocess, "run", run)
        result = deploy(*BASE)
        assert result.exit_code == 0, result.output
        assert "'oc' command not found. Skipping route creation." in result.output
        assert "Deployment Complete" in result.output

    def test_route_output_is_silenced(self, calls):
        deploy(*BASE)
        for call in calls[1:]:
            assert call["stdout"] is subprocess.DEVNULL
            assert call["stderr"] is subprocess.DEVNULL

    def test_helm_command_is_printed(self, calls):
        result = deploy(*BASE)
        assert "Helm Command:" in result.output
        assert "helm" in result.output and "upgrade" in result.output

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "BUG: deploy prints the full helm command, including --set values for "
            "kafka.sasl.password, postgresql.auth.* and observability.elasticLog.password, "
            "so secrets land in the terminal and CI logs"
        ),
    )
    def test_printed_helm_command_masks_secrets(self, calls):
        result = deploy(
            *BASE,
            "--postgres-password",
            "pg-s3cret",
            "--elastic-log-password",
            "es-s3cret",
        )
        assert result.exit_code == 0, result.output
        # The real command still carries them; only the echo must not.
        assert set_map(calls[0]["cmd"])["postgresql.auth.password"] == "pg-s3cret"
        for secret in ("kpw", "pg-s3cret", "es-s3cret"):
            assert f"={secret}" not in result.output
