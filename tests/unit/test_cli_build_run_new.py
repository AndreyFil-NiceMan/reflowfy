"""`reflowfy build | run | new | init | check | version` and cli/utils.

docker, docker compose and kubectl are never run: the Docker client and
subprocess.run are replaced and the argv they would have received is asserted.
"""

import importlib.util
import stat
import subprocess
import warnings
from pathlib import Path
from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

import reflowfy
from reflowfy import pipeline_registry
from reflowfy.cli import utils
from reflowfy.cli.commands import build as build_mod
from reflowfy.cli.commands import check as check_mod
from reflowfy.cli.commands import init_cmd as init_mod
from reflowfy.cli.commands import run as run_mod
from reflowfy.cli.main import app
from reflowfy.cli.utils import console

runner = CliRunner()


@pytest.fixture(autouse=True)
def _cwd(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(console, "_width", 300)
    for name in ("REGISTRY", "PROJECT", "REFLOWFY_BASE_IMAGE"):
        monkeypatch.delenv(name, raising=False)


def cli(*args: str, **kwargs: Any) -> Any:
    return runner.invoke(app, list(args), **kwargs)


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


@pytest.fixture
def docker(monkeypatch, tmp_path) -> MagicMock:
    client = MagicMock()
    monkeypatch.setattr(build_mod, "docker", client)
    monkeypatch.setattr(build_mod, "get_dockerfiles_path", lambda: Path("/dockerfiles"))
    return client


IMAGES = ["reflowfy-api", "reflowfy-reflow-manager", "reflowfy-worker"]
DOCKERFILES = ["Dockerfile.api", "Dockerfile.reflow-manager", "Dockerfile.worker"]


class TestBuild:
    def test_registry_required(self, docker):
        result = cli("build")
        assert result.exit_code == 1
        assert "Registry is required" in result.output
        assert "No .env file found" in result.output
        docker.build.assert_not_called()

    def test_builds_and_pushes_every_image_with_version_tags(self, docker, tmp_path):
        (tmp_path / "src" / "pipelines").mkdir(parents=True)
        result = cli("build", "--registry", "reg.local")
        assert result.exit_code == 0, result.output
        assert "No 'src/pipelines/' folder" not in result.output

        version = reflowfy.__version__
        assert docker.build.call_count == 3
        for call, image, dockerfile in zip(docker.build.call_args_list, IMAGES, DOCKERFILES):
            assert call.args == (".",)
            assert call.kwargs == {
                "file": f"/dockerfiles/{dockerfile}",
                "tags": [
                    f"reg.local/reflowfy/{image}:latest",
                    f"reg.local/reflowfy/{image}:{version}",
                ],
                "cache": True,
                "load": True,
                "build_args": {"REFLOWFY_BASE_IMAGE": "reflowfy-base:latest"},
            }
        pushed = [c.args[0] for c in docker.push.call_args_list]
        assert pushed == [
            f"reg.local/reflowfy/{image}:{tag}" for image in IMAGES for tag in ("latest", version)
        ]
        assert "Build & Push Complete" in result.output

    def test_custom_tag_project_no_cache_no_push_and_base_image(self, docker, monkeypatch):
        monkeypatch.setenv("REFLOWFY_BASE_IMAGE", "base:local")
        result = cli(
            "build", "--registry", "reg", "--project", "p", "-t", "v1", "--no-cache", "--no-push"
        )
        assert result.exit_code == 0, result.output
        first = docker.build.call_args_list[0].kwargs
        assert first["tags"] == ["reg/p/reflowfy-api:v1"]
        assert first["cache"] is False
        assert first["build_args"] == {"REFLOWFY_BASE_IMAGE": "base:local"}
        docker.push.assert_not_called()
        assert "(no-cache)" in result.output

    def test_registry_and_project_from_env(self, docker, monkeypatch):
        monkeypatch.setenv("REGISTRY", "env.reg")
        monkeypatch.setenv("PROJECT", "envp")
        result = cli("build", "--no-push")
        assert result.exit_code == 0, result.output
        assert (
            docker.build.call_args_list[0].kwargs["tags"][0] == "env.reg/envp/reflowfy-api:latest"
        )

    def test_warns_without_src_pipelines(self, docker):
        result = cli("build", "--registry", "reg", "--no-push")
        assert result.exit_code == 0, result.output
        assert "No 'src/pipelines/' folder found" in result.output

    def test_build_failure_exits_and_stops(self, docker):
        docker.build.side_effect = RuntimeError("daemon not running")
        result = cli("build", "--registry", "reg")
        assert result.exit_code == 1
        assert "Failed to build/push reflowfy-api: daemon not running" in result.output
        assert docker.build.call_count == 1
        docker.push.assert_not_called()

    def test_push_failure_exits(self, docker):
        docker.push.side_effect = RuntimeError("unauthorized")
        result = cli("build", "--registry", "reg")
        assert result.exit_code == 1
        assert "unauthorized" in result.output


# ---------------------------------------------------------------------------
# run / check
# ---------------------------------------------------------------------------


@pytest.fixture
def commands(monkeypatch) -> List[Dict[str, Any]]:
    recorded: List[Dict[str, Any]] = []

    def fake_run(cmd: List[str], **kwargs: Any) -> subprocess.CompletedProcess:
        recorded.append({"cmd": list(cmd), **kwargs})
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(run_mod.subprocess, "run", fake_run)
    return recorded


class TestRun:
    def test_up_in_foreground(self, commands):
        result = cli("run")
        assert result.exit_code == 0, result.output
        assert [c["cmd"] for c in commands] == [["docker", "compose", "up"]]

    def test_detach(self, commands):
        result = cli("run", "-d")
        assert result.exit_code == 0, result.output
        assert [c["cmd"] for c in commands] == [["docker", "compose", "up", "-d"]]

    def test_build_first_then_up(self, commands):
        result = cli("run", "--build", "--detach")
        assert result.exit_code == 0, result.output
        assert [c["cmd"] for c in commands] == [
            ["docker", "compose", "build", "--no-cache"],
            ["docker", "compose", "up", "-d"],
        ]
        assert commands[0]["check"] is True
        assert "Build complete" in result.output

    def test_build_failure_does_not_start_the_stack(self, monkeypatch):
        seen: List[List[str]] = []

        def fail(cmd: List[str], **kwargs: Any) -> None:
            seen.append(cmd)
            raise subprocess.CalledProcessError(1, cmd)

        monkeypatch.setattr(run_mod.subprocess, "run", fail)
        result = cli("run", "-b")
        assert result.exit_code == 1
        assert "Build failed" in result.output
        assert seen == [["docker", "compose", "build", "--no-cache"]]

    def test_ctrl_c_stops_cleanly(self, monkeypatch):
        def interrupted(cmd: List[str], **kwargs: Any) -> None:
            raise KeyboardInterrupt

        monkeypatch.setattr(run_mod.subprocess, "run", interrupted)
        result = cli("run")
        assert result.exit_code == 0, result.output
        assert "Stopping" in result.output


class TestCheck:
    def test_lists_reflowfy_pods(self, monkeypatch):
        seen: List[List[str]] = []
        monkeypatch.setattr(check_mod.subprocess, "run", lambda cmd, **kw: seen.append(cmd))
        result = cli("check")
        assert result.exit_code == 0, result.output
        assert seen == [["kubectl", "get", "pods", "-l", "app.kubernetes.io/instance=reflowfy"]]


class TestVersion:
    @pytest.mark.parametrize("args", [["version"], ["--version"], ["-v"]])
    def test_version(self, args):
        result = cli(*args)
        assert result.exit_code == 0
        assert result.output.strip() == f"reflowfy {reflowfy.__version__}"


# ---------------------------------------------------------------------------
# new
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    saved = dict(pipeline_registry._pipelines)
    pipeline_registry._pipelines.clear()
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)


def _import(path: Path, module_name: str) -> List[warnings.WarningMessage]:
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        spec.loader.exec_module(module)
    return list(caught)


class TestNew:
    def test_new_pipeline_writes_a_typed_registering_pipeline(self, tmp_path, isolated_registry):
        result = cli("new", "pipeline", "order_sync")
        assert result.exit_code == 0, result.output
        path = tmp_path / "src" / "pipelines" / "order_sync.py"
        assert "Created pipeline" in result.output
        text = path.read_text()
        assert "class OrderSyncPipelineParams(RuntimeParams, total=False):" in text
        assert "class OrderSyncPipeline(AbstractPipeline[OrderSyncPipelineParams]):" in text

        caught = _import(path, "generated_order_sync")
        assert not [w for w in caught if issubclass(w.category, DeprecationWarning)]
        pipeline = pipeline_registry.get("order_sync")
        assert pipeline is not None
        assert pipeline._params_type is not None
        assert pipeline._params_type.__name__ == "OrderSyncPipelineParams"
        assert pipeline.rate_limit == 3000
        # The stub hooks fail loudly rather than silently doing nothing.
        with pytest.raises(reflowfy.PipelineError, match="define_source is not implemented"):
            pipeline.define_source({})
        assert list(pipeline.define_transformations([], {})) == []

    def test_pipeline_suffix_is_not_doubled(self, tmp_path):
        assert cli("new", "pipeline", "users_pipeline").exit_code == 0
        text = (tmp_path / "src" / "pipelines" / "users_pipeline.py").read_text()
        assert "class UsersPipeline(AbstractPipeline[UsersPipelineParams])" in text

    @pytest.mark.parametrize(
        "kind, folder, marker",
        [
            ("pipeline", "pipelines", "Created pipeline"),
            ("source", "sources", "Created source"),
            ("destination", "destinations", "Created destination"),
            ("transformation", "transformations", "Created transformation"),
        ],
    )
    def test_refuses_to_overwrite(self, tmp_path, kind, folder, marker):
        first = cli("new", kind, "thing")
        assert first.exit_code == 0, first.output
        assert marker in first.output
        target = tmp_path / "src" / folder / "thing.py"
        target.write_text("# my edits\n")

        second = cli("new", kind, "thing")

        assert second.exit_code == 1
        assert "File already exists" in second.output
        assert target.read_text() == "# my edits\n"

    @pytest.mark.parametrize(
        "kind, folder, expected",
        [
            ("source", "sources", '@source("orders_db")\ndef orders_db(**overrides):'),
            (
                "destination",
                "destinations",
                '@destination("orders_db")\ndef orders_db(**overrides):',
            ),
            ("transformation", "transformations", "class OrdersDb(BaseTransformation):"),
        ],
    )
    def test_component_files_are_valid_python(self, tmp_path, kind, folder, expected):
        assert cli("new", kind, "orders_db").exit_code == 0
        text = (tmp_path / "src" / folder / "orders_db.py").read_text()
        assert expected in text
        compile(text, f"{folder}/orders_db.py", "exec")


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestInit:
    def test_scaffolds_a_project(self, tmp_path):
        result = cli("init", "proj", "--name", "first")
        assert result.exit_code == 0, result.output
        root = tmp_path / "proj"
        for folder in ("pipelines", "sources", "destinations", "transformations", "queries"):
            assert (root / "src" / folder / "__init__.py").exists()
        assert (root / "src" / "pipelines" / "first.py").exists()
        assert (root / "src" / "sources" / "example_source.py").exists()
        assert (root / "src" / "destinations" / "example_destination.py").exists()
        assert (root / "src" / "transformations" / "example_transform.py").exists()
        assert (root / "src" / "queries" / "example_query.sql").exists()
        assert (root / "Dockerfile.worker").exists()
        assert (root / "docker-compose.yml").exists()
        env = root / ".env"
        assert stat.S_IMODE(env.stat().st_mode) == 0o600
        password_line = next(
            line for line in env.read_text().splitlines() if line.startswith("POSTGRES_PASSWORD=")
        )
        assert len(password_line.split("=", 1)[1]) >= 20
        assert "reflowfy test first --dry-run" in result.output

    def test_rerun_keeps_existing_env_and_files(self, tmp_path):
        assert cli("init", "proj").exit_code == 0
        env = tmp_path / "proj" / ".env"
        env.write_text("MINE=1\n")
        compose = tmp_path / "proj" / "docker-compose.yml"
        compose.write_text("# customised\n")

        result = cli("init", "proj")

        assert result.exit_code == 0, result.output
        assert ".env already exists, skipping" in result.output
        assert env.read_text() == "MINE=1\n"
        assert compose.read_text() == "# customised\n"

    def test_falls_back_when_templates_are_missing(self, tmp_path, monkeypatch):
        empty = tmp_path / "no_package"
        empty.mkdir()
        monkeypatch.setattr(init_mod, "get_package_path", lambda: empty)
        monkeypatch.setattr(init_mod, "get_dockerfiles_path", lambda: empty)

        result = cli("init", "proj", "--name", "fallback_pipe")

        assert result.exit_code == 0, result.output
        assert "Could not load pipeline template" in result.output
        text = (tmp_path / "proj" / "src" / "pipelines" / "fallback_pipe.py").read_text()
        assert "class FallbackPipe(AbstractPipeline):" in text
        assert 'name = "fallback_pipe"' in text
        compile(text, "fallback_pipe.py", "exec")
        assert not (tmp_path / "proj" / ".env").exists()


# ---------------------------------------------------------------------------
# cli/utils.py
# ---------------------------------------------------------------------------


class TestUtils:
    def test_package_path(self):
        assert utils.get_package_path() == Path(reflowfy.__file__).parent

    def test_helm_chart_prefers_local_helm_dir(self, tmp_path):
        chart = tmp_path / "helm" / "reflowfy"
        chart.mkdir(parents=True)
        (chart / "Chart.yaml").write_text("name: reflowfy\n")
        (tmp_path / "reflowfy" / "helm" / "reflowfy").mkdir(parents=True)
        (tmp_path / "reflowfy" / "helm" / "reflowfy" / "Chart.yaml").write_text("x")
        assert utils.get_helm_chart_path() == Path("./helm/reflowfy")

    def test_helm_chart_from_repo_root(self, tmp_path):
        (tmp_path / "helm" / "reflowfy").mkdir(parents=True)  # no Chart.yaml: skipped
        chart = tmp_path / "reflowfy" / "helm" / "reflowfy"
        chart.mkdir(parents=True)
        (chart / "Chart.yaml").write_text("x")
        assert utils.get_helm_chart_path() == Path("reflowfy/helm/reflowfy")

    def test_helm_chart_bundled(self):
        assert utils.get_helm_chart_path() == utils.get_package_path() / "helm" / "reflowfy"

    def test_helm_chart_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(utils, "get_package_path", lambda: tmp_path / "pkg")
        with pytest.raises(FileNotFoundError, match="Helm chart not found"):
            utils.get_helm_chart_path()

    @pytest.mark.parametrize("dockerfile", ["Dockerfile.api", "Dockerfile.reflow-manager"])
    def test_dockerfiles_from_project(self, tmp_path, dockerfile):
        (tmp_path / dockerfile).write_text("FROM x\n")
        assert utils.get_dockerfiles_path() == Path(".")

    def test_dockerfiles_from_package_templates(self):
        assert utils.get_dockerfiles_path() == utils.get_package_path() / "templates"
