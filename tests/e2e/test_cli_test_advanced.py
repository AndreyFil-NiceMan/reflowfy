"""
Advanced E2E Tests for CLI 'reflowfy test' command.

Covers parameter type coercion, multi-pipeline selection, IdBasedPipeline
batching, transformation application, and destination error handling.

No external services required — all sources, destinations, and transformations
are written inline as mock objects.
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


# ---------------------------------------------------------------------------
# Fixtures (mirrors test_cli_test.py)
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_workspace():
    """Create a temporary workspace and chdir into it."""
    temp_dir = tempfile.mkdtemp()
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    yield temp_dir
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)


@pytest.fixture(autouse=True)
def clean_pipeline_registry():
    """Isolate the pipeline registry between tests."""
    from reflowfy.core.registry import pipeline_registry

    original = {}
    if hasattr(pipeline_registry, "_pipelines"):
        original = pipeline_registry._pipelines.copy()
        pipeline_registry._pipelines.clear()

    yield

    if hasattr(pipeline_registry, "_pipelines"):
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(original)


# ---------------------------------------------------------------------------
# Pipeline file writers
# ---------------------------------------------------------------------------

def _write_file(workspace, filename, content):
    os.makedirs(os.path.join(workspace, "pipelines"), exist_ok=True)
    path = os.path.join(workspace, "pipelines", filename)
    with open(path, "w") as f:
        f.write(content)
    return path


def _write_int_param_pipeline(workspace):
    """Pipeline with a single int param 'count' (default 10)."""
    content = """\
from reflowfy import AbstractPipeline
from reflowfy.core.abstract_pipeline import PipelineParameter


class IntParamPipeline(AbstractPipeline):
    name = "int_param_test"

    def define_parameters(self):
        return [PipelineParameter(name="count", param_type=int, default=10)]

    def define_source(self, params):
        count = params.get("count", 10)

        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": i, "_count": count} for i in range(min(count, 3))]

        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "int_param_pipeline.py", content)


def _write_float_param_pipeline(workspace):
    """Pipeline with a float param 'ratio' (default 1.0)."""
    content = """\
from reflowfy import AbstractPipeline
from reflowfy.core.abstract_pipeline import PipelineParameter


class FloatParamPipeline(AbstractPipeline):
    name = "float_param_test"

    def define_parameters(self):
        return [PipelineParameter(name="ratio", param_type=float, default=1.0)]

    def define_source(self, params):
        ratio = params.get("ratio", 1.0)

        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1, "_ratio": ratio}]

        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "float_param_pipeline.py", content)


def _write_bool_param_pipeline(workspace):
    """Pipeline with a bool param 'enabled' (default False)."""
    content = """\
from reflowfy import AbstractPipeline
from reflowfy.core.abstract_pipeline import PipelineParameter


class BoolParamPipeline(AbstractPipeline):
    name = "bool_param_test"

    def define_parameters(self):
        return [PipelineParameter(name="enabled", param_type=bool, default=False)]

    def define_source(self, params):
        enabled = params.get("enabled", False)

        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1, "_enabled": enabled}]

        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "bool_param_pipeline.py", content)


def _write_default_param_pipeline(workspace):
    """Pipeline with an optional string param 'env' defaulting to 'hello'."""
    content = """\
from reflowfy import AbstractPipeline
from reflowfy.core.abstract_pipeline import PipelineParameter


class DefaultParamPipeline(AbstractPipeline):
    name = "default_param_test"

    def define_parameters(self):
        return [PipelineParameter(name="env", param_type=str, default="hello")]

    def define_source(self, params):
        env = params.get("env", "hello")

        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1, "_env": env}]

        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "default_param_pipeline.py", content)


def _write_choices_param_pipeline(workspace):
    """Pipeline with a string param 'mode' restricted to choices ['a', 'b']."""
    content = """\
from reflowfy import AbstractPipeline
from reflowfy.core.abstract_pipeline import PipelineParameter


class ChoicesParamPipeline(AbstractPipeline):
    name = "choices_param_test"

    def define_parameters(self):
        return [
            PipelineParameter(
                name="mode",
                param_type=str,
                choices=["a", "b"],
                default="a",
            )
        ]

    def define_source(self, params):
        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1}]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "choices_param_pipeline.py", content)


def _write_multi_pipeline_file(workspace):
    """File containing two distinct pipelines (Alpha and Beta)."""
    content = """\
from reflowfy import AbstractPipeline


class _Src:
    def fetch(self, p, limit=None):
        return [{"id": 1}]


class _Dest:
    async def send_with_retry(self, records, metadata):
        pass
    async def health_check(self):
        return True


class AlphaPipeline(AbstractPipeline):
    name = "multi_alpha"

    def define_source(self, params):
        return _Src()

    def define_destination(self, params):
        return _Dest()

    def define_transformations(self, params):
        return []


class BetaPipeline(AbstractPipeline):
    name = "multi_beta"

    def define_source(self, params):
        return _Src()

    def define_destination(self, params):
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "multi_pipeline.py", content)


def _write_id_based_pipeline(workspace, ids_batch_size=2):
    """IdBasedPipeline file with a configurable ids_batch_size."""
    content = f"""\
from reflowfy.core.id_based_pipeline import IdBasedPipeline


class CliTestIdBasedPipeline(IdBasedPipeline):
    name = "cli_id_based_test"
    ids_batch_size = {ids_batch_size}

    def define_source(self, params, current_ids):
        class _Src:
            def fetch(self, p, limit=None):
                return [{{"id": cid}} for cid in current_ids]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params, current_ids):
        return []
"""
    return _write_file(workspace, "id_based_pipeline.py", content)


def _write_transform_pipeline(workspace):
    """Pipeline whose transformation stamps _tagged=True on every record."""
    content = """\
from reflowfy import AbstractPipeline


class _TagTransformation:
    name = "tag_records"

    def apply(self, records, runtime_params):
        for r in records:
            r["_tagged"] = True
        return records


class TaggedPipeline(AbstractPipeline):
    name = "tagged_pipeline_test"

    def define_source(self, params):
        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1}, {"id": 2}]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return [_TagTransformation()]
"""
    return _write_file(workspace, "transform_pipeline.py", content)


def _write_failing_transform_pipeline(workspace):
    """AbstractPipeline whose single transformation always raises."""
    content = """\
from reflowfy import AbstractPipeline


class _FailTransformation:
    name = "always_fail"

    def apply(self, records, runtime_params):
        raise RuntimeError("deliberate failure in transformation")


class FailTransformPipeline(AbstractPipeline):
    name = "fail_transform_test"

    def define_source(self, params):
        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1}]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return [_FailTransformation()]
"""
    return _write_file(workspace, "failing_transform_pipeline.py", content)


def _write_empty_source_pipeline(workspace):
    """Pipeline whose source always returns an empty list."""
    content = """\
from reflowfy import AbstractPipeline


class EmptySourcePipeline(AbstractPipeline):
    name = "empty_source_test"

    def define_source(self, params):
        class _Src:
            def fetch(self, p, limit=None):
                return []
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "empty_source_pipeline.py", content)


def _write_failing_health_pipeline(workspace):
    """Pipeline whose destination health_check always returns False."""
    content = """\
from reflowfy import AbstractPipeline


class FailHealthPipeline(AbstractPipeline):
    name = "fail_health_test"

    def define_source(self, params):
        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1}]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return False
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "fail_health_pipeline.py", content)


def _write_failing_send_pipeline(workspace):
    """Pipeline whose send_with_retry raises an exception."""
    content = """\
from reflowfy import AbstractPipeline


class FailSendPipeline(AbstractPipeline):
    name = "fail_send_test"

    def define_source(self, params):
        class _Src:
            def fetch(self, p, limit=None):
                return [{"id": 1}]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                raise ConnectionError("destination is down")
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
    return _write_file(workspace, "fail_send_pipeline.py", content)


# ---------------------------------------------------------------------------
# Tests: parameter type coercion
# ---------------------------------------------------------------------------

class TestCliTestParameters:
    """Verify that parameter values are correctly coerced by param.coerce()."""

    def test_int_param_coerced(self, temp_workspace):
        """Prompt returns '42'; source sees params['count'] == 42 (int), not '42'."""
        path = _write_int_param_pipeline(temp_workspace)

        with patch("rich.prompt.Prompt.ask", return_value="42"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        # The source builds records with _count = params["count"] (int)
        # JSON output shows "\"_count\": 42" (no quotes around the number)
        assert '"_count": 42' in result.stdout, (
            f"Expected int 42 in dry-run output but got:\n{result.stdout}"
        )

    def test_float_param_coerced(self, temp_workspace):
        """Prompt returns '1.5'; pipeline receives ratio == 1.5 (float)."""
        path = _write_float_param_pipeline(temp_workspace)

        with patch("rich.prompt.Prompt.ask", return_value="1.5"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert '"_ratio": 1.5' in result.stdout, (
            f"Expected float 1.5 in dry-run output but got:\n{result.stdout}"
        )

    def test_bool_param_uses_confirm(self, temp_workspace):
        """Bool params use Confirm.ask (not Prompt.ask); returns True directly."""
        path = _write_bool_param_pipeline(temp_workspace)

        with patch("rich.prompt.Confirm.ask", return_value=True):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert '"_enabled": true' in result.stdout, (
            f"Expected bool true in dry-run output but got:\n{result.stdout}"
        )

    def test_optional_param_uses_default_on_empty(self, temp_workspace):
        """When Prompt returns the default value for an optional param, it is used."""
        path = _write_default_param_pipeline(temp_workspace)

        # Prompt.ask returns "hello" (the default) — coerce("hello") → "hello"
        with patch("rich.prompt.Prompt.ask", return_value="hello"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert '"_env": "hello"' in result.stdout, (
            f"Expected default 'hello' but got:\n{result.stdout}"
        )

    def test_choices_invalid_shows_warning_not_error(self, temp_workspace):
        """
        Invalid choice value emits a yellow warning but the pipeline still runs
        (exit 0). The command does NOT enforce choices as a hard error.
        """
        path = _write_choices_param_pipeline(temp_workspace)

        with patch("rich.prompt.Prompt.ask", return_value="c"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, (
            f"Expected exit 0 for invalid choice (warning only) but got:\n{result.stdout}"
        )
        assert "not in choices" in result.stdout, (
            f"Expected 'not in choices' warning in output:\n{result.stdout}"
        )

    def test_multiple_pipelines_select_first(self, temp_workspace):
        """With two pipelines in the file, selecting '1' runs AlphaPipeline."""
        path = _write_multi_pipeline_file(temp_workspace)

        with patch("rich.prompt.Prompt.ask", return_value="1"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert "multi_alpha" in result.stdout, (
            f"Expected pipeline 'multi_alpha' to run:\n{result.stdout}"
        )

    def test_multiple_pipelines_invalid_choice_exits(self, temp_workspace):
        """Selecting an out-of-range number (e.g. '99') exits with code 1."""
        path = _write_multi_pipeline_file(temp_workspace)

        with patch("rich.prompt.Prompt.ask", return_value="99"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 1, result.stdout
        assert "Invalid choice" in result.stdout or "invalid" in result.stdout.lower(), (
            f"Expected 'Invalid choice' error:\n{result.stdout}"
        )


# ---------------------------------------------------------------------------
# Tests: IdBasedPipeline
# ---------------------------------------------------------------------------

class TestCliTestIdBased:
    """Verify IdBasedPipeline-specific behaviour in the test command."""

    def test_id_based_pipeline_detected(self, temp_workspace):
        """Command output should mention 'IdBasedPipeline' when the type is detected."""
        path = _write_id_based_pipeline(temp_workspace, ids_batch_size=2)

        # Prompt: first call for 'ids' param, mock returning 6 IDs
        with patch("rich.prompt.Prompt.ask", return_value="[1, 2, 3, 4, 5, 6]"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert "IdBasedPipeline" in result.stdout, (
            f"Expected 'IdBasedPipeline' in output:\n{result.stdout}"
        )

    def test_id_based_batched_by_batch_size(self, temp_workspace):
        """6 IDs with ids_batch_size=2 should produce 3 batches."""
        path = _write_id_based_pipeline(temp_workspace, ids_batch_size=2)

        with patch("rich.prompt.Prompt.ask", return_value="[1, 2, 3, 4, 5, 6]"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert "3 batch" in result.stdout, (
            f"Expected '3 batch(es)' in output:\n{result.stdout}"
        )

    def test_id_based_no_ids_exits(self, temp_workspace):
        """Empty ids list exits with code 1 and a clear error message."""
        path = _write_id_based_pipeline(temp_workspace)

        with patch("rich.prompt.Prompt.ask", return_value="[]"):
            result = runner.invoke(app, ["test", path])

        assert result.exit_code == 1, result.stdout
        assert "No IDs" in result.stdout or "ids" in result.stdout.lower(), (
            f"Expected 'No IDs provided' error:\n{result.stdout}"
        )

    def test_id_based_dry_run_skips_destination(self, temp_workspace):
        """--dry-run should skip destination send even for IdBasedPipeline."""
        path = _write_id_based_pipeline(temp_workspace, ids_batch_size=3)

        with patch("rich.prompt.Prompt.ask", return_value="[1, 2, 3]"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert "Dry run" in result.stdout, (
            f"Expected 'Dry run' in output:\n{result.stdout}"
        )

    def test_id_based_source_fetch_failure_continues(self, temp_workspace):
        """
        If source.fetch raises for one batch the command should continue
        to process remaining batches (no hard exit 1 on partial failure).
        """
        # Write a pipeline where fetch raises only for ids [1, 2]
        content = """\
from reflowfy.core.id_based_pipeline import IdBasedPipeline


class PartialFailPipeline(IdBasedPipeline):
    name = "partial_fail_id_based"
    ids_batch_size = 2

    def define_source(self, params, current_ids):
        class _Src:
            def fetch(self, p, limit=None):
                if 1 in current_ids:
                    raise RuntimeError("intentional first-batch failure")
                return [{"id": cid} for cid in current_ids]
        return _Src()

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params, current_ids):
        return []
"""
        path = _write_file(temp_workspace, "partial_fail_pipeline.py", content)

        with patch("rich.prompt.Prompt.ask", return_value="[1, 2, 3, 4]"):
            result = runner.invoke(app, ["test", path, "--dry-run"])

        # IdBasedPipeline continues on per-batch source failures
        assert result.exit_code == 0, (
            f"Expected exit 0 (continue on batch failure):\n{result.stdout}"
        )
        assert "failed" in result.stdout.lower() or "error" in result.stdout.lower(), (
            f"Expected error message for failed batch:\n{result.stdout}"
        )


# ---------------------------------------------------------------------------
# Tests: transformation application
# ---------------------------------------------------------------------------

class TestCliTestTransformationApplication:
    """Verify that transformations are applied to records during the test command."""

    def test_transformation_applied_in_dry_run(self, temp_workspace):
        """The _TagTransformation should add _tagged=true to each record."""
        path = _write_transform_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert '"_tagged": true' in result.stdout, (
            f"Expected '_tagged: true' in dry-run sample output:\n{result.stdout}"
        )

    def test_transformation_failure_exits_with_error(self, temp_workspace):
        """AbstractPipeline: transformation exception should exit with code 1."""
        path = _write_failing_transform_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 1, (
            f"Expected exit 1 on transformation failure but got:\n{result.stdout}"
        )
        assert "failed" in result.stdout.lower() or "error" in result.stdout.lower(), (
            f"Expected error message:\n{result.stdout}"
        )


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------

class TestCliTestEdgeCases:
    """Verify edge-case behaviour in the test command."""

    def test_empty_source_exits_cleanly(self, temp_workspace):
        """Source returning [] should print a warning and exit 0."""
        path = _write_empty_source_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", path, "--dry-run"])

        assert result.exit_code == 0, result.stdout
        assert (
            "No records" in result.stdout or "0 records" in result.stdout
        ), f"Expected 'No records' message:\n{result.stdout}"

    def test_destination_health_check_failure_exits(self, temp_workspace):
        """health_check returning False should cause exit code 1."""
        path = _write_failing_health_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", path])

        assert result.exit_code == 1, (
            f"Expected exit 1 on health_check failure:\n{result.stdout}"
        )
        assert "health" in result.stdout.lower(), (
            f"Expected 'health check' error in output:\n{result.stdout}"
        )

    def test_destination_send_failure_exits(self, temp_workspace):
        """send_with_retry raising ConnectionError should exit with code 1."""
        path = _write_failing_send_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", path])

        assert result.exit_code == 1, (
            f"Expected exit 1 on send failure:\n{result.stdout}"
        )
        assert (
            "failed" in result.stdout.lower()
            or "error" in result.stdout.lower()
            or "destination" in result.stdout.lower()
        ), f"Expected destination error message:\n{result.stdout}"

    def test_pipeline_setup_failure_exits(self, temp_workspace):
        """define_source raising an exception should exit with code 1."""
        content = """\
from reflowfy import AbstractPipeline


class SetupFailPipeline(AbstractPipeline):
    name = "setup_fail_test"

    def define_source(self, params):
        raise RuntimeError("source configuration error")

    def define_destination(self, params):
        class _Dest:
            async def send_with_retry(self, records, metadata):
                pass
            async def health_check(self):
                return True
        return _Dest()

    def define_transformations(self, params):
        return []
"""
        path = _write_file(temp_workspace, "setup_fail.py", content)

        result = runner.invoke(app, ["test", path])

        assert result.exit_code == 1, (
            f"Expected exit 1 on setup failure:\n{result.stdout}"
        )
        assert "setup" in result.stdout.lower() or "failed" in result.stdout.lower(), (
            f"Expected setup error message:\n{result.stdout}"
        )
