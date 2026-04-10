"""
E2E Tests for CLI 'reflowfy test' command.

Tests the test command's pipeline loading, dry-run behavior,
limit enforcement, and error handling.
"""

import os
import pytest
import tempfile
import shutil
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


@pytest.fixture(autouse=True)
def clean_pipeline_registry():
    """Clean pipeline registry before and after each test to prevent auto-discovery pollution."""
    from reflowfy.core.registry import pipeline_registry
    
    # Save the original registry state
    original = {}
    if hasattr(pipeline_registry, '_pipelines'):
        original = pipeline_registry._pipelines.copy()
        # Clear for the test
        pipeline_registry._pipelines.clear()
        
    yield
    
    # Restore after the test
    if hasattr(pipeline_registry, '_pipelines'):
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(original)


def _write_minimal_pipeline(workspace, name="test_pipe", records=None):
    """Write a minimal valid pipeline file to the workspace."""
    if records is None:
        records = [{"id": 1, "data": "hello"}, {"id": 2, "data": "world"}]

    pipeline_path = os.path.join(workspace, "pipelines")
    os.makedirs(pipeline_path, exist_ok=True)

    filepath = os.path.join(pipeline_path, f"{name}.py")
    class_name = "".join(word.capitalize() for word in name.split("_")) + "Pipeline"

    content = f'''"""Test pipeline for CLI test command e2e tests."""

from reflowfy import AbstractPipeline


class MockSource:
    """A mock source that returns fixed records."""
    def fetch(self, params, limit=None):
        records = {repr(records)}
        if limit is not None:
            return records[:int(limit)]
        return records

    def __repr__(self):
        return "MockSource()"


class MockDestination:
    """A mock destination that does nothing."""
    async def send_with_retry(self, records, metadata):
        pass

    async def health_check(self):
        return True

    def __repr__(self):
        return "MockDestination()"


class {class_name}(AbstractPipeline):
    """{class_name} test pipeline."""

    name = "{name}"

    def define_parameters(self):
        return []

    def define_source(self, params):
        return MockSource()

    def define_destination(self, params):
        return MockDestination()

    def define_transformations(self, params):
        return []
'''
    with open(filepath, "w") as f:
        f.write(content)

    return filepath


def _write_empty_module(workspace):
    """Write a Python file that doesn't register any pipeline."""
    filepath = os.path.join(workspace, "empty_module.py")
    with open(filepath, "w") as f:
        f.write('"""This module has no pipelines."""\n\nx = 42\n')
    return filepath


class TestTestCommand:
    """Tests for the 'reflowfy test' command."""

    def test_missing_file_exits_with_error(self, temp_workspace):
        """Test with nonexistent file should fail with exit code 1."""
        result = runner.invoke(app, ["test", "nonexistent_pipeline.py"])

        assert result.exit_code == 1
        assert "File not found" in result.stdout

    def test_no_registered_pipelines(self, temp_workspace):
        """Test a file with no registered pipelines should fail."""
        filepath = _write_empty_module(temp_workspace)

        result = runner.invoke(app, ["test", filepath])

        assert result.exit_code == 1
        assert "No pipelines found" in result.stdout

    def test_valid_pipeline_dry_run(self, temp_workspace):
        """Test a valid pipeline with --dry-run should succeed without sending."""
        filepath = _write_minimal_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", filepath, "--dry-run"])

        assert result.exit_code == 0
        assert "Dry run" in result.stdout
        assert "Test complete" in result.stdout

    def test_limit_flag_respected(self, temp_workspace):
        """Test --limit should restrict the number of records processed."""
        records = [{"id": i} for i in range(10)]
        filepath = _write_minimal_pipeline(temp_workspace, records=records)

        result = runner.invoke(app, ["test", filepath, "--dry-run", "--limit", "3"])

        assert result.exit_code == 0
        # The output should show 3 records processed (the limit)
        assert "3 records" in result.stdout

    def test_valid_pipeline_sends_to_destination(self, temp_workspace):
        """Test a valid pipeline without --dry-run should send records."""
        filepath = _write_minimal_pipeline(temp_workspace)

        result = runner.invoke(app, ["test", filepath])

        assert result.exit_code == 0
        assert "records sent successfully" in result.stdout
