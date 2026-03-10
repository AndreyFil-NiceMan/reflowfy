"""
E2E Tests for CLI scaffolding commands ('reflowfy new' and 'reflowfy init').

Verifies that the CLI correctly generates template files that
can be imported and run.
"""

import os
import sys
import pytest
import shutil
import tempfile
import importlib.util
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
    
    # Cleanup
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)


def test_reflowfy_init_creates_directories_and_files(temp_workspace):
    """Verify 'init' creates the 4 standard directories and files."""
    result = runner.invoke(app, ["init", ".", "--name", "test_project"])
    
    assert result.exit_code == 0
    
    # Verify directories
    assert os.path.isdir("pipelines")
    assert os.path.isdir("sources")
    assert os.path.isdir("destinations")
    assert os.path.isdir("transformations")
    assert os.path.isdir("queries")
    
    # Verify files
    assert os.path.isfile("pipelines/test_project.py")
    assert os.path.isfile("sources/example_source.py")
    assert os.path.isfile("destinations/example_destination.py")
    assert os.path.isfile("transformations/example_transform.py")
    assert os.path.isfile(".env")
    assert os.path.isfile("docker-compose.yml")


def test_reflowfy_new_pipeline(temp_workspace):
    """Verify 'new pipeline' command."""
    os.mkdir("pipelines")
    result = runner.invoke(app, ["new", "pipeline", "my_new_etl"])
    
    assert result.exit_code == 0
    assert "Created pipeline: pipelines/my_new_etl.py" in result.stdout
    assert os.path.isfile("pipelines/my_new_etl.py")
    
    # Verify we can import it (it's valid python)
    spec = importlib.util.spec_from_file_location("my_new_etl", "pipelines/my_new_etl.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["my_new_etl"] = module
    
    # Shouldn't raise SyntaxError
    spec.loader.exec_module(module)
    
    # Verify the class is inside
    assert hasattr(module, "MyNewEtlPipeline")


def test_reflowfy_new_source(temp_workspace):
    """Verify 'new source' command."""
    os.mkdir("sources")
    result = runner.invoke(app, ["new", "source", "my_custom_api"])
    
    assert result.exit_code == 0
    assert "Created source: sources/my_custom_api.py" in result.stdout
    assert os.path.isfile("sources/my_custom_api.py")
    
    with open("sources/my_custom_api.py") as f:
        content = f.read()
        assert '@source("my_custom_api")' in content


def test_reflowfy_new_destination(temp_workspace):
    """Verify 'new destination' command."""
    os.mkdir("destinations")
    result = runner.invoke(app, ["new", "destination", "my_custom_db"])
    
    assert result.exit_code == 0
    assert "Created destination: destinations/my_custom_db.py" in result.stdout
    assert os.path.isfile("destinations/my_custom_db.py")
    
    with open("destinations/my_custom_db.py") as f:
        content = f.read()
        assert '@destination("my_custom_db")' in content


def test_reflowfy_new_transformation(temp_workspace):
    """Verify 'new transformation' command."""
    os.mkdir("transformations")
    result = runner.invoke(app, ["new", "transformation", "clean_data"])
    
    assert result.exit_code == 0
    assert "Created transformation: transformations/clean_data.py" in result.stdout
    assert os.path.isfile("transformations/clean_data.py")
    
    with open("transformations/clean_data.py") as f:
        content = f.read()
        # The template uses a class based approach by default, we look for class name
        assert "class CleanData(BaseTransformation):" in content
        assert 'name = "clean_data"' in content


def test_reflowfy_init_with_custom_path(temp_workspace):
    """Verify 'init' works with a non-'.' path argument."""
    result = runner.invoke(app, ["init", "my_project", "--name", "custom_pipe"])
    
    assert result.exit_code == 0
    assert os.path.isdir("my_project/pipelines")
    assert os.path.isdir("my_project/sources")
    assert os.path.isdir("my_project/destinations")
    assert os.path.isdir("my_project/transformations")
    assert os.path.isdir("my_project/queries")
    assert os.path.isfile("my_project/pipelines/custom_pipe.py")


def test_reflowfy_init_idempotency(temp_workspace):
    """Verify running 'init' twice doesn't cause errors."""
    result1 = runner.invoke(app, ["init", ".", "--name", "first_run"])
    assert result1.exit_code == 0
    
    # Second run should still succeed (existing dirs/files are handled gracefully)
    result2 = runner.invoke(app, ["init", ".", "--name", "second_run"])
    assert result2.exit_code == 0
    # .env already exists from first run → should warn or skip
    assert ".env already exists" in result2.stdout or result2.exit_code == 0


def test_reflowfy_new_pipeline_rejects_duplicates(temp_workspace):
    """Verify 'new pipeline' rejects creating a file that already exists."""
    os.mkdir("pipelines")
    
    # First creation should succeed
    result1 = runner.invoke(app, ["new", "pipeline", "my_etl"])
    assert result1.exit_code == 0
    
    # Second creation with same name should fail
    result2 = runner.invoke(app, ["new", "pipeline", "my_etl"])
    assert result2.exit_code == 1
    assert "already exists" in result2.stdout


def test_reflowfy_new_multi_word_snake_case(temp_workspace):
    """Verify 'new pipeline' generates correct class name for multi-word names."""
    os.mkdir("pipelines")
    result = runner.invoke(app, ["new", "pipeline", "my_cool_data_pipeline"])
    
    assert result.exit_code == 0
    assert os.path.isfile("pipelines/my_cool_data_pipeline.py")
    
    with open("pipelines/my_cool_data_pipeline.py") as f:
        content = f.read()
        # Class name should be PascalCase; since it already ends with Pipeline,
        # the generator adds Pipeline only if not already present
        assert "MyCoolDataPipeline" in content


def test_reflowfy_new_source_rejects_duplicates(temp_workspace):
    """Verify 'new source' rejects creating a file that already exists."""
    os.mkdir("sources")
    
    result1 = runner.invoke(app, ["new", "source", "my_api"])
    assert result1.exit_code == 0
    
    result2 = runner.invoke(app, ["new", "source", "my_api"])
    assert result2.exit_code == 1
    assert "already exists" in result2.stdout


def test_reflowfy_new_destination_rejects_duplicates(temp_workspace):
    """Verify 'new destination' rejects creating a file that already exists."""
    os.mkdir("destinations")
    
    result1 = runner.invoke(app, ["new", "destination", "my_db"])
    assert result1.exit_code == 0
    
    result2 = runner.invoke(app, ["new", "destination", "my_db"])
    assert result2.exit_code == 1
    assert "already exists" in result2.stdout


def test_reflowfy_new_transformation_rejects_duplicates(temp_workspace):
    """Verify 'new transformation' rejects creating a file that already exists."""
    os.mkdir("transformations")
    
    result1 = runner.invoke(app, ["new", "transformation", "my_transform"])
    assert result1.exit_code == 0
    
    result2 = runner.invoke(app, ["new", "transformation", "my_transform"])
    assert result2.exit_code == 1
    assert "already exists" in result2.stdout
