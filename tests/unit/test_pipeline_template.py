"""The sample pipeline `reflowfy init` writes must register, typed, and run."""

import importlib.util
import warnings
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

from reflowfy import pipeline_registry
from reflowfy.cli.commands.test import TestOptions as Opts
from reflowfy.cli.commands.test import run_single
from reflowfy.cli.main import app
from reflowfy.cli.utils import get_package_path
from reflowfy.transformations.registry import transformation_registry

TEMPLATE = get_package_path() / "templates" / "pipeline_template.py"


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Empty pipeline + transformation registries; restored afterwards.

    The template's transformations register by name and a second class under
    the same name raises, so each import needs a clean transformation registry.
    """
    saved = dict(pipeline_registry._pipelines)
    saved_tx = dict(transformation_registry._transformations)
    pipeline_registry._pipelines.clear()
    transformation_registry._transformations.clear()
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)
        transformation_registry._transformations.clear()
        transformation_registry._transformations.update(saved_tx)


def _import(path: Path, module_name: str) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        spec.loader.exec_module(module)
    deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert deprecations == [], [str(w.message) for w in deprecations]
    return module


@pytest.fixture
def rendered(tmp_path, monkeypatch) -> Path:
    """Render the template the way users get it, through `reflowfy init`.

    `init` copies it verbatim, so the packaged file itself is what gets
    imported (which also lets coverage attribute the lines to it).
    """
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["init", "proj", "--name", "first_pipe"])
    assert result.exit_code == 0, result.output
    path = tmp_path / "proj" / "src" / "pipelines" / "first_pipe.py"
    assert path.read_text() == TEMPLATE.read_text()
    return TEMPLATE


def test_rendered_template_registers_with_a_params_type(rendered, isolated_registry):
    module = _import(rendered, "rendered_pipeline_template")

    pipeline = pipeline_registry.get("simple_test_pipeline")
    assert pipeline is not None
    assert type(pipeline) is module.SimpleTestPipeline
    assert pipeline._params_type is module.SimpleTestParams
    assert pipeline.rate_limit == 600
    # No user parameters declared, so nothing is required at /run.
    assert pipeline.get_all_parameters() == []


def test_rendered_template_runs_its_transformations(rendered, isolated_registry):
    module = _import(rendered, "rendered_pipeline_template_run")
    pipeline = pipeline_registry.get("simple_test_pipeline")
    assert pipeline is not None

    report = run_single(pipeline, {}, Opts(limit=50, dry_run=True))

    assert report.ok, report.error
    assert report.fetched == 50
    # mock_source(batch_size=10): 5 jobs, each running the same three steps.
    assert [s.name for s in report.applied] == [
        "filter_active_users",
        "uppercase_names",
        "add_processing_info",
    ] * 5
    assert report.transformed, "sample data should contain active users"
    for record in report.transformed:
        assert record["data"]["active"] is True
        assert record["data"]["first_name"] == record["data"]["first_name"].upper()
        assert record["_processed_by"] == "reflowfy"
        assert record["_execution_id"] != "unknown"
    assert len(module.SAMPLE_DATA) == 500


def test_template_destination_is_the_console(rendered, isolated_registry):
    _import(rendered, "rendered_pipeline_template_dest")
    pipeline = pipeline_registry.get("simple_test_pipeline")
    assert pipeline is not None
    destination = pipeline.define_destination([], {})
    assert type(destination).__name__ == "ConsoleDestination"


def test_template_sends_through_destination(rendered, isolated_registry, monkeypatch):
    module = _import(rendered, "rendered_pipeline_template_send")
    pipeline = pipeline_registry.get("simple_test_pipeline")
    assert pipeline is not None
    sink = AsyncMock()
    sink.health_check.return_value = True
    monkeypatch.setattr(module, "console_destination", lambda **kwargs: sink)

    report = run_single(pipeline, {}, Opts(limit=20, dry_run=False))

    assert report.ok, report.error
    assert report.sent_count == 2  # mock_source(batch_size=10) -> 2 jobs
    sent = [r for call in sink.send_with_retry.call_args_list for r in call.args[0]]
    assert sent == report.transformed
