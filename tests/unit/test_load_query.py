"""Unit tests for AbstractPipeline.load_query / load_query_text.

These exercise the queries/ folder resolution (upward search), recursive
by-filename lookup with subfolders, extension-based parsing, and caching.
"""

import sys
import types
from pathlib import Path

import pytest

from reflowfy.core.abstract_pipeline import AbstractPipeline


def _make_pipeline(pipeline_file: Path, name: str) -> AbstractPipeline:
    """Build a concrete pipeline instance whose defining module lives at
    ``pipeline_file`` (so queries/ resolution walks up from there)."""
    module_name = f"faux_pipeline_{name}"
    module = types.ModuleType(module_name)
    module.__file__ = str(pipeline_file)
    sys.modules[module_name] = module

    class _Pipeline(AbstractPipeline):
        pass

    _Pipeline.name = name
    _Pipeline.__module__ = module_name

    # Provide the abstract method bodies so the class is instantiable.
    _Pipeline.define_source = lambda self, runtime_params: None  # type: ignore[assignment]
    _Pipeline.define_destination = (  # type: ignore[assignment]
        lambda self, records, runtime_params: None
    )
    _Pipeline.define_transformations = (  # type: ignore[assignment]
        lambda self, records, runtime_params: []
    )
    _Pipeline.__abstractmethods__ = frozenset()

    return _Pipeline()


def test_resolves_queries_next_to_pipeline_file(tmp_path: Path) -> None:
    """e2e-style layout: queries/ sits next to the pipeline module file."""
    pkg = tmp_path / "test_pipelines"
    (pkg / "queries").mkdir(parents=True)
    (pkg / "queries" / "events.sql").write_text("SELECT 1")

    pipeline = _make_pipeline(pkg / "my_pipeline.py", "adjacent")

    assert pipeline.load_query("events.sql") == "SELECT 1"


def test_resolves_queries_at_project_root_via_upward_walk(tmp_path: Path) -> None:
    """scaffold-style layout: pipeline in pipelines/, queries/ at project root."""
    (tmp_path / "pipelines").mkdir()
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "events.sql").write_text("SELECT 2")

    pipeline = _make_pipeline(tmp_path / "pipelines" / "my_pipeline.py", "root")

    assert pipeline.load_query("events.sql") == "SELECT 2"


def test_finds_query_in_subfolder_by_filename(tmp_path: Path) -> None:
    """queries/ may be organized into subfolders; lookup is by filename only."""
    (tmp_path / "queries" / "sql").mkdir(parents=True)
    (tmp_path / "queries" / "sql" / "events.sql").write_text("SELECT 3")

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "subfolder")

    assert pipeline.load_query("events.sql") == "SELECT 3"


def test_ambiguous_filename_raises(tmp_path: Path) -> None:
    """Same filename in two subfolders is an error, not a silent guess."""
    (tmp_path / "queries" / "a").mkdir(parents=True)
    (tmp_path / "queries" / "b").mkdir(parents=True)
    (tmp_path / "queries" / "a" / "events.sql").write_text("SELECT a")
    (tmp_path / "queries" / "b" / "events.sql").write_text("SELECT b")

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "ambiguous")

    with pytest.raises(ValueError) as exc:
        pipeline.load_query("events.sql")
    msg = str(exc.value)
    assert "a/events.sql" in msg
    assert "b/events.sql" in msg


def test_missing_query_raises_filenotfound(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "missing")

    with pytest.raises(FileNotFoundError) as exc:
        pipeline.load_query("nope.sql")
    assert "nope.sql" in str(exc.value)
    assert "queries" in str(exc.value)


def test_json_is_parsed_to_dict(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "q.json").write_text('{"range": {"ts": {"gte": "now-1d"}}}')

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "json")

    result = pipeline.load_query("q.json")
    assert result == {"range": {"ts": {"gte": "now-1d"}}}


def test_sql_returns_raw_string(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "q.sql").write_text("SELECT * FROM t")

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "sqltext")

    result = pipeline.load_query("q.sql")
    assert isinstance(result, str)
    assert result == "SELECT * FROM t"


def test_load_query_text_bypasses_json_parsing(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "q.json").write_text('{"a": 1}')

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "rawtext")

    result = pipeline.load_query_text("q.json")
    assert isinstance(result, str)
    assert result == '{"a": 1}'


def test_result_is_cached_and_file_read_once(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    query_file = tmp_path / "queries" / "q.sql"
    query_file.write_text("SELECT original")

    pipeline = _make_pipeline(tmp_path / "pipelines" / "p.py", "cache")

    first = pipeline.load_query("q.sql")
    # Mutate the file on disk; a cached loader must not observe the change.
    query_file.write_text("SELECT changed")
    second = pipeline.load_query("q.sql")

    assert first == "SELECT original"
    assert second == "SELECT original"
