"""pytest plugin for testing reflowfy pipelines (registered as ``reflowfy_pytest``).

Installed with reflowfy and loaded by pytest automatically. It adds:

- **fixtures** ``mock_http`` and ``mock_source`` (canned answers for what a step
  calls; any other HTTP request fails the test), ``capture_destination``, and
  ``reflowfy_pipelines`` (the registry, after discovery);
- **collection of recorded fixtures**: every ``*.json`` file under
  ``tests/fixtures`` written by ``reflowfy test <pipeline> --save-fixture`` is a
  test of its own, ``test_fixture[<file name>]``. Change the directories with::

      [tool.pytest.ini_options]
      reflowfy_fixtures = ["tests/fixtures", "tests/golden"]

Nothing of reflowfy is imported until a test uses it. This package lives outside
``reflowfy`` for that reason: a plugin is imported before coverage starts, and
importing ``reflowfy`` here would hide every module-level line of it from a
coverage run.
"""

import json
from pathlib import Path
from typing import Any, Iterator, Optional

import pytest

DEFAULT_FIXTURE_DIRS = ["tests/fixtures"]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addini(
        "reflowfy_fixtures",
        "Directories (relative to the rootdir) whose reflowfy fixture files are run as tests",
        type="args",
        default=DEFAULT_FIXTURE_DIRS,
    )


def is_fixture_file(path: Path) -> bool:
    """True for a JSON file written by ``reflowfy test --save-fixture``."""
    try:
        content: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(content, dict) and "reflowfy_fixture" in content


def pytest_collect_file(file_path: Path, parent: pytest.Collector) -> Optional[pytest.Collector]:
    if file_path.suffix != ".json":
        return None
    root = parent.config.rootpath
    try:
        names = parent.config.getini("reflowfy_fixtures")
    except ValueError:  # registered after the options were parsed (a conftest did it)
        names = DEFAULT_FIXTURE_DIRS
    directories = [(root / name).resolve() for name in names]
    resolved = file_path.resolve()
    if not any(directory in resolved.parents for directory in directories):
        return None
    if not is_fixture_file(file_path):
        return None
    return FixtureFile.from_parent(  # pyright: ignore[reportUnknownMemberType]
        parent, path=file_path
    )


class FixtureFile(pytest.File):
    """One recorded fixture file; holds one test."""

    def collect(self) -> Iterator["FixtureItem"]:
        yield FixtureItem.from_parent(  # pyright: ignore[reportUnknownMemberType]
            self, name=f"test_fixture[{self.path.stem}]"
        )


class FixtureItem(pytest.Item):
    """Replays the fixture offline and compares with its expected output."""

    def runtest(self) -> None:
        from reflowfy.testing import run_fixture

        run_fixture(self.path)

    def repr_failure(self, excinfo: Any, style: Any = None) -> Any:
        from reflowfy.testing import FixtureError, UnmockedCallError

        # These messages are the whole story; a traceback into reflowfy adds nothing.
        if isinstance(excinfo.value, (AssertionError, FixtureError, UnmockedCallError)):
            return str(excinfo.value)
        return super().repr_failure(excinfo, style)

    def reportinfo(self) -> Any:
        return self.path, None, self.name


@pytest.fixture
def reflowfy_network() -> Iterator[Any]:
    """The session standing in for the outside world during this test.

    ``mock_http`` and ``mock_source`` are its two halves. While it is active an
    HTTP request nothing answers raises ``UnmockedCallError``.
    """
    from reflowfy.testing import mock_network

    with mock_network() as session:
        yield session


@pytest.fixture
def mock_http(reflowfy_network: Any) -> Any:
    """Canned HTTP answers: ``mock_http.get(url, json={...})``; ``mock_http.calls``."""
    return reflowfy_network.http


@pytest.fixture
def mock_source(reflowfy_network: Any) -> Any:
    """Canned records for a source a step fetches from itself:
    ``mock_source(ElasticSource, records=[...])``."""
    return reflowfy_network.sources


@pytest.fixture
def capture_destination() -> Any:
    """A fresh in-memory destination (``reflowfy.testing.CaptureDestination``)."""
    from reflowfy.testing import CaptureDestination

    return CaptureDestination()


@pytest.fixture(scope="session")
def reflowfy_pipelines() -> Any:
    """The pipeline registry, after discovering ``PIPELINE_MODULE`` (default ``pipelines/``)."""
    from reflowfy.core.pipeline_discovery import discover_and_load_pipelines
    from reflowfy.core.registry import pipeline_registry

    discover_and_load_pipelines()
    return pipeline_registry
