"""The reflowfy pytest plugin, and the tests `reflowfy init` / `reflowfy new` write.

Each test builds a throwaway project with the real CLI and runs pytest in it as a
user would, in a subprocess: the plugin's hooks only exist inside a pytest run.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import List

import pytest

import reflowfy_pytest

REPO = Path(__file__).resolve().parents[2]


def run(project: Path, *args: str) -> "subprocess.CompletedProcess[str]":
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO)
    env["LOG_JSON"] = "false"
    env.pop("PIPELINE_MODULE", None)
    env.pop("COV_CORE_SOURCE", None)  # no nested coverage run
    return subprocess.run(
        [sys.executable, *args], cwd=project, env=env, capture_output=True, text=True, timeout=120
    )


def cli(project: Path, *args: str) -> "subprocess.CompletedProcess[str]":
    return run(project, "-m", "reflowfy.cli.main", *args)


def pytest_in(project: Path, *args: str) -> "subprocess.CompletedProcess[str]":
    # -p: the plugin by name, for a checkout where reflowfy is not installed (CI).
    return run(project, "-m", "pytest", "-p", "reflowfy_pytest", "-p", "no:cacheprovider", *args)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "proj"
    created = cli(tmp_path, "init", str(root), "--name", "people")
    assert created.returncode == 0, created.stdout + created.stderr
    return root


def test_the_generated_tests_pass_in_a_fresh_project(project: Path):
    made = cli(project, "new", "pipeline", "billing")
    assert made.returncode == 0, made.stdout + made.stderr

    result = pytest_in(project, "-q")

    assert result.returncode == 0, result.stdout + result.stderr
    assert "2 passed, 1 xfailed" in result.stdout


def test_a_recorded_fixture_is_a_test(project: Path):
    fixtures = project / "tests" / "fixtures"
    recorded = cli(
        project, "test", "simple_test_pipeline", "--no-input", "--limit", "20",
        "--save-fixture", "tests/fixtures/sample.json",
    )
    assert recorded.returncode == 0, recorded.stdout + recorded.stderr
    assert "Fixture written" in recorded.stdout
    # Neither of these is a fixture; both are left alone.
    (fixtures / "notes.json").write_text('{"just": "json"}')
    (fixtures / "broken.json").write_text("{nope")
    (project / "tests" / "other.json").write_text(
        json.dumps({"reflowfy_fixture": 1, "pipeline": "simple_test_pipeline"})
    )

    # tests/other.json looks like a fixture but is outside tests/fixtures: not collected.
    passing = pytest_in(project, "-v", "tests")
    assert passing.returncode == 0, passing.stdout + passing.stderr
    assert "tests/fixtures/sample.json::test_fixture[sample] PASSED" in passing.stdout
    assert "3 passed" in passing.stdout and "other.json" not in passing.stdout

    pipeline = project / "src" / "pipelines" / "people.py"
    pipeline.write_text(pipeline.read_text().replace(".upper()", ".title()"))
    failing = pytest_in(project, "tests/fixtures")
    assert failing.returncode == 1
    assert "no longer matches its fixture" in failing.stdout
    assert "data.first_name: " in failing.stdout and "--update-fixture" in failing.stdout
    assert "Traceback" not in failing.stdout and "reflowfy/testing" not in failing.stdout

    updated = cli(
        project, "test", "simple_test_pipeline", "--fixture", "tests/fixtures/sample.json",
        "--update-fixture",
    )
    assert updated.returncode == 0, updated.stdout + updated.stderr
    assert pytest_in(project, "tests/fixtures").returncode == 0


def test_fixture_directories_are_configurable_and_other_errors_keep_their_traceback(project: Path):
    golden = project / "golden"
    golden.mkdir()
    (golden / "missing.json").write_text(json.dumps({"reflowfy_fixture": 1, "pipeline": "nope"}))
    (project / "pytest.ini").write_text("[pytest]\nreflowfy_fixtures = golden\n")

    result = pytest_in(project, "golden")

    assert result.returncode == 1
    assert "test_fixture[missing]" in result.stdout
    assert "LookupError" in result.stdout and "No pipeline named 'nope'" in result.stdout


def test_is_fixture_file(tmp_path: Path):
    def write(name: str, text: str) -> Path:
        path = tmp_path / name
        path.write_text(text)
        return path

    results: List[bool] = [
        reflowfy_pytest.is_fixture_file(write("a.json", '{"reflowfy_fixture": 1}')),
        reflowfy_pytest.is_fixture_file(write("b.json", '{"other": 1}')),
        reflowfy_pytest.is_fixture_file(write("c.json", "[1, 2]")),
        reflowfy_pytest.is_fixture_file(write("d.json", "not json")),
        reflowfy_pytest.is_fixture_file(tmp_path / "absent.json"),
    ]
    assert results == [True, False, False, False, False]
