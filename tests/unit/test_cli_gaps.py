"""CLI paths the command suites leave out: `build`/`deploy` validation messages,
`init` on a broken install, and the `reflowfy test` failure/step renderers."""

import importlib.util
import sys
import types
from pathlib import Path
from typing import Any, Iterator, List

import pytest
import typer
from typer.testing import CliRunner

from reflowfy.cli.commands import init_cmd
from reflowfy.cli.commands import test as test_cmd
from reflowfy.cli.commands.test import BatchReport
from reflowfy.cli.commands.test import TestOptions as Opts
from reflowfy.cli.main import app
from reflowfy.cli.utils import console
from reflowfy.core.exceptions import PipelineError
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.execution.transformation_runner import AppliedStep
from reflowfy.sources.mock import MockSource
from reflowfy.transformations.base import BaseTransformation, TransformationError
from reflowfy.transformations.registry import transformation_registry
from test_cli_deploy_unit import BASE, calls, deploy  # type: ignore[import-not-found]  # noqa: F401

# Built into URLs at runtime: a credentialed URL literal reads as a leaked secret
# to secret scanners (SonarCloud).
_DB_PW = "pw"

runner = CliRunner()


def test_the_cli_reads_the_project_env_file_on_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`reflowfy` run from a project root picks up its .env (REGISTRY, KAFKA_..., ...)."""
    import os

    import reflowfy.cli.main as main_mod

    probe = "REFLOWFY_GAPS_DOTENV_PROBE"
    monkeypatch.delenv(probe, raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(f"{probe}=from-dotenv\n")

    # Execute the entry module afresh under another name: its .env loading runs at
    # import, and re-importing reflowfy.cli.main itself would replace the shared app.
    spec = importlib.util.spec_from_file_location("gaps_cli_main", main_mod.__file__)
    assert spec is not None and spec.loader is not None
    try:
        spec.loader.exec_module(importlib.util.module_from_spec(spec))
        assert os.environ.get(probe) == "from-dotenv"
    finally:
        os.environ.pop(probe, None)


# ----- build / deploy validation ------------------------------------------------------


def test_build_without_a_registry_does_not_blame_a_missing_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("REGISTRY", raising=False)
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text("PROJECT=x\n")

    result = runner.invoke(app, ["build"])

    assert result.exit_code == 1
    assert "Registry is required" in result.output
    assert "No .env file found" not in result.output


def test_deploy_without_a_registry_does_not_blame_a_missing_env_file(
    calls: List[Any], tmp_path: Path  # noqa: F811
) -> None:
    (tmp_path / ".env").write_text("NAMESPACE=x\n")

    result = deploy("--kafka", "k:9092")

    assert result.exit_code == 1
    assert "Registry is required" in result.output
    assert "No .env file found" not in result.output
    assert calls == []


def test_deploy_rejects_an_unparseable_database_url(
    calls: List[Any], monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    monkeypatch.setenv("DATABASE_URL", f"postgresql://u:{_DB_PW}@db:not-a-port/reflow")

    result = deploy(*BASE, "--no-deploy-postgres")

    assert result.exit_code == 1
    assert "Failed to parse DATABASE_URL" in result.output
    assert calls == []  # helm never ran


# ----- init on a broken install ---------------------------------------------------------


def test_init_still_scaffolds_a_project_when_the_package_files_are_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken() -> Path:
        raise OSError("package data missing")

    monkeypatch.setattr(init_cmd, "get_package_path", broken)
    monkeypatch.setattr(init_cmd, "get_dockerfiles_path", broken)
    monkeypatch.setattr(console, "_width", 300)

    result = runner.invoke(app, ["init", str(tmp_path / "proj"), "--name", "orders_sync"])

    assert result.exit_code == 0, result.output
    project = tmp_path / "proj" / "src"
    fallback = (project / "pipelines" / "orders_sync.py").read_text()
    assert "class OrdersSync(AbstractPipeline):" in fallback
    for what in (
        "example source",
        "example destination",
        "example transformation",
        "example_query.sql",
        "example_query.json",
        "Dockerfiles",
        ".env",
    ):
        assert (
            f"Could not create {what}" in result.output or f"Could not copy {what}" in result.output
        )
    assert not (project / "sources" / "example_source.py").exists()


# ----- `reflowfy test` renderers --------------------------------------------------------


def _raised(exc: BaseException) -> BaseException:
    """`exc` with a real traceback, raised from this (user) file."""
    try:
        raise exc
    except BaseException as caught:  # noqa: BLE001
        return caught


def _render(fn: Any, *args: Any) -> str:
    console_quiet, width = console.quiet, console._width
    console.quiet, console._width = False, 300
    try:
        with console.capture() as capture:
            fn(*args)
        return capture.get()
    finally:
        console.quiet, console._width = console_quiet, width


class TestRenderSteps:
    def test_a_step_that_adds_records_shows_the_growth_and_timing(self) -> None:
        report = BatchReport(applied=[AppliedStep("explode_rows", 0.0012, 2, 5)])

        text = _render(test_cmd.render_steps, report, Opts(verbose=1))

        assert "explode_rows  2 → 5 records  (+3)  1.2ms" in text
        assert "total transform time: 1.2ms" in text

    def test_a_per_record_step_lists_why_it_skipped_records(self) -> None:
        step = AppliedStep(
            "enrich", 0.5, 10, 7, "per_record", 8, {"user inactive": 2, "no email": 1}
        )

        text = _render(test_cmd.render_steps, BatchReport(applied=[step]), Opts(verbose=1))

        assert "enrich  10 → 7 records  (-3)  500.0ms, per_record ×8" in text
        assert text.index("skipped 2: user inactive") < text.index("skipped 1: no email")


class TestRenderFailure:
    def test_the_failing_record_is_truncated_without_verbose(self) -> None:
        big = {"payload": "x" * 1000}
        exc = _raised(
            TransformationError(
                "enrich",
                "boom",
                ValueError("bad"),
                step_index=0,
                culprit_index=0,
                culprit_record=big,
            )
        )

        text = _render(test_cmd.render_failure, exc, Opts())

        assert "Transformation #1 failed: enrich" in text
        assert "received" not in text  # records_in unknown
        assert "Failing record (index 0):" in text  # no "of N" without a count
        assert "… (-v for the full record)" in text

    def test_a_key_error_without_a_culprit_hints_at_nested_values(self) -> None:
        exc = _raised(TransformationError("flatten", "boom", KeyError("city"), records_in=3))

        text = _render(test_cmd.render_failure, exc, Opts())

        assert "received 3 records" in text
        assert "no single record identified" in text

    def test_an_error_without_a_traceback_has_no_where_section(self) -> None:
        text = _render(test_cmd.render_failure, RuntimeError("never raised"), Opts())

        assert "❌ Failed" in text
        assert "RuntimeError: never raised" in text
        assert "Where" not in text

    def test_long_user_tracebacks_are_trimmed_to_the_last_frames(self) -> None:
        def level(n: int) -> None:
            if n == 0:
                raise ValueError("deep")
            level(n - 1)

        try:
            level(4)
        except ValueError as exc:
            error: BaseException = exc

        text = _render(test_cmd.render_failure, error, Opts())

        assert text.count(" in level") == 3
        assert "(-v for the full traceback)" in text

    def test_frames_without_source_lines_are_still_listed(self, tmp_path: Path) -> None:
        # Code compiled from a file that no longer exists has no source line to show.
        code = compile(
            "def f():\n    raise ValueError('gone')\n", str(tmp_path / "gone.py"), "exec"
        )
        namespace: dict = {}
        exec(code, namespace)
        try:
            namespace["f"]()
        except ValueError as exc:
            error: BaseException = exc

        text = _render(test_cmd.render_failure, error, Opts())

        assert "gone.py:2 in f" in text


class TestReportBatch:
    def test_a_failure_before_anything_was_fetched_shows_only_the_card(self) -> None:
        report = BatchReport(error=PipelineError("source unreachable"))

        text = _render(test_cmd.report_batch, report, Opts())

        assert "Pipeline step failed" in text
        assert "Fetched" not in text

    def test_an_empty_source_is_called_out(self) -> None:
        text = _render(test_cmd.report_batch, BatchReport(), Opts())

        assert "Fetched 0 records" in text
        assert "No records returned from source" in text


def _stub_pipeline() -> Any:
    """The least run_batch needs of a pipeline when the job core is stubbed out."""
    stub = types.SimpleNamespace(name="stub", define_transformations=lambda records, params: [])
    stub.fork = lambda: stub
    return stub


class TestRunBatchErrors:
    @pytest.fixture
    def jobs(self, monkeypatch: pytest.MonkeyPatch) -> List[Any]:
        """Each planned slice is one item; run_job_records returns or raises it."""
        script: List[Any] = []

        def run_job_records(sub: Any, pipeline: Any, params: Any, limit: int) -> Any:
            item = script.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item

        # run_batch drives the shared loop in reflowfy.testing.runner.
        from reflowfy.testing import runner

        monkeypatch.setattr(runner, "run_job_records", run_job_records)
        monkeypatch.setattr(runner, "plan_slices", lambda source, params: iter(source))
        monkeypatch.setattr(runner, "job_runtime_params", lambda sub, params: dict(params))
        return script

    def test_a_non_transformation_error_is_recorded_as_is(self, jobs: List[Any]) -> None:
        jobs.append(RuntimeError("fetch timed out"))

        report = test_cmd.run_batch(_stub_pipeline(), ["slice"], {}, Opts(dry_run=True))

        assert isinstance(report.error, RuntimeError)
        assert (report.fetched, report.applied) == (0, [])

    def test_a_later_job_failing_keeps_the_earlier_fetch_count(self, jobs: List[Any]) -> None:
        step = AppliedStep("first", 0.0, 5, 5)
        jobs.append(([1, 2], [1, 2], [], None))
        jobs.append(TransformationError("second", "boom", applied_steps=[step]))

        report = test_cmd.run_batch(_stub_pipeline(), ["a", "b"], {}, Opts(dry_run=True))

        assert report.fetched == 2  # not overwritten by the failing step's input count
        assert [s.name for s in report.applied] == ["first"]

    def test_a_first_step_failure_with_an_unknown_input_count(self, jobs: List[Any]) -> None:
        jobs.append(TransformationError("only", "boom"))

        report = test_cmd.run_batch(_stub_pipeline(), ["a"], {}, Opts(dry_run=True))

        assert report.fetched == 0
        assert isinstance(report.error, TransformationError)


class TestRunBatchRealRuns:
    """run_batch on the real shared loop, for the cases the stubs above cannot reach."""

    def _pipeline(self, steps: List[Any]) -> Any:
        from reflowfy import AbstractPipeline, RuntimeParams

        class Plain(AbstractPipeline[RuntimeParams]):
            def define_source(self, runtime_params: Any) -> Any:
                raise AssertionError("run_batch is given the source")

            def define_transformations(self, records: Any, runtime_params: Any) -> Any:
                return [step() for step in steps]

            def define_destination(self, records: Any, runtime_params: Any) -> Any:
                from reflowfy.testing import CaptureDestination

                return CaptureDestination()

        Plain.name = "gaps_plain"  # set afterwards, so the class is not registered
        return Plain()

    class Duck:
        """A source that is not a BaseSource: its fetch cannot be observed."""

        def fetch(self, runtime_params: Any) -> List[Any]:
            return [{"data": 1}, {"data": 2}]

    def test_a_later_step_failing_on_a_duck_source_reports_its_input(self) -> None:
        from reflowfy import BaseTransformation

        class Passes(BaseTransformation):
            def apply(self, records: Any, runtime_params: Any) -> Any:
                return records

        class Fails(BaseTransformation):
            def apply(self, records: Any, runtime_params: Any) -> Any:
                raise ValueError("boom")

        report = test_cmd.run_batch(
            self._pipeline([Passes, Fails]), self.Duck(), {}, Opts(dry_run=True)
        )
        assert isinstance(report.error, TransformationError)
        assert report.fetched == 2 and [s.records_in for s in report.applied] == [2]

    def test_a_first_step_failing_on_a_duck_source_reports_its_input(self) -> None:
        from reflowfy import BaseTransformation

        class Fails(BaseTransformation):
            def apply(self, records: Any, runtime_params: Any) -> Any:
                raise ValueError("boom")

        report = test_cmd.run_batch(self._pipeline([Fails]), self.Duck(), {}, Opts(dry_run=True))
        assert report.fetched == 2 and report.applied == []

    def test_a_job_its_source_drops_is_reported_as_dropped(self) -> None:
        from reflowfy import SkipJob
        from reflowfy.sources.static import StaticSource

        class Dropping(StaticSource):
            def fetch(self, runtime_params: Any, limit: Any = None) -> Any:
                raise SkipJob("closed today")

        report = test_cmd.run_batch(self._pipeline([]), Dropping([1]), {}, Opts(dry_run=True))
        assert report.ok and report.fetched == 0 and report.jobs_dropped == ["closed today"]
        text = _render(test_cmd.report_batch, report, Opts())
        assert "closed today" in text and "No records returned" not in text

    def test_inside_a_test_session_the_preview_uses_it(self) -> None:
        import httpx

        from reflowfy import BaseTransformation
        from reflowfy.testing import mock_network

        class Calls(BaseTransformation):
            def apply(self, records: Any, runtime_params: Any) -> Any:
                return [{**r, "x": httpx.get("https://a.test/x").json()} for r in records]

        with mock_network() as net:
            net.http.get("https://a.test/x", json=7)
            report = test_cmd.run_batch(
                self._pipeline([Calls]), self.Duck(), {}, Opts(dry_run=True)
            )
        assert report.ok and [r["x"] for r in report.transformed] == [7, 7]


def test_a_file_that_cannot_be_loaded_as_a_module_exits(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(importlib.util, "spec_from_file_location", lambda *a, **k: None)
    printed: List[str] = []
    monkeypatch.setattr(console, "print", lambda msg, *a, **k: printed.append(str(msg)))

    with pytest.raises(typer.Exit):
        test_cmd.load_from_path(tmp_path / "p.py")
    assert printed == [f"[red]❌ Cannot load: {tmp_path / 'p.py'}[/red]"]


# ----- the `test` command end to end ----------------------------------------------------


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    saved = dict(pipeline_registry._pipelines)
    saved_failures = dict(pipeline_registry._failures)
    saved_tx = dict(transformation_registry._transformations)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setenv("LOG_JSON", "false")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setattr(test_cmd, "setup_logging", lambda service_name="cli": None)
    monkeypatch.setattr(
        "reflowfy.core.pipeline_discovery.discover_and_load_pipelines", lambda *a, **k: None
    )
    monkeypatch.setattr(console, "_width", 250)
    monkeypatch.setattr(console, "quiet", False)
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)
        pipeline_registry._failures.clear()
        pipeline_registry._failures.update(saved_failures)
        transformation_registry._transformations.clear()
        transformation_registry._transformations.update(saved_tx)


def _failing_id_pipeline(name: str, bad_id: Any) -> None:
    def fail_on(self: Any, records: Any, params: Any) -> Any:
        if bad_id in params["current_ids"]:
            raise ValueError(f"cannot enrich {bad_id}")
        return records

    tx_ns = {"name": f"{name}_enrich", "apply": fail_on, "__module__": __name__}
    enrich = types.new_class("Enrich", (BaseTransformation,), {}, lambda n: n.update(tx_ns))()

    ns = {
        "name": name,
        "ids_batch_size": 1,
        "__module__": __name__,
        "define_source": lambda self, p: MockSource(
            data=[{"id": i} for i in p["current_ids"]], batch_size=10
        ),
        "define_transformations": lambda self, records, p: [enrich],
        "define_destination": lambda self, records, p: None,
    }
    types.new_class(f"_{name}", (IdBasedPipeline[RuntimeParams],), {}, lambda n: n.update(ns))


def test_fail_fast_stops_at_the_first_batch_whose_chain_fails(cli_env: None) -> None:
    _failing_id_pipeline("gaps_cli_ids_fail_fast", bad_id="2")  # -p values are strings
    # cwd is already importable: it is not added a second time
    sys.path.insert(0, str(Path.cwd()))

    result = runner.invoke(
        app,
        [
            "test",
            "gaps_cli_ids_fail_fast",
            "--no-input",
            "--dry-run",
            "--fail-fast",
            "-p",
            "input_ids=1,2,3",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "cannot enrich 2" in result.output
    assert "stopping — --fail-fast is set" in result.output
    assert "Batch 3/3" not in result.output
    assert sys.path.count(str(Path.cwd())) == 1
