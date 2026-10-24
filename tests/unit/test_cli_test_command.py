"""`reflowfy test`, driven through the real typer app with CliRunner."""

import json
import logging
import sys
import textwrap
import types
from typing import Any, Dict, Iterator, List, Literal, Optional
from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner
from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    IdBasedPipeline,
    Param,
    PipelineError,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    pipeline_registry,
    record_transformation,
)
from reflowfy.cli.commands import test as test_cmd
from reflowfy.cli.main import app
from reflowfy.cli.utils import console
from reflowfy.sources.mock import MockSource
from reflowfy.transformations.base import BaseTransformation

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixtures and pipeline builders
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def cli_env(monkeypatch, tmp_path) -> Iterator[None]:
    """Isolated registry, cwd, sys.path and env; discovery and logging stubbed."""
    saved = dict(pipeline_registry._pipelines)
    saved_failures = dict(pipeline_registry._failures)
    pipeline_registry._pipelines.clear()

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


def _destination() -> AsyncMock:
    destination = AsyncMock()
    destination.health_check.return_value = True
    return destination


class DropJob(BaseTransformation):
    name = "drop_job"

    def apply(self, records: Any, runtime_params: Dict[str, Any]) -> Any:
        raise SkipJob("not this one")


@pytest.fixture
def skip_logs() -> Iterator[List[logging.LogRecord]]:
    """Records the test command logs (setup_logging is stubbed in this module)."""
    seen: List[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = seen.append  # type: ignore[method-assign]
    cli_logger = logging.getLogger("reflowfy.cli.test")
    cli_logger.addHandler(handler)
    cli_logger.setLevel(logging.INFO)
    try:
        yield seen
    finally:
        cli_logger.removeHandler(handler)


class Upper(BaseTransformation):
    name = "upper_names"

    def apply(self, records: Any, runtime_params: Dict[str, Any]) -> Any:
        return [{**r, "data": {**r["data"], "name": r["data"]["name"].upper()}} for r in records]


class DropAll(BaseTransformation):
    name = "drop_all"

    def apply(self, records: Any, runtime_params: Dict[str, Any]) -> Any:
        return []


class Explode(BaseTransformation):
    name = "explode_on_bob"

    def apply(self, records: Any, runtime_params: Dict[str, Any]) -> Any:
        for r in records:
            if r["data"]["name"].lower() == "bob":
                raise KeyError("missing_field")
        return records


# MockSource wraps each of these as {"data": <record>}.
RECORDS = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "carol"}]


@record_transformation("skip_odd")
async def skip_odd(record: Any, runtime_params: Any) -> Any:
    if record["data"]["id"] % 2:
        raise SkipRecord("odd id")
    return record


class CliParams(RuntimeParams, total=False):
    env: Required[str]
    limit_rows: Annotated[NotRequired[int], Param("Rows", default=10)]
    tags: NotRequired[List[str]]
    flag: NotRequired[bool]


def _pipeline(
    name: str,
    *,
    transforms: Optional[List[BaseTransformation]] = None,
    destination: Optional[AsyncMock] = None,
    params_type: Any = RuntimeParams,
    source_error: Optional[BaseException] = None,
    batch_size: int = 10,
) -> AbstractPipeline[Any]:
    dest = destination if destination is not None else _destination()

    def define_source(self, runtime_params):  # type: ignore[no-untyped-def]
        if source_error is not None:
            raise source_error
        return MockSource(data=[dict(r) for r in RECORDS], batch_size=batch_size)

    def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return list(transforms or [])

    def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return dest

    ns = {
        "name": name,
        "define_source": define_source,
        "define_transformations": define_transformations,
        "define_destination": define_destination,
    }
    types.new_class(f"_{name}", (AbstractPipeline[params_type],), {}, lambda n: n.update(ns))
    pipeline = pipeline_registry.get(name)
    assert pipeline is not None
    return pipeline


def _id_pipeline(name: str, destination: AsyncMock, bad_id: Any = None) -> None:
    def define_source(self, runtime_params):  # type: ignore[no-untyped-def]
        ids = runtime_params["current_ids"]
        if bad_id in ids:
            raise PipelineError(f"cannot resolve id {bad_id}")
        return MockSource(data=[{"id": i, "name": f"n{i}"} for i in ids], batch_size=10)

    def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return []

    def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return destination

    ns = {
        "name": name,
        "ids_batch_size": 2,
        "define_source": define_source,
        "define_transformations": define_transformations,
        "define_destination": define_destination,
    }
    types.new_class(f"_{name}", (IdBasedPipeline[RuntimeParams],), {}, lambda n: n.update(ns))


def invoke(*args: str, input: Optional[str] = None) -> Any:
    return runner.invoke(app, ["test", *args], input=input)


# ---------------------------------------------------------------------------
# resolve_pipeline
# ---------------------------------------------------------------------------


class TestResolvePipeline:
    def test_by_name(self):
        _pipeline("cli_by_name")
        result = invoke("cli_by_name", "--no-input", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "Testing cli_by_name" in result.output
        assert "Fetched 3 records" in result.output
        assert "Test complete: 3 records processed" in result.output

    def test_unknown_name_lists_the_registered_ones(self):
        _pipeline("cli_known_b")
        _pipeline("cli_known_a")
        result = invoke("cli_typo", "--no-input")
        assert result.exit_code == 1
        assert "No pipeline named 'cli_typo'" in result.output
        assert "Registered pipelines: cli_known_a, cli_known_b" in result.output

    def test_unknown_name_explains_a_registration_failure(self):
        pipeline_registry.record_failure("cli_broken", ValueError("missing ES_URL"))
        result = invoke("cli_broken", "--no-input")
        assert result.exit_code == 1
        assert "it failed to register: ValueError: missing ES_URL" in result.output

    def test_by_path(self, tmp_path):
        path = tmp_path / "my_pipe.py"
        path.write_text(textwrap.dedent("""
                from reflowfy import AbstractPipeline, RuntimeParams
                from reflowfy.sources.mock import MockSource
                from reflowfy.destinations.console import console_destination

                class FilePipe(AbstractPipeline[RuntimeParams]):
                    name = "cli_from_file"

                    def define_source(self, runtime_params):
                        return MockSource(data=[{"id": 1}, {"id": 2}])

                    def define_transformations(self, records, runtime_params):
                        return []

                    def define_destination(self, records, runtime_params):
                        return console_destination()
                """))
        result = invoke(str(path), "--no-input", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "Testing cli_from_file" in result.output
        assert "Fetched 2 records" in result.output

    def test_missing_file(self):
        result = invoke("nowhere/missing.py", "--no-input")
        assert result.exit_code == 1
        assert "File not found: nowhere/missing.py" in result.output

    def test_file_that_fails_to_import(self, tmp_path):
        path = tmp_path / "broken.py"
        path.write_text("raise RuntimeError('import time boom')\n")
        result = invoke(str(path), "--no-input")
        assert result.exit_code == 1
        assert "Error loading pipeline file: import time boom" in result.output

    def test_file_without_pipelines(self, tmp_path):
        path = tmp_path / "empty.py"
        path.write_text("X = 1\n")
        result = invoke(str(path), "--no-input")
        assert result.exit_code == 1
        assert "No pipelines found in the file" in result.output

    def test_several_pipelines_with_no_input_refuses_to_guess(self, tmp_path):
        _pipeline("cli_multi_one")
        _pipeline("cli_multi_two")
        (tmp_path / "several.py").write_text("X = 1\n")
        result = invoke("several.py", "--no-input")
        assert result.exit_code == 1
        assert "2 pipelines in that file; name the one you want" in result.output
        assert "Found: cli_multi_one, cli_multi_two" in result.output

    @pytest.mark.parametrize("choice, expected", [("2", "cli_pick_two"), ("", "cli_pick_one")])
    def test_several_pipelines_prompts_for_one(self, tmp_path, choice, expected):
        _pipeline("cli_pick_one")
        _pipeline("cli_pick_two")
        (tmp_path / "several.py").write_text("X = 1\n")
        result = invoke("several.py", "--dry-run", input=f"{choice}\n")
        assert result.exit_code == 0, result.output
        assert "Found multiple pipelines" in result.output
        assert f"Testing {expected}" in result.output

    def test_several_pipelines_invalid_choice(self, tmp_path):
        _pipeline("cli_bad_one")
        _pipeline("cli_bad_two")
        (tmp_path / "several.py").write_text("X = 1\n")
        result = invoke("several.py", input="9\n")
        assert result.exit_code == 1
        assert "Invalid choice" in result.output


# ---------------------------------------------------------------------------
# collect_params / parse_param_flags
# ---------------------------------------------------------------------------


class TestParams:
    def test_flags_are_coerced_to_declared_types(self):
        _pipeline("cli_params", params_type=CliParams)
        result = invoke(
            "cli_params",
            "--no-input",
            "--dry-run",
            "--json",
            "-p",
            "env=prod",
            "-p",
            "limit_rows=5",
            "-p",
            "tags=a, b",
            "-p",
            "flag=yes",
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["params"] == {
            "env": "prod",
            "limit_rows": 5,
            "tags": ["a", "b"],
            "flag": True,
        }

    def test_no_input_uses_defaults(self):
        _pipeline("cli_defaults", params_type=CliParams)
        result = invoke("cli_defaults", "--no-input", "--dry-run", "-p", "env=dev")
        assert result.exit_code == 0, result.output
        assert "limit_rows = 10 (default)" in result.output
        assert "env = 'dev' (--param)" in result.output

    def test_no_input_missing_required_fails_with_exit_2(self):
        _pipeline("cli_required", params_type=CliParams)
        result = invoke("cli_required", "--no-input")
        assert result.exit_code == 2
        assert "Required parameter 'env' not supplied" in result.output

    def test_malformed_param_flag(self):
        _pipeline("cli_malformed", params_type=CliParams)
        result = invoke("cli_malformed", "--no-input", "-p", "env")
        assert result.exit_code == 2
        assert "--param expects key=value, got 'env'" in result.output

    def test_params_for_a_pipeline_that_declares_none_pass_through(self):
        _pipeline("cli_no_params")
        result = invoke("cli_no_params", "--no-input", "--dry-run", "--json", "-p", "x=1")
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["params"] == {"x": "1"}

    def test_no_params_message(self):
        _pipeline("cli_no_params_msg")
        result = invoke("cli_no_params_msg", "--no-input", "--dry-run")
        assert "No parameters needed for this pipeline" in result.output

    def test_interactive_prompts(self):
        _pipeline("cli_prompted", params_type=CliParams)
        # env (required str), limit_rows (default 10 -> accept), tags (optional,
        # blank), flag (bool Confirm without default -> "y")
        result = invoke("cli_prompted", "--dry-run", "--json", input="staging\n\n\ny\n")
        assert result.exit_code == 0, result.output
        params = json.loads(result.stdout[result.stdout.index("{") :])["params"]
        assert params["env"] == "staging"
        assert params["limit_rows"] == 10
        assert params["flag"] is True
        assert params["tags"] == []

    def test_prompt_value_outside_choices_warns(self):
        class ChoiceParams(RuntimeParams, total=False):
            env: Annotated[NotRequired[Literal["dev", "prod"]], Param("Environment", default="dev")]

        _pipeline("cli_choices", params_type=ChoiceParams)
        result = invoke("cli_choices", "--dry-run", input="qa\n")
        assert result.exit_code == 0, result.output
        assert "choices: ['dev', 'prod']" in result.output
        assert "'qa' not in choices ['dev', 'prod'], using anyway" in result.output


# ---------------------------------------------------------------------------
# Running: --json, --dry-run, sending, failures
# ---------------------------------------------------------------------------


class TestRun:
    def test_json_report_shape(self):
        _pipeline("cli_json", transforms=[Upper()])
        result = invoke("cli_json", "--no-input", "--dry-run", "--json", "--limit", "2")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert set(payload) == {
            "pipeline",
            "params",
            "limit",
            "dry_run",
            "batches",
            "records_out",
            "records_skipped",
            "ok",
        }
        assert payload["pipeline"] == "cli_json"
        assert payload["limit"] == 2
        assert payload["dry_run"] is True
        assert payload["ok"] is True
        assert payload["records_out"] == 2
        [batch] = payload["batches"]
        assert batch["records_fetched"] == 2
        assert batch["sent"] is False
        assert [s["name"] for s in batch["steps"]] == ["upper_names"]
        assert batch["steps"][0]["records_in"] == 2
        assert "error" not in batch

    def test_skip_record_reported(self):
        dest = _destination()
        _pipeline("cli_skip", transforms=[skip_odd()], destination=dest)
        result = invoke("cli_skip", "--no-input")
        assert result.exit_code == 0, result.output
        assert "skipped 2: odd id" in result.output
        assert "1 records sent successfully, 2 skipped by SkipRecord" in result.output
        sent = dest.send_with_retry.call_args.args[0]
        assert [r["data"]["id"] for r in sent] == [2]

    def test_skip_record_in_json(self):
        _pipeline("cli_skip_json", transforms=[skip_odd()])
        result = invoke("cli_skip_json", "--no-input", "--dry-run", "--json")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["records_out"] == 1
        assert payload["records_skipped"] == 2
        [batch] = payload["batches"]
        assert batch["records_skipped"] == 2
        [step] = batch["steps"]
        assert step["records_skipped"] == 2
        assert step["skip_reasons"] == {"odd id": 2}

    def test_skips_are_logged_like_the_worker(self, skip_logs):
        _pipeline("cli_skip_log", transforms=[skip_odd()])
        result = invoke("cli_skip_log", "--no-input", "--dry-run")
        assert result.exit_code == 0, result.output
        [rec] = [r for r in skip_logs if getattr(r, "event", None) == "records_skipped"]
        assert rec.getMessage() == "skip_odd skipped 2 records ('odd id' x2)"
        assert rec.__dict__["transformation"] == "skip_odd"
        assert rec.__dict__["records_skipped"] == 2
        assert rec.__dict__["pipeline_name"] == "cli_skip_log"
        assert rec.__dict__["execution_id"]

    def test_skip_job_is_logged_and_reported(self, skip_logs):
        dest = _destination()
        _pipeline("cli_skip_job", transforms=[DropJob()], destination=dest)
        result = invoke("cli_skip_job", "--no-input")
        assert result.exit_code == 0, result.output
        assert "Job dropped by pipeline (SkipJob): not this one" in result.output
        assert "No records returned from source" not in result.output
        dest.send_with_retry.assert_not_called()
        [rec] = [r for r in skip_logs if getattr(r, "event", None) == "job_dropped"]
        assert rec.__dict__["skip_reason"] == "not this one"
        assert rec.__dict__["pipeline_name"] == "cli_skip_job"

    def test_skip_job_in_json_and_logs_kept_off_stdout(self, monkeypatch):
        root = logging.getLogger("reflowfy")
        handlers: List[logging.Handler] = []

        def setup(service_name: str = "cli") -> None:
            # Built inside the run, as setup_logging does: sys.stdout is the runner's.
            handlers.append(logging.StreamHandler(sys.stdout))
            root.addHandler(handlers[0])

        monkeypatch.setattr(test_cmd, "setup_logging", setup)
        monkeypatch.setattr(root, "level", logging.INFO)
        try:
            _pipeline("cli_skip_job_json", transforms=[DropJob()])
            result = invoke("cli_skip_job_json", "--no-input", "--dry-run", "--json")
        finally:
            root.removeHandler(handlers[0])
        assert result.exit_code == 0, result.output
        assert "Job dropped by pipeline" not in result.stdout
        [batch] = json.loads(result.stdout)["batches"]
        assert batch["jobs_dropped"] == ["not this one"]

    def test_dry_run_sends_nothing(self):
        dest = _destination()
        _pipeline("cli_dry", destination=dest)
        result = invoke("cli_dry", "--no-input", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "Dry run — skipped destination send" in result.output
        dest.send_with_retry.assert_not_called()
        dest.health_check.assert_not_called()

    def test_real_run_sends_each_job(self):
        dest = _destination()
        _pipeline("cli_send", destination=dest, batch_size=2)
        result = invoke("cli_send", "--no-input")
        assert result.exit_code == 0, result.output
        assert dest.send_with_retry.call_count == 2  # 3 records, 2 per job
        assert "Test complete: 3 records sent successfully" in result.output

    def test_failed_health_check_fails_the_run(self):
        dest = _destination()
        dest.health_check.return_value = False
        _pipeline("cli_unhealthy", destination=dest)
        result = invoke("cli_unhealthy", "--no-input", "--json")
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert payload["batches"][0]["error"]["message"] == "destination health check failed"
        dest.send_with_retry.assert_not_called()

    def test_step_that_empties_the_batch_is_flagged(self):
        _pipeline("cli_emptied", transforms=[DropAll()])
        result = invoke("cli_emptied", "--no-input", "--dry-run")
        assert result.exit_code == 0, result.output
        assert "this step emptied the batch" in result.output

    def test_transformation_failure_names_the_step_and_record(self):
        _pipeline("cli_tx_fail", transforms=[Upper(), Explode()])
        result = invoke("cli_tx_fail", "--no-input", "--dry-run")
        assert result.exit_code == 1
        out = result.output
        assert "Transformation #2 failed: explode_on_bob" in out
        # KeyError culprit = first record without the missing top-level key; it
        # is shown as it entered the failing step (after upper_names ran).
        assert "Failing record (index 0 of 3)" in out
        assert '"name": "ALICE"' in out
        assert "test_cli_test_command.py" in out  # "Where" points at user code
        assert "KeyError" in out
        assert "Test failed: 1 of 1 batch(es) errored" in out

    def test_transformation_failure_json(self):
        _pipeline("cli_tx_fail_json", transforms=[Explode()])
        result = invoke("cli_tx_fail_json", "--no-input", "--dry-run", "--json")
        assert result.exit_code == 1
        error = json.loads(result.stdout)["batches"][0]["error"]
        assert error["type"] == "TransformationError"
        assert error["transformation"] == "explode_on_bob"
        assert error["step_index"] == 0
        assert error["records_in"] == 3

    def test_source_failure_names_the_pipeline_step(self):
        _pipeline("cli_src_fail", source_error=ValueError("bad index"))
        result = invoke("cli_src_fail", "--no-input", "--dry-run")
        assert result.exit_code == 1
        assert "ValueError: bad index" in result.output
        assert "Test failed" in result.output

    def test_verbose_shows_params_timings_and_full_traceback(self):
        _pipeline("cli_verbose", transforms=[Upper(), Explode()])
        result = invoke("cli_verbose", "--no-input", "--dry-run", "-vv")
        assert result.exit_code == 1
        assert "runtime_params:" in result.output
        assert "pipeline_name = 'cli_verbose'" in result.output
        assert "ms" in result.output
        assert "--- full traceback ---" in result.output


class TestIdBatches:
    def test_ids_are_previewed_in_batches(self):
        dest = _destination()
        _id_pipeline("cli_ids", dest)
        result = invoke("cli_ids", "--no-input", "-p", "input_ids=1,2,3")
        assert result.exit_code == 0, result.output
        assert "3 ID(s) in 2 batch(es) (ids_batch_size=2)" in result.output
        assert "Batch 1/2: ['1', '2']" in result.output
        assert "Batch 2/2: ['3']" in result.output
        assert dest.send_with_retry.call_count == 2

    def test_one_bad_batch_is_tolerated_without_fail_fast(self):
        _id_pipeline("cli_ids_partial", _destination(), bad_id="1")
        result = invoke("cli_ids_partial", "--no-input", "-p", "input_ids=1,2,3")
        assert result.exit_code == 0, result.output
        assert "cannot resolve id 1" in result.output
        assert "Finished with failures" in result.output
        assert "from 1 of 2 batch(es)" in result.output

    def test_fail_fast_stops_and_exits_non_zero(self):
        _id_pipeline("cli_ids_fast", _destination(), bad_id="3")
        result = invoke(
            "cli_ids_fast", "--no-input", "--dry-run", "--fail-fast", "-p", "input_ids=1,2,3,4,5"
        )
        assert result.exit_code == 1
        assert "Batch 2/3" in result.output
        assert "Batch 3/3" not in result.output

    def test_json_partial_failure_exits_zero_but_reports_not_ok(self):
        _id_pipeline("cli_ids_json", _destination(), bad_id="1")
        result = invoke("cli_ids_json", "--no-input", "--json", "-p", "input_ids=1,2,3")
        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert [b["ok"] for b in payload["batches"]] == [False, True]

    def test_no_ids(self):
        _id_pipeline("cli_ids_none", _destination())
        result = invoke("cli_ids_none", "--no-input", "-p", "input_ids=")
        assert result.exit_code == 1
        assert "No IDs provided" in result.output


# ---------------------------------------------------------------------------
# Helpers used by the renderers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_summarize_truncates_unless_verbose(self):
        long = "x" * 500
        short = test_cmd.summarize(long, verbose=0)
        assert len(short) < 300
        assert "-v for full" in short
        assert test_cmd.summarize(long, verbose=1) == repr(long)

    def test_describe_error_includes_wrapped_cause(self):
        err = PipelineError("step failed", original_error=ValueError("root"))
        info = test_cmd.describe_error(err)
        assert info["type"] == "PipelineError"
        assert info["cause"] == "ValueError: root"
        assert test_cmd.root_cause(err).args == ("root",)

    def test_user_frames_without_traceback_is_empty(self):
        assert test_cmd.user_frames(ValueError("no tb")) == []
