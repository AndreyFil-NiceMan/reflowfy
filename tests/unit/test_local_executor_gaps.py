"""LocalExecutor outcomes the main suites don't reach: the preview cap, dropped jobs,
empty runs, failures, and duck-typed pipelines without fork()/resolve()."""

import logging
import types
from typing import Any, Dict, Iterator, List, Optional

import pytest

from reflowfy import SkipJob, job
from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.execution.base import ExecutionState
from reflowfy.execution.local_executor import LocalExecutor
from reflowfy.sources.base import BaseSource, SourceJob
from reflowfy.sources.static import StaticSource
from reflowfy.transformations.base import BaseTransformation
from reflowfy.transformations.registry import transformation_registry

LOGGER = "reflowfy.execution.local_executor"


@pytest.fixture(autouse=True)
def isolated_registries() -> Iterator[None]:
    saved = dict(pipeline_registry._pipelines)
    saved_failures = dict(pipeline_registry._failures)
    saved_tx = dict(transformation_registry._transformations)
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)
        pipeline_registry._failures.clear()
        pipeline_registry._failures.update(saved_failures)
        transformation_registry._transformations.clear()
        transformation_registry._transformations.update(saved_tx)


@pytest.fixture
def logs() -> Iterator[List[str]]:
    """Messages of the executor's logger (the 'reflowfy' logger may not propagate)."""
    messages: List[str] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    target = logging.getLogger(LOGGER)
    handler = _Handler(logging.DEBUG)
    old_level = target.level
    target.setLevel(logging.DEBUG)  # setLevel, not .level=: it clears isEnabledFor's cache
    target.addHandler(handler)
    try:
        yield messages
    finally:
        target.removeHandler(handler)
        target.setLevel(old_level)


class _Dest:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self.error = error
        self.sent: List[List[Any]] = []

    def send_with_retry(self, records: List[Any], metadata: Dict[str, Any]) -> None:
        if self.error is not None:
            raise self.error
        self.sent.append(list(records))


def _make(base: Any, name: str, **body: Any) -> Any:
    namespace = {"__module__": __name__, "name": name, **body}
    cls = types.new_class("Gap", (base,), exec_body=lambda ns: ns.update(namespace))
    return cls()


def _transformation(tx_name: str, fn: Any) -> BaseTransformation:
    namespace = {"__module__": __name__, "name": tx_name, "apply": fn}
    cls = types.new_class("GapTx", (BaseTransformation,), exec_body=lambda ns: ns.update(namespace))
    return cls()


# ----- AbstractPipeline path --------------------------------------------------------


def test_preview_stops_once_max_records_is_reached(logs: List[str]) -> None:
    dest = _Dest()
    double = _transformation("gaps_local_double", lambda self, r, p: r + r)

    def define_jobs(self: Any, runtime_params: Any) -> Any:
        for n in range(3):
            yield job([{"job": n}, {"job": n}])

    pipeline = _make(
        AbstractPipeline[RuntimeParams],
        "gaps_local_capped",
        define_jobs=define_jobs,
        define_transformations=lambda self, records, p: [double],
        define_destination=lambda self, records, p: dest,
    )

    status = LocalExecutor(max_records=2).execute(pipeline, {}, execution_id="exec-1")

    assert status.execution_id == "exec-1"
    assert status.state is ExecutionState.COMPLETED
    assert status.metadata == {"records_fetched": 2, "records_sent": 4}
    assert dest.sent == [[{"job": 0}] * 4]  # jobs 1 and 2 never ran
    assert any("Applied transformation: gaps_local_double (2 -> 4" in m for m in logs)


def test_a_dropped_job_is_logged_with_its_reason_and_sends_nothing(logs: List[str]) -> None:
    dest = _Dest()

    def drop(self: Any, records: Any, params: Any) -> Any:
        raise SkipJob("upstream not ready")

    dropper = _transformation("gaps_local_drop", drop)
    pipeline = _make(
        AbstractPipeline[RuntimeParams],
        "gaps_local_skip",
        define_source=lambda self, p: StaticSource([{"id": 1}]),
        define_transformations=lambda self, records, p: [dropper],
        define_destination=lambda self, records, p: dest,
    )

    status = LocalExecutor().execute(pipeline, {}, execution_id="exec-skip")

    assert status.state is ExecutionState.COMPLETED
    assert status.metadata == {"records_count": 0}
    assert dest.sent == []
    assert "Execution exec-skip: job dropped (upstream not ready)" in logs
    assert "Execution exec-skip: no records fetched" in logs


def test_a_destination_failure_fails_the_execution(logs: List[str]) -> None:
    pipeline = _make(
        AbstractPipeline[RuntimeParams],
        "gaps_local_fail",
        define_source=lambda self, p: StaticSource([{"id": 1}]),
        define_transformations=lambda self, records, p: [],
        define_destination=lambda self, records, p: _Dest(error=RuntimeError("broker down")),
    )

    status = LocalExecutor().execute(pipeline, {})

    assert status.state is ExecutionState.FAILED
    assert (status.failed_jobs, status.completed_jobs) == (1, 0)
    assert status.error_message == "broker down"


class _DuckPipeline:
    """Not an AbstractPipeline: no fork(), no resolve(); just the hooks and a source."""

    name = "gaps_local_duck"

    def __init__(self, dest: _Dest) -> None:
        self.source = StaticSource([{"id": 1}, {"id": 2}])
        self.dest = dest

    def define_transformations(self, records: Any, runtime_params: Any) -> List[Any]:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> _Dest:
        return self.dest


def test_a_duck_typed_pipeline_runs_as_is() -> None:
    dest = _Dest()
    status = LocalExecutor().execute(_DuckPipeline(dest), {"x": 1})

    assert status.state is ExecutionState.COMPLETED
    assert dest.sent == [[{"id": 1}, {"id": 2}]]


# ----- IdBasedPipeline path ---------------------------------------------------------


class _TwoSlices(BaseSource):
    """Splits into two jobs of two records each."""

    def __init__(self, current_id: Any) -> None:
        super().__init__({"current_id": current_id})

    def split(self, runtime_params: Dict[str, Any]) -> Iterator[BaseSource]:
        cid = self.config["current_id"]
        yield StaticSource([{"id": cid, "n": 0}, {"id": cid, "n": 1}])
        yield StaticSource([{"id": cid, "n": 2}, {"id": cid, "n": 3}])

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        raise AssertionError("the slices are fetched, not the parent")

    def split_jobs(self, runtime_params: Dict[str, Any], batch_size: int = 1000) -> Any:
        return iter([SourceJob(records=[], metadata={})])

    def health_check(self) -> bool:
        return True


def _id_pipeline(name: str, dest: _Dest, **body: Any) -> Any:
    hooks: Dict[str, Any] = {
        "define_source": lambda self, p: _TwoSlices(p["current_ids"][0]),
        "define_transformations": lambda self, records, p: [],
        "define_destination": lambda self, records, p: dest,
        **body,
    }
    return _make(IdBasedPipeline[RuntimeParams], name, **hooks)


def test_id_preview_caps_each_id_and_logs_applied_steps(logs: List[str]) -> None:
    dest = _Dest()
    tag = _transformation("gaps_local_tag", lambda self, r, p: [{**x, "t": 1} for x in r])
    pipeline = _id_pipeline(
        "gaps_local_id_capped", dest, define_transformations=lambda self, records, p: [tag]
    )

    status = LocalExecutor(max_records=2).execute(pipeline, {"input_ids": [7, 8]})

    assert status.state is ExecutionState.COMPLETED
    assert status.completed_jobs == 2
    assert status.metadata == {"ids_processed": 2, "records_fetched": 4, "records_sent": 4}
    # one slice per ID: the second slice of each ID is past the cap
    assert [[r["n"] for r in batch] for batch in dest.sent] == [[0, 1], [0, 1]]
    assert any(m.startswith("Applied: gaps_local_tag (2 -> 2") for m in logs)


def test_id_with_a_dropped_job_counts_as_done_without_records(logs: List[str]) -> None:
    dest = _Dest()

    def drop_seven(self: Any, records: Any, params: Any) -> Any:
        if params["current_id"] == 7:
            raise SkipJob("seven is archived")
        return records

    dropper = _transformation("gaps_local_drop_seven", drop_seven)
    pipeline = _id_pipeline(
        "gaps_local_id_skip",
        dest,
        define_source=lambda self, p: [{"id": p["current_ids"][0]}],
        define_transformations=lambda self, records, p: [dropper],
    )

    status = LocalExecutor().execute(pipeline, {"input_ids": [7, 8]})

    assert status.state is ExecutionState.COMPLETED
    assert status.completed_jobs == 2
    assert status.metadata["records_fetched"] == 1
    assert dest.sent == [[{"id": 8}]]
    assert "ID 7: job dropped (seven is archived)" in logs
    assert "No records for ID: 7" in logs


def test_id_failure_counts_the_ids_that_did_not_finish() -> None:
    def fail_on_nine(self: Any, records: Any, params: Any) -> Any:
        if params["current_id"] == 9:
            raise RuntimeError("nine is cursed")
        return _Dest()

    pipeline = _id_pipeline(
        "gaps_local_id_fail",
        _Dest(),
        define_source=lambda self, p: [{"id": p["current_ids"][0]}],
        define_destination=fail_on_nine,
    )

    status = LocalExecutor().execute(pipeline, {"input_ids": [8, 9, 10]})

    assert status.state is ExecutionState.FAILED
    assert (status.completed_jobs, status.failed_jobs) == (1, 2)
    assert "nine is cursed" in (status.error_message or "")


class _EmptySource(BaseSource):
    """A slice that turns out to hold nothing when fetched (e.g. rows deleted since)."""

    def __init__(self) -> None:
        super().__init__({})

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        return []

    def split_jobs(self, runtime_params: Dict[str, Any], batch_size: int = 1000) -> Any:
        return iter([])

    def health_check(self) -> bool:
        return True


def test_an_empty_fetch_is_not_reported_as_a_drop(logs: List[str]) -> None:
    dest = _Dest()
    pipeline = _make(
        AbstractPipeline[RuntimeParams],
        "gaps_local_empty",
        define_source=lambda self, p: _EmptySource(),
        define_transformations=lambda self, records, p: [],
        define_destination=lambda self, records, p: dest,
    )

    status = LocalExecutor().execute(pipeline, {}, execution_id="exec-empty")

    assert status.state is ExecutionState.COMPLETED
    assert status.metadata == {"records_count": 0}
    assert not any("dropped" in m for m in logs)


def test_an_id_with_an_empty_fetch_is_not_reported_as_a_drop(logs: List[str]) -> None:
    dest = _Dest()
    pipeline = _id_pipeline(
        "gaps_local_id_empty", dest, define_source=lambda self, p: _EmptySource()
    )

    status = LocalExecutor().execute(pipeline, {"input_ids": [1]})

    assert status.state is ExecutionState.COMPLETED
    assert status.completed_jobs == 1
    assert dest.sent == []
    assert "No records for ID: 1" in logs
    assert not any("dropped" in m for m in logs)
