"""SkipJob / SkipRecord carry ``extra=`` fields onto their log lines, like a logger call."""

import logging
from typing import Any, Dict, List

import pytest

from reflowfy import SkipJob, SkipRecord
from reflowfy.core.record_steps import skip_log_extra
from reflowfy.destinations.record import RecordDestination
from reflowfy.execution.job_runner import log_job_dropped, run_job_records
from reflowfy.observability.context import log_context
from reflowfy.transformations.decorators import transformation


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: List[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    def events(self, event: str) -> List[logging.LogRecord]:
        return [r for r in self.records if getattr(r, "event", None) == event]


@pytest.fixture
def captured():
    """Capture on the 'reflowfy' logger itself: it may not propagate to root."""
    log = logging.getLogger("reflowfy")
    handler, old_level = _Capture(), log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(old_level)


class _Source:
    def __init__(self, records: List[Any]):
        self._records = records

    def fetch(self, runtime_params: Any) -> List[Any]:
        return self._records


@transformation(name="skip_extra_test_inactive", per_record=True)
def _skip_inactive(record: Dict[str, Any], runtime_params: Any) -> Dict[str, Any]:
    if not record["active"]:
        raise SkipRecord("user inactive", extra={"user_id": record["id"], "tier": "gold"})
    if record["id"] == 3:
        raise SkipRecord("no extra")
    return record


class _Pipeline:
    name = "skip_extra_p"

    def __init__(self, drop: bool = False):
        self.drop = drop

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return [_skip_inactive()]

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        if self.drop:
            raise SkipJob("tenant disabled", extra={"tenant": "acme", "message": "clash"})
        return None


def test_skip_extras_are_stored():
    assert SkipRecord("x", extra={"a": 1}).extra == {"a": 1}
    assert SkipJob("x").extra == {}
    assert SkipRecord("x").extra == {}


def test_skip_record_with_extra_logs_one_line_per_record(captured):
    records = [
        {"id": 1, "active": False},
        {"id": 2, "active": True},
        {"id": 3, "active": True},
        {"id": 4, "active": False},
    ]
    with log_context(job_id="j1"):
        _, kept, applied, _ = run_job_records(_Source(records), _Pipeline(), {})

    assert [r["id"] for r in kept] == [2]
    assert applied[0].skip_reasons == {"user inactive": 2, "no extra": 1}
    lines = captured.events("record_skipped")
    # Only the skips that carry `extra` get a line of their own.
    assert [(r.user_id, r.record_index) for r in lines] == [(1, 0), (4, 3)]
    line = lines[0]
    assert line.levelno == logging.INFO
    assert line.tier == "gold"
    assert line.skip_reason == "user inactive"
    assert line.transformation == "skip_extra_test_inactive"
    assert line.job_id == "j1"  # the bound job context is still there


def test_skip_job_extra_lands_on_job_dropped(captured):
    runtime_params: Dict[str, Any] = {}
    run_job_records(_Source([{"id": 2, "active": True}]), _Pipeline(drop=True), runtime_params)
    assert runtime_params["skip_extra"] == {"tenant": "acme", "message": "clash"}

    log_job_dropped(
        logging.getLogger("reflowfy.test"),
        runtime_params["skip_reason"],
        runtime_params["skip_extra"],
    )
    (line,) = captured.events("job_dropped")
    assert line.tenant == "acme"
    assert line.skip_reason == "tenant disabled"
    # A LogRecord attribute would make the log call raise: it is kept as a label.
    assert getattr(line, "labels.message") == "clash"
    assert line.getMessage() == "Job dropped by pipeline (tenant disabled)"


def test_skip_job_without_extra_leaves_runtime_params_alone():
    runtime_params: Dict[str, Any] = {}

    class _P(_Pipeline):
        def define_destination(self, records: Any, runtime_params: Any) -> Any:
            raise SkipJob("plain")

    run_job_records(_Source([{"id": 2, "active": True}]), _P(), runtime_params)
    assert "skip_extra" not in runtime_params


def test_framework_fields_win_and_clashes_become_labels():
    merged = skip_log_extra(
        {"event": "mine", "skip_reason": "mine", "name": "n", "_private": 1, "_": 2, "ok": 3},
        {"event": "job_dropped", "skip_reason": "why"},
    )
    assert merged == {
        "labels.event": "mine",
        "labels.skip_reason": "mine",
        "labels.name": "n",
        "labels.private": 1,
        "labels.extra": 2,
        "ok": 3,
        "event": "job_dropped",
        "skip_reason": "why",
    }
    assert skip_log_extra(None, {"event": "e"}) == {"event": "e"}


async def test_record_destination_skip_record_extra_is_logged(captured):
    class _Route(RecordDestination):
        name = "skip_extra_route"

        def destination_for_record(self, record: Any, runtime_params: Any) -> Any:
            raise SkipRecord("no region", extra={"order_id": record["id"]})

    routed = await _Route().route([{"id": 7}], {})
    assert routed.skip_reasons == {"no region": 1}
    (line,) = captured.events("record_skipped")
    assert line.order_id == 7
    assert line.transformation == "skip_extra_route"
