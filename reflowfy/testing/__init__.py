"""Tools for testing your own pipelines.

Everything here runs a pipeline through the same per-job core the worker uses,
with no Docker, Kafka, Postgres or network:

    from reflowfy.testing import run_pipeline

    def test_orders_are_taxed():
        result = run_pipeline("orders", params={"region": "eu"},
                              records=[{"id": 1, "amount": 10.0}])
        assert result.ok
        assert result.sent == [{"data": {"id": 1, "amount": 10.0}, "tax": 2.0}]

- :func:`run_pipeline` — a whole pipeline on records you supply.
- :func:`apply_transformation` — one transformation on its own.
- ``mock_http`` / ``mock_source`` (pytest fixtures) or :func:`mock_network` — canned
  answers for what a step calls.
- :func:`run_fixture` — replay a run recorded with
  ``reflowfy test <pipeline> --save-fixture <file>``.

Install with ``pip install "reflowfy[test]"``. Guide: ``docs/TESTING_PIPELINES.md``.
"""

from reflowfy.testing.assertions import (
    assert_job_dropped,
    assert_ok,
    assert_sent,
    assert_skipped,
    assert_valid_records,
)
from reflowfy.testing.cassette import (
    FixtureError,
    HttpCall,
    MockHttp,
    MockSources,
    Session,
    SourceCall,
    UnmockedCallError,
    mock_network,
)
from reflowfy.testing.destinations import CaptureDestination, Sent
from reflowfy.testing.fixtures import (
    PipelineFixture,
    assert_matches_fixture,
    diff_fixture,
    load_fixture,
    record_fixture,
    run_fixture,
    update_fixture,
)
from reflowfy.testing.result import JobResult, PipelineResult
from reflowfy.testing.runner import make_params, run_pipeline
from reflowfy.testing.transforms import StepResult, apply_transformation, make_records, record

__all__ = [
    # Running
    "run_pipeline",
    "PipelineResult",
    "JobResult",
    "apply_transformation",
    "StepResult",
    # Building inputs
    "make_records",
    "record",
    "make_params",
    # Standing in for the outside world
    "CaptureDestination",
    "Sent",
    "mock_network",
    "Session",
    "MockHttp",
    "HttpCall",
    "MockSources",
    "SourceCall",
    "UnmockedCallError",
    # Assertions
    "assert_ok",
    "assert_sent",
    "assert_skipped",
    "assert_job_dropped",
    "assert_valid_records",
    # Fixtures recorded with `reflowfy test --save-fixture`
    "PipelineFixture",
    "load_fixture",
    "record_fixture",
    "run_fixture",
    "assert_matches_fixture",
    "diff_fixture",
    "update_fixture",
    "FixtureError",
]
