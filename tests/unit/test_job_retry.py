"""JobRetryConfig: which errors are retried, the backoff, and its trip through the job message."""

import asyncio
from typing import Any

import httpx
import pytest

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.exceptions import PipelineError, RecordValidationError
from reflowfy.core.job_retry import (
    ERROR_HISTORY_MAX,
    JobRetryConfig,
    _load_type,  # pyright: ignore[reportPrivateUsage]
    describe_error,
    error_chain,
)
from reflowfy.core.record_templates import TemplateError
from reflowfy.core.runtime_params import RuntimeParams
from reflowfy.destinations.base import DestinationError, PermanentDestinationError
from reflowfy.sources.base import SourceError
from reflowfy.transformations.base import StepOutputError, TransformationError


class Outer:
    class InnerError(Exception):
        pass


class CustomError(Exception):
    pass


def _wrapped(inner: BaseException) -> PipelineError:
    try:
        raise inner
    except BaseException as exc:
        try:
            raise PipelineError("fetch failed", original_error=exc) from exc
        except PipelineError as wrapped:
            return wrapped
    raise AssertionError  # pragma: no cover


# ----- which errors are retried -------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        SourceError("es", "es down", None),
        DestinationError("kafka", "broker unavailable"),
        TimeoutError("slow"),
        asyncio.TimeoutError(),
        ConnectionResetError("reset"),
        httpx.ConnectError("refused"),
    ],
)
def test_transient_errors_are_retried_by_default(error: BaseException) -> None:
    assert JobRetryConfig().should_retry(error)


@pytest.mark.parametrize(
    "error",
    [
        RecordValidationError("bad", {0: "x"}, 1),
        StepOutputError("step", "broke its contract"),
        PermanentDestinationError("api", "HTTP 404"),
        KeyError("field"),
        TypeError("bug"),
        ValueError("bug"),
        TemplateError("missing placeholder"),
        RuntimeError("unknown"),  # matches neither list: not retried
    ],
)
def test_bad_data_and_bugs_are_never_retried(error: BaseException) -> None:
    assert not JobRetryConfig().should_retry(error)


def test_a_wrapped_error_is_matched_through_its_cause() -> None:
    config = JobRetryConfig()
    assert config.should_retry(_wrapped(SourceError("es", "es down", None)))
    source_bug = SourceError("es", "bad", None)
    source_bug.__cause__ = KeyError("k")
    assert not config.should_retry(source_bug)


def test_never_retry_wins_over_retry_on() -> None:
    config = JobRetryConfig(retry_on=(CustomError,), never_retry=(CustomError,))
    assert not config.should_retry(CustomError())


def test_a_single_class_is_accepted_for_retry_on() -> None:
    config = JobRetryConfig(retry_on=CustomError, never_retry=())  # type: ignore[arg-type]
    assert config.retry_on == (CustomError,)
    assert config.should_retry(CustomError())
    assert not config.should_retry(SourceError("es", "x", None))


def test_error_chain_follows_original_error_and_stops_on_cycles() -> None:
    inner = ValueError("inner")
    outer = TransformationError("step", "failed")
    outer.original_error = inner  # type: ignore[attr-defined]
    assert error_chain(outer) == [outer, inner]
    a, b = KeyError("a"), KeyError("b")
    a.__cause__, b.__cause__ = b, a
    assert error_chain(a) == [a, b]


def test_describe_error_keeps_the_type_and_a_short_message() -> None:
    entry = describe_error(SourceError("es", "x" * 500, None))
    assert entry["type"] == "SourceError"
    assert len(entry["message"]) == 200 and entry["message"].endswith("…")
    assert describe_error(KeyError("k")) == {"type": "KeyError", "message": "'k'"}
    assert ERROR_HISTORY_MAX == 5


# ----- backoff ---------------------------------------------------------------------------


def test_backoff_is_exponential_and_capped() -> None:
    config = JobRetryConfig(backoff_seconds=30, multiplier=2, max_backoff_seconds=100)
    assert [config.delay_for(n) for n in (1, 2, 3, 4)] == [30.0, 60.0, 100.0, 100.0]
    assert JobRetryConfig(backoff_seconds=0).delay_for(1) == 0.0


def test_a_server_that_asked_for_longer_is_given_it_up_to_the_cap() -> None:
    config = JobRetryConfig(backoff_seconds=10, max_backoff_seconds=120)
    asked = DestinationError("api", "HTTP 429", retry_after=50)
    assert config.delay_for(1, asked) == 50.0
    assert config.delay_for(1, DestinationError("api", "429", retry_after=999)) == 120.0
    assert config.delay_for(1, DestinationError("api", "429", retry_after=1)) == 10.0
    assert config.delay_for(1, _wrapped(asked)) == 50.0  # found through the wrapper
    assert config.delay_for(1, DestinationError("api", "x")) == 10.0  # retry_after None


# ----- validation -------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, error",
    [
        ({"max_retries": -1}, ValueError),
        ({"max_retries": 1.5}, TypeError),
        ({"max_retries": True}, TypeError),
        ({"backoff_seconds": -1}, ValueError),
        ({"max_backoff_seconds": -1}, ValueError),
        ({"multiplier": 0.5}, ValueError),
        ({"retry_on": ("SourceError",)}, TypeError),
        ({"never_retry": [KeyError]}, TypeError),
        ({"retry_on": (int,)}, TypeError),
    ],
)
def test_invalid_configs_are_rejected(kwargs: Any, error: type) -> None:
    with pytest.raises(error):
        JobRetryConfig(**kwargs)


# ----- the job message ---------------------------------------------------------------------


def test_round_trip_through_the_job_message() -> None:
    config = JobRetryConfig(
        max_retries=3,
        backoff_seconds=5,
        multiplier=3,
        max_backoff_seconds=60,
        retry_on=(SourceError, Outer.InnerError, CustomError),
        never_retry=(KeyError,),
    )
    data = config.to_dict()
    assert data["retry_on"][0] == "reflowfy.core.exceptions.SourceError"
    assert data["never_retry"] == ["builtins.KeyError"]
    payload = {"metadata": {"job_retry": data}}
    assert JobRetryConfig.from_payload(payload) == config


def test_messages_written_before_the_errors_moved_to_core_still_load() -> None:
    """Jobs already queued name the classes by their old modules, which re-export them."""
    data = JobRetryConfig().to_dict()
    data["retry_on"] = [
        "reflowfy.sources.base.SourceError",
        "reflowfy.destinations.base.DestinationError",
    ]
    data["never_retry"] = [
        "reflowfy.destinations.base.PermanentDestinationError",
        "reflowfy.transformations.base.StepOutputError",
    ]
    config = JobRetryConfig.from_payload({"metadata": {"job_retry": data}})
    assert config is not None
    assert config.retry_on == (SourceError, DestinationError)
    assert config.never_retry == (PermanentDestinationError, StepOutputError)


def test_an_exception_class_the_worker_cannot_import_is_left_out() -> None:
    data = JobRetryConfig().to_dict()
    data["retry_on"] = [
        "no_such_module.Error",
        "reflowfy.sources.base.NoSuchError",
        "reflowfy.sources.base.SourceError",
        "reflowfy.sources.base.logger",  # not an exception class
        "Plain",
    ]
    config = JobRetryConfig.from_dict(data)
    assert config.retry_on == (SourceError,)


def test_load_type_walks_nested_classes() -> None:
    assert _load_type(f"{__name__}.Outer.InnerError") is Outer.InnerError
    assert _load_type(f"{__name__}.Outer.Missing") is None


def test_no_config_in_the_message_means_no_retries() -> None:
    assert JobRetryConfig.from_payload({}) is None
    assert JobRetryConfig.from_payload({"metadata": None}) is None
    assert JobRetryConfig.from_payload({"metadata": {"job_retry": None}}) is None
    assert JobRetryConfig.from_payload({"metadata": ["x"]}) is None


def test_from_dict_defaults() -> None:
    config = JobRetryConfig.from_dict({})
    assert (config.max_retries, config.backoff_seconds, config.multiplier) == (2, 30.0, 2.0)
    assert config.retry_on == () and config.never_retry == ()


# ----- on the pipeline -----------------------------------------------------------------------


class _PlainPipeline(AbstractPipeline[RuntimeParams]):
    name = "job_retry_plain_pipeline"

    def define_source(self, runtime_params: Any) -> Any:
        return []

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        return None


class _RetryingPipeline(_PlainPipeline):
    name = "job_retry_attr_pipeline"
    job_retry = JobRetryConfig(max_retries=1)


def test_the_pipeline_attribute_is_opt_in_and_reported() -> None:
    assert _PlainPipeline().job_retry is None
    assert _PlainPipeline().to_dict()["job_retry"] is None
    retrying = _RetryingPipeline()
    assert retrying.to_dict()["job_retry"]["max_retries"] == 1


def test_job_retry_can_be_given_to_init() -> None:
    config = JobRetryConfig(max_retries=4)
    assert _PlainPipeline(job_retry=config).job_retry is config


def test_a_job_retry_that_is_not_a_config_is_rejected() -> None:
    with pytest.raises(TypeError, match="job_retry must be a JobRetryConfig"):
        _PlainPipeline(job_retry={"max_retries": 2})  # type: ignore[arg-type]


def test_default_hooks_do_nothing() -> None:
    pipeline = _PlainPipeline()
    assert pipeline.on_execution_complete(None, {}) is None  # type: ignore[arg-type]
    assert pipeline.on_execution_failed(None, {}) is None  # type: ignore[arg-type]
    assert pipeline.on_execution_cancelled(None, {}) is None  # type: ignore[arg-type]
