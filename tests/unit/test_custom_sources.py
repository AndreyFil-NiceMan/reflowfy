"""Custom sources run on workers: auto-registration, from_config and the planning check."""

import logging
from abc import abstractmethod
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

import pytest

from reflowfy.factories.source_factory import SourceFactory, SourceRoundTripError
from reflowfy.reflow_manager.pipeline_runner import build_job_payload
from reflowfy.sources.base import BaseSource


@pytest.fixture(autouse=True)
def capture_reflowfy_logs(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """setup_logging() in other tests stops "reflowfy" propagating to caplog's root handler."""
    logger = logging.getLogger("reflowfy")
    attach = not logger.propagate
    if attach:
        logger.addHandler(caplog.handler)
    yield
    if attach:
        logger.removeHandler(caplog.handler)


class PagesSource(BaseSource):
    """A user-written source: kwargs constructor, config == kwargs."""

    def __init__(self, pages: int, page_size: int = 2, start: int = 0):
        super().__init__({"pages": pages, "page_size": page_size, "start": start})

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["PagesSource"]:
        size = self.config["page_size"]
        for page in range(self.config["pages"]):
            yield PagesSource(pages=1, page_size=size, start=page * size)

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        start, size = self.config["start"], self.config["page_size"]
        return [{"n": n} for n in range(start, start + self.config["pages"] * size)]

    def health_check(self) -> bool:
        return True


class ConfigDictSource(BaseSource):
    """Keeps BaseSource's ``__init__(self, config)``: rebuilt with ``cls(config)``."""

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        return [self.config]

    def health_check(self) -> bool:
        return True


class OwnFromConfigSource(BaseSource):
    """Constructor takes something else; ``from_config`` says how to rebuild it."""

    def __init__(self, endpoint: str, *, retries: int):
        super().__init__({"url": endpoint, "retries": retries})

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "OwnFromConfigSource":
        return cls(config["url"], retries=config["retries"])

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        return []

    def health_check(self) -> bool:
        return True


class AbstractFamily(BaseSource):
    """Still abstract: not registered."""

    @abstractmethod
    def family(self) -> str: ...


def test_concrete_subclasses_register_themselves() -> None:
    assert SourceFactory._registry["PagesSource"] is PagesSource
    assert SourceFactory.is_registered("ConfigDictSource")
    assert not SourceFactory.is_registered("AbstractFamily")


def test_builtins_are_registered() -> None:
    for name in ("StaticSource", "MockSource", "SqlSource", "IDBasedAPISource"):
        assert SourceFactory.is_registered(name)


def test_plan_serialize_and_rebuild_on_the_worker() -> None:
    planned = list(PagesSource(pages=3, page_size=2).split({}))
    payloads = [
        build_job_payload("exec", f"job-{i}", "pages_pipeline", sub, metadata={})
        for i, sub in enumerate(planned)
    ]
    rebuilt = [SourceFactory.create(p["source"]["type"], p["source"]["config"]) for p in payloads]
    assert all(isinstance(s, PagesSource) for s in rebuilt)
    assert [r["n"] for s in rebuilt for r in s.fetch({})] == [0, 1, 2, 3, 4, 5]


def test_config_signature_source_is_rebuilt_with_the_dict() -> None:
    src = ConfigDictSource({"a": 1})
    descriptor = SourceFactory.serialize(src)
    rebuilt = SourceFactory.create(descriptor["type"], descriptor["config"])
    assert rebuilt.fetch({}) == [{"a": 1}]


def test_from_config_override_is_used() -> None:
    src = OwnFromConfigSource("http://h", retries=3)
    descriptor = SourceFactory.serialize(src)
    rebuilt = SourceFactory.create(descriptor["type"], descriptor["config"])
    assert isinstance(rebuilt, OwnFromConfigSource)
    assert rebuilt.config == {"url": "http://h", "retries": 3}


def test_constructor_that_does_not_match_config_fails_at_planning() -> None:
    class RenamedSource(BaseSource):
        def __init__(self, endpoint: str):
            super().__init__({"url": endpoint})

        def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
            return []

        def health_check(self) -> bool:
            return True

    with pytest.raises(SourceRoundTripError, match="from_config") as err:
        SourceFactory.serialize(RenamedSource("http://h"))
    assert "RenamedSource cannot be rebuilt on a worker" in str(err.value)
    assert "TypeError" in str(err.value)


def test_config_that_changes_on_rebuild_fails_at_planning() -> None:
    class LossySource(BaseSource):
        def __init__(self, n: int, cache: Optional[List[int]] = None):
            super().__init__({"n": n, "cache": None})
            if cache is not None:
                self.config["cache"] = cache
            self.config["n"] = n + 1  # not idempotent: n drifts on every rebuild

        def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
            return []

        def health_check(self) -> bool:
            return True

    with pytest.raises(SourceRoundTripError, match=r"does not round-trip.*keys n"):
        SourceFactory.serialize(LossySource(1))


def test_non_json_config_fails_at_planning() -> None:
    with pytest.raises(SourceRoundTripError, match="not JSON"):
        SourceFactory.serialize(ConfigDictSource({"when": datetime(2026, 1, 1)}))


def test_unregistered_registry_type_fails_at_planning() -> None:
    class AliasedSource(ConfigDictSource):
        @property
        def registry_type(self) -> str:
            return "NoSuchAlias"

    with pytest.raises(SourceRoundTripError, match="'NoSuchAlias' is not registered"):
        SourceFactory.serialize(AliasedSource({"a": 1}))


def test_registry_type_registered_to_another_class_fails_at_planning() -> None:
    class Impostor(ConfigDictSource):
        @property
        def registry_type(self) -> str:
            return "PagesSource"

    with pytest.raises(SourceRoundTripError, match="is registered to"):
        SourceFactory.serialize(Impostor({"a": 1}))


def test_round_trip_check_runs_once_per_config_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[str] = []
    real_create = SourceFactory.create.__func__  # type: ignore[attr-defined]

    def counting(cls: Any, type_name: str, config: Dict[str, Any]) -> BaseSource:
        calls.append(type_name)
        return real_create(cls, type_name, config)

    monkeypatch.setattr(SourceFactory, "create", classmethod(counting))
    monkeypatch.setattr(SourceFactory, "_checked", set())
    for start in range(5):
        SourceFactory.serialize(PagesSource(pages=1, start=start))
    assert calls == ["PagesSource"]


def test_same_name_from_another_module_warns(caplog: pytest.LogCaptureFixture) -> None:
    saved = SourceFactory._registry["PagesSource"]
    try:
        with caplog.at_level(logging.WARNING, logger="reflowfy.factories.source_factory"):

            class PagesSource(BaseSource):  # noqa: F811  # pyright: ignore[reportRedeclaration]
                def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Any:
                    return []

                def health_check(self) -> bool:
                    return True

        assert any(r.__dict__.get("event") == "source_type_replaced" for r in caplog.records)
    finally:
        SourceFactory.register("PagesSource", saved)


def test_reregistering_the_same_class_is_silent(caplog: pytest.LogCaptureFixture) -> None:
    # A module imported twice redefines the class at the same module + qualname: the
    # same source, not a clash.
    twin = type("PagesSource", (PagesSource,), {"__module__": PagesSource.__module__})
    try:
        with caplog.at_level(logging.WARNING, logger="reflowfy.factories.source_factory"):
            SourceFactory.register("PagesSource", PagesSource)
            SourceFactory.register("PagesSource", twin)
        assert not caplog.records
    finally:
        SourceFactory.register("PagesSource", PagesSource)


def test_unknown_type_points_at_discovery() -> None:
    with pytest.raises(ValueError, match="pipeline discovery imports"):
        SourceFactory.create("NoSuchSource", {})
