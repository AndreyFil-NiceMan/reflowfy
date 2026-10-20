"""The component templates `reflowfy init` copies into a project must import and work,
plus two small BaseSource / IDBasedAPISource behaviours."""

import importlib.util
from pathlib import Path
from typing import Any, Iterator

import pytest

from reflowfy import PipelineError
from reflowfy.cli.utils import get_package_path
from reflowfy.core.registry import pipeline_registry
from reflowfy.destinations.decorators import destination_registry
from reflowfy.destinations.kafka import KafkaDestination
from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.api import id_based_api_source
from reflowfy.sources.base import SourceError
from reflowfy.sources.decorators import source_registry
from reflowfy.sources.elastic import ElasticSource
from reflowfy.sources.static import StaticSource
from reflowfy.transformations.registry import transformation_registry

TEMPLATES = get_package_path() / "templates"


@pytest.fixture
def isolated_component_registries() -> Iterator[None]:
    saved_sources = dict(source_registry._sources)
    saved_destinations = dict(destination_registry._destinations)
    saved_tx = dict(transformation_registry._transformations)
    try:
        yield
    finally:
        source_registry._sources.clear()
        source_registry._sources.update(saved_sources)
        destination_registry._destinations.clear()
        destination_registry._destinations.update(saved_destinations)
        transformation_registry._transformations.clear()
        transformation_registry._transformations.update(saved_tx)


def _import(filename: str) -> Any:
    """Import the packaged template file itself (what `init` copies verbatim)."""
    path: Path = TEMPLATES / filename
    spec = importlib.util.spec_from_file_location(f"gaps_{path.stem}", str(path))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestSourceTemplate:
    def test_registers_a_reusable_elastic_source_with_overrides(
        self, isolated_component_registries: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ELASTICSEARCH_URL", "http://es.internal:9200")
        module = _import("source_template.py")

        assert source_registry.get("example_elastic") is module.example_elastic
        src = module.example_elastic(index="orders-*", size=50)
        assert isinstance(src, ElasticSource)
        assert src.config["url"] == "http://es.internal:9200"
        assert (src.config["index"], src.config["size"]) == ("orders-*", 50)
        assert src.config["base_query"] == {"query": {"match_all": {}}}


class TestDestinationTemplate:
    def test_registers_a_reusable_kafka_destination_with_overrides(
        self, isolated_component_registries: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        module = _import("destination_template.py")

        assert destination_registry.get("example_kafka") is module.example_kafka
        dest = module.example_kafka(topic="out-topic")
        assert isinstance(dest, KafkaDestination)
        assert dest.config["topic"] == "out-topic"
        assert dest.config["bootstrap_servers"] == "kafka:9092"


class TestTransformationTemplate:
    @pytest.fixture
    def transform(self, isolated_component_registries: None) -> Any:
        module = _import("transformation_template.py")
        assert transformation_registry.get("example_transform") is module.ExampleTransform
        return module.ExampleTransform()

    def test_tags_every_record_with_the_execution(self, transform: Any) -> None:
        records = transform.apply([{"id": 1}, {"id": 2}], {"execution_id": "e-1"})
        assert [(r["_processed_by"], r["_execution_id"]) for r in records] == [
            ("reflowfy", "e-1"),
            ("reflowfy", "e-1"),
        ]

    def test_strict_mode_fails_an_empty_batch(self, transform: Any) -> None:
        with pytest.raises(PipelineError, match="strict mode"):
            transform.apply([], {"strict": True})
        assert transform.apply([], {}) == []


class TestPipelineTemplate:
    @pytest.fixture
    def module(self, isolated_component_registries: None) -> Iterator[Any]:
        saved = dict(pipeline_registry._pipelines)
        saved_failures = dict(pipeline_registry._failures)
        pipeline_registry._pipelines.clear()
        transformation_registry._transformations.clear()  # the template's names are global
        try:
            yield _import("pipeline_template.py")
        finally:
            pipeline_registry._pipelines.clear()
            pipeline_registry._pipelines.update(saved)
            pipeline_registry._failures.clear()
            pipeline_registry._failures.update(saved_failures)

    def test_uppercase_leaves_records_without_names_untouched(self, module: Any) -> None:
        records = [{"data": {"id": 1}}, {"data": {"first_name": "ada", "id": 2}}]

        out = module.UppercaseNames().apply(records, {})

        assert out == [{"data": {"id": 1}}, {"data": {"first_name": "ADA", "id": 2}}]
        assert records[1]["data"]["first_name"] == "ada"  # input not mutated


# ----- sources -----------------------------------------------------------------------


def test_a_source_lists_the_parameters_its_templates_use() -> None:
    src = StaticSource([])
    src.config = {"index": "logs-{{ env }}", "query": {"gte": "{{ since }}"}}
    assert sorted(src.get_runtime_parameters()) == ["env", "since"]


def test_split_jobs_without_a_resolved_config_is_a_source_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    src = id_based_api_source(base_url="http://api", endpoint_template="/u/{id}", ids=[1])
    monkeypatch.setattr(src, "resolve_parameters", lambda params: None)

    with pytest.raises(SourceError, match="No valid configuration resolved"):
        next(src.split_jobs({}))


def test_source_factory_lists_the_builtin_types() -> None:
    types = SourceFactory.list_types()
    assert {"StaticSource", "MockSource", "ElasticSource", "IDBasedAPISource"} <= set(types)
    assert SourceFactory.is_registered("StaticSource")
    assert not SourceFactory.is_registered("NoSuchSource")
