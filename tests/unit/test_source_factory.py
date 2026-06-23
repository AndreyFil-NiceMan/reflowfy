"""Round-trip (type, config) -> instance for every built-in source."""

from reflowfy.factories.source_factory import SourceFactory
from reflowfy.sources.static import StaticSource
from reflowfy.sources.mock import MockSource
from reflowfy.sources.api import IDBasedAPISource
from reflowfy.sources.sql import SqlSource


def test_serialize_returns_type_and_config():
    src = StaticSource([1, 2, 3])
    serialized = SourceFactory.serialize(src)
    assert serialized == {"type": "StaticSource", "config": {"records": [1, 2, 3]}}


def test_roundtrip_static_source():
    src = StaticSource([{"a": 1}])
    rebuilt = SourceFactory.create("StaticSource", src.config)
    assert isinstance(rebuilt, StaticSource)
    assert rebuilt.config == src.config


def test_roundtrip_mock_source():
    src = MockSource(data=[{"x": 1}], batch_size=5)
    rebuilt = SourceFactory.create("MockSource", src.config)
    assert isinstance(rebuilt, MockSource)
    assert rebuilt.config == src.config


def test_roundtrip_api_source():
    src = IDBasedAPISource(base_url="http://h", endpoint_template="/u/{id}", ids=[1, 2])
    rebuilt = SourceFactory.create("IDBasedAPISource", src.config)
    assert isinstance(rebuilt, IDBasedAPISource)
    assert rebuilt.config == src.config


def test_roundtrip_sql_source():
    src = SqlSource(connection_url="sqlite://", query="SELECT 1", id_column="id")
    rebuilt = SourceFactory.create("SqlSource", src.config)
    assert isinstance(rebuilt, SqlSource)
    assert rebuilt.config == src.config


def test_unknown_type_raises():
    import pytest
    with pytest.raises(ValueError, match="Unknown source type"):
        SourceFactory.create("NopeSource", {})
