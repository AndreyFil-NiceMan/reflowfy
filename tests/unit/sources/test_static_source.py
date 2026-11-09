"""Unit tests for StaticSource (in-memory records-as-source)."""

from reflowfy.sources.base import BaseSource
from reflowfy.sources.static import StaticSource


class TestStaticSourceFetch:
    def test_fetch_returns_records(self):
        src = StaticSource([101, 102, 103])
        assert src.fetch({}) == [101, 102, 103]

    def test_fetch_respects_limit(self):
        src = StaticSource([1, 2, 3, 4, 5])
        assert src.fetch({}, limit=2) == [1, 2]

    def test_fetch_preserves_raw_values(self):
        src = StaticSource(["a", "b"])
        assert src.fetch({}) == ["a", "b"]


class TestStaticSourceMisc:
    def test_is_a_base_source(self):
        assert isinstance(StaticSource([]), BaseSource)

    def test_health_check_true(self):
        assert StaticSource([]).health_check() is True
