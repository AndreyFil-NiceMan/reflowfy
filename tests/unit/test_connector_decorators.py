"""@source / @destination reusable-factory registries, and MockSource."""

from typing import Any, Dict, Iterator

import pytest

from reflowfy import destination, destination_registry, source, source_registry
from reflowfy.destinations.console import ConsoleDestination, console_destination
from reflowfy.destinations.decorators import DestinationRegistry
from reflowfy.sources.decorators import SourceRegistry
from reflowfy.sources.mock import MockSource, generate_sample_data, mock_source


@pytest.fixture
def isolated_registries() -> Iterator[None]:
    """Both registries are class-level dicts shared by the whole process."""
    saved_src = dict(SourceRegistry._sources)
    saved_dst = dict(DestinationRegistry._destinations)
    yield
    SourceRegistry._sources.clear()
    SourceRegistry._sources.update(saved_src)
    DestinationRegistry._destinations.clear()
    DestinationRegistry._destinations.update(saved_dst)


# -- @source -------------------------------------------------------------------------


def test_source_decorator_registers_and_returns_the_function_unchanged(isolated_registries):
    def users(**overrides: Any) -> MockSource:
        return mock_source([{"id": 1}], batch_size=overrides.get("batch_size", 10))

    decorated = source("unit_users")(users)

    assert decorated is users
    assert source_registry.get("unit_users") is users
    assert "unit_users" in source_registry.list_all()
    built = source_registry.get("unit_users")(batch_size=3)  # type: ignore[misc]
    assert isinstance(built, MockSource) and built.config["batch_size"] == 3


def test_source_unknown_name_is_none(isolated_registries):
    assert source_registry.get("unit_never_registered") is None


def test_source_duplicate_name_last_definition_wins(isolated_registries):
    @source("unit_dup")
    def first() -> str:
        return "first"

    @source("unit_dup")
    def second() -> str:
        return "second"

    assert source_registry.get("unit_dup") is second
    assert source_registry.list_all().count("unit_dup") == 1


def test_source_wrong_signature_is_only_caught_when_called(isolated_registries):
    @source("unit_needs_index")
    def needs_index(index: str) -> str:
        return index

    factory = source_registry.get("unit_needs_index")
    assert factory is needs_index  # registration does not validate the signature
    with pytest.raises(TypeError):
        factory(wrong="x")  # type: ignore[misc]


def test_source_clear(isolated_registries):
    source("unit_a")(lambda: None)
    source_registry.clear()
    assert source_registry.list_all() == []


# -- @destination ------------------------------------------------------------------


def test_destination_decorator_registers_and_returns_the_function(isolated_registries):
    @destination("unit_console")
    def quiet(**overrides: Any) -> ConsoleDestination:
        return console_destination(max_records_display=overrides.get("n", 1))

    factory = destination_registry.get("unit_console")
    assert factory is quiet
    assert "unit_console" in destination_registry.list_all()
    assert factory(n=4).config["max_records_display"] == 4  # type: ignore[misc]


def test_destination_duplicate_unknown_wrong_signature_and_clear(isolated_registries):
    @destination("unit_d")
    def one() -> int:
        return 1

    @destination("unit_d")
    def two() -> int:
        return 2

    assert destination_registry.get("unit_d") is two
    assert destination_registry.get("unit_missing") is None
    with pytest.raises(TypeError):
        destination_registry.get("unit_d")("unexpected")  # type: ignore[misc]
    destination_registry.clear()
    assert destination_registry.list_all() == []


def test_registries_are_separate(isolated_registries):
    source("unit_shared_name")(lambda: "src")
    assert destination_registry.get("unit_shared_name") is None


# -- MockSource -----------------------------------------------------------------------


_DATA: list[Dict[str, Any]] = [{"id": i} for i in range(7)]


def test_mock_fetch_wraps_records_and_honours_limit():
    src = mock_source(_DATA)
    assert src.fetch({}) == [{"data": {"id": i}} for i in range(7)]
    assert src.fetch({}, limit=2) == [{"data": {"id": 0}}, {"data": {"id": 1}}]


def test_mock_split_slices_by_batch_size():
    subs = list(mock_source(_DATA, batch_size=3).split({}))
    assert [len(s.config["data"]) for s in subs] == [3, 3, 1]
    assert [r["data"]["id"] for s in subs for r in s.fetch({})] == list(range(7))
    assert all(s.config["batch_size"] == 3 for s in subs)


def test_mock_split_jobs_uses_configured_batch_size():
    jobs = list(MockSource(_DATA, batch_size=4).split_jobs({}, batch_size=1000))
    assert [j.metadata for j in jobs] == [
        {"batch_num": 0, "count": 4},
        {"batch_num": 1, "count": 3},
    ]


def test_mock_empty_data_plans_nothing_and_is_healthy():
    src = mock_source([])
    assert list(src.split({})) == [] and list(src.split_jobs({})) == []
    assert src.health_check() is True


def test_generate_sample_data_shape():
    rows = generate_sample_data(5)
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5]
    assert rows[2]["email"] == "user3@example.com"
    assert all(18 <= r["age"] <= 70 and isinstance(r["active"], bool) for r in rows)
    assert set(rows[0]) == {
        "id",
        "first_name",
        "last_name",
        "email",
        "age",
        "city",
        "salary",
        "active",
    }
