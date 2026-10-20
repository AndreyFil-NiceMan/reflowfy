"""DestinationFactory: built-in type names, unknown types, custom types and the
config round-trip (``create(type, dest.config)`` rebuilding an equal destination)."""

from typing import Any, Dict, Iterator, List, Optional, Type

import pytest

from reflowfy.destinations.api import ApiDestination, api_destination
from reflowfy.destinations.base import BaseDestination
from reflowfy.destinations.console import ConsoleDestination, console_destination
from reflowfy.destinations.kafka import KafkaDestination, kafka_destination
from reflowfy.factories import DestinationFactory


class _DictDest(BaseDestination):
    """A destination whose constructor takes the config dict itself."""

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        return None

    async def health_check(self) -> bool:
        return True


@pytest.fixture
def clean_registry() -> Iterator[None]:
    saved = dict(DestinationFactory._registry)
    yield
    DestinationFactory._registry.clear()
    DestinationFactory._registry.update(saved)


@pytest.mark.parametrize(
    "name,cls",
    [("kafka", KafkaDestination), ("api", ApiDestination), ("console", ConsoleDestination)],
)
def test_builtin_types_are_registered(name: str, cls: Type[BaseDestination]) -> None:
    assert DestinationFactory.is_registered(name)
    assert name in DestinationFactory.list_types()
    assert DestinationFactory._registry[name] is cls


def test_unknown_type_names_the_available_ones() -> None:
    with pytest.raises(ValueError, match="Unknown destination type: 'nope'") as err:
        DestinationFactory.create("nope", {})
    assert "api" in str(err.value) and "console" in str(err.value)
    assert not DestinationFactory.is_registered("nope")


def test_unknown_type_with_an_empty_registry_says_none(clean_registry) -> None:
    DestinationFactory._registry.clear()
    with pytest.raises(ValueError, match="Available types: none"):
        DestinationFactory.create("api", {})


def test_custom_type_register_and_create(clean_registry) -> None:
    DestinationFactory.register("dict", _DictDest)
    dest = DestinationFactory.create("dict", {"x": 1})
    assert isinstance(dest, _DictDest) and dest.config == {"x": 1}
    assert DestinationFactory.list_types()[-1] == "dict"


def test_register_same_name_replaces(clean_registry) -> None:
    DestinationFactory.register("console", _DictDest)
    assert isinstance(DestinationFactory.create("console", {}), _DictDest)


_BUILTINS = [
    ("api", lambda: api_destination(url="https://sink.test/in", method="PUT", body={"a": 1})),
    ("console", lambda: console_destination(pretty_print=False, max_records_display=3)),
    ("kafka", lambda: kafka_destination(bootstrap_servers="k:9092", topic="out")),
]


@pytest.mark.parametrize("name,build", _BUILTINS, ids=[n for n, _ in _BUILTINS])
def test_builtin_round_trip(name: str, build: Any) -> None:
    original = build()
    rebuilt = DestinationFactory.create(name, dict(original.config))
    assert type(rebuilt) is type(original)
    assert rebuilt.config == original.config


class _OwnInitDest(BaseDestination):
    """A subclass that overrides __init__ but still takes the config dict."""

    def __init__(self, config: Dict[str, Any], retry_config: Any = None) -> None:
        super().__init__(config, retry_config)

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        return None

    async def health_check(self) -> bool:
        return True


def test_a_config_taking_init_override_gets_the_dict(clean_registry) -> None:
    DestinationFactory.register("own_init", _OwnInitDest)
    dest = DestinationFactory.create("own_init", {"url": "https://x"})
    assert isinstance(dest, _OwnInitDest) and dest.config == {"url": "https://x"}
