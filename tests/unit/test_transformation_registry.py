"""TransformationRegistry and the @transformation decorator that feeds it.

The registry is a process-wide singleton and every BaseTransformation subclass with
a ``name`` registers itself at class-definition time, so each test restores the
registry's contents afterwards.
"""

from typing import Any, Dict, Iterator, List

import pytest

from reflowfy.transformations.base import BaseTransformation
from reflowfy.transformations.decorators import transformation
from reflowfy.transformations.registry import TransformationRegistry, transformation_registry


@pytest.fixture(autouse=True)
def restore_registry() -> Iterator[None]:
    saved = dict(transformation_registry._transformations)
    yield
    with transformation_registry._registry_lock:
        transformation_registry._transformations.clear()
        transformation_registry._transformations.update(saved)


def test_registry_is_a_singleton() -> None:
    assert TransformationRegistry() is transformation_registry


def test_subclass_with_a_name_registers_itself() -> None:
    class Upper(BaseTransformation):
        name = "unit_upper"

        def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
            return [str(r).upper() for r in records]

    assert transformation_registry.get("unit_upper") is Upper
    assert transformation_registry.exists("unit_upper")
    assert "unit_upper" in transformation_registry.list_all()
    instance = transformation_registry.create_instance("unit_upper")
    assert isinstance(instance, Upper) and instance.apply(["a"], {}) == ["A"]
    assert transformation_registry.create_instance("unit_upper") is not instance  # fresh each time


def test_subclass_without_a_name_is_not_registered() -> None:
    before = transformation_registry.list_all()

    class Abstractish(BaseTransformation):
        def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
            return records

    assert transformation_registry.list_all() == before


def test_unknown_name() -> None:
    assert transformation_registry.get("unit_nope") is None
    assert not transformation_registry.exists("unit_nope")
    with pytest.raises(ValueError, match="Transformation 'unit_nope' not found. Available:"):
        transformation_registry.create_instance("unit_nope")


def test_re_registering_a_name_is_rejected_and_keeps_the_first() -> None:
    class First(BaseTransformation):
        name = "unit_dup"

        def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
            return records

    with pytest.raises(ValueError, match="'unit_dup' is already registered by First"):

        class Second(BaseTransformation):
            name = "unit_dup"

            def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
                return records

    assert transformation_registry.get("unit_dup") is First


def test_register_rejects_an_empty_name() -> None:
    class Nameless:
        name = ""

    with pytest.raises(ValueError, match="Nameless must define a non-empty 'name'"):
        transformation_registry.register(Nameless)


def test_clear_empties_the_registry() -> None:
    transformation("unit_x")(lambda records, params: records)
    transformation_registry.clear()
    assert transformation_registry.list_all() == []


# -- @transformation ---------------------------------------------------------------


def test_decorator_builds_a_registered_transformation_class() -> None:
    @transformation("unit_double")
    def double(records: List[int], runtime_params: Dict[str, Any]) -> List[int]:
        """Double every value by a factor."""
        return [r * runtime_params.get("factor", 2) for r in records]

    assert isinstance(double, type) and issubclass(double, BaseTransformation)
    assert double.name == "unit_double"
    assert double.__name__ == "double"
    assert double.__doc__ == "Double every value by a factor."
    assert double.__module__ == __name__
    assert transformation_registry.get("unit_double") is double
    assert double().apply([1, 2], {"factor": 3}) == [3, 6]
    assert repr(double()) == "double(name='unit_double')"


def test_decorator_default_doc_and_duplicate_name() -> None:
    @transformation("unit_nodoc")
    def nodoc(records: Any, runtime_params: Any) -> Any:
        return records

    assert nodoc.__doc__ == "Transformation: unit_nodoc"
    with pytest.raises(ValueError, match="already registered"):
        transformation("unit_nodoc")(lambda records, params: records)
