"""Base transformation class with automatic registration."""

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class TransformationMeta(ABCMeta):
    """
    Metaclass for automatic transformation registration.

    When a class inherits from BaseTransformation and defines a 'name' attribute,
    it is automatically registered in the transformation registry.

    Inherits from ABCMeta to be compatible with ABC.
    """

    def __new__(mcs, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)

        # Only register concrete transformations (not the base class)
        if name != "BaseTransformation" and bases:
            # Check if this is a concrete transformation with a name
            if "name" in namespace and namespace["name"]:
                # Import here to avoid circular dependency
                from reflowfy.transformations.registry import transformation_registry

                transformation_registry.register(cls)

        return cls


class BaseTransformation(metaclass=TransformationMeta):
    """
    Base class for all transformations.

    Users create custom transformations by inheriting from this class:

    Example:
        >>> class XmlToJson(BaseTransformation):
        ...     name = "xml_to_json"
        ...
        ...     def apply(self, records, runtime_params):
        ...         return [self.parse_xml(r) for r in records]
        ...
        ...     def parse_xml(self, record):
        ...         # Custom XML parsing logic
        ...         return {"parsed": record}

    The transformation is automatically registered and can be used in pipelines.
    """

    # Concrete transformations MUST set this
    name: str = ""

    @abstractmethod
    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        """
        Apply transformation to a batch of records.

        Args:
            records: List of records to transform
            runtime_params: Flat dict of all pipeline params — both the user-supplied runtime
                parameters (e.g. env, multiplier) and execution-context keys (execution_id,
                batch_id, pipeline_name, created_at, current_ids for id-based pipelines).
                This is the same dict that define_source / define_destination receive.
                Mutations are visible to subsequent transformations and the destination.

        Returns:
            Transformed list of records

        Raises:
            TransformationError: If transformation fails
        """
        pass

    def validate_input(self, records: List[Any]) -> None:
        """
        Optional: Validate input records before transformation.

        Args:
            records: Records to validate

        Raises:
            ValueError: If validation fails
        """
        pass

    def validate_output(self, records: List[Any]) -> None:
        """
        Optional: Validate output records after transformation.

        Args:
            records: Records to validate

        Raises:
            ValueError: If validation fails
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"


class TransformationError(Exception):
    """Raised when a transformation fails."""

    def __init__(
        self, transformation_name: str, message: str, original_error: Optional[Exception] = None
    ):
        self.transformation_name = transformation_name
        self.original_error = original_error
        super().__init__(f"Transformation '{transformation_name}' failed: {message}")
