"""Abstract base class for configurable pipelines.

This module provides the AbstractPipeline base class that allows users to create
pipelines with dynamic source/destination selection and conditional transformations
based on runtime parameters.

Example:
    >>> class MyPipeline(AbstractPipeline):
    ...     name = "my_pipeline"
    ...     rate_limit = 20
    ...
    ...     def define_parameters(self):
    ...         return [
    ...             PipelineParameter(name="env", required=True, choices=["dev", "prod"]),
    ...         ]
    ...
    ...     def define_source(self, params):
    ...         if params.get("env") == "prod":
    ...             return elastic_source(...)
    ...         return mock_source(...)
    ...
    ...     def define_destination(self, records, params):
    ...         return console_destination()
    ...
    ...     def define_transformations(self, records, params):
    ...         return [MyTransformation()]
"""

import logging
import re
import typing
import warnings
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

import typing_extensions

from reflowfy.core.exceptions import pipeline_step
from reflowfy.core.query_loader import QueryLoaderMixin
from reflowfy.core.runtime_params import P, Param, RuntimeParams

if TYPE_CHECKING:
    from reflowfy.destinations.base import BaseDestination
    from reflowfy.sources.base import BaseSource
    from reflowfy.transformations.base import BaseTransformation

logger = logging.getLogger(__name__)


class PipelineMeta(ABCMeta):
    """
    Metaclass for automatic pipeline registration.

    When a class inherits from AbstractPipeline and defines a 'name' attribute,
    it is automatically instantiated and registered in the pipeline registry.

    It also records the pipeline's params TypedDict, taken from the generic
    argument (``AbstractPipeline[MyParams]``), so `define_parameters()` can be
    derived from the same declaration that types the hooks.

    Inherits from ABCMeta to be compatible with ABC.
    """

    def __new__(mcs, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)

        # Only register concrete pipelines (not the base class)
        if name != "AbstractPipeline" and bases:
            # Read the params TypedDict off the subscripted base, before the
            # registration below instantiates the class. Only a subscripted base
            # puts __orig_bases__ in the namespace; an unsubscripted one leaves
            # it absent, and _params_type stays inherited (None on the base).
            for orig_base in namespace.get("__orig_bases__", ()):
                params_type = next(
                    (a for a in get_args(orig_base) if hasattr(a, "__required_keys__")),
                    None,
                )
                if params_type is not None:
                    cls._params_type = params_type  # type: ignore[attr-defined]
                    break

            # Validate cron expression at class-definition time (before instantiation)
            schedule = namespace.get("schedule")
            if schedule is not None:
                try:
                    from croniter import croniter as _croniter

                    if not _croniter.is_valid(schedule):
                        raise ValueError(
                            f"Pipeline '{namespace.get('name', name)}' has invalid cron "
                            f"expression: '{schedule}'. "
                            f"(reflowfy uses 5-field cron: minute hour day month weekday)"
                        )
                except ImportError:
                    pass  # croniter not installed; validated at runtime by scheduler

            # Check if this is a concrete pipeline with a name
            if "name" in namespace and namespace["name"]:
                # Import here to avoid circular dependency
                from reflowfy.core.registry import pipeline_registry

                pipeline_name = namespace["name"]

                # getattr, not namespace: a subclass of a typed pipeline inherits
                # its parent's params type and is not deprecated.
                if getattr(cls, "_params_type", None) is None:
                    warnings.warn(
                        f"Pipeline '{pipeline_name}' does not declare a params type. "
                        f"Untyped runtime_params is deprecated; pass a RuntimeParams "
                        f"subclass so the hooks are typed and define_parameters() is "
                        f"derived:\n"
                        f"    class {name}(AbstractPipeline[MyParams]):\n"
                        f"Use AbstractPipeline[RuntimeParams] if the pipeline takes no "
                        f"parameters of its own.",
                        DeprecationWarning,
                        # 2 = the `class ...` statement in the user's module.
                        stacklevel=2,
                    )

                try:
                    instance = cls()
                    pipeline_registry.register(instance)
                except Exception as exc:
                    # Don't raise: one misconfigured pipeline must not take down a
                    # whole service (e.g. a worker without ES creds). Log loudly and
                    # remember why, so the later "not found in registry" says the cause.
                    logger.error(
                        "Pipeline '%s' failed to register and will not be available: %s",
                        pipeline_name,
                        exc,
                        exc_info=True,
                    )
                    pipeline_registry.record_failure(pipeline_name, exc)

        return cls


@dataclass
class PipelineParameter:
    """
    Describes a parameter that the pipeline accepts at runtime.

    Attributes:
        name: Parameter name (used in API requests)
        description: Human-readable description
        required: Whether this parameter is mandatory
        param_type: Expected Python type (str, int, bool, float, list, dict)
        default: Default value if not provided
        choices: Optional list of valid values

    Example:
        >>> PipelineParameter(name="count", param_type=int, required=True)
        >>> PipelineParameter(name="env", param_type=str, choices=["dev", "prod"])
        >>> PipelineParameter(name="enabled", param_type=bool, default=False)
    """

    name: str
    description: str = ""
    required: bool = False
    param_type: type = str  # Actual Python type: str, int, bool, float, list, dict
    default: Any = None
    choices: Optional[List[Any]] = None

    # Type name mapping for serialization
    _TYPE_NAMES = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        list: "array",
        dict: "object",
    }

    def validate(self, value: Any) -> Optional[str]:
        """
        Validate a value against this parameter's type and constraints.

        Args:
            value: The value to validate

        Returns:
            Error message string if invalid, None if valid
        """
        if value is None:
            if self.required:
                return f"Missing required parameter: {self.name}"
            return None

        # Type validation
        if not self._check_type(value):
            type_name = self._TYPE_NAMES.get(self.param_type, self.param_type.__name__)
            return f"Parameter '{self.name}' must be {type_name}, got {type(value).__name__}"

        # Choices validation
        if self.choices and value not in self.choices:
            return f"Invalid value for {self.name}: '{value}'. Must be one of: {self.choices}"

        return None

    def _check_type(self, value: Any) -> bool:
        """Check if value matches the expected type."""
        # Special handling for bool (since bool is subclass of int in Python)
        if self.param_type is bool:
            return isinstance(value, bool)

        # Special handling for int (don't accept bool as int)
        if self.param_type is int:
            return isinstance(value, int) and not isinstance(value, bool)

        # Special handling for float (accept int as float)
        if self.param_type is float:
            return isinstance(value, (int, float)) and not isinstance(value, bool)

        # Standard type check
        return isinstance(value, self.param_type)

    def coerce(self, value: Any) -> Any:
        """
        Attempt to coerce a value to the expected type.

        Useful for converting string inputs from APIs to proper types.

        Args:
            value: The value to coerce

        Returns:
            Coerced value, or original if coercion fails
        """
        if value is None:
            return self.default

        # Already correct type
        if self._check_type(value):
            return value

        try:
            if self.param_type is bool:
                # Handle string booleans
                if isinstance(value, str):
                    if value.lower() in ("true", "1", "yes", "on"):
                        return True
                    if value.lower() in ("false", "0", "no", "off"):
                        return False
                return bool(value)

            if self.param_type is int:
                return int(value)

            if self.param_type is float:
                return float(value)

            if self.param_type is str:
                return str(value)

            if self.param_type is list and isinstance(value, str):
                import json

                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    # Fallback: treat as comma-separated values
                    return [v.strip() for v in value.split(",") if v.strip()]

            if self.param_type is dict and isinstance(value, str):
                import json

                return json.loads(value)

        except (ValueError, TypeError):
            pass

        return value  # Return original if coercion fails

    def to_dict(self) -> Dict[str, Any]:
        """Serialize parameter info for API documentation."""
        type_name = self._TYPE_NAMES.get(self.param_type, self.param_type.__name__)
        result: Dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "type": type_name,
        }
        if self.default is not None:
            result["default"] = self.default
        if self.choices:
            result["choices"] = self.choices
        return result


# Required[...]/NotRequired[...] say nothing __required_keys__ doesn't already
# say, so they are stripped and ignored. Both spellings, since a TypedDict may
# use either module's and typing's only exist on 3.11+.
_REQUIREDNESS_MARKERS = frozenset(
    marker
    for marker in (
        typing_extensions.Required,
        typing_extensions.NotRequired,
        getattr(typing, "Required", None),
        getattr(typing, "NotRequired", None),
    )
    if marker is not None
)


def params_from_typeddict(params_type: type) -> List[PipelineParameter]:
    """Derive the runtime parameter declarations from a params TypedDict.

    Turns a :class:`~reflowfy.RuntimeParams` subclass into the
    ``List[PipelineParameter]`` that validation, defaults, CLI prompting and the
    generated OpenAPI schema already consume — so a pipeline's parameters are
    declared once, as types, instead of twice.

    Keys inherited from ``RuntimeParams`` are skipped: those are the framework's
    own execution-context keys, not user parameters. So are keys marked
    ``Param(internal=True)`` — enrichment keys the pipeline writes at runtime.

    Args:
        params_type: A TypedDict subclassing ``RuntimeParams``.

    Returns:
        One PipelineParameter per user-declared key.

    Example:
        >>> class MyParams(RuntimeParams, total=False):
        ...     env: Required[Literal["dev", "prod"]]
        ...     limit: Annotated[NotRequired[int], Param("max rows", default=10)]
        >>> [(p.name, p.param_type, p.choices, p.default)
        ...  for p in params_from_typeddict(MyParams)]
        [('env', <class 'str'>, ['dev', 'prod'], None), ('limit', <class 'int'>, None, 10)]
    """
    hints = get_type_hints(params_type, include_extras=True)
    required: frozenset[str] = getattr(params_type, "__required_keys__", frozenset())
    reserved = set(RuntimeParams.__annotations__)

    params: List[PipelineParameter] = []
    for key, hint in hints.items():
        if key in reserved:
            continue

        # Peel the wrappers off, in any nesting order: Annotated carries the
        # Param() marker, Required/NotRequired only restate __required_keys__.
        # Each pass strips one layer, so this terminates.
        marker = Param()
        while True:
            origin = get_origin(hint)
            if origin is Annotated:
                args = get_args(hint)
                hint = args[0]
                marker = next((a for a in args[1:] if isinstance(a, Param)), marker)
            elif origin in _REQUIREDNESS_MARKERS:
                hint = get_args(hint)[0]
            else:
                break

        # Enrichment keys type the dict but are not caller-supplied parameters.
        if marker.internal:
            continue

        # Optional[X] / X | None means "not required", and the parameter is an X.
        if get_origin(hint) is Union:
            non_none = [a for a in get_args(hint) if a is not type(None)]
            if len(non_none) == 1:
                hint = non_none[0]

        choices: Optional[List[Any]] = None
        if get_origin(hint) is Literal:
            choices = list(get_args(hint))
            param_type = type(choices[0]) if choices else str
        else:
            # isinstance() and the `param_type in (list, dict)` body/query split
            # both need a bare runtime type, so list[int] must collapse to list.
            param_type = get_origin(hint) or hint

        params.append(
            PipelineParameter(
                name=key,
                description=marker.description,
                required=key in required,
                param_type=param_type if isinstance(param_type, type) else str,
                default=marker.default,
                choices=choices,
            )
        )
    return params


class AbstractPipeline(QueryLoaderMixin, Generic[P], metaclass=PipelineMeta):
    """
    Abstract base class for configurable pipelines.

    Extend this class to create pipelines with dynamic source/destination
    selection and conditional transformations based on runtime parameters.

    Subclasses MUST:
    - Set the `name` class attribute
    - Implement `define_source()` OR `define_jobs()` (see below)
    - Implement `define_destination()`
    - Implement `define_transformations()`

    Subclasses MAY:
    - Override `define_jobs()` instead of `define_source()` to own how the work
      is split into jobs (e.g. call an API and chunk the response yourself)
    - Override `define_parameters()` to expose required runtime parameters
    - Override `define_rate_limit()` for dynamic rate limiting
    - Override `should_apply_transformation()` for runtime condition checks
    - Declare their parameters as a type: subclass :class:`RuntimeParams` and
      pass it as the type argument (``AbstractPipeline[MyParams]``). Every hook
      then receives that type instead of an opaque dict, and
      `define_parameters()` is derived from it. Omit it and nothing changes.

    Attributes:
        name: Unique pipeline identifier (must be set by subclass)
        rate_limit: Optional rate limit in jobs per minute (e.g., 50)
        config: Additional pipeline-specific configuration
    """

    # Must be set by concrete subclass
    name: str = ""

    # Optional rate limit in jobs per minute (overridable per-request via define_rate_limit)
    rate_limit: Optional[float] = None

    # Additional configuration
    config: Dict[str, Any] = {}

    # Duplicate job control:
    #   True  = jobs may run multiple times (default, current behavior)
    #   False = each unique job (by content hash) runs at most once
    enable_duplicate_jobs: bool = True

    # The params TypedDict, set by PipelineMeta from AbstractPipeline[MyParams].
    # None means the pipeline didn't declare one.
    _params_type: Optional[type] = None

    # Optional cron schedule for automatic execution (e.g. "*/5 * * * *").
    # None means the pipeline is never auto-scheduled.
    schedule: Optional[str] = None

    def __init__(
        self,
        rate_limit: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None,
        enable_duplicate_jobs: Optional[bool] = None,
    ):
        """
        Initialize the abstract pipeline.

        Args:
            rate_limit: Rate limit in jobs per minute
            config: Additional configuration options
            enable_duplicate_jobs: True = jobs may run multiple times (default);
                False = each unique job (by content hash) runs at most once
        """
        if rate_limit is not None:
            self.rate_limit = rate_limit
        if config is not None:
            self.config = config
        if enable_duplicate_jobs is not None:
            self.enable_duplicate_jobs = enable_duplicate_jobs

        # Validate pipeline name
        if not self.name:
            raise ValueError(f"{self.__class__.__name__} must define a 'name' attribute")

        if not re.match(r"^[a-zA-Z0-9_-]+$", self.name):
            raise ValueError(
                f"Pipeline name '{self.name}' must contain only alphanumeric "
                "characters, underscores, or hyphens"
            )

        # Validate cron expression if schedule is set
        if self.schedule is not None:
            try:
                from croniter import croniter as _croniter

                if not _croniter.is_valid(self.schedule):
                    raise ValueError(
                        f"Pipeline '{self.name}' has invalid cron expression: '{self.schedule}'"
                    )
            except ImportError:
                pass  # croniter not installed; validated at runtime by scheduler

    def define_source(self, runtime_params: P) -> "BaseSource | List[Any]":
        """
        Define the source to use based on runtime parameters.

        The source's ``split()`` decides how the work is divided into jobs.
        To own that splitting yourself, override :meth:`define_jobs` instead —
        implement one or the other, not both.

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A configured BaseSource instance

        Example:
            >>> def define_source(self, params):
            ...     if params.get("env") == "production":
            ...         return elastic_source(url="http://prod:9200", ...)
            ...     return mock_source(data=[...])
        """
        raise NotImplementedError(
            f"Pipeline '{self.name}' must implement define_source() or define_jobs()"
        )

    def define_jobs(self, runtime_params: P) -> "BaseSource | Iterable[Any]":
        """
        Define how this pipeline's work is split into jobs.

        By default this delegates to :meth:`define_source` and lets the source's
        ``split()`` decide. Override it to own the splitting: return an iterable
        where each element is exactly one job — either a list of records (use
        :func:`reflowfy.chunk` to size them) or a ``BaseSource`` that splits
        itself. The two shapes can be mixed in one iterable.

        Runs on the manager, so keep it cheap. Records returned here travel
        inside the job message; for payloads near Kafka's 1MB limit, return
        sources that re-fetch worker-side instead.

        Jobs are consumed lazily, so ``yield`` instead of building a list when
        the plan is large: the manager writes each job to the database as it
        arrives and never holds the whole plan in memory.

        Every job runs with the execution's runtime_params. To give one job
        params of its own — which ID it covers, which shard, which tenant —
        wrap its records with :func:`reflowfy.job`; those keys are merged into
        that job's runtime_params and nothing else's.

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A BaseSource, or an iterable of jobs.

        Example:
            >>> def define_jobs(self, params):
            ...     rows = httpx.get(params["url"]).json()["items"]
            ...     return chunk(rows, size=params["job_size"])

        Example (streaming — millions of jobs, constant memory):
            >>> def define_jobs(self, params):
            ...     for row in stream_ids(params["query"]):
            ...         yield user_source(id=row.id)

        Example (per-job params):
            >>> def define_jobs(self, params):
            ...     for tenant in params["tenants"]:
            ...         yield job(rows_for(tenant), tenant=tenant)
        """
        return self.define_source(runtime_params)

    @abstractmethod
    def define_destination(self, records: List[Any], runtime_params: P) -> "BaseDestination":
        """
        Define the destination to use based on post-transformation records and runtime params.

        Args:
            records: Post-transformation records for the current job/batch
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A configured BaseDestination instance

        Example:
            >>> def define_destination(self, records, params):
            ...     if len(records) > 1000:
            ...         return kafka_destination(topic="bulk-output")
            ...     return console_destination()
        """
        pass

    @abstractmethod
    def define_transformations(
        self, records: List[Any], runtime_params: P
    ) -> Sequence["BaseTransformation"]:
        """
        Define list of transformations to apply based on records and runtime parameters.

        Args:
            records: Current records for this job/batch (before transformations)
            runtime_params: Parameters provided by the user at runtime

        Returns:
            List of BaseTransformation instances to apply in order

        Example:
            >>> def define_transformations(self, records, params):
            ...     transforms = [FilterActive()]
            ...     if len(records) > 1000:
            ...         transforms.append(ChunkLargePayloads())
            ...     if params.get("uppercase"):
            ...         transforms.append(UppercaseNames())
            ...     return transforms

        Note:
            This method is re-evaluated after each transformation is applied, so a
            transformation that adds a key to ``runtime_params`` can cause a later
            transformation to be appended to the returned list on the next pass.
            The list must be **append-only** with respect to growing
            ``runtime_params``: re-resolution may only grow the list. The ``records``
            argument is always the original pre-transformation records (it does not
            change between passes); only ``runtime_params`` changes. Already-applied
            transformations are never re-applied, and changes to earlier
            (already-applied) positions on a later pass are ignored.
        """
        pass

    def define_parameters(self) -> List[PipelineParameter]:
        """
        Define the parameters this pipeline accepts.

        Derived from the pipeline's params TypedDict when it declares one
        (``AbstractPipeline[MyParams]``) — that is the whole point of declaring
        it: the parameters are written down once, as types, and the validation,
        defaults and API schema follow from them.

        Override this method to declare them by hand instead; an override always
        wins over the derived list.

        Returns:
            List of PipelineParameter instances

        Example:
            >>> def define_parameters(self):
            ...     return [
            ...         PipelineParameter(
            ...             name="env",
            ...             description="Environment to use",
            ...             required=True,
            ...             choices=["dev", "staging", "production"]
            ...         ),
            ...         PipelineParameter(
            ...             name="uppercase",
            ...             description="Apply uppercase transformation",
            ...             param_type=bool,
            ...             default=False
            ...         ),
            ...     ]
        """
        if self._params_type is not None:
            return params_from_typeddict(self._params_type)
        return []

    def define_rate_limit(self, runtime_params: P) -> Optional[float]:
        """
        Define rate limit configuration based on runtime parameters.

        Override to provide dynamic rate limiting based on parameters.
        Default implementation returns the static rate_limit attribute.

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            Jobs per minute (float) or None

        Example:
            >>> def define_rate_limit(self, params):
            ...     if params.get("env") == "production":
            ...         return 600  # 10 jobs/s for prod
            ...     return 6000  # 100 jobs/s for dev
        """
        return self.rate_limit

    def should_apply_transformation(
        self, transformation: "BaseTransformation", runtime_params: P, records: List[Any]
    ) -> bool:
        """
        Determine if a transformation should be applied at runtime.

        Override to conditionally skip transformations based on runtime state.
        Default implementation returns True (apply all transformations).

        Args:
            transformation: The transformation instance
            runtime_params: Runtime parameters
            records: Current batch of records

        Returns:
            True if transformation should be applied, False to skip
        """
        return True

    # =========================================================================
    # Utility Methods (not meant to be overridden)
    # =========================================================================

    def get_all_parameters(self) -> List[PipelineParameter]:
        """
        Every parameter this pipeline accepts.

        Defaults to :meth:`define_parameters`. Subclasses override this to add
        parameters the framework injects rather than the user declaring them —
        see :class:`~reflowfy.core.id_based_pipeline.IdBasedPipeline`, which
        prepends the built-in ``ids``. Validation, defaults and API metadata all
        read this, so an injected parameter behaves like a declared one.
        """
        return self.define_parameters()

    def get_required_parameters(self) -> Set[str]:
        """Get names of required parameters."""
        return {p.name for p in self.get_all_parameters() if p.required}

    def validate_parameters(self, runtime_params: P) -> List[str]:
        """
        Validate runtime parameters against defined parameters.

        Args:
            runtime_params: Parameters to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        for param in self.get_all_parameters():
            value = runtime_params.get(param.name)
            error = param.validate(value)
            if error:
                errors.append(error)

        return errors

    def apply_defaults(self, runtime_params: P) -> P:
        """
        Apply default values to runtime parameters.

        Args:
            runtime_params: User-provided parameters

        Returns:
            Parameters with defaults applied
        """
        result: Dict[str, Any] = dict(runtime_params)

        for param in self.get_all_parameters():
            if param.name not in result and param.default is not None:
                result[param.name] = param.default

        return cast(P, result)

    def get_transformation_names(self) -> List[str]:
        """
        Return list of all possible transformation names.

        This is used for worker registration.
        """
        try:
            return [t.name for t in self.define_transformations([], cast(P, {}))]
        except Exception:
            return []

    def get_runtime_parameters(self) -> List[str]:
        """
        Return list of runtime parameter names.

        Used by sources that have Jinja templates (e.g., Elasticsearch queries).
        """
        return [p.name for p in self.get_all_parameters()]

    # =========================================================================
    # Execution Support - Properties for compatibility with execution engine
    # =========================================================================

    _resolved_params: Optional[Dict[str, Any]] = None
    _source: Any = None
    _destination: Any = None
    _transformations: List[Any] | None = None

    def resolve(self, runtime_params: P) -> "AbstractPipeline[P]":
        """
        Resolve the pipeline with specific runtime parameters.

        This method prepares the pipeline for execution by calling all
        define_* methods and caching the results. Should be called before
        passing the pipeline to an executor.

        Args:
            runtime_params: Runtime parameters for this execution

        Returns:
            self (for chaining)
        """
        # Apply defaults and validate
        params = self.apply_defaults(runtime_params)
        errors = self.validate_parameters(params)
        if errors:
            raise ValueError(f"Invalid parameters: {'; '.join(errors)}")

        self._resolved_params = cast(Dict[str, Any], params)
        with pipeline_step("define_jobs", self.name):
            self._source = self.define_jobs(params)
        # destination and transformations are resolved per-job/per-batch,
        # once records are available.
        self._destination = None
        self._transformations = None

        return self

    @property
    def source(self) -> Any:
        """Get the resolved source. Call resolve() first."""
        if self._source is None:
            raise RuntimeError(
                "Pipeline not resolved. Call pipeline.resolve(runtime_params) before execution."
            )
        return self._source

    @property
    def destination(self) -> Any:
        """Get the resolved destination. Call resolve() first."""
        if self._destination is None:
            raise RuntimeError(
                "Pipeline not resolved. Call pipeline.resolve(runtime_params) before execution."
            )
        return self._destination

    @property
    def transformations(self) -> List[Any]:
        """Get the resolved transformations. Call resolve() first."""
        if self._transformations is None:
            raise RuntimeError(
                "Pipeline not resolved. Call pipeline.resolve(runtime_params) before execution."
            )
        return self._transformations

    @property
    def is_scheduled(self) -> bool:
        """Return True if this pipeline has a cron schedule configured."""
        return self.schedule is not None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline metadata for API responses."""
        return {
            "name": self.name,
            "parameters": [p.to_dict() for p in self.get_all_parameters()],
            "rate_limit": self.rate_limit,
            "config": self.config,
            "transformations": self.get_transformation_names(),
            "enable_duplicate_jobs": self.enable_duplicate_jobs,
            "schedule": self.schedule,
            "is_scheduled": self.is_scheduled,
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"
