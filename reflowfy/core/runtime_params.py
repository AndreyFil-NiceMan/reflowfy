"""Typed `runtime_params` for pipeline authors.

Every user-facing hook receives one flat dict merging the user's own pipeline
parameters with the framework's reserved execution-context keys. This module
gives that dict a name, so an IDE can complete its keys.

Declare your own parameters by extending :class:`RuntimeParams` and passing the
result as the pipeline's type argument::

    from typing import Literal
    from typing_extensions import Annotated, NotRequired, Required
    from reflowfy import AbstractPipeline, Param, RuntimeParams

    class MyParams(RuntimeParams, total=False):
        env: Required[Literal["dev", "prod"]]
        limit: Annotated[NotRequired[int], Param("max rows", default=100)]

    class MyPipeline(AbstractPipeline[MyParams]):
        name = "mine"

        def define_source(self, runtime_params: MyParams):
            return my_source(limit=runtime_params.get("limit", 100))

That one declaration is also the source of ``define_parameters()`` — the runtime
validation, defaults and OpenAPI schema are derived from it, so the params are
written down once instead of twice.

Passing no type argument (``class MyPipeline(AbstractPipeline)``) keeps the old
untyped behavior, which is why every existing pipeline still works.
"""

from dataclasses import dataclass
from typing import Any, List

from typing_extensions import TypedDict, TypeVar

# typing_extensions, not typing: on Python 3.10 `typing.TypedDict` computes
# __required_keys__ incorrectly for Required/NotRequired/Annotated, which the
# define_parameters derivation depends on.


class RuntimeParams(TypedDict, total=False):
    """The keys reflowfy itself puts into ``runtime_params``.

    Every key is optional on purpose. The execution context is built per job
    (``build_flat_runtime_params``), so ``define_source``/``define_jobs`` run
    *before* these keys exist — only ``define_transformations``,
    ``define_destination`` and transformations are guaranteed to see them.
    Read them with ``.get()`` rather than subscripting.

    User parameters live in the same flat namespace; subclass this TypedDict to
    declare yours. Keys your own code *writes* into ``runtime_params`` (source
    enrichments, values a transformation passes down the chain) need declaring
    too, otherwise a type checker will reject the assignment.
    """

    execution_id: str
    batch_id: str
    pipeline_name: str
    created_at: str  # ISO 8601 string, not a datetime
    batch_number: int
    total_batches: int
    retry_count: int
    is_retry: bool
    # IdBasedPipeline only: the IDs this job is about. (`ids` -- the caller's
    # full list -- is a user parameter, not an execution-context key, so it is
    # declared per pipeline rather than here.)
    current_ids: List[Any]
    current_id: Any
    # Set by the DLQ scheduler on a replayed run.
    _dlq_source: bool
    _dlq_job_ids: List[Any]


# Bound so a params type must extend RuntimeParams; defaulted to Any so that a
# bare `AbstractPipeline` subclass behaves exactly as it did before this module
# existed. Defaulting to RuntimeParams instead would make every pre-existing
# hook annotated `Dict[str, Any]` a Liskov violation.
P = TypeVar("P", bound=RuntimeParams, default=Any)


@dataclass
class Param:
    """Description and default for a declared parameter.

    A TypedDict key cannot carry a default, so attach one with ``Annotated``::

        limit: Annotated[NotRequired[int], Param("max rows", default=100)]

    Both fields feed the derived :class:`~reflowfy.PipelineParameter`, and so
    the API docs and CLI prompts.

    ``internal=True`` marks a key that is *not* a caller-supplied parameter —
    an enrichment key the pipeline's own code writes into ``runtime_params``
    (from ``define_source``, or from a transformation, for something downstream
    to read). It still types the dict, but it stays out of ``define_parameters()``
    and therefore out of the API schema, validation and CLI prompts::

        step1_count: Annotated[int, Param(internal=True)]
    """

    description: str = ""
    default: Any = None
    internal: bool = False
