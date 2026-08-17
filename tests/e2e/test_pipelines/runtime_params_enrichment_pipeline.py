"""
runtime_params enrichment E2E test pipelines.

These pipelines verify the full runtime_params flow:
  1. define_source can inject keys into runtime_params before dispatch.
  2. A transformation can write new keys into runtime_params.
  3. The next transformation in the chain sees those keys.
  4. An IdBasedPipeline carries per-batch enrichments correctly.
"""

from typing import Any, List, Sequence

from typing_extensions import Annotated

from reflowfy import AbstractPipeline, IdBasedPipeline, Param, RuntimeParams
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from reflowfy.transformations.base import BaseTransformation
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import (
    params_step1_enrich,
    params_step2_verify,
    reveal_set_flag,
    reveal_stamp_second,
)


class EnrichParams(RuntimeParams, total=False):
    """Params for :class:`E2EParamsEnrichPipeline`.

    Note the enrichment keys are declared here alongside any user params: a key
    this pipeline's own code *writes* into runtime_params needs a declaration
    just as much as one it reads, and declaring it is what lets the reader see
    its type.
    """

    injected_by_source: Annotated[str, Param(internal=True)]  # written by define_source
    step1_count: Annotated[int, Param(internal=True)]  # written by params_step1_enrich
    step1_ran: Annotated[bool, Param(internal=True)]  # written by params_step1_enrich


class E2EParamsEnrichPipeline(AbstractPipeline[EnrichParams]):
    """
    Verifies end-to-end runtime_params enrichment for AbstractPipeline.

    define_source injects 'injected_by_source'.
    params_step1_enrich writes 'step1_count' and 'step1_ran'.
    params_step2_verify reads both and stamps them onto every record.
    """

    name = "e2e_params_enrich"
    rate_limit = 3000  # jobs per minute

    def define_source(self, runtime_params: EnrichParams) -> BaseSource:
        runtime_params["injected_by_source"] = "hello_from_source"
        return e2e_mock(count=10, batch_size=10)

    def define_destination(
        self, records: List[Any], runtime_params: EnrichParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})

    def define_transformations(
        self, records: List[Any], runtime_params: EnrichParams
    ) -> Sequence[BaseTransformation]:
        return [params_step1_enrich(), params_step2_verify()]


class RevealParams(RuntimeParams, total=False):
    """Params for :class:`E2ERevealMidChainPipeline`."""

    # written mid-chain by the reveal_set_flag transformation
    should_add_second: Annotated[bool, Param(internal=True)]


class E2ERevealMidChainPipeline(AbstractPipeline[RevealParams]):
    """
    Verifies dynamic (iterative) transformation resolution.

    reveal_set_flag sets runtime_params['should_add_second'] = True. Because that
    key is only present *after* the first transformation runs, define_transformations
    must be re-evaluated mid-chain for reveal_stamp_second to be appended and applied.
    Every record should end up stamped with second_applied=True.
    """

    name = "e2e_reveal_midchain"
    rate_limit = 3000  # jobs per minute

    def define_source(self, runtime_params: RevealParams) -> BaseSource:
        return e2e_mock(count=10, batch_size=10)

    def define_destination(
        self, records: List[Any], runtime_params: RevealParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})

    def define_transformations(
        self, records: List[Any], runtime_params: RevealParams
    ) -> Sequence[BaseTransformation]:
        trans = [reveal_set_flag()]
        if runtime_params.get("should_add_second"):
            trans.append(reveal_stamp_second())
        return trans


class IdBasedEnrichParams(RuntimeParams, total=False):
    """Params for :class:`E2EIdBasedParamsEnrichPipeline`.

    ``current_ids`` needs no declaration — IdBasedPipeline injects it, so it is
    already on :class:`RuntimeParams`.
    """

    injected_by_source: Annotated[str, Param(internal=True)]  # written per batch
    step1_count: Annotated[int, Param(internal=True)]  # written by params_step1_enrich


class E2EIdBasedParamsEnrichPipeline(IdBasedPipeline[IdBasedEnrichParams]):
    """
    Verifies per-batch runtime_params enrichment for IdBasedPipeline.

    define_source injects 'injected_by_source' with the current ID.
    params_step1_enrich writes 'step1_count'.
    params_step2_verify reads it and confirms the injected source key is present.
    """

    name = "e2e_id_based_params_enrich"
    rate_limit = 3000  # jobs per minute

    def define_source(self, runtime_params: IdBasedEnrichParams) -> BaseSource:
        current_ids = runtime_params.get("current_ids", [])
        runtime_params["injected_by_source"] = f"source_for_{current_ids[0]}"
        return e2e_mock(count=5, batch_size=5)

    def define_destination(
        self, records: List[Any], runtime_params: IdBasedEnrichParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})

    def define_transformations(
        self, records: List[Any], runtime_params: IdBasedEnrichParams
    ) -> Sequence[BaseTransformation]:
        return [params_step1_enrich(), params_step2_verify()]
