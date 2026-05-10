"""
runtime_params enrichment E2E test pipelines.

These pipelines verify the full runtime_params flow:
  1. define_source can inject keys into runtime_params before dispatch.
  2. A transformation can write new keys into runtime_params.
  3. The next transformation in the chain sees those keys.
  4. An IdBasedPipeline carries per-batch enrichments correctly.
"""

from reflowfy import AbstractPipeline, IdBasedPipeline
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.transformations import (
    params_step1_enrich,
    params_step2_verify,
)


class E2EParamsEnrichPipeline(AbstractPipeline):
    """
    Verifies end-to-end runtime_params enrichment for AbstractPipeline.

    define_source injects 'injected_by_source'.
    params_step1_enrich writes 'step1_count' and 'step1_ran'.
    params_step2_verify reads both and stamps them onto every record.
    """

    name = "e2e_params_enrich"
    rate_limit = 50

    def define_source(self, runtime_params):
        runtime_params["injected_by_source"] = "hello_from_source"
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, runtime_params):
        return e2e_http()

    def define_transformations(self, runtime_params):
        return [params_step1_enrich(), params_step2_verify()]


class E2EIdBasedParamsEnrichPipeline(IdBasedPipeline):
    """
    Verifies per-batch runtime_params enrichment for IdBasedPipeline.

    define_source injects 'injected_by_source' with the current ID.
    params_step1_enrich writes 'step1_count'.
    params_step2_verify reads it and confirms the injected source key is present.
    """

    name = "e2e_id_based_params_enrich"
    rate_limit = 50

    def define_source(self, runtime_params, current_ids):
        runtime_params["injected_by_source"] = f"source_for_{current_ids[0]}"
        return e2e_mock(count=5, batch_size=5)

    def define_destination(self, runtime_params):
        return e2e_http()

    def define_transformations(self, runtime_params, current_ids):
        return [params_step1_enrich(), params_step2_verify()]
