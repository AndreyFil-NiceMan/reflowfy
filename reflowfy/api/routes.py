"""Dynamic route generation for pipelines."""

from typing import Any, Dict, Literal, Optional
from inspect import Parameter, Signature

import pydantic
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from reflowfy.api.execution import execution_tracker
from reflowfy.execution.base import ExecutionState


class PipelineRunResponse(BaseModel):
    """Response model for pipeline execution."""

    execution_id: str
    pipeline_name: str
    mode: str
    rate_limit: float | None = None
    status: Dict[str, Any]


def _check_status(status: Any) -> None:
    """Raise HTTP 422 if the execution failed."""
    if status.state == ExecutionState.FAILED:
        raise HTTPException(
            status_code=422,
            detail={"error": status.error_message, "execution_id": status.execution_id},
        )


def _run_body(
    pipeline: Any,
    local_executor: Any,
    distributed_executor: Any,
    pipeline_name: str,
    runtime_params_dict: Dict[str, Any],
    mode: str,
    rate_limit: float | None,
) -> PipelineRunResponse:
    """Shared execution logic used by all route variants."""
    if mode == "local":
        executor = local_executor
        mode_label = "local"
    else:
        executor = distributed_executor
        mode_label = "distributed"

    rate_limit_override = rate_limit

    print(f"\n{'=' * 60}")
    print(f"Running pipeline: {pipeline_name} (mode: {mode_label})")
    print(f"{'=' * 60}")
    print(f"Runtime params: {runtime_params_dict}")
    if rate_limit:
        print(f"Rate limit override: {rate_limit} jobs/sec")

    try:
        status = executor.execute(
            pipeline=pipeline,
            runtime_params=runtime_params_dict,
            rate_limit_override=rate_limit_override,
        )
    except Exception as e:
        print(f"Execution raised: {e}\n")
        raise HTTPException(status_code=500, detail=str(e))

    execution_tracker.track(status)
    _check_status(status)

    print(f"Execution completed: {status.execution_id} state={status.state}\n")

    return PipelineRunResponse(
        execution_id=status.execution_id,
        pipeline_name=pipeline_name,
        mode=mode_label,
        rate_limit=rate_limit,
        status=status.to_dict(),
    )


def _param_annotation(p: Any):
    """Return the type annotation for a PipelineParameter.

    Parameters with `choices` become a Literal type so that
    Swagger UI renders them as a dropdown.
    """
    if p.choices:
        return Literal[tuple(p.choices)]
    return p.param_type


# list/dict parameters are accepted in the JSON request body; scalar parameters
# (str/int/float/bool, or anything with `choices`) are exposed as query params.
_BODY_PARAM_TYPES = (list, dict)


def _is_body_param(p: Any) -> bool:
    """Whether a PipelineParameter is carried in the request body vs the query string.

    List/dict values (e.g. an `ids` list) don't round-trip well as query params —
    they get clunky Swagger rendering and string-coerced items — so they go in the
    body. Anything with `choices` stays a query-string dropdown.
    """
    return p.choices is None and p.param_type in _BODY_PARAM_TYPES


def _create_run_route(
    app: FastAPI,
    pipeline: Any,
    local_executor: Any,
    distributed_executor: Any,
    params: Any,
) -> None:
    """
    Register the POST /pipelines/{name}/run route for a pipeline.

    Parameters are split by type: list/dict params go in the JSON request body,
    scalar params are exposed as typed query parameters. `mode` and `rate_limit`
    are always query parameters.
    """
    pipeline_name = pipeline.name
    body_params = [p for p in params if _is_body_param(p)]
    query_params = [p for p in params if not _is_body_param(p)]

    mode_param = Parameter(
        "mode",
        Parameter.KEYWORD_ONLY,
        default=Query("distributed", description="Execution mode: 'local' or 'distributed'"),
        annotation=Literal["local", "distributed"],
    )
    rate_limit_param = Parameter(
        "rate_limit",
        Parameter.KEYWORD_ONLY,
        default=Query(None, description="Override rate limit (jobs per second)"),
        annotation=float,
    )
    sig_params = [mode_param, rate_limit_param]

    if body_params:
        fields: Dict[str, Any] = {}
        for p in body_params:
            default = ... if p.required else p.default
            fields[p.name] = (
                _param_annotation(p),
                pydantic.Field(default, description=p.description or f"Runtime parameter: {p.name}"),
            )
        BodyModel = pydantic.create_model(f"{pipeline_name}_Body", **fields)
        sig_params.insert(
            0,
            Parameter("body", Parameter.KEYWORD_ONLY, default=..., annotation=BodyModel),
        )

    sig_params += [
        Parameter(
            p.name,
            Parameter.KEYWORD_ONLY,
            default=Query(
                p.default if not p.required else ...,
                description=p.description or f"Runtime parameter: {p.name}",
            ),
            annotation=_param_annotation(p),
        )
        for p in query_params
    ]

    async def run_pipeline(
        mode: str = "distributed",
        rate_limit: Optional[float] = None,
        body: Any = None,
        **kwargs: Any,
    ):
        """Execute pipeline. List/dict params come from the JSON body; scalars from the query string."""
        runtime_params = dict(kwargs)
        if body is not None:
            runtime_params.update(body.model_dump())
        return _run_body(
            pipeline=pipeline,
            local_executor=local_executor,
            distributed_executor=distributed_executor,
            pipeline_name=pipeline_name,
            runtime_params_dict=runtime_params,
            mode=mode,
            rate_limit=rate_limit,
        )

    setattr(run_pipeline, "__signature__", Signature(parameters=sig_params))

    app.post(
        f"/pipelines/{pipeline_name}/run",
        response_model=PipelineRunResponse,
        tags=["pipelines"],
        summary=f"Run {pipeline_name}",
        description=f"Execute {pipeline_name}. Use mode='local' for testing, 'distributed' for production.",
    )(run_pipeline)

    body_str = ("body: " + ", ".join(p.name for p in body_params)) if body_params else "no body"
    query_str = "&".join(p.name + "=..." for p in query_params)
    full_query = "?mode=distributed&rate_limit=..." + (f"&{query_str}" if query_str else "")
    print(f"  ✓ POST /pipelines/{pipeline_name}/run{full_query}  [{body_str}]")


def create_pipeline_routes(
    app: FastAPI,
    pipeline: Any,
    local_executor: Any,
    distributed_executor: Any,
) -> None:
    """
    Create dynamic routes for a pipeline.

    Creates:
    - POST /pipelines/{name}/run
    - GET  /pipelines/{name}/status
    """
    from reflowfy.core.id_based_pipeline import IdBasedPipeline

    # IdBasedPipeline exposes the auto-injected 'ids' param via get_all_parameters();
    # AbstractPipeline exposes only its declared params.
    if isinstance(pipeline, IdBasedPipeline):
        params = pipeline.get_all_parameters()
    else:
        params = pipeline.define_parameters()

    _create_run_route(app, pipeline, local_executor, distributed_executor, params)

    pipeline_name = pipeline.name

    @app.get(
        f"/pipelines/{pipeline_name}/status",
        tags=["pipelines"],
        summary=f"Get {pipeline_name} info",
    )
    async def get_pipeline_status():
        """Get pipeline configuration and metadata."""
        return {
            "name": pipeline.name,
            "transformations": pipeline.get_transformation_names(),
            "runtime_parameters": pipeline.get_runtime_parameters(),
            "rate_limit": pipeline.rate_limit,
            "config": pipeline.config,
        }

    print(f"  ✓ GET  /pipelines/{pipeline_name}/status")
