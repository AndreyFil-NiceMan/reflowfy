"""Dynamic route generation for pipelines."""

from typing import Any, Dict, List, Literal
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


def _create_id_based_route(
    app: FastAPI,
    pipeline: Any,
    local_executor: Any,
    distributed_executor: Any,
) -> None:
    """
    Register a run route for an IdBasedPipeline.

    `ids` and any extra user-defined parameters are accepted in the request body.
    """
    pipeline_name = pipeline.name
    extra_params = pipeline.define_parameters()  # user-defined params beyond ids

    # Build dynamic Pydantic body model: ids + extra typed fields
    fields: Dict[str, Any] = {"ids": (List[Any], ...)}
    for p in extra_params:
        default = ... if p.required else p.default
        fields[p.name] = (_param_annotation(p), default)

    BodyModel = pydantic.create_model(f"{pipeline_name}_Body", **fields)

    async def run_pipeline(
        body: BodyModel,
        mode: Literal["local", "distributed"] = Query(
            "distributed", description="Execution mode: 'local' or 'distributed'"
        ),
        rate_limit: float = Query(None, description="Override rate limit (jobs per second)"),
    ):
        """
        Execute IdBasedPipeline.

        Send `ids` (and any extra parameters) in the JSON request body.
        """
        return _run_body(
            pipeline=pipeline,
            local_executor=local_executor,
            distributed_executor=distributed_executor,
            pipeline_name=pipeline_name,
            runtime_params_dict=body.model_dump(),
            mode=mode,
            rate_limit=rate_limit,
        )

    app.post(
        f"/pipelines/{pipeline_name}/run",
        response_model=PipelineRunResponse,
        tags=["pipelines"],
        summary=f"Run {pipeline_name}",
        description=f"Execute {pipeline_name} (IdBasedPipeline). Send ids list in JSON body.",
    )(run_pipeline)

    print(f"  ✓ POST /pipelines/{pipeline_name}/run  [body: ids + params]")


def _create_standard_route(
    app: FastAPI,
    pipeline: Any,
    local_executor: Any,
    distributed_executor: Any,
) -> None:
    """
    Register a run route for a standard AbstractPipeline.

    Runtime parameters are exposed as typed query parameters.
    """
    pipeline_name = pipeline.name
    param_objects = pipeline.define_parameters()

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

    if param_objects:
        typed_params = [
            Parameter(
                p.name,
                Parameter.KEYWORD_ONLY,
                default=Query(
                    p.default if not p.required else ...,
                    description=p.description or f"Runtime parameter: {p.name}",
                ),
                annotation=_param_annotation(p),
            )
            for p in param_objects
        ]

        async def run_pipeline(mode: str = "distributed", rate_limit: float = None, **kwargs):
            """Execute pipeline with typed query parameters."""
            return _run_body(
                pipeline=pipeline,
                local_executor=local_executor,
                distributed_executor=distributed_executor,
                pipeline_name=pipeline_name,
                runtime_params_dict=kwargs,
                mode=mode,
                rate_limit=rate_limit,
            )

        run_pipeline.__signature__ = Signature(
            parameters=[mode_param, rate_limit_param] + typed_params
        )
    else:

        async def run_pipeline(
            mode: Literal["local", "distributed"] = Query(
                "distributed", description="Execution mode: 'local' or 'distributed'"
            ),
            rate_limit: float = Query(None, description="Override rate limit (jobs per second)"),
        ):
            """Execute pipeline (no runtime parameters)."""
            return _run_body(
                pipeline=pipeline,
                local_executor=local_executor,
                distributed_executor=distributed_executor,
                pipeline_name=pipeline_name,
                runtime_params_dict={},
                mode=mode,
                rate_limit=rate_limit,
            )

    app.post(
        f"/pipelines/{pipeline_name}/run",
        response_model=PipelineRunResponse,
        tags=["pipelines"],
        summary=f"Run {pipeline_name}",
        description=f"Execute {pipeline_name}. Use mode='local' for testing, 'distributed' for production.",
    )(run_pipeline)

    params_str = "&".join(p.name + "=..." for p in param_objects) if param_objects else ""
    full_params = "?mode=distributed&rate_limit=..." + (f"&{params_str}" if params_str else "")
    print(f"  ✓ POST /pipelines/{pipeline_name}/run{full_params}")


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

    if isinstance(pipeline, IdBasedPipeline):
        _create_id_based_route(app, pipeline, local_executor, distributed_executor)
    else:
        _create_standard_route(app, pipeline, local_executor, distributed_executor)

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
