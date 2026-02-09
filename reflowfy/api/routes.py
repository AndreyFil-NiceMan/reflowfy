"""Dynamic route generation for pipelines."""

from typing import Dict, Any, Literal
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from reflowfy.api.execution import execution_tracker
from inspect import Parameter, Signature


class PipelineRunResponse(BaseModel):
    """Response model for pipeline execution."""
    
    execution_id: str
    pipeline_name: str
    mode: str
    rate_limit: float | None = None
    status: Dict[str, Any]


def create_pipeline_routes(
    app: FastAPI,
    pipeline: Any,
    local_executor: Any,
    distributed_executor: Any,
) -> None:
    """
    Create dynamic routes for a pipeline.
    
    Creates:
    - POST /pipelines/{name}/run - Unified execution (mode: local or distributed)
    - GET /pipelines/{name}/status - Pipeline info
    
    Runtime parameters are exposed as query parameters for easy form-based input
    in Swagger UI.
    
    Args:
        app: FastAPI application
        pipeline: Pipeline instance
        local_executor: LocalExecutor instance
        distributed_executor: DistributedExecutor instance
    """
    pipeline_name = pipeline.name
    runtime_params_list = pipeline.get_runtime_parameters()
    
    # Create query parameter definitions dynamically
    if runtime_params_list:
        runtime_params = [
            Parameter(name, Parameter.KEYWORD_ONLY, default=Query(..., description=f"Runtime parameter: {name}"))
            for name in runtime_params_list
        ]
    else:
        runtime_params = []
    
    # Mode parameter (local or distributed)
    mode_param = Parameter(
        'mode',
        Parameter.KEYWORD_ONLY,
        default=Query("distributed", description="Execution mode: 'local' (testing) or 'distributed' (via Kafka)"),
        annotation=str,
    )
    
    # Rate limit parameter
    rate_limit_param = Parameter(
        'rate_limit',
        Parameter.KEYWORD_ONLY,
        default=Query(None, description="Override rate limit (jobs per second)"),
        annotation=float,
    )
    
    # Unified execution endpoint
    if runtime_params_list:
        # Create function with dynamic parameters
        async def run_pipeline(mode: str = "distributed", rate_limit: float = None, **kwargs):
            """
            Execute pipeline with specified mode.
            
            - mode='distributed': Jobs dispatched to Kafka, processed by workers
            - mode='local': Runs synchronously for testing (limited data)
            """
            runtime_params_dict = kwargs
            
            # Select executor based on mode
            if mode == "local":
                executor = local_executor
                mode_label = "local"
            else:
                executor = distributed_executor
                mode_label = "distributed"
            
            rate_limit_override = {"jobs_per_second": rate_limit} if rate_limit else None
            
            print(f"\n{'=' * 60}")
            print(f"🚀 Running pipeline: {pipeline_name} (mode: {mode_label})")
            print(f"{'=' * 60}")
            print(f"Runtime params: {runtime_params_dict}")
            if rate_limit:
                print(f"Rate limit override: {rate_limit} jobs/sec")
            
            try:
                # Execute pipeline
                if mode == "local":
                    status = executor.execute(
                        pipeline=pipeline,
                        runtime_params=runtime_params_dict,
                    )
                else:
                    status = executor.execute(
                        pipeline=pipeline,
                        runtime_params=runtime_params_dict,
                        rate_limit_override=rate_limit_override,
                    )
                
                # Track execution
                execution_tracker.track(status)
                
                print(f"✓ Execution started: {status.execution_id}\n")
                
                return PipelineRunResponse(
                    execution_id=status.execution_id,
                    pipeline_name=pipeline_name,
                    mode=mode_label,
                    rate_limit=rate_limit,
                    status=status.to_dict(),
                )
            
            except Exception as e:
                print(f"❌ Execution failed: {e}\n")
                raise HTTPException(status_code=500, detail=str(e))
        
        # Set the signature to include all parameters
        sig_params = [mode_param, rate_limit_param] + runtime_params
        run_pipeline.__signature__ = Signature(parameters=sig_params)
    else:
        # No runtime parameters
        async def run_pipeline(
            mode: str = Query("distributed", description="Execution mode: 'local' or 'distributed'"),
            rate_limit: float = Query(None, description="Override rate limit (jobs per second)"),
        ):
            """
            Execute pipeline with specified mode.
            
            - mode='distributed': Jobs dispatched to Kafka, processed by workers
            - mode='local': Runs synchronously for testing (limited data)
            """
            runtime_params_dict = {}
            
            # Select executor based on mode
            if mode == "local":
                executor = local_executor
                mode_label = "local"
            else:
                executor = distributed_executor
                mode_label = "distributed"
            
            rate_limit_override = {"jobs_per_second": rate_limit} if rate_limit else None
            
            print(f"\n{'=' * 60}")
            print(f"🚀 Running pipeline: {pipeline_name} (mode: {mode_label})")
            print(f"{'=' * 60}")
            if rate_limit:
                print(f"Rate limit override: {rate_limit} jobs/sec")
            
            try:
                if mode == "local":
                    status = executor.execute(
                        pipeline=pipeline,
                        runtime_params=runtime_params_dict,
                    )
                else:
                    status = executor.execute(
                        pipeline=pipeline,
                        runtime_params=runtime_params_dict,
                        rate_limit_override=rate_limit_override,
                    )
                
                execution_tracker.track(status)
                print(f"✓ Execution started: {status.execution_id}\n")
                
                return PipelineRunResponse(
                    execution_id=status.execution_id,
                    pipeline_name=pipeline_name,
                    mode=mode_label,
                    rate_limit=rate_limit,
                    status=status.to_dict(),
                )
            
            except Exception as e:
                print(f"❌ Execution failed: {e}\n")
                raise HTTPException(status_code=500, detail=str(e))
    
    # Register the unified run route
    app.post(
        f"/pipelines/{pipeline_name}/run",
        response_model=PipelineRunResponse,
        tags=["pipelines"],
        summary=f"Run {pipeline_name}",
        description=f"Execute {pipeline_name} pipeline. Use mode='local' for testing, 'distributed' for production.",
    )(run_pipeline)
    
    # Pipeline info endpoint
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
    
    # Log created routes
    params_str = '&'.join([p + '=...' for p in runtime_params_list]) if runtime_params_list else ''
    mode_str = "mode=distributed"
    full_params = f"?{mode_str}&rate_limit=..." + (f"&{params_str}" if params_str else "")
    
    print(f"  ✓ POST /pipelines/{pipeline_name}/run{full_params}")
    print(f"  ✓ GET  /pipelines/{pipeline_name}/status")
