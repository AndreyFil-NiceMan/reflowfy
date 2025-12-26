"""Dynamic route generation for pipelines."""

from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from reflowfy.api.execution import execution_tracker
from inspect import Parameter, Signature


class PipelineRunResponse(BaseModel):
    """Response model for pipeline execution."""
    
    execution_id: str
    pipeline_name: str
    mode: str
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
    - POST /pipelines/{name}/run - Distributed execution
    - POST /pipelines/{name}/test - Local execution
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
    # We'll use **kwargs to capture dynamic query params
    
    # Build the function signature dynamically
    # Determine required runtime parameters from source
    runtime_params_list = pipeline.source.get_runtime_parameters()
    
    if runtime_params_list:
        params = [
            Parameter(name, Parameter.KEYWORD_ONLY, default=Query(..., description=f"Runtime parameter: {name}"))
            for name in runtime_params_list
        ]
    else:
        params = []
    
    # Always add rate_limit parameter
    rate_limit_param = Parameter(
        'rate_limit',
        Parameter.KEYWORD_ONLY,
        default=Query(None, description="Override rate limit (jobs per second)"),
        annotation=float,
    )
    
    # Distributed execution endpoint
    if runtime_params_list:
        # Create function with dynamic parameters
        async def run_pipeline(rate_limit: float = None, **kwargs):
            """
            Execute pipeline in distributed mode via Kafka.
            
            Jobs are dispatched to Kafka and processed asynchronously by workers.
            """
            runtime_params = kwargs
            rate_limit_override = {"jobs_per_second": rate_limit} if rate_limit else None
            
            print(f"\n{'=' * 60}")
            print(f"🚀 Running pipeline: {pipeline_name} (distributed)")
            print(f"{'=' * 60}")
            print(f"Runtime params: {runtime_params}")
            
            try:
                # Execute pipeline
                status = distributed_executor.execute(
                    pipeline=pipeline,
                    runtime_params=runtime_params,
                    rate_limit_override=rate_limit_override,
                )
                
                # Track execution
                execution_tracker.track(status)
                
                print(f"✓ Execution started: {status.execution_id}\n")
                
                return PipelineRunResponse(
                    execution_id=status.execution_id,
                    pipeline_name=pipeline_name,
                    mode="distributed",
                    status=status.to_dict(),
                )
            
            except Exception as e:
                print(f"❌ Execution failed: {e}\n")
                raise HTTPException(status_code=500, detail=str(e))
        
        # Set the signature to include rate_limit param and all dynamic parameters
        sig_params = [rate_limit_param] + params
        run_pipeline.__signature__ = Signature(parameters=sig_params)
    else:
        # No runtime parameters
        async def run_pipeline(rate_limit: float = Query(None, description="Override rate limit (jobs per second)")):
            """Execute pipeline in distributed mode via Kafka."""
            runtime_params = {}
            rate_limit_override = {"jobs_per_second": rate_limit} if rate_limit else None
            
            print(f"\n{'=' * 60}")
            print(f"🚀 Running pipeline: {pipeline_name} (distributed)")
            print(f"{'=' * 60}")
            
            try:
                status = distributed_executor.execute(
                    pipeline=pipeline,
                    runtime_params=runtime_params,
                    rate_limit_override=rate_limit_override,
                )
                
                execution_tracker.track(status)
                print(f"✓ Execution started: {status.execution_id}\n")
                
                return PipelineRunResponse(
                    execution_id=status.execution_id,
                    pipeline_name=pipeline_name,
                    mode="distributed",
                    status=status.to_dict(),
                )
            
            except Exception as e:
                print(f"❌ Execution failed: {e}\n")
                raise HTTPException(status_code=500, detail=str(e))
    
    # Register the route
    app.post(
        f"/pipelines/{pipeline_name}/run",
        response_model=PipelineRunResponse,
        tags=["pipelines"],
        summary=f"Run {pipeline_name} (distributed)",
    )(run_pipeline)
    
    # Local execution endpoint (for testing)
    if runtime_params_list:
        async def test_pipeline(**kwargs):
            """
            Execute pipeline in local mode for testing.
            
            Runs synchronously with limited data. No Kafka or workers.
            """
            runtime_params = kwargs
            
            print(f"\n{'=' * 60}")
            print(f"🧪 Testing pipeline: {pipeline_name} (local)")
            print(f"{'=' * 60}")
            print(f"Runtime params: {runtime_params}")
            
            try:
                # Execute pipeline
                status = local_executor.execute(
                    pipeline=pipeline,
                    runtime_params=runtime_params,
                )
                
                # Track execution
                execution_tracker.track(status)
                
                print(f"✓ Test complete: {status.execution_id}\n")
                
                return PipelineRunResponse(
                    execution_id=status.execution_id,
                    pipeline_name=pipeline_name,
                    mode="local",
                    status=status.to_dict(),
                )
            
            except Exception as e:
                print(f"❌ Test failed: {e}\n")
                raise HTTPException(status_code=500, detail=str(e))
        
        # Set signature
        sig_params = [Parameter('kwargs', Parameter.VAR_KEYWORD)] if not params else params
        test_pipeline.__signature__ = Signature(parameters=sig_params)
    else:
        async def test_pipeline():
            """Execute pipeline in local mode for testing."""
            runtime_params = {}
            
            print(f"\n{'=' * 60}")
            print(f"🧪 Testing pipeline: {pipeline_name} (local)")
            print(f"{'=' * 60}")
            
            try:
                status = local_executor.execute(
                    pipeline=pipeline,
                    runtime_params=runtime_params,
                )
                
                execution_tracker.track(status)
                print(f"✓ Test complete: {status.execution_id}\n")
                
                return PipelineRunResponse(
                    execution_id=status.execution_id,
                    pipeline_name=pipeline_name,
                    mode="local",
                    status=status.to_dict(),
                )
            
            except Exception as e:
                print(f"❌ Test failed: {e}\n")
                raise HTTPException(status_code=500, detail=str(e))
    
    # Register the route
    app.post(
        f"/pipelines/{pipeline_name}/test",
        response_model=PipelineRunResponse,
        tags=["pipelines"],
        summary=f"Test {pipeline_name} (local)",
    )(test_pipeline)
    
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
    
    print(f"  ✓ POST /pipelines/{pipeline_name}/run?{('&'.join([p + '=...' for p in runtime_params_list]) if runtime_params_list else '')}")
    print(f"  ✓ POST /pipelines/{pipeline_name}/test?{('&'.join([p + '=...' for p in runtime_params_list]) if runtime_params_list else '')}")
    print(f"  ✓ GET  /pipelines/{pipeline_name}/status")
