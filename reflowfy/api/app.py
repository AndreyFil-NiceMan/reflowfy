"""FastAPI application factory."""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from reflowfy import __version__
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.pipeline_discovery import discover_and_load_pipelines
from reflowfy.api.routes import create_pipeline_routes
from reflowfy.api.execution import execution_tracker
from reflowfy.execution.distributed_executor import get_executor


def create_app() -> FastAPI:
    """
    Create and configure FastAPI application.

    Returns:
        Configured FastAPI instance
    """
    app = FastAPI(
        title="Reflowfy API",
        description="Data movement and transformation framework",
        version=__version__,
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Health check endpoint
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "service": "reflowfy-api", "version": __version__}

    # List all pipelines
    @app.get("/pipelines")
    async def list_pipelines():
        pipelines = pipeline_registry.list_all()
        return {
            "pipelines": [
                {
                    "name": p.name,
                    "transformations": p.get_transformation_names(),
                    "runtime_parameters": p.get_runtime_parameters(),
                    "rate_limit": p.rate_limit,
                }
                for p in pipelines
            ]
        }

    # Get execution status
    @app.get("/executions/{execution_id}/status")
    async def get_execution_status(execution_id: str):
        status = execution_tracker.get_status(execution_id)

        if status is None:
            return JSONResponse(
                status_code=404,
                content={"error": f"Execution '{execution_id}' not found"},
            )

        return status.to_dict()

    # Startup event to load pipelines
    @app.on_event("startup")
    async def startup_event():
        """Initialize pipelines on startup."""
        print("=" * 60)
        print("🚀 Starting Reflowfy API (Startup)")
        print(f"📦 Version: {__version__}")
        print("=" * 60)

        # Load pipelines using global discovery
        pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
        print(f"\n📂 Loading pipelines from '{pipeline_module}'...")
        discover_and_load_pipelines(pipeline_module)

        # Setup routes after pipelines are registered
        setup_pipeline_routes(app)

    return app


def setup_pipeline_routes(app: FastAPI) -> None:
    """
    Dynamically create routes for all registered pipelines.

    Called at startup after pipelines are registered.

    Args:
        app: FastAPI application instance
    """
    pipelines = pipeline_registry.list_all()

    if not pipelines:
        print("⚠️  No pipelines registered")
        return

    print(f"\n🔧 Setting up routes for {len(pipelines)} pipeline(s)...")

    # Get ReflowManager URL from environment
    reflow_manager_url = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")

    # Create executors - both go through ReflowManager for proper DB tracking
    # Local mode: ReflowManager uses LocalDispatcher (in-process execution)
    # Distributed mode: ReflowManager uses KafkaDispatcher (Kafka + workers)
    local_executor = get_executor(
        "distributed",
        reflow_manager_url=reflow_manager_url,
        execution_mode="local",
    )
    distributed_executor = get_executor(
        "distributed",
        reflow_manager_url=reflow_manager_url,
        execution_mode="distributed",
    )

    # Create routes for each pipeline
    for pipeline in pipelines:
        create_pipeline_routes(
            app=app,
            pipeline=pipeline,
            local_executor=local_executor,
            distributed_executor=distributed_executor,
        )

    print("✓ Routes configured\n")


def main():
    """Application entry point."""
    import uvicorn

    # Create app
    app = create_app()

    # Note: User must import their pipeline definitions before starting the server
    # This triggers registration via metaclass
    print("=" * 60)
    print("🚀 Starting Reflowfy API")
    print(f"📦 Version: {__version__}")
    print("=" * 60)

    # Auto-discover and load pipelines
    pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
    discover_and_load_pipelines(pipeline_module)

    # Setup routes after pipelines are registered
    setup_pipeline_routes(app)

    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))

    print(f"🌐 Server starting on http://{host}:{port}")
    print("📖 API docs at http://localhost:8000/docs\n")

    # Start server
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
