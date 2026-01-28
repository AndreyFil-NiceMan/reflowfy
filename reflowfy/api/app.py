"""FastAPI application factory."""

import os
import importlib
import pkgutil
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from reflowfy import __version__
from reflowfy.core.registry import pipeline_registry
from reflowfy.api.routes import create_pipeline_routes
from reflowfy.api.execution import execution_tracker
from reflowfy.execution.distributed_executor import get_executor


def discover_and_load_pipelines(module_name: str = "pipelines") -> int:
    """
    Auto-discover and import all pipeline modules from specified directory.
    
    Args:
        module_name: Name of the module/directory containing pipelines
        
    Returns:
        Number of pipeline files loaded
    """
    loaded_count = 0
    
    # Ensure current directory is in sys.path
    import sys
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    
    try:
        # Try to import the pipelines package
        pipelines_package = importlib.import_module(module_name)
        package_path = Path(pipelines_package.__file__).parent
        
        print(f"\n📂 Discovering pipelines in '{module_name}'...")
        
        # Import all Python files in the pipelines directory
        for _, module_name_inner, is_pkg in pkgutil.iter_modules([str(package_path)]):
            if not is_pkg:  # Only import Python files, not subdirectories
                try:
                    full_module = f"{module_name}.{module_name_inner}"
                    importlib.import_module(full_module)
                    print(f"  ✓ Loaded {module_name_inner}.py")
                    loaded_count += 1
                except Exception as e:
                    print(f"  ✗ Failed to load {module_name_inner}.py: {e}")
                    import traceback
                    traceback.print_exc()
        
        if loaded_count == 0:
            print(f"  ⚠️  No pipeline files found in '{module_name}'")
        else:
            print(f"  ✓ Loaded {loaded_count} pipeline file(s)")
            
    except ImportError as e:
        print(f"  ⚠️  Module '{module_name}' not found - no pipelines loaded: {e}")
        import traceback
        traceback.print_exc()
        print(f"  ℹ️  sys.path: {sys.path}")
        print(f"  ℹ️  CWD: {cwd}")
        try:
             print(f"  ℹ️  Contents of CWD: {os.listdir(cwd)}")
        except:
             pass
    
    return loaded_count


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
        import importlib
        import pkgutil
        from pathlib import Path

        print("=" * 60)
        print("🚀 Starting Reflowfy API (Startup)")
        print(f"📦 Version: {__version__}")
        print("=" * 60)
        
        # Load pipelines
        pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
        print(f"\n📂 Discovering pipelines in '{pipeline_module}'...")
        
        try:
            pipelines_package = importlib.import_module(pipeline_module)
            package_path = Path(pipelines_package.__file__).parent
            
            loaded_count = 0
            for _, module_name, is_pkg in pkgutil.iter_modules([str(package_path)]):
                if not is_pkg:
                    try:
                        full_module = f"{pipeline_module}.{module_name}"
                        importlib.import_module(full_module)
                        print(f"  ✓ Loaded {module_name}.py")
                        loaded_count += 1
                    except Exception as e:
                        print(f"  ✗ Failed to load {module_name}.py: {e}")
            
            if loaded_count == 0:
                print(f"  ⚠️  No pipeline files found in '{pipeline_module}'")
            else:
                print(f"  ✓ Loaded {loaded_count} pipeline file(s)")
                
        except ImportError as e:
            print(f"  ⚠️  Module '{pipeline_module}' not found - no pipelines loaded: {e}")
            # Try to print sys.path to help debug
            import sys
            print(f"  ℹ️  sys.path: {sys.path}")
        
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
    
    # Create executors
    local_executor = get_executor("local")
    distributed_executor = get_executor(
        "distributed",
        reflow_manager_url=reflow_manager_url,
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
