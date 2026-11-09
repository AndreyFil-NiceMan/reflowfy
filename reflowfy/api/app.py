"""FastAPI application factory."""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, cast

import httpx
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from reflowfy import __version__
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from reflowfy.core.api_auth import auth_headers, install_api_auth
from reflowfy.core.registry import pipeline_registry
from reflowfy.core.pipeline_discovery import discover_and_load_pipelines
from reflowfy.observability import (  # noqa: F401  (register families)
    metrics as _metrics,  # pyright: ignore[reportUnusedImport]
)
from reflowfy.observability.logging import setup_logging
from reflowfy.observability.tracing import init_tracing, instrument_fastapi
from reflowfy.api.routes import create_pipeline_routes, pipeline_folder, pipeline_route_base
from reflowfy.execution.distributed_executor import DistributedExecutor

# Named explicitly: the Dockerfiles start this with `python -m`, where __name__ is "__main__" and a
# logger named after it sits outside the "reflowfy" logger the JSON/Elasticsearch handlers hang off.
logger = logging.getLogger("reflowfy.api.app")


def create_app() -> FastAPI:
    """
    Create and configure FastAPI application.

    Returns:
        Configured FastAPI instance
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        """Initialize pipelines on startup."""
        setup_logging(service_name="api")
        logger.info(
            "Starting Reflowfy API (version %s)", __version__, extra={"event": "service_start"}
        )

        # Load pipelines using global discovery (module from PIPELINE_MODULE env)
        discover_and_load_pipelines()

        # Setup routes after pipelines are registered
        setup_pipeline_routes(app)

        yield

    app = FastAPI(
        title="Reflowfy API",
        description="Data movement and transformation framework",
        version=__version__,
        lifespan=lifespan,
    )

    # Optional bearer token (REFLOWFY_API_TOKEN); CORS stays outermost.
    install_api_auth(app, service="api")

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Observability: /metrics (direct route, no mount redirect) + tracing.
    @app.get("/metrics")
    def metrics_endpoint() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    init_tracing(service_name="api")
    instrument_fastapi(app)

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
                    "folder": pipeline_folder(p),
                    "run_path": f"{pipeline_route_base(p)}/run",
                    "transformations": p.get_transformation_names(),
                    "runtime_parameters": p.get_runtime_parameters(),
                    "rate_limit": p.rate_limit,
                }
                for p in pipelines
            ]
        }

    @app.get("/executions/{execution_id}/status")
    async def get_execution_status(execution_id: str) -> Any:
        """The execution's live state, read from the manager (which owns it).

        It used to come from an in-memory record of the 202 response that nothing
        updated: an execution read ``pending`` forever, and was unknown to any other
        API replica or after a restart. The response keeps that shape.
        """
        manager_url = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")
        try:
            async with httpx.AsyncClient(timeout=10.0, headers=auth_headers()) as client:
                response = await client.get(f"{manager_url}/executions/{execution_id}/stats")
        except httpx.HTTPError as e:
            logger.warning(
                "Could not reach the manager for execution status: %s",
                type(e).__name__,
                extra={"event": "status_manager_unreachable", "execution_id": execution_id},
            )
            return JSONResponse(
                status_code=502, content={"error": "Execution status is unavailable"}
            )
        if response.status_code == 404:
            return JSONResponse(
                status_code=404,
                content={"error": f"Execution '{execution_id}' not found"},
            )
        if response.status_code >= 400:
            return JSONResponse(
                status_code=502, content={"error": "Execution status is unavailable"}
            )
        stats: Dict[str, Any] = response.json()
        total = stats.get("total_jobs") or 0
        completed = stats.get("jobs_completed", 0)
        return {
            "execution_id": stats.get("execution_id", execution_id),
            "pipeline_name": stats.get("pipeline_name"),
            "state": stats.get("state"),
            "total_jobs": total,
            "completed_jobs": completed,
            "failed_jobs": stats.get("jobs_failed", 0),
            "retry_count": stats.get("jobs_retry_scheduled", 0),
            "success_rate": completed / total if total else 0.0,
            "error_message": stats.get("error_message"),
            "metadata": cast(Dict[str, Any], {}),
        }

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
        logger.warning("No pipelines registered", extra={"event": "no_pipelines"})
        return

    logger.info(
        "Setting up routes for %d pipeline(s)",
        len(pipelines),
        extra={"event": "routes_setup", "pipelines_registered": len(pipelines)},
    )

    # Get ReflowManager URL from environment
    reflow_manager_url = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")

    # Create executors - both go through ReflowManager for proper DB tracking
    # Local mode: ReflowManager uses LocalDispatcher (in-process execution)
    # Distributed mode: ReflowManager uses KafkaDispatcher (Kafka + workers)
    local_executor = DistributedExecutor(reflow_manager_url=reflow_manager_url, mode="local")
    distributed_executor = DistributedExecutor(
        reflow_manager_url=reflow_manager_url, mode="distributed"
    )

    # Create routes for each pipeline. Sorted by folder so Swagger lists the
    # top-level "pipelines" section first, then each subfolder's section.
    for pipeline in sorted(pipelines, key=lambda p: (pipeline_folder(p), p.name)):
        try:
            create_pipeline_routes(
                app=app,
                pipeline=pipeline,
                local_executor=local_executor,
                distributed_executor=distributed_executor,
            )
        except ValueError as exc:
            # One misconfigured pipeline (e.g. a parameter named like a reserved route
            # parameter) must not take every other pipeline's routes down with it.
            logger.error(
                "Pipeline '%s' has no API route: %s",
                pipeline.name,
                exc,
                extra={"event": "pipeline_route_failed"},
            )

    logger.info("Routes configured", extra={"event": "routes_ready"})


def main():
    """Application entry point."""
    import uvicorn

    setup_logging(service_name="api")

    # Create app
    app = create_app()

    # Note: User must import their pipeline definitions before starting the server
    # This triggers registration via metaclass
    logger.info("Starting Reflowfy API (version %s)", __version__, extra={"event": "service_start"})

    # Auto-discover and load pipelines
    discover_and_load_pipelines()

    # Setup routes after pipelines are registered
    setup_pipeline_routes(app)

    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))

    logger.info(
        "Server starting on http://%s:%d (docs at /docs)",
        host,
        port,
        extra={"event": "service_listen"},
    )

    # Start server
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
