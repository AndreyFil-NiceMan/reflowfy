"""Starting an execution, the same way from every entry point.

``POST /run``, ``run_pipeline`` (chaining from a hook), the cron scheduler and the DLQ
all start executions. Each used to check the pipeline and its params in its own way
(the scheduler not at all) and to carry its own fallback for marking a crashed run
``failed``; these two functions are now the only copies.
"""

import logging
from typing import Any, Callable, Dict

from sqlalchemy.orm import Session

from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.store.states import ExecutionState

logger = logging.getLogger(__name__)


class PipelineNotFoundError(ValueError):
    """No pipeline of that name is registered in this process (``/run`` answers 404)."""


class InvalidParametersError(ValueError):
    """The runtime params fail the pipeline's own validation (``/run`` answers 422)."""


def check_runnable(pipeline_name: str, runtime_params: Dict[str, Any]) -> Any:
    """The registered pipeline, once its params (defaults applied) validate.

    Called before an execution is created, so what can never run is refused to the
    caller instead of failing later in the background.
    """
    from reflowfy.core.registry import pipeline_registry

    pipeline = pipeline_registry.get(pipeline_name)
    if pipeline is None:
        raise PipelineNotFoundError(
            f"Pipeline '{pipeline_name}' not found in registry"
            f"{pipeline_registry.describe_missing(pipeline_name)}"
        )
    errors = pipeline.validate_parameters(pipeline.apply_defaults(dict(runtime_params)))
    if errors:
        raise InvalidParametersError(f"Invalid parameters: {'; '.join(errors)}")
    return pipeline


def drive_execution(
    db: Session,
    make_runner: Callable[[Session], Any],
    execution_id: str,
    pipeline_name: str,
    runtime_params: Dict[str, Any],
    *,
    log: logging.Logger = logger,
    **run_kwargs: Any,
) -> None:
    """Plan and drive an existing execution to its end in this thread, then close ``db``.

    ``make_runner(db)`` builds what runs it (a ``ReflowManager`` or ``PipelineRunner``);
    ``run_kwargs`` go to its ``run_pipeline_jobs``. Anything that goes wrong, building
    the runner included, is logged (``dispatch_failed``) and ends the execution
    ``failed`` with the error as its message. Never raises: it is the body of a
    background thread or task.
    """
    try:
        make_runner(db).run_pipeline_jobs(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            **run_kwargs,
        )
    except Exception as exc:
        log.exception(
            "Background job dispatch failed",
            extra={
                "event": "dispatch_failed",
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
            },
        )
        try:
            db.rollback()
            ExecutionManager(db).update_execution_state(
                execution_id, ExecutionState.FAILED, error_message=str(exc)
            )
        except Exception:
            log.exception(
                "Failed to mark execution as failed",
                extra={"event": "execution_mark_failed_error", "execution_id": execution_id},
            )
    finally:
        try:
            db.close()
        except Exception:
            # The session's connection is already gone; nothing is left to release.
            log.warning(
                "Closing the database session failed",
                exc_info=True,
                extra={"event": "db_close_error", "execution_id": execution_id},
            )
