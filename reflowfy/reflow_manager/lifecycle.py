"""Lifecycle hooks, the ``/run`` callback and chaining (``run_pipeline``).

When an execution reaches a terminal state (``completed``, ``failed``, ``cancelled``),
:meth:`ExecutionManager.update_execution_state` tells its terminal listeners; the
manager service installs :func:`on_terminal` as one at startup (:func:`install`). Every
terminal transition goes through there: the drive loop's end, ``/cancel``, ``PATCH
/executions/{id}/state``, planning-crash recovery, a run that raised.

The work then runs in a thread, off the caller (an HTTP request, the drive loop):

1. the execution's token bucket is deleted (``rate_limit_state``);
2. the hooks are claimed — ``UPDATE executions SET hooks_fired_at = now WHERE
   hooks_fired_at IS NULL`` — so of any number of callers (two pods, a cancel racing
   the loop's end) exactly one goes on;
3. the pipeline's ``on_execution_complete`` / ``_failed`` / ``_cancelled`` runs in its
   own thread with ``HOOK_TIMEOUT_SECONDS`` (60) to finish;
4. the ``callback_url`` given to ``/run``, if any, is POSTed the summary.

A hook or callback that fails or times out is logged (``hook_failed`` /
``callback_failed``) and stored in ``executions.hook_error``; it never changes the
execution's state. ``POST /executions/{id}/retry-failed`` clears ``hooks_fired_at``, so
the retried run's end fires them again.
"""

import contextvars
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, cast
from urllib.parse import urlsplit

from sqlalchemy.orm import Session

from reflowfy.core.execution_summary import ExecutionSummary
from reflowfy.reflow_manager.execution import (
    ExecutionManager,
    add_terminal_listener,
    remove_terminal_listener,
)
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.store.models import Execution, Job
from reflowfy.reflow_manager.rate_limiter import RateLimiter, execution_bucket_key
from reflowfy.store.states import JobState
from reflowfy.reflow_manager.start import check_runnable, drive_execution

logger = logging.getLogger(__name__)

HOOK_NAMES: Dict[str, str] = {
    "completed": "on_execution_complete",
    "failed": "on_execution_failed",
    "cancelled": "on_execution_cancelled",
}
FIRST_ERRORS_MAX = 5
FIRST_ERROR_CHARS = 300
HOOK_ERROR_CHARS = 2000

# (bind, execution_id) of the hook running in this thread: run_pipeline() called from a
# hook uses the same database and records the execution as the child's parent.
_hook_context: contextvars.ContextVar[Optional[Tuple[Any, str]]] = contextvars.ContextVar(
    "reflowfy_hook_context", default=None
)

# Terminal work started by on_terminal and not finished yet (see wait_for_hooks).
_running: Set[threading.Thread] = set()
_running_lock = threading.Lock()


def hook_timeout_seconds() -> float:
    """How long a hook may run (env ``HOOK_TIMEOUT_SECONDS``, default 60)."""
    return float(os.getenv("HOOK_TIMEOUT_SECONDS", "60"))


def callback_timeout_seconds() -> float:
    """How long the ``callback_url`` POST may take (env ``CALLBACK_TIMEOUT_SECONDS``, 10)."""
    return float(os.getenv("CALLBACK_TIMEOUT_SECONDS", "10"))


def _short(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def build_summary(db: Session, execution: Execution) -> ExecutionSummary:
    """Summarize a finished execution from its job rows (streamed, a page at a time)."""
    execution_id = execution.execution_id
    counts = JobManager(db).get_job_counts(execution_id)
    totals = {
        "records_in": 0,
        "records_out": 0,
        "records_skipped": 0,
        "records_deduplicated": 0,
        "retries": 0,
        "jobs_skipped": 0,
    }
    rows = (
        db.query(Job.state, Job.stats, Job.processed_records)
        .filter(Job.execution_id == execution_id, Job.stats.isnot(None))
        .yield_per(1000)
    )
    for state, stats, processed in rows:
        if not isinstance(stats, dict):
            continue
        job_stats = cast(Dict[str, Any], stats)
        totals["records_in"] += int(job_stats.get("records_input") or 0)
        totals["records_skipped"] += int(job_stats.get("records_skipped") or 0)
        totals["records_deduplicated"] += int(job_stats.get("records_deduplicated") or 0)
        totals["retries"] += int(job_stats.get("retry_count") or 0)
        if state == "completed":
            totals["records_out"] += int(processed or 0)
            if job_stats.get("skip_reason"):
                totals["jobs_skipped"] += 1
    first_errors = [
        _short(message, FIRST_ERROR_CHARS)
        for (message,) in db.query(Job.error_message)
        .filter(
            Job.execution_id == execution_id,
            Job.state == JobState.FAILED,
            Job.error_message.isnot(None),
        )
        .order_by(Job.completed_at)
        .limit(FIRST_ERRORS_MAX)
        .all()
        if message
    ]
    finished = execution.completed_at or datetime.now(timezone.utc).replace(tzinfo=None)
    duration = round((finished - execution.created_at).total_seconds(), 3)
    return ExecutionSummary(
        execution_id=execution_id,
        pipeline_name=execution.pipeline_name,
        state=execution.state,
        jobs_total=counts["total"],
        jobs_completed=counts["completed"],
        jobs_failed=counts["failed"],
        jobs_deduplicated=counts["deduplicated"],
        jobs_cancelled=counts["cancelled"],
        jobs_skipped=totals["jobs_skipped"],
        records_in=totals["records_in"],
        records_out=totals["records_out"],
        records_skipped=totals["records_skipped"],
        records_deduplicated=totals["records_deduplicated"],
        retries=totals["retries"],
        duration_seconds=duration,
        created_at=_iso(execution.created_at),
        finished_at=_iso(execution.completed_at),
        error_message=execution.error_message,
        first_errors=first_errors,
        parent_execution_id=execution.parent_execution_id,
    )


def _overridden_hook(pipeline: Any, state: str) -> Optional[Callable[..., Any]]:
    """The pipeline's hook for ``state``, or None if it does not define one of its own."""
    from reflowfy.core.abstract_pipeline import AbstractPipeline

    name = HOOK_NAMES.get(state)
    if name is None or pipeline is None:
        return None
    pipeline_type: type = type(cast(object, pipeline))
    method = getattr(pipeline_type, name, None)
    if method is None or method is getattr(AbstractPipeline, name):
        return None
    hook: Callable[..., Any] = getattr(pipeline, name)
    return hook


def _run_hook(
    hook: Callable[..., Any],
    hook_name: str,
    summary: ExecutionSummary,
    params: Dict[str, Any],
    bind: Any,
) -> Optional[str]:
    """Run one hook in a thread, waiting at most HOOK_TIMEOUT_SECONDS. The error, or None.

    A hook that times out is left running (a thread cannot be stopped) and reported.
    """
    outcome: Dict[str, BaseException] = {}

    def target() -> None:
        token = _hook_context.set((bind, summary.execution_id))
        try:
            hook(summary, params)
        except BaseException as exc:  # a hook's SystemExit must not end the manager
            outcome["error"] = exc
        finally:
            _hook_context.reset(token)

    timeout = hook_timeout_seconds()
    started = time.time()
    thread = threading.Thread(
        target=target, name=f"hook-{summary.execution_id[:8]}", daemon=True
    )
    thread.start()
    thread.join(timeout)
    duration_ms = round((time.time() - started) * 1000)
    extra = {
        "execution_id": summary.execution_id,
        "pipeline_name": summary.pipeline_name,
        "hook": hook_name,
        "state": summary.state,
        "duration_ms": duration_ms,
    }
    if thread.is_alive():
        error = f"{hook_name} timed out after {timeout:g}s"
        logger.warning(
            "Lifecycle hook %s timed out after %gs",
            hook_name,
            timeout,
            extra={**extra, "event": "hook_failed", "error_type": "TimeoutError"},
        )
        return error
    if "error" in outcome:
        exc = outcome["error"]
        logger.warning(
            "Lifecycle hook %s failed: %s",
            hook_name,
            exc,
            exc_info=(type(exc), exc, exc.__traceback__),
            extra={**extra, "event": "hook_failed", "error_type": type(exc).__name__},
        )
        return f"{hook_name} raised {type(exc).__name__}: {exc}"
    logger.info(
        "Lifecycle hook %s ran (%dms)",
        hook_name,
        duration_ms,
        extra={**extra, "event": "hook_fired"},
    )
    return None


def _post_callback(url: str, summary: ExecutionSummary) -> Optional[str]:
    """POST the summary to the run's ``callback_url``. The error, or None.

    Only the host is logged: a callback URL may carry a token in its path or query.
    """
    import httpx

    host = urlsplit(url).hostname or "?"
    extra = {
        "execution_id": summary.execution_id,
        "pipeline_name": summary.pipeline_name,
        "callback_host": host,
    }
    try:
        response = httpx.post(url, json=summary.to_dict(), timeout=callback_timeout_seconds())
        if response.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"HTTP {response.status_code}", request=response.request, response=response
            )
    except Exception as exc:
        logger.warning(
            "Execution callback to %s failed: %s",
            host,
            type(exc).__name__,
            extra={**extra, "event": "callback_failed", "error_type": type(exc).__name__},
        )
        detail = str(exc) if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
        return f"callback to {host} failed: {detail}"
    logger.info(
        "Execution callback sent to %s", host, extra={**extra, "event": "callback_sent"}
    )
    return None


def fire_execution_hooks(bind: Any, execution_id: str) -> bool:
    """Fire the hooks and the callback of an execution that has ended, at most once.

    Synchronous (the caller provides the thread). Returns True if this call claimed
    them, False if the execution is unknown, not finished, or already claimed.
    """
    from reflowfy.core.registry import pipeline_registry

    with Session(bind=bind) as db:
        manager = ExecutionManager(db)
        execution = manager.get_execution(execution_id)
        if execution is None or execution.state not in ExecutionManager.TERMINAL_STATES:
            return False
        if not manager.claim_hooks(execution_id):
            logger.debug(
                "Lifecycle hooks already fired",
                extra={"event": "hooks_already_fired", "execution_id": execution_id},
            )
            return False
        db.refresh(execution)
        pipeline = pipeline_registry.get(execution.pipeline_name)
        hook = _overridden_hook(pipeline, execution.state)
        callback_url = execution.callback_url
        if hook is None and not callback_url:
            return True  # nothing to run: the claim only marks it done

        summary = build_summary(db, execution)
        runtime_params = dict(execution.runtime_params or {})
        state = execution.state
        db.commit()  # no transaction held open while the hook runs
        errors: List[str] = []
        if hook is not None:
            params: Dict[str, Any] = runtime_params
            apply_defaults: Any = getattr(pipeline, "apply_defaults", None)
            if callable(apply_defaults):
                params = cast(Dict[str, Any], apply_defaults(params))
            error = _run_hook(hook, HOOK_NAMES[state], summary, params, bind)
            if error:
                errors.append(error)
        if callback_url:
            error = _post_callback(callback_url, summary)
            if error:
                errors.append(error)
        if errors:
            manager.set_hook_error(execution_id, _short("; ".join(errors), HOOK_ERROR_CHARS))
        return True


def _drop_rate_bucket(bind: Any, execution_id: str) -> None:
    """Delete the ended execution's token bucket (the drive loop does too; either wins)."""
    with Session(bind=bind) as db:
        execution = ExecutionManager(db).get_execution(execution_id)
        if execution is not None:
            RateLimiter(db).delete_bucket(
                execution_bucket_key(execution.pipeline_name, execution_id)
            )


def run_terminal_work(bind: Any, execution_id: str) -> None:
    """Everything that follows an execution's end; never raises."""
    try:
        _drop_rate_bucket(bind, execution_id)
    except Exception:
        logger.warning(
            "Could not delete the rate-limit bucket",
            exc_info=True,
            extra={"event": "rate_bucket_cleanup_error", "execution_id": execution_id},
        )
    try:
        fire_execution_hooks(bind, execution_id)
    except Exception:
        logger.exception(
            "Lifecycle hooks could not be fired",
            extra={"event": "hook_failed", "execution_id": execution_id},
        )


def on_terminal(bind: Any, execution_id: str, state: str) -> None:
    """Terminal listener: run :func:`run_terminal_work` in a thread, off the caller."""

    def work() -> None:
        try:
            run_terminal_work(bind, execution_id)
        finally:
            with _running_lock:
                _running.discard(threading.current_thread())

    thread = threading.Thread(target=work, name=f"terminal-{execution_id[:8]}")
    with _running_lock:
        _running.add(thread)
    thread.start()


def install() -> None:
    """Fire lifecycle hooks on every terminal transition in this process (idempotent)."""
    add_terminal_listener(on_terminal)


def uninstall() -> None:
    """Undo :func:`install`."""
    remove_terminal_listener(on_terminal)


def wait_for_hooks(timeout: Optional[float] = None) -> bool:
    """Wait for terminal work in flight (shutdown, tests). True if none is left."""
    deadline = None if timeout is None else time.time() + timeout
    while True:
        with _running_lock:
            threads = list(_running)
        if not threads:
            return True
        for thread in threads:
            remaining = None if deadline is None else max(deadline - time.time(), 0.0)
            thread.join(remaining)
            if deadline is not None and time.time() >= deadline:
                with _running_lock:
                    return not _running


# ----- chaining --------------------------------------------------------------------


def current_execution_id() -> Optional[str]:
    """The execution whose hook is running in this thread, or None outside a hook."""
    context = _hook_context.get()
    return context[1] if context is not None else None


def _open_session(bind: Any) -> Session:
    if bind is not None:
        return Session(bind=bind)
    from reflowfy.reflow_manager.database import SessionLocal

    return SessionLocal()


def _drive_child(
    bind: Any,
    execution_id: str,
    pipeline_name: str,
    params: Dict[str, Any],
    rate_limit: Optional[float],
    mode: Optional[str],
) -> None:
    """Plan and dispatch a chained execution to its end (its own thread)."""
    from reflowfy.reflow_manager import factory

    drive_execution(
        _open_session(bind),
        lambda db: factory.make_manager(db, mode=mode),
        execution_id,
        pipeline_name,
        params,
        log=logger,
        rate_limit_override=rate_limit,
    )


def run_pipeline(
    pipeline_name: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    parent_execution_id: Optional[str] = None,
    rate_limit: Optional[float] = None,
    execution_id: Optional[str] = None,
    callback_url: Optional[str] = None,
    mode: Optional[str] = None,
) -> str:
    """Start an execution of a registered pipeline, as ``POST /run`` does. Returns its id.

    For chaining from a lifecycle hook::

        def on_execution_complete(self, summary, params):
            run_pipeline("load_warehouse", {"day": params["day"]})

    Called from a hook, the new execution records the hook's execution as its
    ``parent_execution_id`` (pass one to override) and uses the same database. It is
    planned and dispatched in a thread of its own; this returns as soon as it is
    created, so the hook is not held up by it. Runs on the manager (hooks do); from
    elsewhere it needs the manager's ``DATABASE_URL``.

    Raises:
        ValueError: the pipeline is not registered, or the params are invalid.
    """
    runtime_params: Dict[str, Any] = dict(params or {})
    check_runnable(pipeline_name, runtime_params)

    context = _hook_context.get()
    bind = context[0] if context is not None else None
    if parent_execution_id is None and context is not None:
        parent_execution_id = context[1]
    execution_id = execution_id or str(uuid.uuid4())

    db = _open_session(bind)
    try:
        ExecutionManager(db).create_execution(
            execution_id=execution_id,
            pipeline_name=pipeline_name,
            runtime_params=runtime_params,
            rate_limit=rate_limit,
            parent_execution_id=parent_execution_id,
            callback_url=callback_url,
        )
        bind = bind if bind is not None else db.get_bind()
    finally:
        db.close()

    logger.info(
        "Started execution %s of %s",
        execution_id,
        pipeline_name,
        extra={
            "event": "execution_chained",
            "execution_id": execution_id,
            "pipeline_name": pipeline_name,
            "parent_execution_id": parent_execution_id,
        },
    )
    threading.Thread(
        target=_drive_child,
        args=(bind, execution_id, pipeline_name, runtime_params, rate_limit, mode),
        name=f"chained-{execution_id[:8]}",
    ).start()
    return execution_id

