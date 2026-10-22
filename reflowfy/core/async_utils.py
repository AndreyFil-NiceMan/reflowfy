"""Running async code from the framework's synchronous entry points."""

import asyncio
import concurrent.futures
import contextvars
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


def run_coroutine_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run ``coro`` to completion from synchronous code and return its result.

    With no event loop running in this thread it is plain ``asyncio.run``. With one
    running (``LocalExecutor.execute`` is called from a FastAPI handler, and sync
    callers inside async tests), the coroutine runs on a fresh loop in a helper
    thread: blocking the running loop on itself would deadlock. The caller's
    contextvars travel along, so ``log_context`` fields stay bound.

    Unlike the manager's ``_run_async`` this never patches the running loop with
    ``nest_asyncio``: a job's transformations should not run on the server's loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    context = contextvars.copy_context()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(context.run, asyncio.run, coro).result()
