"""Running async code from the framework's synchronous entry points."""

import asyncio
import concurrent.futures
import contextvars
from typing import Any, Awaitable, Callable, Coroutine, Sequence, TypeVar

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


async def for_each_concurrently(
    items: Sequence[T], concurrency: int, fn: Callable[[int, T], Awaitable[None]]
) -> None:
    """Await ``fn(index, item)`` for every item, at most ``concurrency`` at once.

    ``concurrency=1`` is a plain loop, in order. Above that, a fixed pool of workers
    pulls from one iterator (``next()`` never awaits, so no item is taken twice):
    memory stays flat for a job of any size, unlike a task per item. Items start in
    order but may finish out of order. The first failure cancels the rest and is
    raised.
    """
    if concurrency <= 1:
        for index, item in enumerate(items):
            await fn(index, item)
        return

    pending = iter(enumerate(items))

    async def worker() -> None:
        for index, item in pending:
            await fn(index, item)

    workers = [asyncio.ensure_future(worker()) for _ in range(min(concurrency, len(items)))]
    try:
        await asyncio.gather(*workers)
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        raise
