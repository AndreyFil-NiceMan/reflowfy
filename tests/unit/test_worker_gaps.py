"""WorkerExecutor: a shutdown mid-job when no content claim was taken."""

import asyncio
from unittest.mock import AsyncMock

import pytest

import reflowfy.worker.executor as executor_mod
from test_worker_executor_paths import (  # type: ignore[import-not-found]  # noqa: F401
    FakeDest,
    FakePipeline,
    _payload,
    register,
    stub_worker,
)


async def test_shutdown_mid_send_without_dedup_propagates_and_releases_nothing(
    register, stub_worker, monkeypatch  # noqa: F811
):
    """Without `dedup_check` no content hash was claimed, so there is nothing to release —
    the cancellation still propagates and the job row is left for the redelivery."""
    dest = FakeDest(send_error=asyncio.CancelledError())
    register(FakePipeline("gaps-cancel-nodedup", dest))
    release = AsyncMock()
    monkeypatch.setattr(executor_mod, "release_content_hash", release)

    with pytest.raises(asyncio.CancelledError):
        await stub_worker.execute_job(_payload("gaps-cancel-nodedup"))

    release.assert_not_awaited()
    assert stub_worker.updates == []
    assert dest.closed
