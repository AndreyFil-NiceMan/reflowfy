"""Unit tests for ReflowManager lifecycle."""

from unittest.mock import AsyncMock

from reflowfy.reflow_manager.manager import ReflowManager


class TestReflowManagerClose:
    async def test_close_awaits_dispatcher_close(self):
        # Build without running the DB-heavy __init__; we only exercise close().
        manager = ReflowManager.__new__(ReflowManager)
        manager.dispatcher = AsyncMock()

        await manager.close()

        manager.dispatcher.close.assert_awaited_once()
