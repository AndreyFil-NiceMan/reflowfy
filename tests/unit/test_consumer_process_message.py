"""Unit tests for KafkaJobConsumer per-message handling."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from reflowfy.worker.consumer import KafkaJobConsumer


def _consumer():
    # Build without the DB-touching __init__; only exercise _process_message.
    c = KafkaJobConsumer.__new__(KafkaJobConsumer)
    c.consumer = AsyncMock()
    c.executor = AsyncMock()
    return c


class TestProcessMessage:
    async def test_null_value_message_is_skipped_not_crashed(self):
        """A null-value Kafka record (e.g. tombstone) must be skipped cleanly,
        not crash with AttributeError on msg.value.decode()."""
        c = _consumer()
        msg = SimpleNamespace(value=None)

        await c._process_message(msg)

        c.executor.execute_job.assert_not_awaited()
        c.consumer.commit.assert_awaited_once()

    async def test_valid_message_executes_job_and_commits(self):
        c = _consumer()
        c.executor.execute_job = AsyncMock(return_value=True)
        msg = SimpleNamespace(
            value=json.dumps({"schema_version": 2, "job_id": "j1"}).encode("utf-8")
        )

        await c._process_message(msg)

        c.executor.execute_job.assert_awaited_once()
        c.consumer.commit.assert_awaited_once()

    async def test_unsupported_schema_version_is_skipped_and_committed(self):
        """A job without schema_version==2 must be skipped (not executed) and
        committed so the bad/legacy message is not redelivered forever."""
        c = _consumer()
        c.executor.execute_job = AsyncMock(return_value=True)
        msg = SimpleNamespace(value=json.dumps({"job_id": "j1"}).encode("utf-8"))

        await c._process_message(msg)

        c.executor.execute_job.assert_not_awaited()
        c.consumer.commit.assert_awaited_once()
