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

    async def test_failed_job_is_still_committed(self):
        """A failed job (execute_job -> False) has its failure recorded in
        Postgres; the offset must still be committed. Not committing does not
        retry (arg-less commit advances the whole partition) and risks a
        duplicate reprocess of a deterministic failure on restart."""
        c = _consumer()
        c.executor.execute_job = AsyncMock(return_value=False)
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


class TestGracefulDrain:
    async def test_stop_during_a_job_lets_it_finish_then_exits(self):
        """SIGTERM mid-job used to sys.exit and leave a half-written job; now the
        job finishes and commits, and no further message is taken."""
        c = _consumer()
        c._running = True
        msgs = [SimpleNamespace(value=b"1"), SimpleNamespace(value=b"2")]
        c.consumer.getmany = AsyncMock(side_effect=[{"tp": [m]} for m in msgs])
        processed = []

        async def process(msg):
            processed.append(msg)
            c.request_stop()  # signal lands while the job runs

        c._process_message = process

        await c._consume_until_stopped()

        assert processed == [msgs[0]]
        assert c.consumer.getmany.await_count == 1

    async def test_idle_loop_notices_stop(self):
        c = _consumer()
        c._running = True

        async def empty_poll(**kwargs):
            c.request_stop()
            return {}

        c.consumer.getmany = empty_poll

        await c._consume_until_stopped()  # returns instead of polling forever
