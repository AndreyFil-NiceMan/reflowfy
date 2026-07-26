"""The batch-completion poll loop must not hold a DB connection while sleeping.

Regression guard for QueuePool exhaustion: the poll query opens a read
transaction, and holding it across the sleep pins one pooled connection per
in-flight execution. Scheduled pipelines overlap by design, so a pinned
connection per run exhausts the pool (10 + 20 overflow) and every caller
then fails with "QueuePool limit of size 10 overflow 20 reached".
"""

from unittest.mock import MagicMock, patch

from reflowfy.reflow_manager.pipeline_runner import PipelineRunner


def _runner(job_states_sequence, order):
    """Build a runner whose poll returns each dict in turn, recording call order."""
    job_manager = MagicMock()
    job_manager.get_job_states.side_effect = job_states_sequence
    job_manager.db.commit.side_effect = lambda: order.append("commit")

    return PipelineRunner(
        execution_manager=MagicMock(),
        job_manager=job_manager,
        dispatcher=MagicMock(),
    )


def test_connection_released_before_each_sleep():
    """Every sleep must be preceded by a commit that returns the connection."""
    order: list[str] = []
    runner = _runner(
        [
            {"j1": "dispatched"},  # still pending -> sleeps
            {"j1": "dispatched"},  # still pending -> sleeps
            {"j1": "completed"},   # done -> returns
        ],
        order,
    )

    with patch("time.sleep", side_effect=lambda _: order.append("sleep")):
        completed, failed = runner._wait_for_batch_completion(
            job_ids=["j1"], timeout=60, poll_interval=0.01
        )

    assert (completed, failed) == (1, 0)
    # Two waits happened, and each was preceded by a commit.
    assert order == ["commit", "sleep", "commit", "sleep"], order


def test_no_commit_needed_when_batch_already_done():
    """Fast path: nothing pending, so no sleep and no extra commit."""
    order: list[str] = []
    runner = _runner([{"j1": "completed", "j2": "failed"}], order)

    with patch("time.sleep", side_effect=lambda _: order.append("sleep")):
        completed, failed = runner._wait_for_batch_completion(
            job_ids=["j1", "j2"], timeout=60, poll_interval=0.01
        )

    assert (completed, failed) == (1, 1)
    assert order == [], order


def test_sleep_never_happens_while_transaction_open():
    """A real-ish guard: track transaction state and assert it is closed at sleep."""
    state = {"in_txn": False}
    job_manager = MagicMock()

    def _poll(_job_ids):
        state["in_txn"] = True  # the SELECT opens a read transaction
        return {"j1": "dispatched"} if len(calls) < 2 else {"j1": "completed"}

    calls: list[int] = []

    def _query(job_ids):
        calls.append(1)
        return _poll(job_ids)

    job_manager.get_job_states.side_effect = _query
    job_manager.db.commit.side_effect = lambda: state.update(in_txn=False)

    runner = PipelineRunner(
        execution_manager=MagicMock(),
        job_manager=job_manager,
        dispatcher=MagicMock(),
    )

    def _assert_not_in_txn(_seconds):
        assert not state["in_txn"], "slept while holding an open DB transaction"

    with patch("time.sleep", side_effect=_assert_not_in_txn):
        runner._wait_for_batch_completion(
            job_ids=["j1"], timeout=60, poll_interval=0.01
        )
