"""Unit tests for the _dispatch_pipeline_jobs background task error handling."""

from unittest.mock import MagicMock, patch

import reflowfy.reflow_manager.app as app


class TestDispatchFailureMarksExecution:
    def test_execution_marked_failed_when_manager_construction_fails(self):
        """If ReflowManager construction raises, the execution must still be
        marked 'failed' (regression: 'manager' was unbound in the except block)."""
        fake_db = MagicMock()
        exec_manager = MagicMock()

        with (
            patch.object(app, "_get_kafka_config", return_value={}),
            patch.object(app, "ReflowManager", side_effect=RuntimeError("boom")),
            patch.object(app, "ExecutionManager", return_value=exec_manager) as em_cls,
            patch.object(app, "SessionLocal", return_value=fake_db),
        ):
            # Must not raise NameError; must mark the execution failed.
            app._dispatch_pipeline_jobs("exec-1", "my_pipeline", {})

        em_cls.assert_called_once_with(fake_db)
        args, _ = exec_manager.update_execution_state.call_args
        assert args[0] == "exec-1"
        assert args[1] == "failed"
        fake_db.close.assert_called_once()
