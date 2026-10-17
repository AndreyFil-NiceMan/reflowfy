"""Shared unit-test fixtures."""

from unittest.mock import AsyncMock

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "real_claim: run WorkerExecutor._claim_job against a real database"
    )


@pytest.fixture(autouse=True)
def _no_worker_claim_db(request, monkeypatch):
    """Worker tests build WorkerExecutor on a fake postgresql://x/y URL. The job
    claim would try to connect (a ~15s DNS timeout, then fail open); stub it to
    "claimed" unless a test opts in with @pytest.mark.real_claim."""
    if request.node.get_closest_marker("real_claim"):
        return
    from reflowfy.worker.executor import WorkerExecutor

    monkeypatch.setattr(WorkerExecutor, "_claim_job", AsyncMock(return_value=None))
