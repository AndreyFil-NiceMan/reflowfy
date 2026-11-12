"""Shared unit-test fixtures."""

from unittest.mock import AsyncMock

import pytest


def pytest_configure(config):
    # The reflowfy pytest plugin (mock_http, mock_source, ...) loads from its entry
    # point when reflowfy is installed. CI runs from the checkout without
    # installing it, so register the plugin here when it is not there yet.
    if not config.pluginmanager.has_plugin("reflowfy_pytest"):
        import reflowfy_pytest

        config.pluginmanager.register(reflowfy_pytest, "reflowfy_pytest")
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


@pytest.fixture(autouse=True)
def _no_uv_lock(monkeypatch):
    """`reflowfy init` runs `uv lock`, which resolves against PyPI. Unit tests
    must not touch the network: report uv as missing unless a test stubs it."""
    from reflowfy.cli.commands import init_cmd

    monkeypatch.setattr(init_cmd, "_find_uv", lambda: None)


@pytest.fixture(autouse=True)
def _no_lifecycle_listener():
    """`_startup` installs the lifecycle hooks' terminal listener process-wide; a later
    test's terminal transition would then fire hooks in a thread on its database."""
    yield
    from reflowfy.reflow_manager import lifecycle

    lifecycle.uninstall()
