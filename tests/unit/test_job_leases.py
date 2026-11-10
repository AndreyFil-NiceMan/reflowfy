"""JobLeases: the worker heartbeat and the manager's running lease, checked together."""

import logging

from reflowfy.store.leases import JobLeases


def test_defaults_from_an_empty_environment(monkeypatch):
    for name in (
        "JOB_HEARTBEAT_SECONDS",
        "JOB_RUNNING_LEASE_SECONDS",
        "JOB_DISPATCH_LEASE_SECONDS",
        "JOB_MAX_ATTEMPTS",
    ):
        monkeypatch.delenv(name, raising=False)
    assert JobLeases.from_env() == JobLeases(30.0, 300.0, 3600.0, 3)


def test_reads_the_environment(monkeypatch):
    monkeypatch.setenv("JOB_HEARTBEAT_SECONDS", "5")
    monkeypatch.setenv("JOB_RUNNING_LEASE_SECONDS", "60")
    monkeypatch.setenv("JOB_DISPATCH_LEASE_SECONDS", "120")
    monkeypatch.setenv("JOB_MAX_ATTEMPTS", "7")
    assert JobLeases.from_env() == JobLeases(5.0, 60.0, 120.0, 7)


def test_a_lease_of_three_beats_is_fine(caplog):
    with caplog.at_level(logging.WARNING):
        assert not JobLeases(heartbeat_seconds=10, running_lease_seconds=30).warn_if_inconsistent(
            logging.getLogger("t")
        )
    assert caplog.records == []


def test_a_lease_below_three_beats_warns(caplog):
    with caplog.at_level(logging.WARNING):
        assert JobLeases(heartbeat_seconds=30, running_lease_seconds=60).warn_if_inconsistent(
            logging.getLogger("t")
        )
    assert [r.event for r in caplog.records] == ["job_lease_too_short"]  # type: ignore[attr-defined]
