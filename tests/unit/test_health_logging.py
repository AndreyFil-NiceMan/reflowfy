"""A failed health probe says why, without the connection URL's password."""

import logging

from reflowfy.core.health import log_unhealthy
from reflowfy.sources.sql import SqlSource


def test_log_unhealthy_names_the_cause_and_masks_url_passwords(caplog):
    log = logging.getLogger("t.health")
    with caplog.at_level(logging.WARNING, logger="t.health"):
        result = log_unhealthy(
            log, "SQL source", ConnectionError("cannot reach postgresql://u:s3cret@db/x")
        )

    assert result is False
    (record,) = caplog.records
    assert "SQL source health check failed: ConnectionError" in record.getMessage()
    assert "s3cret" not in record.getMessage()
    assert record.event == "health_check_failed"  # type: ignore[attr-defined]


def test_an_unreachable_sql_source_logs_why(caplog, monkeypatch):
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)
    source = SqlSource(connection_url="sqlite:////nonexistent/dir/x.db", query="SELECT 1")

    with caplog.at_level(logging.WARNING):
        assert source.health_check() is False

    assert any(
        getattr(r, "event", None) == "health_check_failed" for r in caplog.records
    )
