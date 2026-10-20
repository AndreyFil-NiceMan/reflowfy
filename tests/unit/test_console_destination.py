"""ConsoleDestination: what it prints, truncation, and records json can't encode."""

from datetime import datetime, timezone

import pytest

from reflowfy.destinations.base import RetryConfig
from reflowfy.destinations.console import ConsoleDestination, console_destination


async def test_pretty_send_prints_header_metadata_records_and_footer(capsys) -> None:
    dest = console_destination()

    await dest.send([{"id": 1, "name": "a"}, {"id": 2}], metadata={"execution_id": "e1"})

    out = capsys.readouterr().out
    assert "CONSOLE DESTINATION - Sending 2 records" in out
    assert '"execution_id": "e1"' in out
    assert "Records (showing 2 of 2)" in out
    assert 'Record 1:\n{\n  "id": 1,\n  "name": "a"\n}' in out
    assert 'Record 2:\n{\n  "id": 2\n}' in out
    assert "Successfully sent 2 records to console" in out
    assert "more records" not in out


async def test_compact_send_uses_repr_per_line(capsys) -> None:
    dest = console_destination(pretty_print=False)

    await dest.send([{"id": 1}], metadata={"k": "v"})

    out = capsys.readouterr().out
    assert "Record 1: {'id': 1}" in out
    assert "{'k': 'v'}" in out


async def test_no_metadata_section_without_metadata(capsys) -> None:
    await console_destination().send([{"id": 1}])
    assert "Metadata" not in capsys.readouterr().out


async def test_large_batch_is_truncated_to_max_display(capsys) -> None:
    dest = console_destination(max_records_display=3)

    await dest.send([{"i": i} for i in range(1000)])

    out = capsys.readouterr().out
    assert "Sending 1000 records" in out
    assert "Records (showing 3 of 1000)" in out
    assert "Record 3:" in out and "Record 4:" not in out
    assert "... and 997 more records" in out
    assert "Successfully sent 1000 records" in out


async def test_empty_batch(capsys) -> None:
    await console_destination().send([])
    out = capsys.readouterr().out
    assert "Records (showing 0 of 0)" in out and "Successfully sent 0 records" in out


async def test_non_dict_records_print(capsys) -> None:
    await console_destination().send(["raw text", 42, None])
    out = capsys.readouterr().out
    assert 'Record 1:\n"raw text"' in out
    assert "Record 2:\n42" in out
    assert "Record 3:\nnull" in out


async def test_non_json_record_prints_in_compact_mode(capsys) -> None:
    when = datetime(2026, 1, 2, tzinfo=timezone.utc)
    await console_destination(pretty_print=False).send([{"at": when, "tags": {"x"}}])
    out = capsys.readouterr().out
    assert "datetime.datetime(2026, 1, 2" in out and "{'x'}" in out


async def test_pretty_print_survives_a_datetime_in_a_record(capsys) -> None:
    when = datetime(2026, 1, 2, tzinfo=timezone.utc)
    await console_destination().send([{"at": when}])
    assert "2026-01-02" in capsys.readouterr().out


async def test_health_check_and_close() -> None:
    dest = ConsoleDestination()
    assert await dest.health_check() is True
    assert await dest.close() is None  # default no-op from BaseDestination


def test_factory_passes_config_through() -> None:
    retry = RetryConfig(max_attempts=7)
    dest = console_destination(pretty_print=False, max_records_display=2, retry_config=retry)
    assert dest.config == {"pretty_print": False, "max_records_display": 2}
    assert dest.retry_config is retry
    assert (
        repr(dest) == "ConsoleDestination(config={'pretty_print': False, 'max_records_display': 2})"
    )
