"""``now`` date math in an Elasticsearch query is fixed at planning, not per job."""

from datetime import datetime, timezone

from reflowfy.sources.elastic import _pin_now

NOW = datetime(2026, 10, 20, 12, 34, 56, tzinfo=timezone.utc)


def _range(**bounds):
    return {"bool": {"filter": [{"range": {"@timestamp": bounds}}]}}


def _bounds(query):
    return query["bool"]["filter"][0]["range"]["@timestamp"]


def test_now_minus_minutes_becomes_absolute():
    out = _pin_now(_range(gte="now-120m", lte="now"), NOW)
    assert _bounds(out) == {
        "gte": "2026-10-20T10:34:56.000+00:00",
        "lte": "2026-10-20T12:34:56.000+00:00",
    }


def test_rounding_matches_elasticsearch_direction():
    out = _pin_now(_range(gte="now/d", lte="now/d"), NOW)
    assert _bounds(out) == {
        "gte": "2026-10-20T00:00:00.000+00:00",
        "lte": "2026-10-20T23:59:59.999+00:00",
    }


def test_leaves_absolute_dates_formats_and_input_alone():
    original = _range(gte="2026-01-01", lt="now-1h")
    assert _bounds(_pin_now(original, NOW))["gte"] == "2026-01-01"
    assert _bounds(original)["lt"] == "now-1h"  # not mutated
    with_format = _range(gte="now-1h", format="epoch_millis")
    assert _bounds(_pin_now(with_format, NOW))["gte"] == "now-1h"


def test_unsupported_math_is_left_unchanged():
    assert _bounds(_pin_now(_range(gte="now-1M"), NOW))["gte"] == "now-1M"


def test_rounding_with_time_zone_is_left_to_elasticsearch():
    q = _range(gte="now/d", lte="now-1h", time_zone="+03:00")
    out = _bounds(_pin_now(q, NOW))
    assert out["gte"] == "now/d"  # would round at UTC midnight, not +03:00
    assert out["lte"] == "2026-10-20T11:34:56.000+00:00"  # plain offsets are zone-independent
