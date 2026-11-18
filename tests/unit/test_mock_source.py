"""MockSource: fetch, split and the sample-data helper."""

from typing import Any, Dict

from reflowfy.sources.mock import generate_sample_data, mock_source

_DATA: list[Dict[str, Any]] = [{"id": i} for i in range(7)]


def test_mock_fetch_wraps_records_and_honours_limit():
    src = mock_source(_DATA)
    assert src.fetch({}) == [{"data": {"id": i}} for i in range(7)]
    assert src.fetch({}, limit=2) == [{"data": {"id": 0}}, {"data": {"id": 1}}]


def test_mock_split_slices_by_batch_size():
    subs = list(mock_source(_DATA, batch_size=3).split({}))
    assert [len(s.config["data"]) for s in subs] == [3, 3, 1]
    assert [r["data"]["id"] for s in subs for r in s.fetch({})] == list(range(7))
    assert all(s.config["batch_size"] == 3 for s in subs)


def test_mock_empty_data_plans_nothing_and_is_healthy():
    src = mock_source([])
    assert list(src.split({})) == []
    assert src.health_check() is True


def test_generate_sample_data_shape():
    rows = generate_sample_data(5)
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5]
    assert rows[2]["email"] == "user3@example.com"
    assert all(18 <= r["age"] <= 70 and isinstance(r["active"], bool) for r in rows)
    assert set(rows[0]) == {
        "id",
        "first_name",
        "last_name",
        "email",
        "age",
        "city",
        "salary",
        "active",
    }
