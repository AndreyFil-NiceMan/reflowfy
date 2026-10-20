"""S3Source against a real boto3 client wired to botocore's Stubber.

Unlike a MagicMock client, the Stubber validates every request against the S3
service model and serves real paginators, so continuation-token paging and the
exact ``list_objects_v2`` / ``get_object`` parameters are exercised.
"""

import io
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytest

boto3 = pytest.importorskip("boto3")

from botocore.response import StreamingBody  # noqa: E402
from botocore.stub import ANY, Stubber  # noqa: E402

from reflowfy.sources.base import SourceError  # noqa: E402
from reflowfy.sources.s3 import S3Source, s3_source  # noqa: E402

_WHEN = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def _obj(key: str, size: int = 10) -> Dict[str, Any]:
    return {"Key": key, "Size": size, "LastModified": _WHEN, "ETag": f'"{key}"'}


def _body(payload: bytes) -> StreamingBody:
    return StreamingBody(io.BytesIO(payload), len(payload))


@pytest.fixture
def stubbed() -> Iterator[Tuple[Any, Stubber]]:
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    with Stubber(client) as stub:
        yield client, stub
        stub.assert_no_pending_responses()


def _source(client: Any, **kw: Any) -> S3Source:
    src = s3_source(bucket="bkt", **kw)
    src._client = client  # the Stubber-wrapped client instead of a real one
    return src


def _list(
    stub: Stubber,
    keys: List[str],
    prefix: str = "",
    token: Optional[str] = None,
    next_token: Optional[str] = None,
) -> None:
    response: Dict[str, Any] = {"IsTruncated": next_token is not None, "KeyCount": len(keys)}
    if keys:
        response["Contents"] = [_obj(k) for k in keys]
    if next_token:
        response["NextContinuationToken"] = next_token
    expected: Dict[str, Any] = {"Bucket": "bkt", "Prefix": prefix, "MaxKeys": ANY}
    if token:
        expected["ContinuationToken"] = token
    stub.add_response("list_objects_v2", response, expected)


def _get(stub: Stubber, key: str, payload: bytes) -> None:
    stub.add_response("get_object", {"Body": _body(payload)}, {"Bucket": "bkt", "Key": key})


# -- fetch ---------------------------------------------------------------------


def test_fetch_metadata_across_pages_with_pattern(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["logs/a.json", "logs/skip.txt"], prefix="logs/", next_token="t1")
    _list(stub, ["logs/b.json"], prefix="logs/", token="t1")
    src = _source(client, prefix="logs/", file_pattern="*.json", read_content=False)

    records = src.fetch({})

    assert [r["data"]["key"] for r in records] == ["logs/a.json", "logs/b.json"]
    assert records[0]["data"] == {
        "key": "logs/a.json",
        "size": 10,
        "last_modified": _WHEN.isoformat(),
        "etag": '"logs/a.json"',
    }


def test_fetch_json_content_list_is_flattened_object_is_one_record(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["a.json", "b.json"])
    _get(stub, "a.json", json.dumps([{"n": 1}, {"n": 2}]).encode())
    _get(stub, "b.json", json.dumps({"n": 3}).encode())

    records = _source(client).fetch({})

    assert [r["data"]["n"] for r in records] == [1, 2, 3]


def test_fetch_limit_stops_reading_early(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["a.json", "b.json", "c.json"])
    _get(stub, "a.json", b'[{"n": 1}, {"n": 2}, {"n": 3}]')
    # b.json and c.json are never read: the Stubber would fail on an unexpected call
    records = _source(client).fetch({}, limit=2)
    assert [r["data"]["n"] for r in records] == [1, 2]


def test_fetch_text_objects_come_back_as_raw_text(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["notes.txt"])
    _get(stub, "notes.txt", "plain text, not JSON — ünïcode".encode())

    records = _source(client, content_type="text").fetch({})

    assert records == [{"data": "plain text, not JSON — ünïcode"}]


def test_fetch_csv_rows_become_records(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["t.csv"])
    _get(stub, "t.csv", b"id,name\n1,a\n2,b\n")
    records = _source(client, content_type="csv").fetch({})
    assert [r["data"] for r in records] == [{"id": "1", "name": "a"}, {"id": "2", "name": "b"}]


def test_fetch_binary_objects_are_bytes(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["blob.bin"])
    _get(stub, "blob.bin", b"\x00\xff")
    assert _source(client, content_type="binary").fetch({}) == [{"data": b"\x00\xff"}]


def test_fetch_non_json_object_under_json_content_type_raises(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["bad.json"])
    _get(stub, "bad.json", b"not json at all")
    with pytest.raises(ValueError):  # json.JSONDecodeError, not swallowed into an empty job
        _source(client).fetch({})


def test_fetch_empty_bucket(stubbed) -> None:
    client, stub = stubbed
    _list(stub, [])
    assert _source(client).fetch({}) == []


def test_fetch_list_error_is_a_source_error(stubbed) -> None:
    client, stub = stubbed
    stub.add_client_error("list_objects_v2", "NoSuchBucket", http_status_code=404)
    with pytest.raises(SourceError, match="Failed to fetch data"):
        _source(client).fetch({})


def test_fetch_missing_object_names_the_key(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["gone.json"])
    stub.add_client_error("get_object", "NoSuchKey", http_status_code=404)
    with pytest.raises(SourceError, match="gone.json"):
        _source(client).fetch({})


def test_fetch_prefix_from_runtime_params(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["2026-01-02/x"], prefix="2026-01-02/")
    src = _source(client, prefix="{{ day }}/", read_content=False)
    assert [r["data"]["key"] for r in src.fetch({"day": "2026-01-02"})] == ["2026-01-02/x"]


# -- split + explicit-key fetch (the worker path) --------------------------------


def test_split_yields_one_source_per_page_with_its_keys(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["a.json", "x.txt"], next_token="t1")
    _list(stub, ["x2.txt"], token="t1", next_token="t2")  # nothing matches: no job
    _list(stub, ["b.json"], token="t2")
    src = _source(client, file_pattern="*.json", page_size=2)

    subs = list(src.split({}))

    assert [s.config["keys"] for s in subs] == [["a.json"], ["b.json"]]
    assert all(s.config["page_size"] == 2 and s.config["bucket"] == "bkt" for s in subs)


def test_sub_source_fetch_reads_only_its_keys(stubbed) -> None:
    client, stub = stubbed
    _get(stub, "a.json", b'[{"n": 1}, {"n": 2}]')
    _get(stub, "b.json", b'{"n": 3}')
    sub = _source(client)
    sub.config["keys"] = ["a.json", "b.json"]

    assert [r["data"]["n"] for r in sub.fetch({})] == [1, 2, 3]


def test_sub_source_fetch_keys_with_limit_and_metadata_only(stubbed) -> None:
    client, stub = stubbed
    _get(stub, "a.json", b'[{"n": 1}, {"n": 2}]')
    sub = _source(client)
    sub.config["keys"] = ["a.json", "b.json"]
    assert [r["data"]["n"] for r in sub.fetch({}, limit=1)] == [1]

    meta = _source(client, read_content=False)
    meta.config["keys"] = ["a.json", "b.json"]
    assert meta.fetch({}) == [{"data": {"key": "a.json"}}, {"data": {"key": "b.json"}}]


def test_split_empty_bucket_plans_no_jobs(stubbed) -> None:
    client, stub = stubbed
    _list(stub, [])
    assert list(_source(client).split({})) == []


# -- split_jobs (legacy) --------------------------------------------------------


def test_split_jobs_one_job_per_matching_page(stubbed) -> None:
    client, stub = stubbed
    # the paginator is lazy: each page is listed only after the previous job is read
    _list(stub, ["a.json"], next_token="t1")
    _get(stub, "a.json", b'{"n": 1}')
    _list(stub, [], token="t1", next_token="t2")
    _list(stub, ["b.json", "c.json"], token="t2")
    _get(stub, "b.json", b'{"n": 2}')
    _get(stub, "c.json", b'[{"n": 3}, {"n": 4}]')

    jobs = list(_source(client, prefix="").split_jobs({}))

    assert [[r["data"]["n"] for r in j.records] for j in jobs] == [[1], [2, 3, 4]]
    assert jobs[1].metadata == {
        "page_num": 1,
        "bucket": "bkt",
        "prefix": "",
        "object_count": 2,
        "record_count": 3,
    }


def test_split_jobs_metadata_only_and_pattern_filtered_page_skipped(stubbed) -> None:
    client, stub = stubbed
    _list(stub, ["skip.txt"], next_token="t1")
    _list(stub, ["a.json"], token="t1")
    jobs = list(_source(client, file_pattern="*.json", read_content=False).split_jobs({}))
    assert len(jobs) == 1
    assert jobs[0].records[0]["data"]["key"] == "a.json"
    assert jobs[0].metadata["page_num"] == 0


def test_split_jobs_error_is_a_source_error(stubbed) -> None:
    client, stub = stubbed
    stub.add_client_error("list_objects_v2", "AccessDenied", http_status_code=403)
    with pytest.raises(SourceError, match="Failed to split jobs"):
        list(_source(client).split_jobs({}))


# -- health_check ----------------------------------------------------------------


def test_health_check_up_and_down(stubbed) -> None:
    client, stub = stubbed
    stub.add_response("head_bucket", {}, {"Bucket": "bkt"})
    stub.add_client_error("head_bucket", "403", http_status_code=403)
    src = _source(client)
    assert src.health_check() is True
    assert src.health_check() is False
