from reflowfy.sources.static import StaticSource
from reflowfy.sources.mock import MockSource


def test_default_split_yields_self():
    from reflowfy.sources.base import BaseSource

    class _PlainSource(BaseSource):
        def fetch(self, runtime_params, limit=None):
            return []

        def split_jobs(self, runtime_params, batch_size=1000):
            yield from ()

        def health_check(self):
            return True

    src = _PlainSource(config={"k": "v"})
    subs = list(src.split({}))
    assert subs == [src]  # default: one job, identity


def test_static_split_yields_self():
    src = StaticSource([1, 2, 3])
    subs = list(src.split({}))
    assert len(subs) == 1
    assert subs[0].config == {"records": [1, 2, 3]}


def test_mock_split_by_batch_size():
    src = MockSource(data=[{"i": i} for i in range(25)], batch_size=10)
    subs = list(src.split({}))
    assert len(subs) == 3
    assert [len(s.config["data"]) for s in subs] == [10, 10, 5]
    assert subs[0].fetch({}) == [{"i": i} for i in range(10)]


def test_sql_split_id_range(monkeypatch):
    from reflowfy.sources.sql import SqlSource

    src = SqlSource(
        connection_url="sqlite://", query="SELECT * FROM t", id_column="id", batch_size=100
    )

    class _Row:
        def __getitem__(self, i):
            return (0, 250)[i]

    class _Result:
        def fetchone(self):
            return _Row()

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *a, **k):
            return _Result()

    monkeypatch.setattr(
        src, "_get_engine", lambda: type("E", (), {"connect": lambda self: _Conn()})()
    )

    subs = list(src.split({}))
    bounds = [(s.config["slice"]["lo"], s.config["slice"]["hi"]) for s in subs]
    assert bounds == [(0, 100), (100, 200), (200, 300)]
    assert "BETWEEN" in subs[0].config["query"] or ">=" in subs[0].config["query"]


def test_api_split_per_id_mode_groups_ids():
    from reflowfy.sources.api import IDBasedAPISource

    src = IDBasedAPISource(
        base_url="http://h", endpoint_template="/u/{id}", ids=[1, 2, 3, 4, 5], batch_size=2
    )
    subs = list(src.split({}))
    assert [s.config["ids"] for s in subs] == [[1, 2], [3, 4], [5]]
    assert all(s.config["base_url"] == "http://h" for s in subs)


def test_api_split_batch_mode_yields_self():
    from reflowfy.sources.api import IDBasedAPISource

    src = IDBasedAPISource(
        base_url="http://h",
        endpoint_template="/batch",
        ids=[1, 2, 3],
        method="POST",
        body={"ids": [1, 2, 3]},
    )
    subs = list(src.split({}))
    assert len(subs) == 1  # one batch request -> one job


def test_s3_split_lists_keys_only(monkeypatch):
    from reflowfy.sources.s3 import S3Source

    src = S3Source(bucket="b", prefix="p/", page_size=2, read_content=False)

    pages = [
        {
            "Contents": [
                {"Key": "p/a", "Size": 1, "ETag": "x", "LastModified": _Dt()},
                {"Key": "p/b", "Size": 1, "ETag": "y", "LastModified": _Dt()},
            ]
        },
        {"Contents": [{"Key": "p/c", "Size": 1, "ETag": "z", "LastModified": _Dt()}]},
    ]

    class _Paginator:
        def paginate(self, **k):
            return iter(pages)

    class _Client:
        def get_paginator(self, n):
            return _Paginator()

    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    subs = list(src.split({}))
    assert [s.config["keys"] for s in subs] == [["p/a", "p/b"], ["p/c"]]


class _Dt:
    def isoformat(self):
        return "t"


def test_s3_split_resolves_templated_config(monkeypatch):
    from reflowfy.sources.s3 import S3Source

    src = S3Source(
        bucket="{{ env }}-data", prefix="logs/{{ env }}/", page_size=2, read_content=False
    )

    pages = [{"Contents": [{"Key": "logs/prod/a", "Size": 1, "ETag": "x", "LastModified": _Dt()}]}]

    class _Paginator:
        def paginate(self, **k):
            return iter(pages)

    class _Client:
        def get_paginator(self, n):
            return _Paginator()

    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    subs = list(src.split({"env": "prod"}))
    assert subs[0].config["bucket"] == "prod-data"
    assert subs[0].config["prefix"] == "logs/prod/"
    assert subs[0].config["keys"] == ["logs/prod/a"]


def test_elastic_split_opens_pit_and_yields_slices(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(
        url="http://es:9200", index="logs-*", base_query={"query": {"match_all": {}}}, size=1000
    )
    src.config["num_slices"] = 4

    class _Client:
        def open_point_in_time(self, **k):
            return {"id": "PIT123"}

    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    subs = list(src.split({}))
    assert len(subs) == 4
    assert all(s.config["pit_id"] == "PIT123" for s in subs)
    assert [s.config["slice"] for s in subs] == [
        {"id": 0, "max": 4},
        {"id": 1, "max": 4},
        {"id": 2, "max": 4},
        {"id": 3, "max": 4},
    ]


def test_elastic_fetch_slice_paginates(monkeypatch):
    from reflowfy.sources.elastic import ElasticSource

    src = ElasticSource(url="http://es", index="i", base_query={"query": {"match_all": {}}}, size=2)
    src.config["pit_id"] = "PIT"
    src.config["slice"] = {"id": 0, "max": 2}

    calls = {"n": 0}

    class _Client:
        def search(self, body=None, size=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return {
                    "hits": {
                        "hits": [
                            {"_source": {"a": 1}, "sort": [1]},
                            {"_source": {"a": 2}, "sort": [2]},
                        ]
                    }
                }
            return {"hits": {"hits": []}}

    monkeypatch.setattr(src, "_get_client", lambda: _Client())

    out = src.fetch({})
    assert out == [{"a": 1}, {"a": 2}]
    assert calls["n"] == 2
