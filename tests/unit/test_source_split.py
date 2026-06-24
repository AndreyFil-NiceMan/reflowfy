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
    src = SqlSource(connection_url="sqlite://", query="SELECT * FROM t",
                    id_column="id", batch_size=100)

    class _Row:
        def __getitem__(self, i): return (0, 250)[i]
    class _Result:
        def fetchone(self): return _Row()
    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **k): return _Result()
    monkeypatch.setattr(src, "_get_engine", lambda: type("E", (), {"connect": lambda self: _Conn()})())

    subs = list(src.split({}))
    bounds = [(s.config["slice"]["lo"], s.config["slice"]["hi"]) for s in subs]
    assert bounds == [(0, 100), (100, 200), (200, 300)]
    assert "BETWEEN" in subs[0].config["query"] or ">=" in subs[0].config["query"]
