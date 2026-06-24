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
