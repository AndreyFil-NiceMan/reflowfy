from reflowfy.sources.static import StaticSource
from reflowfy.sources.mock import MockSource


def test_default_split_yields_self():
    src = MockSource(data=[{"a": 1}], batch_size=1000)
    subs = list(src.split({}))
    assert subs == [src]  # default: one job, identity


def test_static_split_yields_self():
    src = StaticSource([1, 2, 3])
    subs = list(src.split({}))
    assert len(subs) == 1
    assert subs[0].config == {"records": [1, 2, 3]}
