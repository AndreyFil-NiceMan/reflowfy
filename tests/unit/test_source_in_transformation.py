"""A transformation may fetch from a source itself (e.g. enrich from Elastic).

Sources are plain sync objects: construct one, call ``fetch(runtime_params)``.
Nothing in the framework has to be involved — this test is the canary that
keeps that true.
"""

from typing import Any, Dict, List

from reflowfy.sources.base import BaseSource
from reflowfy.sources.static import StaticSource
from reflowfy.transformations.base import BaseTransformation


class EnrichFromSource(BaseTransformation):
    name = "enrich_from_source"

    def __init__(self) -> None:
        # Built once, reused across batches — a real ElasticSource holds a client.
        self._lookup: BaseSource = StaticSource([{"id": 1, "country": "PL"}])

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
        by_id = {r["id"]: r for r in self._lookup.fetch(runtime_params)}
        return [{**r, **by_id.get(r["id"], {})} for r in records]


def test_transformation_can_fetch_from_a_source() -> None:
    out = EnrichFromSource().apply([{"id": 1, "n": "a"}, {"id": 2, "n": "b"}], {})
    assert out == [{"id": 1, "n": "a", "country": "PL"}, {"id": 2, "n": "b"}]
