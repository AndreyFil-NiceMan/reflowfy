"""In-memory static source.

Wraps a pre-computed list of records as a :class:`BaseSource`, so a pipeline
that already has its data (e.g. an :class:`~reflowfy.core.id_based_pipeline.IdBasedPipeline`
whose ``define_source`` returns a list of IDs) can skip the fetch step and feed
those records straight into the transformation/destination chain.

This source is constructed internally by ``IdBasedPipeline.resolve_source`` when
``define_source`` returns a list; it is not registered as a user-facing
``@source`` connector.
"""

from typing import Any, Dict, Iterator, List, Optional

from reflowfy.sources.base import BaseSource, SourceJob


class StaticSource(BaseSource):
    """A source backed by an in-memory list of records.

    ``split_jobs`` yields exactly one :class:`SourceJob` containing all the
    records, so one resolution (e.g. one ID batch) maps to one job.
    """

    def __init__(self, records: List[Any]):
        """
        Initialize the static source.

        Args:
            records: The records this source serves. Used verbatim — values are
                not wrapped or transformed.
        """
        super().__init__(config={"records": list(records)})

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """Return the in-memory records (honoring ``limit`` if given)."""
        records: List[Any] = self.config["records"]
        if limit is not None:
            return records[:limit]
        return records

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """Yield a single job containing all records."""
        records: List[Any] = self.config["records"]
        yield SourceJob(records=records, metadata={"count": len(records)})

    def health_check(self) -> bool:
        """An in-memory source is always healthy."""
        return True
