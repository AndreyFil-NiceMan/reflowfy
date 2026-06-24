"""Mock data source for testing without external dependencies."""

from typing import Any, Dict, Iterator, List, Optional
from reflowfy.sources.base import BaseSource, SourceJob


class MockSource(BaseSource):
    """
    Mock data source that generates fake data.

    Perfect for testing without external dependencies like Elasticsearch or databases.
    """

    def __init__(
        self,
        data: List[Dict[str, Any]],
        batch_size: int = 10,
    ):
        """
        Initialize mock source with sample data.

        Args:
            data: List of records to return
            batch_size: Records per batch for job splitting
        """
        config = {
            "data": data,
            "batch_size": batch_size,
        }
        super().__init__(config)

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch data from mock source.

        Args:
            runtime_params: Runtime parameters (not used in mock)
            limit: Optional limit

        Returns:
            List of mock records
        """
        data = self.config["data"]

        if limit:
            return data[:limit]

        return data

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split mock data into jobs.

        Args:
            runtime_params: Runtime parameters
            batch_size: Records per job

        Yields:
            SourceJob instances
        """
        data = self.config["data"]
        batch_size = self.config.get("batch_size", batch_size)

        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]

            yield SourceJob(
                records=batch,
                metadata={
                    "batch_num": i // batch_size,
                    "count": len(batch),
                },
            )

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["MockSource"]:
        """Slice the in-memory data into batch_size-sized MockSources."""
        data = self.config["data"]
        size = self.config["batch_size"]

        if len(data) <= size:
            yield self
            return

        for i in range(0, len(data), size):
            yield MockSource(data=data[i : i + size], batch_size=size)

    def health_check(self) -> bool:
        """Mock source is always healthy."""
        return True


def mock_source(data: List[Dict[str, Any]], batch_size: int = 10) -> MockSource:
    """
    Factory function for mock source.

    Example:
        >>> source = mock_source([
        ...     {"id": 1, "name": "Alice", "age": 30},
        ...     {"id": 2, "name": "Bob", "age": 25},
        ... ])
    """
    return MockSource(data=data, batch_size=batch_size)


def generate_sample_data(count: int = 100) -> List[Dict[str, Any]]:
    """
    Generate sample data for testing.

    Args:
        count: Number of records to generate

    Returns:
        List of sample records
    """
    import random

    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"]

    data = []
    for i in range(count):
        data.append({
            "id": i + 1,
            "first_name": random.choice(first_names),
            "last_name": random.choice(last_names),
            "email": f"user{i+1}@example.com",
            "age": random.randint(18, 70),
            "city": random.choice(cities),
            "salary": random.randint(30000, 150000),
            "active": random.choice([True, False]),
        })

    return data
