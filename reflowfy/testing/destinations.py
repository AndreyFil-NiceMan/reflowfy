"""A destination that keeps what it was sent, for tests."""

from typing import Any, Dict, List, NamedTuple, Optional

from reflowfy.destinations.base import BaseDestination, RetryConfig


class Sent(NamedTuple):
    """One ``send()`` call: the records of one job and the params it ran with."""

    records: List[Any]
    metadata: Optional[Dict[str, Any]]


class CaptureDestination(BaseDestination):
    """Collects records in memory instead of writing them anywhere.

    ``run_pipeline`` sends to one of these by default. Return it from your own
    ``define_destination`` when a test should not depend on the real one, or pass
    one in to test a failing destination:

        >>> flaky = CaptureDestination(fail_times=1, error=DestinationError("x", "down"))
        >>> result = run_pipeline("orders", records=[...], destination=flaky)

    ``fail_times`` sends fail with ``error`` before one succeeds; a
    ``DestinationError`` is retried the way production retries it (without the
    wait), anything else fails the job at once.
    """

    def __init__(self, fail_times: int = 0, error: Optional[Exception] = None):
        super().__init__(
            config={"type": "capture"},
            retry_config=RetryConfig(min_wait_seconds=0.0, max_wait_seconds=0.0, multiplier=0.0),
        )
        self.sends: List[Sent] = []
        self.attempts = 0
        self._fail_times = fail_times
        self._error = error

    @property
    def records(self) -> List[Any]:
        """Every record sent so far, across all jobs, in order."""
        return [record for sent in self.sends for record in sent.records]

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        self.attempts += 1
        if self.attempts <= self._fail_times:
            raise self._error or RuntimeError("CaptureDestination was told to fail")
        self.sends.append(Sent(list(records), metadata))

    async def health_check(self) -> bool:
        return True
