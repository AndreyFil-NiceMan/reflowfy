"""Several destinations for one job: ``fan_out``.

Return a list, or ``fan_out(...)`` to mark some optional, from ``define_destination``
and every record goes to each destination::

    def define_destination(self, records, runtime_params):
        archive = kafka_destination(bootstrap_servers=..., topic="orders-archive")
        audit = api_destination(url="https://audit.example.com/events", per_record=True)
        return fan_out(archive, audit, optional=[audit])

It is a :class:`~reflowfy.destinations.record.RecordDestination` that sends every
record to all of them, so everything a record destination does applies: each
destination is sent and retried on its own (one that fails does not re-send the
others), connections are shared, content dedup hashes each (record, destination) pair
— a record already written to one is still written to the other. A required
destination that fails fails the job; an ``optional`` one is logged and counted
(``destinations_failed_optional`` in the job's stats) and the job completes.

A ``@record_destination`` function may return ``fan_out(...)`` (or a list) as well, for
a per-record multicast.
"""

from typing import Any, ClassVar, Iterable, List, Sequence, Union, cast

from reflowfy.destinations.base import BaseDestination
from reflowfy.destinations.record import RecordDestination, is_routable

DestinationOrList = Union[BaseDestination, Sequence[BaseDestination]]


class FanOut(RecordDestination):
    """Every record to every one of ``destinations``; ``optional`` ones may fail."""

    name: ClassVar[str] = "fan_out"

    def __init__(
        self,
        destinations: Sequence[BaseDestination],
        optional: Iterable[BaseDestination] = (),
    ) -> None:
        super().__init__()
        if not destinations:
            raise ValueError("fan_out: give at least one destination")
        for destination in destinations:
            if not is_routable(destination):  # an untyped caller can pass anything
                raise TypeError(
                    f"fan_out: {type(destination).__name__} is not a destination it can send "
                    "to; give destinations such as kafka_destination(...) (not another "
                    "fan_out or @record_destination)"
                )
        self._targets: List[BaseDestination] = list(destinations)
        target_ids = {id(d) for d in self._targets}
        self._optional = {id(d) for d in optional}
        if not self._optional <= target_ids:
            raise ValueError("fan_out: every optional= destination must also be passed to fan_out")

    def targets(self) -> List[tuple[BaseDestination, bool]]:
        """(destination, optional) for each destination, in the order given."""
        return [(d, id(d) in self._optional) for d in self._targets]

    def destination_for_record(self, record: Any, runtime_params: Any) -> Any:
        return self


def fan_out(
    *destinations: DestinationOrList,
    optional: Iterable[BaseDestination] = (),
) -> FanOut:
    """Send every record to each of ``destinations``.

    Args:
        *destinations: Destinations, or one list of them.
        optional: Those of ``destinations`` whose failure is logged and counted
            (``destinations_failed_optional``) instead of failing the job.
    """
    flat: List[BaseDestination] = []
    for item in destinations:
        if isinstance(item, (list, tuple)):
            flat.extend(cast(Sequence[BaseDestination], item))
        else:
            flat.append(cast(BaseDestination, item))
    return FanOut(flat, optional)


def as_destination(produced: Any) -> Any:
    """What ``define_destination`` returned, with a list or tuple made a :func:`fan_out`."""
    if isinstance(produced, (list, tuple)):
        return fan_out(*cast(Sequence[BaseDestination], produced))
    return produced
