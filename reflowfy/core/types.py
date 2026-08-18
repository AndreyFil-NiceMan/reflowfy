"""Names for the values that flow through a pipeline.

A record is whatever a source yields — almost always a JSON-ish dict. These
aliases exist so pipeline and transformation authors can annotate the `records`
argument in one token instead of writing `List[Dict[str, Any]]` everywhere::

    from reflowfy import Records, RuntimeParams, transformation

    @transformation("stamp")
    def stamp(records: Records, runtime_params: RuntimeParams) -> Records:
        for record in records:
            record["seen"] = True
        return records

The framework's own signatures stay `List[Any]`, because a source may yield
non-dict records (raw S3 text, scalars). `List[Any]` accepts a `Records`
override, so annotating your hooks is always safe.
"""

from typing import Any, Dict, List, Sequence

from reflowfy.transformations.base import BaseTransformation

Record = Dict[str, Any]
"""One record: a JSON-ish mapping."""

Records = List[Record]
"""A batch of records — what every hook receives and returns."""

Transformations = Sequence[BaseTransformation]
"""What ``define_transformations`` returns."""
