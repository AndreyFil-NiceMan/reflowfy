"""Source connectors with job splitting strategies."""

from reflowfy.sources.base import BaseSource, SourceJob, SourceError

# API sources (httpx is a core dependency)
from reflowfy.sources.api import (
    IDBasedAPISource,
    id_based_api_source,
)

# Optional S3 source (requires boto3)
try:
    from reflowfy.sources.s3 import S3Source, s3_source
    _s3_available = True
except ImportError:
    S3Source = None
    s3_source = None
    _s3_available = False

__all__ = [
    "BaseSource",
    "SourceJob",
    "SourceError",
    "IDBasedAPISource",
    "id_based_api_source",
]

# Add S3 exports only if available
if _s3_available:
    __all__.extend(["S3Source", "s3_source"])
