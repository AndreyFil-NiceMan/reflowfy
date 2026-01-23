"""Source connectors with job splitting strategies."""

from reflowfy.sources.base import BaseSource, SourceJob, SourceError

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
]

# Add S3 exports only if available
if _s3_available:
    __all__.extend(["S3Source", "s3_source"])
