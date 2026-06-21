"""When resolve_parameters() returns None, sources must raise a clean SourceError
instead of crashing with TypeError ('NoneType' is not subscriptable)."""

from unittest.mock import patch

import pytest

from reflowfy.sources.base import SourceError
from reflowfy.sources.sql import SqlSource
from reflowfy.sources.elastic import ElasticSource
from reflowfy.sources.s3 import S3Source


def _sources():
    return [
        SqlSource(connection_url="sqlite://", query="SELECT 1"),
        ElasticSource(url="http://localhost:9200", index="idx", base_query={}),
        S3Source(bucket="my-bucket"),
    ]


@pytest.mark.parametrize("source", _sources())
def test_fetch_raises_source_error_when_config_unresolved(source):
    with patch.object(source, "resolve_parameters", return_value=None):
        with pytest.raises(SourceError):
            source.fetch({})


@pytest.mark.parametrize("source", _sources())
def test_split_jobs_raises_source_error_when_config_unresolved(source):
    with patch.object(source, "resolve_parameters", return_value=None):
        with pytest.raises(SourceError):
            list(source.split_jobs({}))
