"""Unit tests for S3Source."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

boto3 = pytest.importorskip("boto3")

from reflowfy.sources.s3 import S3Source, s3_source


class TestS3Source:
    """Tests for S3Source class."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client."""
        with patch("boto3.client") as mock_boto:
            mock_client = MagicMock()
            mock_boto.return_value = mock_client
            yield mock_client

    @pytest.fixture
    def sample_objects(self):
        """Sample S3 object listing response."""
        return {
            "Contents": [
                {
                    "Key": "data/file1.json",
                    "Size": 1024,
                    "LastModified": datetime(2024, 1, 15, 10, 30, 0),
                    "ETag": '"abc123"',
                },
                {
                    "Key": "data/file2.json",
                    "Size": 2048,
                    "LastModified": datetime(2024, 1, 15, 11, 0, 0),
                    "ETag": '"def456"',
                },
                {
                    "Key": "data/readme.txt",
                    "Size": 512,
                    "LastModified": datetime(2024, 1, 15, 9, 0, 0),
                    "ETag": '"ghi789"',
                },
            ]
        }

    def test_init(self):
        """Test S3Source initialization."""
        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            page_size=100,
        )

        assert source.config["bucket"] == "test-bucket"
        assert source.config["prefix"] == "data/"
        assert source.config["page_size"] == 100
        assert source.config["read_content"] is True
        assert source.config["content_type"] == "json"

    def test_factory_function(self):
        """Test s3_source factory function."""
        source = s3_source(
            bucket="my-bucket",
            prefix="logs/",
            file_pattern="*.json",
        )

        assert isinstance(source, S3Source)
        assert source.config["bucket"] == "my-bucket"
        assert source.config["file_pattern"] == "*.json"

    def test_matches_pattern_no_pattern(self):
        """Test pattern matching when no pattern is set."""
        source = S3Source(bucket="test", prefix="")
        assert source._matches_pattern("any/file.json") is True
        assert source._matches_pattern("file.csv") is True

    def test_matches_pattern_with_pattern(self):
        """Test pattern matching with glob pattern."""
        source = S3Source(bucket="test", prefix="", file_pattern="*.json")
        assert source._matches_pattern("data/file.json") is True
        assert source._matches_pattern("file.csv") is False
        assert source._matches_pattern("file.json") is True

    def test_health_check_success(self, mock_s3_client):
        """Test health check when bucket is accessible."""
        mock_s3_client.head_bucket.return_value = {}

        source = S3Source(bucket="test-bucket", prefix="")
        assert source.health_check() is True

        mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

    def test_health_check_failure(self, mock_s3_client):
        """Test health check when bucket is not accessible."""
        mock_s3_client.head_bucket.side_effect = Exception("Access denied")

        source = S3Source(bucket="test-bucket", prefix="")
        assert source.health_check() is False

    def test_fetch_metadata_only(self, mock_s3_client, sample_objects):
        """Test fetching S3 object metadata only."""
        # Setup paginator mock
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [sample_objects]
        mock_s3_client.get_paginator.return_value = mock_paginator

        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            read_content=False,
        )

        records = source.fetch({})

        assert len(records) == 3
        assert records[0]["key"] == "data/file1.json"
        assert records[0]["size"] == 1024
        assert "last_modified" in records[0]

    def test_fetch_with_limit(self, mock_s3_client, sample_objects):
        """Test fetching with limit."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [sample_objects]
        mock_s3_client.get_paginator.return_value = mock_paginator

        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            read_content=False,
        )

        records = source.fetch({}, limit=2)

        assert len(records) == 2

    def test_fetch_with_pattern_filter(self, mock_s3_client, sample_objects):
        """Test fetching with file pattern filter."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [sample_objects]
        mock_s3_client.get_paginator.return_value = mock_paginator

        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            file_pattern="*.json",
            read_content=False,
        )

        records = source.fetch({})

        # Should only include .json files
        assert len(records) == 2
        assert all(r["key"].endswith(".json") for r in records)

    def test_fetch_json_content(self, mock_s3_client, sample_objects):
        """Test fetching and parsing JSON content."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [sample_objects]
        mock_s3_client.get_paginator.return_value = mock_paginator

        # Mock get_object to return JSON content
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(read=lambda: b'{"name": "test", "value": 123}')
        }

        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            file_pattern="*.json",
            read_content=True,
            content_type="json",
        )

        records = source.fetch({})

        # Each JSON file becomes a record
        assert len(records) == 2
        assert records[0]["name"] == "test"
        assert records[0]["value"] == 123

    def test_split_jobs_pagination(self, mock_s3_client):
        """Test job splitting with pagination."""
        # Create two pages of results
        page1 = {
            "Contents": [
                {
                    "Key": "data/file1.json",
                    "Size": 100,
                    "LastModified": datetime.now(),
                    "ETag": '"a"',
                },
                {
                    "Key": "data/file2.json",
                    "Size": 100,
                    "LastModified": datetime.now(),
                    "ETag": '"b"',
                },
            ]
        }
        page2 = {
            "Contents": [
                {
                    "Key": "data/file3.json",
                    "Size": 100,
                    "LastModified": datetime.now(),
                    "ETag": '"c"',
                },
            ]
        }

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [page1, page2]
        mock_s3_client.get_paginator.return_value = mock_paginator

        source = S3Source(
            bucket="test-bucket",
            prefix="data/",
            read_content=False,
            page_size=2,
        )

        jobs = list(source.split_jobs({}))

        assert len(jobs) == 2
        assert jobs[0].metadata["page_num"] == 0
        assert jobs[0].metadata["object_count"] == 2
        assert jobs[1].metadata["page_num"] == 1
        assert jobs[1].metadata["object_count"] == 1

    def test_split_jobs_empty_bucket(self, mock_s3_client):
        """Test job splitting with empty bucket."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"Contents": []}]
        mock_s3_client.get_paginator.return_value = mock_paginator

        source = S3Source(bucket="empty-bucket", prefix="")

        jobs = list(source.split_jobs({}))

        assert len(jobs) == 0

    def test_runtime_parameter_resolution(self, mock_s3_client, sample_objects):
        """Test that runtime parameters are resolved in prefix."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [sample_objects]
        mock_s3_client.get_paginator.return_value = mock_paginator

        source = S3Source(
            bucket="test-bucket",
            prefix="logs/{{ date }}/",
            read_content=False,
        )

        # Resolve parameters and verify
        resolved = source.resolve_parameters({"date": "2024-01-15"})
        assert resolved["prefix"] == "logs/2024-01-15/"

    def test_custom_endpoint_url(self, mock_s3_client):
        """Test custom endpoint URL for S3-compatible services."""
        source = S3Source(
            bucket="test-bucket",
            prefix="",
            endpoint_url="http://minio:9000",
        )

        assert source.config["endpoint_url"] == "http://minio:9000"


class TestS3SourceIntegration:
    """Integration-style tests (still mocked but more realistic)."""

    def test_full_pipeline_simulation(self, mocker):
        """Simulate a full pipeline flow with mocked S3."""
        # This would be expanded for actual integration testing
        pass
