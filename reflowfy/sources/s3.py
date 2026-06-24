"""AWS S3 source with pagination support."""

from typing import Any, Dict, Iterator, List, Optional
import boto3
from botocore.exceptions import ClientError
from reflowfy.sources.base import BaseSource, SourceJob, SourceError


class S3Source(BaseSource):
    """
    AWS S3 source connector with pagination.

    Supports:
    - Listing objects with prefix filtering
    - Continuation token-based pagination
    - Reading object contents (JSON, CSV, text)
    - Runtime parameter resolution (Jinja2)
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        file_pattern: Optional[str] = None,
        page_size: int = 1000,
        read_content: bool = True,
        content_type: str = "json",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize S3 source.

        Args:
            bucket: S3 bucket name
            prefix: Object key prefix filter
            file_pattern: Optional glob pattern for filtering (e.g., "*.json")
            page_size: Number of objects to list per page
            read_content: Whether to read object content or just metadata
            content_type: Content type (json, csv, text, binary)
            aws_access_key_id: AWS access key (uses env if not provided)
            aws_secret_access_key: AWS secret key (uses env if not provided)
            region_name: AWS region
            endpoint_url: Custom endpoint URL (for S3-compatible services)
            **kwargs: Additional boto3 client params
        """
        config = {
            "bucket": bucket,
            "prefix": prefix,
            "file_pattern": file_pattern,
            "page_size": page_size,
            "read_content": read_content,
            "content_type": content_type,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region_name": region_name,
            "endpoint_url": endpoint_url,
            **kwargs,
        }
        super().__init__(config)
        self._client = None

    def _get_client(self):
        """Get or create S3 client."""
        if self._client is None:
            client_kwargs = {
                "service_name": "s3",
                "region_name": self.config["region_name"],
            }

            if self.config.get("aws_access_key_id"):
                client_kwargs["aws_access_key_id"] = self.config["aws_access_key_id"]
                client_kwargs["aws_secret_access_key"] = self.config["aws_secret_access_key"]

            if self.config.get("endpoint_url"):
                client_kwargs["endpoint_url"] = self.config["endpoint_url"]

            self._client = boto3.client(**client_kwargs)

        return self._client

    def _matches_pattern(self, key: str) -> bool:
        """Check if key matches file pattern."""
        pattern = self.config.get("file_pattern")
        if not pattern:
            return True

        import fnmatch

        return fnmatch.fnmatch(key.split("/")[-1], pattern)

    def _read_object_content(self, key: str) -> Any:
        """Read and parse object content."""
        client = self._get_client()
        bucket = self.config["bucket"]
        content_type = self.config["content_type"]

        try:
            response = client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()

            if content_type == "json":
                import json

                return json.loads(body.decode("utf-8"))
            elif content_type == "csv":
                import csv
                import io

                reader = csv.DictReader(io.StringIO(body.decode("utf-8")))
                return list(reader)
            elif content_type == "text":
                return body.decode("utf-8")
            else:
                return body

        except ClientError as e:
            raise SourceError("s3", f"Failed to read object {key}: {e}", e)

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """
        Fetch data from S3 (local mode).

        Args:
            runtime_params: Runtime parameters for template resolution
            limit: Optional limit for testing

        Returns:
            List of records (object contents or metadata)
        """
        resolved_config = self.resolve_parameters(runtime_params)
        if resolved_config is None:
            raise SourceError("s3", "No valid configuration resolved", None)

        explicit_keys = resolved_config.get("keys")
        if explicit_keys:
            records: List[Any] = []
            for key in explicit_keys:
                if resolved_config["read_content"]:
                    content = self._read_object_content(key)
                    records.extend(content if isinstance(content, list) else [content])
                else:
                    records.append({"key": key})
                if limit and len(records) >= limit:
                    return records[:limit]
            return records

        client = self._get_client()

        bucket = resolved_config["bucket"]
        prefix = resolved_config["prefix"]
        read_content = resolved_config["read_content"]

        records = []

        try:
            paginator = client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                PaginationConfig={"PageSize": min(limit or 1000, 1000)},
            )

            for page in page_iterator:
                for obj in page.get("Contents", []):
                    if not self._matches_pattern(obj["Key"]):
                        continue

                    if read_content:
                        content = self._read_object_content(obj["Key"])
                        if isinstance(content, list):
                            records.extend(content)
                        else:
                            records.append(content)
                    else:
                        records.append(
                            {
                                "key": obj["Key"],
                                "size": obj["Size"],
                                "last_modified": obj["LastModified"].isoformat(),
                                "etag": obj["ETag"],
                            }
                        )

                    if limit and len(records) >= limit:
                        return records[:limit]

            return records

        except ClientError as e:
            raise SourceError("s3", f"Failed to fetch data: {e}", e)

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 1000
    ) -> Iterator[SourceJob]:
        """
        Split S3 objects into jobs using pagination.

        Each page of objects becomes one job.

        Args:
            runtime_params: Runtime parameters for template resolution
            batch_size: Objects per job (uses config page_size if not specified)

        Yields:
            SourceJob instances
        """
        resolved_config = self.resolve_parameters(runtime_params)
        if resolved_config is None:
            raise SourceError("s3", "No valid configuration resolved", None)
        client = self._get_client()

        bucket = resolved_config["bucket"]
        prefix = resolved_config["prefix"]
        page_size = resolved_config.get("page_size", batch_size)
        read_content = resolved_config["read_content"]

        try:
            paginator = client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": page_size}
            )

            page_num = 0

            for page in page_iterator:
                contents = page.get("Contents", [])
                if not contents:
                    continue

                # Filter by pattern
                filtered_objects = [obj for obj in contents if self._matches_pattern(obj["Key"])]

                if not filtered_objects:
                    continue

                # Build records
                records = []
                for obj in filtered_objects:
                    if read_content:
                        content = self._read_object_content(obj["Key"])
                        if isinstance(content, list):
                            records.extend(content)
                        else:
                            records.append(content)
                    else:
                        records.append(
                            {
                                "key": obj["Key"],
                                "size": obj["Size"],
                                "last_modified": obj["LastModified"].isoformat(),
                                "etag": obj["ETag"],
                            }
                        )

                yield SourceJob(
                    records=records,
                    metadata={
                        "page_num": page_num,
                        "bucket": bucket,
                        "prefix": prefix,
                        "object_count": len(filtered_objects),
                        "record_count": len(records),
                    },
                )

                page_num += 1

        except ClientError as e:
            raise SourceError("s3", f"Failed to split jobs: {e}", e)

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["S3Source"]:
        """List object keys (metadata-only) and yield page_size key batches."""
        resolved = self.resolve_parameters(runtime_params) or self.config
        client = self._get_client()
        page_size = resolved.get("page_size", 1000)
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=resolved["bucket"], Prefix=resolved["prefix"],
            PaginationConfig={"PageSize": page_size},
        )
        c = self.config
        for page in pages:
            keys = [o["Key"] for o in page.get("Contents", []) if self._matches_pattern(o["Key"])]
            if not keys:
                continue
            sub = S3Source(
                bucket=c["bucket"], prefix=c["prefix"], file_pattern=c["file_pattern"],
                page_size=page_size, read_content=c["read_content"],
                content_type=c["content_type"], region_name=c["region_name"],
                endpoint_url=c["endpoint_url"],
                aws_access_key_id=c["aws_access_key_id"],
                aws_secret_access_key=c["aws_secret_access_key"],
            )
            sub.config["keys"] = keys
            yield sub

    def health_check(self) -> bool:
        """Check S3 bucket accessibility."""
        try:
            client = self._get_client()
            client.head_bucket(Bucket=self.config["bucket"])
            return True
        except Exception:
            return False


def s3_source(
    bucket: str,
    prefix: str = "",
    file_pattern: Optional[str] = None,
    page_size: int = 1000,
    read_content: bool = True,
    content_type: str = "json",
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    region_name: str = "us-east-1",
    endpoint_url: Optional[str] = None,
    **kwargs,
) -> S3Source:
    """
    Factory function for S3 source.

    Example:
        >>> source = s3_source(
        ...     bucket="my-data-bucket",
        ...     prefix="logs/{{ date }}/",
        ...     file_pattern="*.json",
        ...     page_size=100,
        ...     read_content=True,
        ...     content_type="json"
        ... )
    """
    return S3Source(
        bucket=bucket,
        prefix=prefix,
        file_pattern=file_pattern,
        page_size=page_size,
        read_content=read_content,
        content_type=content_type,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        endpoint_url=endpoint_url,
        **kwargs,
    )
