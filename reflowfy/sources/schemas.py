"""Pydantic schemas for source configurations.

These schemas provide runtime validation for source configurations,
ensuring type safety and clear error messages.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class S3SourceConfig(BaseModel):
    """Configuration for S3Source."""

    bucket: str = Field(..., description="S3 bucket name")
    prefix: str = Field(default="", description="Key prefix to filter objects")
    pattern: str = Field(default="*", description="Glob pattern for filtering files")
    region: Optional[str] = Field(default=None, description="AWS region")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret key")
    page_size: int = Field(default=1000, ge=1, le=10000, description="Objects per page")

    @field_validator("bucket")
    @classmethod
    def bucket_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("bucket cannot be empty")
        return v.strip()


class IDBasedAPISourceConfig(BaseModel):
    """Configuration for IDBasedAPISource."""

    base_url: str = Field(..., description="Base URL of the API")
    endpoint_template: str = Field(..., description="Endpoint path; include {id} for per-ID mode")
    ids: List[Any] = Field(default_factory=list, description="Static list of IDs to fetch")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        default="GET", description="HTTP method"
    )
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    auth_type: Optional[Literal["bearer", "apikey", "basic"]] = Field(default=None)
    auth_token: Optional[str] = Field(default=None)
    batch_size: int = Field(
        default=50,
        ge=1,
        le=10000,
        description="IDs per job (per-ID mode) or records per job (batch mode)",
    )
    timeout: float = Field(default=30.0, ge=1.0)
    response_key: Optional[str] = Field(
        default=None,
        description="Dotted response key to extract records list; None means response is the list",
    )
    body: Optional[Any] = Field(
        default=None, description="Request body sent verbatim (dict or list); None sends no body"
    )
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Extra query-string parameters appended to every request"
    )
    health_check_enabled: bool = Field(
        default=True, description="Enable/disable source health check"
    )


class ElasticsearchSourceConfig(BaseModel):
    """Configuration for ElasticsearchSource."""

    url: str = Field(..., description="Elasticsearch URL")
    index: str = Field(..., description="Index name or pattern")
    query: Dict[str, Any] = Field(default_factory=dict, description="Elasticsearch query")
    page_size: int = Field(default=1000, ge=1, le=10000, description="Documents per page")
    scroll_time: str = Field(default="5m", description="Scroll context timeout")

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v.rstrip("/")


class SQLSourceConfig(BaseModel):
    """Configuration for SQLSource."""

    connection_string: str = Field(..., description="Database connection string")
    query: str = Field(..., description="SQL query to execute")
    page_size: int = Field(default=1000, ge=1, le=100000, description="Rows per batch")
    id_column: Optional[str] = Field(default=None, description="Column for range-based pagination")

    @field_validator("query")
    @classmethod
    def validate_query(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("query cannot be empty")
        return v.strip()
