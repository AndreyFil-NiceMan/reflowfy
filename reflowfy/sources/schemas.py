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


class PaginatedAPISourceConfig(BaseModel):
    """Configuration for PaginatedAPISource."""
    
    base_url: str = Field(..., description="Base URL of the API")
    endpoint: str = Field(..., description="API endpoint path")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    auth_type: Optional[Literal["bearer", "apikey", "basic"]] = Field(
        default=None, description="Authentication type"
    )
    auth_token: Optional[str] = Field(default=None, description="Auth token/credentials")
    pagination_type: Literal["offset", "page", "cursor", "link"] = Field(
        default="offset", description="Pagination strategy"
    )
    page_size: int = Field(default=100, ge=1, le=10000, description="Records per page")
    data_key: str = Field(default="data", description="JSON key containing records")
    timeout: float = Field(default=30.0, ge=1.0, description="Request timeout in seconds")
    
    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v.rstrip("/")


class IDBasedAPISourceConfig(BaseModel):
    """Configuration for IDBasedAPISource."""
    
    base_url: str = Field(..., description="Base URL of the API")
    endpoint_template: str = Field(..., description="Endpoint with {id} placeholder")
    ids: List[Any] = Field(..., min_length=1, description="List of IDs to fetch")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    auth_type: Optional[Literal["bearer", "apikey", "basic"]] = Field(default=None)
    auth_token: Optional[str] = Field(default=None)
    batch_size: int = Field(default=100, ge=1, le=10000, description="IDs per job")
    timeout: float = Field(default=30.0, ge=1.0)
    
    @field_validator("endpoint_template")
    @classmethod
    def validate_endpoint_template(cls, v: str) -> str:
        if "{id}" not in v:
            raise ValueError("endpoint_template must contain {id} placeholder")
        return v


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
