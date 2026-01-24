"""Pydantic schemas for destination configurations.

These schemas provide runtime validation for destination configurations,
ensuring type safety and clear error messages.
"""

from typing import Any, Dict, Literal, Optional
from pydantic import BaseModel, Field, field_validator


class KafkaDestinationConfig(BaseModel):
    """Configuration for KafkaDestination."""
    
    bootstrap_servers: str = Field(..., description="Kafka broker addresses")
    topic: str = Field(..., description="Target Kafka topic")
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = Field(
        default="gzip", description="Compression algorithm"
    )
    batch_size: int = Field(default=16384, ge=1, description="Batch size in bytes")
    linger_ms: int = Field(default=10, ge=0, description="Time to wait before sending batch")
    
    @field_validator("bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("bootstrap_servers cannot be empty")
        return v.strip()
    
    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("topic cannot be empty")
        return v.strip()


class HttpDestinationConfig(BaseModel):
    """Configuration for HttpDestination."""
    
    url: str = Field(..., description="Target URL")
    method: Literal["POST", "PUT", "PATCH"] = Field(default="POST", description="HTTP method")
    headers: Dict[str, str] = Field(default_factory=dict, description="Custom headers")
    auth_type: Optional[Literal["bearer", "apikey", "basic"]] = Field(
        default=None, description="Authentication type"
    )
    auth_token: Optional[str] = Field(default=None, description="Auth token/credentials")
    timeout: float = Field(default=30.0, ge=1.0, le=300.0, description="Request timeout")
    batch_requests: bool = Field(
        default=False, description="Send all records in one request"
    )
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v


class ConsoleDestinationConfig(BaseModel):
    """Configuration for ConsoleDestination."""
    
    pretty_print: bool = Field(default=True, description="Pretty-print JSON output")
    max_records_display: int = Field(
        default=10, ge=1, le=1000, description="Maximum records to display"
    )


class RetryConfig(BaseModel):
    """Configuration for retry behavior."""
    
    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum retry attempts")
    min_wait_seconds: float = Field(default=1.0, ge=0.1, description="Minimum wait time")
    max_wait_seconds: float = Field(default=60.0, ge=1.0, description="Maximum wait time")
    multiplier: float = Field(default=2.0, ge=1.0, description="Backoff multiplier")
