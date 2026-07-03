"""Pydantic schemas for destination configurations.

These schemas provide runtime validation for destination configurations,
ensuring type safety and clear error messages.
"""

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class ApiDestinationConfig(BaseModel):
    """Configuration for ApiDestination."""

    url: str = Field(..., description="Target URL")
    method: Literal["POST", "PUT", "PATCH"] = Field(default="POST", description="HTTP method")
    headers: Dict[str, str] = Field(default_factory=dict, description="Custom headers")
    auth_type: Optional[Literal["bearer", "apikey", "basic"]] = Field(
        default=None, description="Authentication type"
    )
    auth_token: Optional[str] = Field(default=None, description="Auth token/credentials")
    timeout: float = Field(default=30.0, ge=1.0, le=300.0, description="Request timeout")
    params: Optional[Dict[str, str]] = Field(
        default=None, description="URL query parameters appended to every request"
    )
    body: Optional[Any] = Field(
        default=None,
        description=(
            "Request body sent verbatim. dict/list -> JSON; str -> raw body "
            "(set Content-Type via headers); None sends no body"
        ),
    )
    health_check_enabled: bool = Field(
        default=True, description="Enable/disable destination health check"
    )

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v
