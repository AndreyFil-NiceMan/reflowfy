"""Pydantic schemas for source configurations.

These schemas provide runtime validation for source configurations,
ensuring type safety and clear error messages.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


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
        default=None,
        description=(
            "Request body sent verbatim. dict/list -> JSON; str -> raw body "
            "(set Content-Type via headers); None sends no body"
        ),
    )
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Extra query-string parameters appended to every request"
    )
    health_check_enabled: bool = Field(
        default=True, description="Enable/disable source health check"
    )
