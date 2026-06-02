"""
Mock API Webhook Server for ApiDestination E2E Tests.

A minimal FastAPI server that receives webhook data, tracks received records,
and captures query params for verification.

Usage:
    python -m tests.e2e.destinations.mock_api_server

    # Or with uvicorn:
    uvicorn tests.e2e.destinations.mock_api_server:app --host 0.0.0.0 --port 8091
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel
import uvicorn

app = FastAPI(title="Mock API Webhook Server for E2E Tests")


# ============================================================================
# Data Models
# ============================================================================

class BatchPayload(BaseModel):
    records: List[Dict[str, Any]]
    model_config = {"extra": "allow"}


class SinglePayload(BaseModel):
    record: Dict[str, Any]
    model_config = {"extra": "allow"}


class ReceivedBatch(BaseModel):
    received_at: str
    record_count: int
    query_params: Dict[str, str]
    extra_body_fields: Dict[str, Any]


# ============================================================================
# In-Memory Storage
# ============================================================================

received_records: List[Dict[str, Any]] = []
received_batches: List[ReceivedBatch] = []
VALID_TOKEN = "test-webhook-token"


# ============================================================================
# Helpers
# ============================================================================

def _check_auth(authorization: Optional[str]) -> None:
    if authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid Authorization format")
        if authorization.removeprefix("Bearer ") != VALID_TOKEN:
            raise HTTPException(status_code=403, detail="Invalid token")


# ============================================================================
# Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "mock-api-webhook-server",
        "total_records": len(received_records),
        "total_batches": len(received_batches),
    }


@app.post("/webhook")
async def receive_batch(
    request: Request,
    payload: BatchPayload,
    authorization: Optional[str] = Header(None),
):
    """Receive batched records from ApiDestination (batch_requests=True)."""
    _check_auth(authorization)

    query_params = dict(request.query_params)
    extra = {k: v for k, v in payload.model_extra.items()} if payload.model_extra else {}

    received_records.extend(payload.records)
    received_batches.append(ReceivedBatch(
        received_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        record_count=len(payload.records),
        query_params=query_params,
        extra_body_fields=extra,
    ))

    return {
        "status": "received",
        "record_count": len(payload.records),
        "total_records": len(received_records),
    }


@app.post("/webhook/single")
async def receive_single(
    request: Request,
    payload: SinglePayload,
    authorization: Optional[str] = Header(None),
):
    """Receive individual records from ApiDestination (batch_requests=False)."""
    _check_auth(authorization)

    query_params = dict(request.query_params)
    extra = {k: v for k, v in payload.model_extra.items()} if payload.model_extra else {}

    received_records.append(payload.record)
    received_batches.append(ReceivedBatch(
        received_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        record_count=1,
        query_params=query_params,
        extra_body_fields=extra,
    ))

    return {
        "status": "received",
        "total_records": len(received_records),
    }


@app.get("/stats")
async def get_stats():
    return {
        "total_records": len(received_records),
        "total_batches": len(received_batches),
        "batches": [b.model_dump() for b in received_batches[-10:]],
    }


@app.get("/records")
async def get_records(limit: int = 100, offset: int = 0):
    end = min(offset + limit, len(received_records))
    return {
        "total": len(received_records),
        "offset": offset,
        "limit": limit,
        "records": received_records[offset:end],
    }


@app.delete("/reset")
async def reset_data():
    global received_records, received_batches
    received_records = []
    received_batches = []
    return {"status": "reset"}


@app.head("/webhook")
async def webhook_head():
    """Health check probe used by ApiDestination."""
    return None


@app.options("/webhook")
async def webhook_options():
    """Options probe fallback used by ApiDestination."""
    return {"methods": ["POST", "HEAD", "OPTIONS"]}


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("Starting Mock API Webhook Server on port 8091...")
    uvicorn.run(app, host="0.0.0.0", port=8091)
