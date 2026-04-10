"""
Mock HTTP Webhook Server for HTTP Destination E2E Tests.

A minimal FastAPI server that receives webhook data and tracks received records.
Used to verify that HttpDestination sends data correctly.

Usage:
    # Start the server:
    python -m tests.e2e.destinations.mock_http_server
    
    # Or with uvicorn:
    uvicorn tests.e2e.destinations.mock_http_server:app --host 0.0.0.0 --port 8091
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI(title="Mock HTTP Webhook Server for E2E Tests")


# ============================================================================
# Data Models
# ============================================================================

class WebhookPayload(BaseModel):
    records: List[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]] = None


class SingleRecordPayload(BaseModel):
    record: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None


class ReceivedBatch(BaseModel):
    received_at: str
    record_count: int
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# In-Memory Storage
# ============================================================================

# Store received records for verification
received_records: List[Dict[str, Any]] = []
received_batches: List[ReceivedBatch] = []
VALID_TOKEN = "test-webhook-token"


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "mock-http-webhook-server",
        "total_records_received": len(received_records),
        "total_batches_received": len(received_batches),
    }


@app.post("/webhook")
async def receive_webhook(
    payload: WebhookPayload,
    authorization: Optional[str] = Header(None),
):
    """
    Receive batch webhook data.
    
    This endpoint receives batched records from HttpDestination.
    """
    # Validate authentication (optional but good to test)
    if authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid Authorization format")
        
        token = authorization.replace("Bearer ", "")
        if token != VALID_TOKEN:
            raise HTTPException(status_code=403, detail="Invalid token")
    
    # Store records
    received_records.extend(payload.records)
    
    # Track batch
    batch = ReceivedBatch(
        received_at=datetime.utcnow().isoformat(),
        record_count=len(payload.records),
        metadata=payload.metadata,
    )
    received_batches.append(batch)
    
    print(f"✅ Received batch: {len(payload.records)} records")
    
    return {
        "status": "received",
        "record_count": len(payload.records),
        "total_records": len(received_records),
    }


@app.post("/webhook/single")
async def receive_single_record(
    payload: SingleRecordPayload,
    authorization: Optional[str] = Header(None),
):
    """
    Receive single record webhook data.
    
    This endpoint receives individual records from HttpDestination
    when batch_requests=False.
    """
    # Validate authentication
    if authorization:
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Invalid Authorization format")
        
        token = authorization.replace("Bearer ", "")
        if token != VALID_TOKEN:
            raise HTTPException(status_code=403, detail="Invalid token")
    
    # Store record
    received_records.append(payload.record)
    
    return {
        "status": "received",
        "total_records": len(received_records),
    }


@app.get("/stats")
async def get_stats():
    """Get statistics about received data."""
    return {
        "total_records": len(received_records),
        "total_batches": len(received_batches),
        "batches": [b.model_dump() for b in received_batches[-10:]],  # Last 10 batches
    }


@app.get("/records")
async def get_records(
    limit: int = 100,
    offset: int = 0,
):
    """Get received records for verification."""
    end = min(offset + limit, len(received_records))
    return {
        "total": len(received_records),
        "offset": offset,
        "limit": limit,
        "records": received_records[offset:end],
    }


@app.delete("/reset")
async def reset_data():
    """Reset all received data. Used between tests."""
    global received_records, received_batches
    received_records = []
    received_batches = []
    
    return {"status": "reset", "message": "All data cleared"}


@app.head("/webhook")
async def webhook_health():
    """HEAD request for health check by HttpDestination."""
    return None


@app.options("/webhook")
async def webhook_options():
    """OPTIONS request for health check by HttpDestination."""
    return {"methods": ["POST", "HEAD", "OPTIONS"]}


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("🚀 Starting Mock HTTP Webhook Server for E2E Tests...")
    print("   Endpoints:")
    print("     - POST /webhook (batch records)")
    print("     - POST /webhook/single (individual records)")
    print("     - GET /stats (statistics)")
    print("     - GET /records (view received records)")
    print("     - DELETE /reset (clear data)")
    print("     - GET /health")
    print("")
    
    uvicorn.run(app, host="0.0.0.0", port=8091)
