"""
Mock API Webhook Server for ApiDestination E2E Tests.

A minimal FastAPI server that receives webhook data, tracks received records,
and captures query params for verification.

Usage:
    python -m tests.e2e.destinations.mock_api_server

    # Or with uvicorn:
    uvicorn tests.e2e.destinations.mock_api_server:app --host 0.0.0.0 --port 8091
"""

import asyncio
import json
from collections import Counter
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
    """Receive a single verbatim request from ApiDestination with body={"records": [...], ...}."""
    _check_auth(authorization)

    query_params = dict(request.query_params)
    extra = {k: v for k, v in payload.model_extra.items()} if payload.model_extra else {}

    received_records.extend(payload.records)
    received_batches.append(
        ReceivedBatch(
            received_at=datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            record_count=len(payload.records),
            query_params=query_params,
            extra_body_fields=extra,
        )
    )

    return {
        "status": "received",
        "record_count": len(payload.records),
        "total_records": len(received_records),
    }


@app.get("/stats")
async def get_stats():
    # Tests attribute batches to their own run by filtering this window on
    # execution_id, so it has to be deep enough to still hold every batch of
    # that run after other tests have sent theirs. At 10 — one full run's worth
    # — a single interleaved batch evicted one of the caller's own, and the
    # filter could not tell "evicted" from "never delivered".
    return {
        "total_records": len(received_records),
        "total_batches": len(received_batches),
        "batches": [b.model_dump() for b in received_batches[-200:]],
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
    item_runs.clear()
    return {"status": "reset"}


# ============================================================================
# Per-record endpoint (api_destination(per_record=True))
# ============================================================================


class ItemRun:
    """What one run (a test's own key) sent to /items: every request, and the
    requests open at once per job — the cap a test checks ``concurrency`` against."""

    def __init__(self) -> None:
        self.received: List[Dict[str, Any]] = []
        self.in_flight: Counter = Counter()
        self.peak: Counter = Counter()


item_runs: Dict[str, ItemRun] = {}


@app.api_route("/items/{run}/{item_id}", methods=["PUT", "POST"])
async def receive_item(
    run: str,
    item_id: str,
    request: Request,
    delay_ms: int = 0,
    fail_id: str = "",
    job: str = "",
    authorization: Optional[str] = Header(None),
):
    """One record per request. ``delay_ms`` holds the request open so requests of the
    same job overlap; ``fail_id`` answers 503 for that item."""
    _check_auth(authorization)
    state = item_runs.setdefault(run, ItemRun())
    if item_id == fail_id:
        raise HTTPException(status_code=503, detail=f"injected failure for {item_id}")
    state.in_flight[job] += 1
    state.peak[job] = max(state.peak[job], state.in_flight[job])
    try:
        if delay_ms:
            await asyncio.sleep(delay_ms / 1000)
    finally:
        state.in_flight[job] -= 1
    raw = await request.body()
    try:
        body: Any = json.loads(raw) if raw else None
    except ValueError:
        body = raw.decode("utf-8", "replace")
    state.received.append(
        {"item_id": item_id, "method": request.method, "job": job, "body": body}
    )
    return {"status": "received", "item_id": item_id}


@app.get("/items/{run}")
async def get_items(run: str):
    state = item_runs.get(run, ItemRun())
    return {"received": state.received, "peak_by_job": dict(state.peak)}


@app.head("/items/{run}/")
async def items_head(run: str):
    """Health check probe: a per-record URL is probed up to its first placeholder."""
    return None


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
