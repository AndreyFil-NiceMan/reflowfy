"""
Mock API Server for API Source E2E Tests.

A minimal FastAPI server that provides paginated API endpoints
for testing PaginatedAPISource and IDBasedAPISource.

Usage:
    python -m tests.e2e.sources.mock_api_server
    
Endpoints:
    GET /users - Paginated user list
    GET /users/{id} - Get user by ID
    GET /products - Paginated products with cursor pagination
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, Header, Request
from pydantic import BaseModel
import uvicorn


class BatchRequest(BaseModel):
    ids: List[int]


class BulkPatchRequest(BaseModel):
    ids: List[int]
    active_only: bool = False


class ProductLookupRequest(BaseModel):
    product_ids: List[str]

app = FastAPI(title="Mock API Server for E2E Tests")


# ============================================================================
# Sample Data
# ============================================================================

USERS = [
    {"id": i, "name": f"User {i}", "email": f"user{i}@example.com", "active": i % 3 != 0}
    for i in range(1, 101)  # 100 users
]

PRODUCTS = [
    {"id": f"prod_{i}", "name": f"Product {i}", "price": 10.0 + i, "category": ["A", "B", "C"][i % 3]}
    for i in range(1, 51)  # 50 products
]


# ============================================================================
# Data Models
# ============================================================================

class PaginatedResponse(BaseModel):
    data: List[Dict[str, Any]]
    total: int
    offset: int
    limit: int
    has_more: bool


class CursorPaginatedResponse(BaseModel):
    data: List[Dict[str, Any]]
    total: int
    next_cursor: Optional[str] = None


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "mock-api-server",
        "users_count": len(USERS),
        "products_count": len(PRODUCTS),
    }


@app.get("/users")
async def list_users(
    offset: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    active: Optional[bool] = None,
    authorization: Optional[str] = Header(None),
):
    """
    List users with offset pagination.
    
    Query params:
        offset: Starting offset (default: 0)
        limit: Number of records (default: 10, max: 100)
        active: Filter by active status
    """
    filtered = USERS
    
    if active is not None:
        filtered = [u for u in filtered if u["active"] == active]
    
    total = len(filtered)
    paginated = filtered[offset:offset + limit]
    
    return {
        "data": paginated,
        "total": total,
        "offset": offset,
        "limit": limit,
        "has_more": offset + limit < total,
    }


@app.get("/users/{user_id}")
async def get_user(
    user_id: int,
    authorization: Optional[str] = Header(None),
):
    """Get a single user by ID."""
    user = next((u for u in USERS if u["id"] == user_id), None)
    
    if not user:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    
    return user


@app.get("/products")
async def list_products(
    cursor: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    category: Optional[str] = None,
):
    """
    List products with cursor pagination.
    
    Query params:
        cursor: Cursor token for pagination
        limit: Number of records (default: 10, max: 50)
        category: Filter by category
    """
    filtered = PRODUCTS
    
    if category:
        filtered = [p for p in filtered if p["category"] == category]
    
    # Decode cursor (format: "idx_{index}")
    start_idx = 0
    if cursor:
        try:
            start_idx = int(cursor.split("_")[1])
        except (IndexError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid cursor")
    
    end_idx = start_idx + limit
    paginated = filtered[start_idx:end_idx]
    
    # Generate next cursor if more data exists
    next_cursor = None
    if end_idx < len(filtered):
        next_cursor = f"idx_{end_idx}"
    
    return {
        "data": paginated,
        "total": len(filtered),
        "next_cursor": next_cursor,
    }


@app.get("/products/{product_id}")
async def get_product(product_id: str):
    """Get a single product by ID."""
    product = next((p for p in PRODUCTS if p["id"] == product_id), None)
    
    if not product:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    
    return product


@app.post("/users/search")
async def search_users_raw(request: Request):
    """
    Fetch users by a **raw JSON array** body: ``[1, 2, 3]``

    Tests ``batch_id_key=None`` in IDBasedAPISource — the body is sent
    as a plain list, not wrapped in an object.

    Returns:
        {"results": [...matched users...], "total": N}
    """
    body = await request.json()
    if not isinstance(body, list):
        raise HTTPException(status_code=400, detail="Expected a JSON array body, e.g. [1, 2, 3]")
    id_set = set(body)
    matched = [u for u in USERS if u["id"] in id_set]
    return {"results": matched, "total": len(matched)}


@app.patch("/users/bulk")
async def bulk_patch_users(body: BulkPatchRequest):
    """
    Bulk-fetch users with optional active filter.

    Tests ``method="PATCH"`` + ``request_body`` merging in IDBasedAPISource.

    Request body: ``{"ids": [...], "active_only": bool}``
    Returns:
        {"updated": [...active/all matched users...], "skipped": [...inactive...], "count": N}
    """
    id_set = set(body.ids)
    matched = [u for u in USERS if u["id"] in id_set]
    if body.active_only:
        updated = [u for u in matched if u["active"]]
        skipped = [u for u in matched if not u["active"]]
    else:
        updated = matched
        skipped = []
    return {"updated": updated, "skipped": skipped, "count": len(updated)}


@app.post("/users/{user_id}/enrich")
async def enrich_user(user_id: int, request: Request):
    """
    Per-ID POST endpoint that returns enriched user data.

    Tests per-ID POST mode (``{id}`` in endpoint template, ``method="POST"``).

    Request body: ``{"context": "...", "source_id": "..."}`` (any JSON object)
    Returns: user record with an added ``enrichment`` sub-object.
    """
    user = next((u for u in USERS if u["id"] == user_id), None)
    if not user:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    body = await request.json()
    return {
        **user,
        "enrichment": {
            "context": body.get("context", ""),
            "source_id": body.get("source_id", str(user_id)),
            "score": round(user_id * 1.5, 2),
        },
    }


@app.post("/products/lookup")
async def lookup_products(body: ProductLookupRequest):
    """
    Batch-lookup products using a custom body key (``product_ids``).

    Tests ``batch_id_key="product_ids"`` in IDBasedAPISource.

    Request body: ``{"product_ids": ["prod_1", "prod_2", ...]}``
    Returns:
        {"items": [...found products...], "not_found": [...missing IDs...], "count": N}
    """
    id_set = set(body.product_ids)
    items = [p for p in PRODUCTS if p["id"] in id_set]
    found_ids = {p["id"] for p in items}
    not_found = [pid for pid in body.product_ids if pid not in found_ids]
    return {"items": items, "not_found": not_found, "count": len(items)}


@app.post("/users/batch")
async def batch_users(request: BatchRequest):
    """
    Fetch multiple users by a list of IDs (batch POST).

    Request body:
        {"ids": [1, 2, 3, ...]}

    Returns:
        {"users": [...matched users...], "count": N}
    """
    id_set = set(request.ids)
    matched = [u for u in USERS if u["id"] in id_set]
    return {"users": matched, "count": len(matched)}


@app.head("/users")
async def users_head():
    """HEAD request for health check."""
    return None


@app.head("/products")
async def products_head():
    """HEAD request for health check."""
    return None


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("🚀 Starting Mock API Server for E2E Tests...")
    print("   Endpoints:")
    print("     - GET /users (offset pagination)")
    print("     - GET /users/{id}")
    print("     - GET /products (cursor pagination)")
    print("     - GET /products/{id}")
    print("     - GET /health")
    print("     - POST /users/batch (batch fetch, object body {ids:[...]})")
    print("     - POST /users/search (batch fetch, raw list body [1,2,3])")
    print("     - PATCH /users/bulk (bulk patch with active_only filter)")
    print("     - POST /users/{id}/enrich (per-ID POST with enrichment)")
    print("     - POST /products/lookup (product batch by product_ids key)")
    print("")
    
    uvicorn.run(app, host="0.0.0.0", port=8092)
