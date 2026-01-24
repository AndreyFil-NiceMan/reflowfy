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
from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel
import uvicorn

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
    print("")
    
    uvicorn.run(app, host="0.0.0.0", port=8092)
