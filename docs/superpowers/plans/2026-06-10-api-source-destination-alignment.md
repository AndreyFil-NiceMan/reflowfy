# API Source / Destination Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete `PaginatedAPISource`, align `IDBasedAPISource` and `ApiDestination` on shared parameter names, make the request `body` user-authored and sent verbatim on both connectors (dict, list, or none), and implement working Basic auth via a shared helper.

**Architecture:** Both connectors stop constructing/wrapping the request body. The user builds `body` in the pipeline `define_source(runtime_params)` / `define_destination(records, runtime_params)` hooks and the connector transmits it unchanged (`json=body`, or no `json=` when `body is None`). A single pure helper `build_auth_headers` centralizes bearer/apikey/basic header construction for the sync (source) and async (destination) httpx clients.

**Tech Stack:** Python 3, httpx, pydantic v2, pytest (`asyncio_mode=auto`), uv, ruff/black/mypy.

**Reference spec:** `docs/superpowers/specs/2026-06-09-api-source-destination-alignment-design.md`

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `reflowfy/http_auth.py` | Pure `build_auth_headers` helper | Create |
| `tests/unit/test_http_auth.py` | Unit tests for the helper | Create |
| `reflowfy/sources/api.py` | Delete `PaginatedAPISource`; rework `IDBasedAPISource` (verbatim body, renames, removals, shared auth) | Modify |
| `reflowfy/sources/schemas.py` | Delete `PaginatedAPISourceConfig`; rework `IDBasedAPISourceConfig` | Modify |
| `reflowfy/sources/__init__.py` | Drop paginated exports | Modify |
| `reflowfy/destinations/api.py` | Rework `ApiDestination` (verbatim body, shared auth, drop wrapping/batch_requests) | Modify |
| `reflowfy/destinations/schemas.py` | Drop `batch_requests`; `body` accepts dict or list | Modify |
| `reflowfy/core/id_based_pipeline.py` | Fix two docstring examples that call `paginated_api_source` | Modify |
| `pipelines/api_example_pipeline.py` | Rewrite to use `id_based_api_source` | Modify |
| `tests/unit/sources/test_api_source.py` | Remove paginated tests; migrate ID-based tests | Modify |
| `tests/unit/test_api_destination.py` | Replace wrapping/batch tests with verbatim-body tests | Modify |
| `tests/e2e/sources/test_api_source.py` | Remove paginated E2E class | Modify |
| `tests/e2e/test_pipelines/api_source_test_pipeline.py` | Remove (paginated) | Delete |
| `tests/e2e/test_pipelines/sources/__init__.py` | Remove `e2e_paginated_api`; migrate `e2e_id_based_api` params | Modify |
| `tests/e2e/test_pipelines/shared_sources.py` | Drop `e2e_paginated_api` import | Modify |
| `tests/e2e/test_pipelines/destinations/__init__.py` | Migrate `e2e_http*` to verbatim body | Modify |
| `tests/e2e/test_pipelines/id_based_api_batch_pipeline_test.py` | Migrate `batch_id_key`/`data_key` | Modify |
| `tests/e2e/test_pipelines/id_based_api_advanced_pipeline_test.py` | Migrate `batch_id_key`/`data_key` | Modify |
| `tests/e2e/test_pipelines/api_dest_test_pipeline.py` | Build body from records | Modify |
| `tests/e2e/test_pipelines/elastic_routed_destinations_pipeline.py` | Drop `batch_requests` | Modify |
| `tests/e2e/sources/mock_api_server.py` | Remove paginated endpoints; assert user-built batch bodies | Modify |
| `tests/e2e/destinations/mock_api_server.py` | Collapse to single-request, assert verbatim body | Modify |
| `tests/e2e/test_id_based_pipeline.py` | Migrate `batch_id_key` references | Modify |

---

## Task 1: Shared `build_auth_headers` helper

**Files:**
- Create: `reflowfy/http_auth.py`
- Test: `tests/unit/test_http_auth.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_http_auth.py`:

```python
"""Unit tests for the shared HTTP auth header helper."""

import base64

from reflowfy.http_auth import build_auth_headers


def test_bearer_sets_authorization():
    out = build_auth_headers({}, "bearer", "tok123")
    assert out["Authorization"] == "Bearer tok123"


def test_apikey_sets_x_api_key():
    out = build_auth_headers({}, "apikey", "key123")
    assert out["X-API-Key"] == "key123"


def test_basic_base64_encodes_user_pass():
    out = build_auth_headers({}, "basic", "alice:s3cret")
    expected = base64.b64encode(b"alice:s3cret").decode("ascii")
    assert out["Authorization"] == f"Basic {expected}"


def test_none_auth_type_leaves_headers_unchanged():
    out = build_auth_headers({"X": "1"}, None, "tok")
    assert out == {"X": "1"}


def test_unknown_auth_type_leaves_headers_unchanged():
    out = build_auth_headers({}, "weird", "tok")
    assert out == {}


def test_missing_token_leaves_headers_unchanged():
    out = build_auth_headers({}, "bearer", None)
    assert out == {}


def test_input_dict_not_mutated():
    src = {"Content-Type": "application/json"}
    out = build_auth_headers(src, "bearer", "tok")
    assert "Authorization" not in src
    assert out is not src
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_http_auth.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'reflowfy.http_auth'`

- [ ] **Step 3: Create the implementation**

Create `reflowfy/http_auth.py`:

```python
"""Shared HTTP authentication header construction.

Used by both the API source (sync ``httpx.Client``) and the API destination
(async ``httpx.AsyncClient``) so auth lives in exactly one place.
"""

from __future__ import annotations

import base64
from typing import Dict, Optional


def build_auth_headers(
    headers: Dict[str, str],
    auth_type: Optional[str],
    auth_token: Optional[str],
) -> Dict[str, str]:
    """Return a new headers dict with the auth header applied.

    - ``bearer``  -> ``Authorization: Bearer <token>``
    - ``apikey``  -> ``X-API-Key: <token>``
    - ``basic``   -> ``Authorization: Basic base64(<token>)`` where ``token``
      is ``"username:password"``.

    Unknown/``None`` ``auth_type`` or a falsy ``auth_token`` leaves the headers
    unchanged. The input dict is never mutated.
    """
    result = dict(headers)
    if not auth_token:
        return result
    if auth_type == "bearer":
        result["Authorization"] = f"Bearer {auth_token}"
    elif auth_type == "apikey":
        result["X-API-Key"] = auth_token
    elif auth_type == "basic":
        encoded = base64.b64encode(auth_token.encode("utf-8")).decode("ascii")
        result["Authorization"] = f"Basic {encoded}"
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_http_auth.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Commit**

```bash
git add reflowfy/http_auth.py tests/unit/test_http_auth.py
git commit -m "feat: add shared build_auth_headers helper (bearer/apikey/basic)"
```

---

## Task 2: Delete `PaginatedAPISource`

**Files:**
- Modify: `reflowfy/sources/api.py` (delete class lines ~11-300 and factory ~606-641)
- Modify: `reflowfy/sources/schemas.py` (delete `PaginatedAPISourceConfig`)
- Modify: `reflowfy/sources/__init__.py`
- Modify: `tests/unit/sources/test_api_source.py` (delete paginated tests/imports)

- [ ] **Step 1: Remove the class and factory from `reflowfy/sources/api.py`**

Delete the entire `class PaginatedAPISource(BaseSource):` block (from `class PaginatedAPISource` down to the end of its `health_check` method, immediately before `class IDBasedAPISource`).

Delete the entire `def paginated_api_source(...)` factory function (between the `# Factory functions` comment and `def id_based_api_source(`).

Update the import line at the top of the file from:

```python
from reflowfy.sources.schemas import IDBasedAPISourceConfig, PaginatedAPISourceConfig
```

to:

```python
from reflowfy.sources.schemas import IDBasedAPISourceConfig
```

- [ ] **Step 2: Remove `PaginatedAPISourceConfig` from `reflowfy/sources/schemas.py`**

Delete the entire `class PaginatedAPISourceConfig(BaseModel):` block (lines ~31-70, from `class PaginatedAPISourceConfig` to just before `class IDBasedAPISourceConfig`).

- [ ] **Step 3: Update `reflowfy/sources/__init__.py`**

Replace the API-sources import block and `__all__` so paginated names are gone:

```python
# API sources (httpx is a core dependency)
from reflowfy.sources.api import (
    IDBasedAPISource,
    id_based_api_source,
)
```

```python
__all__ = [
    "BaseSource",
    "SourceJob",
    "SourceError",
    "IDBasedAPISource",
    "id_based_api_source",
]
```

- [ ] **Step 4: Remove paginated unit tests**

In `tests/unit/sources/test_api_source.py`:
- Change the import block to:
  ```python
  from reflowfy.sources.api import IDBasedAPISource, id_based_api_source
  ```
- Delete the entire `class TestPaginatedAPISource:` block.
- In `class TestAuthenticationHeaders:`, the two tests use `PaginatedAPISource` — they will be rewritten in Task 4. For now, change both `PaginatedAPISource(...)` constructions to `IDBasedAPISource(base_url=..., endpoint_template="/users/{id}", auth_type=..., auth_token=...)` and remove the `endpoint=` / pagination kwargs. (Task 4 hardens these.)

- [ ] **Step 5: Run the source unit tests**

Run: `uv run pytest tests/unit/sources/test_api_source.py -v`
Expected: PASS (paginated tests gone; ID-based + auth tests pass)

- [ ] **Step 6: Verify nothing else imports the paginated names**

Run: `grep -rn "PaginatedAPISource\|paginated_api_source\|PaginatedAPISourceConfig" reflowfy/ tests/unit/ | grep -v __pycache__`
Expected: no output (E2E references are handled in Task 8).

- [ ] **Step 7: Commit**

```bash
git add reflowfy/sources/api.py reflowfy/sources/schemas.py reflowfy/sources/__init__.py tests/unit/sources/test_api_source.py
git commit -m "refactor: delete PaginatedAPISource"
```

---

## Task 3: Rework `IDBasedAPISource` — verbatim body, renames, shared auth

**Files:**
- Modify: `reflowfy/sources/api.py` (`IDBasedAPISource` + `id_based_api_source`)
- Modify: `reflowfy/sources/schemas.py` (`IDBasedAPISourceConfig`)
- Test: `tests/unit/sources/test_api_source.py`

- [ ] **Step 1: Write/adjust failing tests**

In `tests/unit/sources/test_api_source.py`, add these tests inside `class TestIDBasedAPISource:` (and keep the existing `test_init`, `test_factory_function`, `test_get_all_ids_from_config`, `test_get_all_ids_from_runtime_params`, `test_fetch_by_id`, `test_fetch_by_id_not_found`, `test_split_jobs_batching`, `test_health_check_disabled_skips_requests`):

```python
    def test_renamed_params_stored(self):
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/batch",
            params={"q": "1"},
            body={"ids": [1, 2]},
            response_key="data.users",
        )
        assert source.config["params"] == {"q": "1"}
        assert source.config["body"] == {"ids": [1, 2]}
        assert source.config["response_key"] == "data.users"

    def test_old_param_names_rejected(self):
        for kwargs in (
            {"query_params": {"a": 1}},
            {"request_body": {"a": 1}},
            {"batch_id_key": "ids"},
            {"data_key": "x"},
            {"ids_source": object()},
            {"ids_field": "id"},
        ):
            with pytest.raises(TypeError):
                IDBasedAPISource(
                    base_url="https://api.example.com",
                    endpoint_template="/users/{id}",
                    **kwargs,
                )

    @patch("httpx.Client")
    def test_fetch_batch_sends_body_verbatim(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"users": [{"id": 1}, {"id": 2}]}
        mock_client.request.return_value = mock_response

        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/batch",
            method="POST",
            body={"ids": [1, 2]},
            response_key="users",
        )
        records = source._fetch_batch([1, 2])

        assert records == [{"id": 1}, {"id": 2}]
        _, kwargs = mock_client.request.call_args
        assert kwargs["json"] == {"ids": [1, 2]}

    @patch("httpx.Client")
    def test_fetch_batch_no_body_omits_json(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": 9}]
        mock_client.request.return_value = mock_response

        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/batch",
            method="POST",
        )
        source._fetch_batch([9])

        _, kwargs = mock_client.request.call_args
        assert kwargs.get("json") is None

    @patch("httpx.Client")
    def test_basic_auth_header_on_client(self, mock_client_class):
        import base64

        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            auth_type="basic",
            auth_token="alice:s3cret",
        )
        source._get_client()
        call_kwargs = mock_client_class.call_args[1]
        expected = base64.b64encode(b"alice:s3cret").decode("ascii")
        assert call_kwargs["headers"]["Authorization"] == f"Basic {expected}"
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `uv run pytest tests/unit/sources/test_api_source.py -k "renamed or old_param or fetch_batch or basic_auth" -v`
Expected: FAIL (old kwargs still accepted; `response_key`/`params`/`body` not yet wired; basic auth not implemented).

- [ ] **Step 3: Rewrite `IDBasedAPISource` in `reflowfy/sources/api.py`**

Replace the whole `IDBasedAPISource` class with this implementation (drops `ids_source`/`ids_field`/`batch_id_key`; renames `query_params`→`params`, `request_body`→`body`, `data_key`→`response_key`; sends `body` verbatim; uses `build_auth_headers`):

```python
class IDBasedAPISource(BaseSource):
    """
    ID-based REST API source with full HTTP method and body control.

    Behaviour is auto-detected from the endpoint template:
    - ``{id}`` in ``endpoint_template`` → **per-ID mode**: one request per ID,
      ID substituted into the URL (and into string values of ``body``).
    - No ``{id}`` in template → **batch mode**: one request whose body is the
      ``body`` you built (e.g. ``{"ids": params["current_ids"]}``).

    The request ``body`` is sent verbatim. Build it yourself in
    ``define_source(runtime_params)`` from the IDs you already have, e.g.
    ``body={"ids": params["current_ids"]}`` or ``body=params["current_ids"]``
    for a raw list. ``body=None`` sends no request body.
    """

    def __init__(
        self,
        base_url: str,
        endpoint_template: str,
        ids: Optional[List[Union[str, int]]] = None,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        batch_size: int = 50,
        timeout: float = 30.0,
        response_key: Optional[str] = None,
        body: Optional[Any] = None,
        params: Optional[Dict[str, Any]] = None,
        health_check_enabled: bool = True,
    ):
        """
        Initialize ID-based API source.

        Note: there is intentionally **no** ``**kwargs`` — unknown keyword
        arguments (including the removed ``batch_id_key`` / ``request_body`` /
        ``query_params`` / ``data_key`` / ``ids_source`` / ``ids_field``) raise
        ``TypeError`` so typos and stale call sites fail loudly.

        Args:
            base_url: Base API URL (e.g. ``"https://api.example.com"``).
            endpoint_template: Endpoint path. Include ``{id}`` for per-ID mode
                (e.g. ``"/users/{id}"``); omit it for batch mode.
            ids: Static list of IDs. May also arrive via ``runtime_params["ids"]``.
            method: HTTP method.
            headers: Custom request headers.
            auth_type: ``"bearer"``, ``"apikey"``, or ``"basic"``.
            auth_token: Credential. For ``"basic"`` pass ``"user:password"``.
            batch_size: Per-ID mode: IDs grouped per SourceJob. Batch mode:
                response records grouped per SourceJob.
            timeout: HTTP request timeout in seconds.
            response_key: Dotted key to extract the records list from the
                response JSON (e.g. ``"data.users"``). ``None`` means the
                response itself is the list.
            body: Request body sent verbatim (dict or list). In per-ID mode,
                string values of a dict body support ``{id}`` substitution.
                ``None`` sends no body.
            params: Extra query-string parameters appended to every request.
            health_check_enabled: Enable/disable source health check.
        """
        config = {
            "base_url": base_url,
            "endpoint_template": endpoint_template,
            "ids": ids or [],
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "batch_size": batch_size,
            "timeout": timeout,
            "response_key": response_key,
            "body": body,
            "params": params or {},
            "health_check_enabled": health_check_enabled,
        }
        super().__init__(config)
        self._client: Optional[httpx.Client] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_per_id_mode(self) -> bool:
        """True when the endpoint template contains ``{id}``."""
        return "{id}" in self.config["endpoint_template"]

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            headers = build_auth_headers(
                self.config["headers"],
                self.config.get("auth_type"),
                self.config.get("auth_token"),
            )
            self._client = httpx.Client(
                base_url=self.config["base_url"],
                headers=headers,
                timeout=self.config["timeout"],
            )
        return self._client

    def _get_all_ids(self, runtime_params: Dict[str, Any]) -> List[Union[str, int]]:
        """Get all IDs from config or runtime params."""
        if self.config["ids"]:
            return self.config["ids"]
        return runtime_params.get("ids", [])

    def _extract_records(self, data: Any) -> List[Any]:
        """Extract records list from a response using ``response_key``."""
        response_key = self.config.get("response_key")
        if not response_key:
            return data if isinstance(data, list) else []
        keys = response_key.split(".")
        result = data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key, [])
            else:
                return []
        return result if isinstance(result, list) else [result]

    def _fetch_by_id(self, id_value: Union[str, int]) -> Optional[Any]:
        """Fetch a single resource by ID (per-ID mode)."""
        client = self._get_client()
        endpoint = self.config["endpoint_template"].format(id=id_value)
        method = self.config["method"]

        body = self.config.get("body")
        if method in ("GET", "HEAD"):
            body = None
        elif isinstance(body, dict):
            body = {
                k: v.format(id=id_value) if isinstance(v, str) else v
                for k, v in body.items()
            }

        query = self.config["params"] or None

        try:
            if body is None:
                response = client.request(method, endpoint, params=query)
            else:
                response = client.request(method, endpoint, json=body, params=query)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError:
            return None

    def _fetch_batch(self, ids_batch: List[Union[str, int]]) -> List[Any]:
        """Send the batch request and return the extracted records.

        The request body is ``config["body"]`` verbatim — the caller is
        expected to have placed any IDs into it. ``ids_batch`` is retained for
        the public signature/metadata only.
        """
        client = self._get_client()
        method = self.config["method"]
        endpoint = self.config["endpoint_template"]
        body = self.config.get("body")
        query = self.config["params"] or None

        try:
            if body is None:
                response = client.request(method, endpoint, params=query)
            else:
                response = client.request(method, endpoint, json=body, params=query)
            response.raise_for_status()
            return self._extract_records(response.json())
        except httpx.HTTPStatusError as e:
            raise SourceError(
                "id_based_api", f"HTTP {e.response.status_code}: {e.response.text}", e
            )
        except httpx.RequestError as e:
            raise SourceError("id_based_api", f"Request failed: {e}", e)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        """Fetch resources by ID (local/preview mode)."""
        self.resolve_parameters(runtime_params)
        ids = self._get_all_ids(runtime_params)
        if limit:
            ids = ids[:limit]

        if not self._is_per_id_mode():
            return self._fetch_batch(ids)

        records = []
        for id_value in ids:
            record = self._fetch_by_id(id_value)
            if record:
                records.append(record)
        return records

    def split_jobs(
        self, runtime_params: Dict[str, Any], batch_size: int = 50
    ) -> Iterator[SourceJob]:
        """Split IDs into SourceJobs (per-ID or batch mode)."""
        _raw = self.resolve_parameters(runtime_params)
        if _raw is None:
            raise SourceError("id_based_api", "No valid configuration resolved", None)
        try:
            resolved_config = IDBasedAPISourceConfig(**_raw)
        except Exception as exc:
            raise SourceError("id_based_api", f"Invalid configuration: {exc}", exc)

        ids = self._get_all_ids(runtime_params)
        batch_size = resolved_config.batch_size
        batch_num = 0

        if not self._is_per_id_mode():
            records = self._fetch_batch(ids)
            for i in range(0, len(records), batch_size):
                chunk = records[i : i + batch_size]
                if chunk:
                    yield SourceJob(
                        records=chunk,
                        metadata={
                            "batch_num": batch_num,
                            "record_count": len(chunk),
                            "ids_count": len(ids),
                        },
                    )
                batch_num += 1
            return

        for i in range(0, len(ids), batch_size):
            id_batch = ids[i : i + batch_size]
            records = []
            for id_value in id_batch:
                record = self._fetch_by_id(id_value)
                if record:
                    records.append(record)
            if records:
                yield SourceJob(
                    records=records,
                    metadata={
                        "batch_num": batch_num,
                        "id_count": len(id_batch),
                        "record_count": len(records),
                        "ids": id_batch,
                    },
                )
            batch_num += 1

    def health_check(self) -> bool:
        """Check if the API is accessible."""
        if not self.config.get("health_check_enabled", True):
            return True
        try:
            client = self._get_client()
            response = client.request("HEAD", "/", timeout=5.0)
            return response.status_code < 500
        except Exception:
            return False
```

Add the helper import near the top of `reflowfy/sources/api.py` (with the other imports):

```python
from reflowfy.http_auth import build_auth_headers
```

- [ ] **Step 4: Rewrite the `id_based_api_source` factory**

Replace the existing `def id_based_api_source(...)` factory with:

```python
def id_based_api_source(
    base_url: str,
    endpoint_template: str,
    ids: Optional[List[Union[str, int]]] = None,
    batch_size: int = 50,
    method: str = "GET",
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    response_key: Optional[str] = None,
    body: Optional[Any] = None,
    params: Optional[Dict[str, Any]] = None,
    health_check_enabled: bool = True,
) -> IDBasedAPISource:
    """
    Factory for the ID-based API source.

    Mode is auto-detected from ``endpoint_template``:
    - ``{id}`` present → per-ID mode (one request per ID)
    - no ``{id}``      → batch mode (one request; you build the body)

    Build the request body yourself from the IDs you have::

        # batch object body
        id_based_api_source(base_url="...", endpoint_template="/users/batch",
            method="POST", body={"ids": params["current_ids"]}, response_key="users")

        # batch raw-list body
        id_based_api_source(base_url="...", endpoint_template="/users/batch",
            method="POST", body=params["current_ids"])

        # per-ID GET (no body)
        id_based_api_source(base_url="...", endpoint_template="/users/{id}",
            ids=[1, 2, 3])
    """
    return IDBasedAPISource(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids,
        method=method,
        batch_size=batch_size,
        auth_type=auth_type,
        auth_token=auth_token,
        response_key=response_key,
        body=body,
        params=params,
        health_check_enabled=health_check_enabled,
    )
```

- [ ] **Step 5: Update `IDBasedAPISourceConfig` in `reflowfy/sources/schemas.py`**

Replace the `IDBasedAPISourceConfig` class body with:

```python
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
```

- [ ] **Step 6: Run the source unit tests**

Run: `uv run pytest tests/unit/sources/test_api_source.py -v`
Expected: PASS (all ID-based tests including the new verbatim/basic-auth/rejection tests).

- [ ] **Step 7: Commit**

```bash
git add reflowfy/sources/api.py reflowfy/sources/schemas.py tests/unit/sources/test_api_source.py
git commit -m "refactor: IDBasedAPISource sends body verbatim, shared auth, renamed params"
```

---

## Task 4: Migrate the source auth-header tests to basic + shared helper

**Files:**
- Test: `tests/unit/sources/test_api_source.py`

- [ ] **Step 1: Rewrite `class TestAuthenticationHeaders`**

Replace the whole class with (now using `IDBasedAPISource` and covering basic):

```python
class TestAuthenticationHeaders:
    """Test authentication handling on the ID-based source client."""

    @patch("httpx.Client")
    def test_bearer_auth(self, mock_client_class):
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            auth_type="bearer",
            auth_token="secret-token",
        )
        source._get_client()
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["headers"]["Authorization"] == "Bearer secret-token"

    @patch("httpx.Client")
    def test_apikey_auth(self, mock_client_class):
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            auth_type="apikey",
            auth_token="my-api-key",
        )
        source._get_client()
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["headers"]["X-API-Key"] == "my-api-key"
```

- [ ] **Step 2: Run the tests**

Run: `uv run pytest tests/unit/sources/test_api_source.py -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/unit/sources/test_api_source.py
git commit -m "test: migrate source auth tests to IDBasedAPISource"
```

---

## Task 5: Rework `ApiDestination` — verbatim body, shared auth

**Files:**
- Modify: `reflowfy/destinations/api.py`
- Modify: `reflowfy/destinations/schemas.py`
- Test: `tests/unit/test_api_destination.py`

- [ ] **Step 1: Replace the destination unit tests with verbatim-body tests**

Overwrite `tests/unit/test_api_destination.py` with:

```python
"""Unit tests for ApiDestination."""

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from reflowfy.destinations.api import ApiDestination, api_destination
from reflowfy.destinations.base import DestinationError
from reflowfy.destinations.schemas import ApiDestinationConfig


class TestApiDestinationFactory:
    def test_factory_returns_instance(self):
        dest = api_destination(url="https://api.example.com/webhook")
        assert isinstance(dest, ApiDestination)

    def test_default_config(self):
        dest = api_destination(url="https://api.example.com/webhook")
        assert dest.config["method"] == "POST"
        assert dest.config["timeout"] == 30.0
        assert dest.config["params"] is None
        assert dest.config["body"] is None

    def test_custom_body_stored_dict(self):
        dest = api_destination(
            url="https://api.example.com/webhook",
            body={"events": [{"id": 1}]},
        )
        assert dest.config["body"] == {"events": [{"id": 1}]}

    def test_custom_body_stored_list(self):
        dest = api_destination(url="https://api.example.com/webhook", body=[{"id": 1}])
        assert dest.config["body"] == [{"id": 1}]

    def test_method_uppercased(self):
        dest = api_destination(url="https://api.example.com/webhook", method="put")
        assert dest.config["method"] == "PUT"

    def test_batch_requests_kwarg_rejected(self):
        with pytest.raises(TypeError):
            api_destination(url="https://api.example.com", batch_requests=True)


class TestApiDestinationConfig:
    def test_valid_config(self):
        cfg = ApiDestinationConfig(url="https://api.example.com/webhook")
        assert cfg.url == "https://api.example.com/webhook"
        assert cfg.method == "POST"
        assert cfg.body is None

    def test_invalid_url_raises(self):
        with pytest.raises(Exception):
            ApiDestinationConfig(url="not-a-url")

    def test_body_accepts_dict_and_list(self):
        assert ApiDestinationConfig(url="https://x.com", body={"a": 1}).body == {"a": 1}
        assert ApiDestinationConfig(url="https://x.com", body=[1, 2]).body == [1, 2]


class TestSendVerbatim:
    @pytest.fixture
    def mock_response(self):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        return resp

    async def _capture(self, dest, mock_response):
        calls = []

        async def fake_request(method, url, *, json=None, params=None):
            calls.append({"method": method, "url": url, "json": json, "params": params})
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request
        return calls

    async def test_dict_body_sent_verbatim_single_request(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/ingest",
            body={"events": [{"id": 1}, {"id": 2}], "src": "x"},
            params={"tenant": "acme"},
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1}, {"id": 2}])
        assert len(calls) == 1
        assert calls[0]["json"] == {"events": [{"id": 1}, {"id": 2}], "src": "x"}
        assert calls[0]["params"] == {"tenant": "acme"}

    async def test_list_body_sent_verbatim(self, mock_response):
        dest = api_destination(url="https://api.example.com/ingest", body=[{"id": 1}, {"id": 2}])
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1}, {"id": 2}])
        assert len(calls) == 1
        assert calls[0]["json"] == [{"id": 1}, {"id": 2}]

    async def test_none_body_omits_json(self, mock_response):
        dest = api_destination(url="https://api.example.com/ingest")
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1}])
        assert len(calls) == 1
        assert calls[0]["json"] is None


class TestAuthentication:
    async def _headers(self, dest):
        with patch.object(httpx.AsyncClient, "request", new_callable=AsyncMock):
            client = await dest._get_client()
            return dict(client.headers)

    async def test_bearer_auth_header(self):
        dest = api_destination(
            url="https://api.example.com", auth_type="bearer", auth_token="my-secret-token"
        )
        headers = await self._headers(dest)
        assert headers.get("authorization") == "Bearer my-secret-token"
        await dest.close()

    async def test_apikey_auth_header(self):
        dest = api_destination(
            url="https://api.example.com", auth_type="apikey", auth_token="my-api-key"
        )
        headers = await self._headers(dest)
        assert headers.get("x-api-key") == "my-api-key"
        await dest.close()

    async def test_basic_auth_header(self):
        dest = api_destination(
            url="https://api.example.com", auth_type="basic", auth_token="alice:s3cret"
        )
        headers = await self._headers(dest)
        expected = base64.b64encode(b"alice:s3cret").decode("ascii")
        assert headers.get("authorization") == f"Basic {expected}"
        await dest.close()

    async def test_no_auth_no_headers_added(self):
        dest = api_destination(url="https://api.example.com")
        headers = await self._headers(dest)
        assert "authorization" not in headers
        assert "x-api-key" not in headers
        await dest.close()


class TestErrorHandling:
    @pytest.fixture
    def dest(self):
        return api_destination(url="https://api.example.com/ingest", body={"k": "v"})

    async def test_http_4xx_raises_destination_error(self, dest):
        error_response = MagicMock()
        error_response.status_code = 401
        error_response.text = "Unauthorized"
        http_error = httpx.HTTPStatusError("401", request=MagicMock(), response=error_response)
        error_response.raise_for_status = MagicMock(side_effect=http_error)
        dest._client = MagicMock()
        dest._client.request = AsyncMock(return_value=error_response)
        with pytest.raises(DestinationError) as exc_info:
            await dest.send([{"id": 1}])
        assert "401" in str(exc_info.value)

    async def test_network_error_raises_destination_error(self, dest):
        dest._client = MagicMock()
        dest._client.request = AsyncMock(
            side_effect=httpx.RequestError("Connection refused", request=MagicMock())
        )
        with pytest.raises(DestinationError) as exc_info:
            await dest.send([{"id": 1}])
        assert "Request failed" in str(exc_info.value)


class TestHealthCheck:
    async def test_health_check_true_on_2xx(self):
        dest = api_destination(url="https://api.example.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        dest._client = MagicMock()
        dest._client.head = AsyncMock(return_value=mock_resp)
        assert await dest.health_check() is True

    async def test_health_check_disabled_skips_requests(self):
        dest = api_destination(url="https://api.example.com", health_check_enabled=False)
        assert await dest.health_check() is True
        assert dest._client is None

    async def test_close_clears_client(self):
        dest = api_destination(url="https://api.example.com")
        mock_client = MagicMock()
        mock_client.aclose = AsyncMock()
        dest._client = mock_client
        await dest.close()
        mock_client.aclose.assert_awaited_once()
        assert dest._client is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_api_destination.py -v`
Expected: FAIL (`batch_requests` still accepted; `_build_payload` wrapping still present; basic auth not implemented).

- [ ] **Step 3: Rewrite `reflowfy/destinations/api.py`**

Overwrite the file with:

```python
"""API destination for webhooks and REST endpoints."""

from typing import Any, Dict, List, Optional

import httpx

from reflowfy.destinations.base import BaseDestination, DestinationError, RetryConfig
from reflowfy.http_auth import build_auth_headers


class ApiDestination(BaseDestination):
    """
    API destination for sending data to webhooks and REST endpoints.

    The request ``body`` is sent verbatim — build it yourself in
    ``define_destination(records, runtime_params)`` (dict or list). ``body=None``
    sends a request with no body. Each ``send()`` issues exactly one request.

    Supports:
    - Configurable authentication (Bearer, API key, Basic)
    - URL query parameters
    - Timeout configuration
    - Custom headers
    """

    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        auth_type: Optional[str] = None,
        auth_token: Optional[str] = None,
        timeout: float = 30.0,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        retry_config: Optional[RetryConfig] = None,
        health_check_enabled: bool = True,
    ):
        """
        Initialize API destination.

        Args:
            url: Target URL.
            method: HTTP method (POST, PUT, PATCH).
            headers: Custom headers.
            auth_type: ``"bearer"``, ``"apikey"``, or ``"basic"``.
            auth_token: Credential. For ``"basic"`` pass ``"user:password"``.
            timeout: Request timeout in seconds.
            params: URL query parameters appended to every request.
            body: Request body sent verbatim (dict or list). ``None`` sends no body.
            retry_config: Optional retry configuration.
            health_check_enabled: Enable/disable destination health check.
        """
        config = {
            "url": url,
            "method": method.upper(),
            "headers": headers or {},
            "auth_type": auth_type,
            "auth_token": auth_token,
            "timeout": timeout,
            "params": params,
            "body": body,
            "health_check_enabled": health_check_enabled,
        }
        super().__init__(config, retry_config)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None:
            headers = build_auth_headers(
                self.config["headers"],
                self.config.get("auth_type"),
                self.config.get("auth_token"),
            )
            self._client = httpx.AsyncClient(
                headers=headers,
                timeout=self.config["timeout"],
            )
        return self._client

    async def send(self, records: List[Any], metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Send the configured body to the API endpoint in a single request.

        The body is ``config["body"]`` verbatim; ``records``/``metadata`` are
        accepted for interface compatibility but the body is authored by the
        user in ``define_destination``.

        Raises:
            DestinationError: If the request fails.
        """
        client = await self._get_client()
        url = self.config["url"]
        method = self.config["method"]
        params = self.config.get("params")
        body = self.config.get("body")

        try:
            if body is None:
                response = await client.request(method, url, params=params)
            else:
                response = await client.request(method, url, json=body, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            raise DestinationError(
                "api",
                f"HTTP {e.response.status_code}: {e.response.text}",
                e,
            )
        except httpx.RequestError as e:
            raise DestinationError("api", f"Request failed: {e}", e)
        except Exception as e:
            raise DestinationError("api", f"Unexpected error: {e}", e)

    async def health_check(self) -> bool:
        """Check if the API endpoint is accessible."""
        if not self.config.get("health_check_enabled", True):
            return True
        try:
            client = await self._get_client()
            try:
                response = await client.head(self.config["url"], timeout=5.0)
                return response.status_code < 500
            except Exception:
                response = await client.request("OPTIONS", self.config["url"], timeout=5.0)
                return response.status_code < 500
        except Exception:
            return False

    async def close(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


def api_destination(
    url: str,
    method: str = "POST",
    headers: Optional[Dict[str, str]] = None,
    auth_type: Optional[str] = None,
    auth_token: Optional[str] = None,
    timeout: float = 30.0,
    params: Optional[Dict[str, str]] = None,
    body: Optional[Any] = None,
    retry_config: Optional[RetryConfig] = None,
    health_check_enabled: bool = True,
) -> ApiDestination:
    """
    Factory function for API destination.

    Build the body in ``define_destination(records, runtime_params)``::

        def define_destination(self, records, runtime_params):
            body = {"events": records, "tenant": runtime_params["tenant"]}
            return api_destination(url="https://api.example.com/ingest", body=body,
                                   auth_type="basic", auth_token="user:pass")
    """
    return ApiDestination(
        url=url,
        method=method,
        headers=headers,
        auth_type=auth_type,
        auth_token=auth_token,
        timeout=timeout,
        params=params,
        body=body,
        retry_config=retry_config,
        health_check_enabled=health_check_enabled,
    )
```

- [ ] **Step 4: Update `ApiDestinationConfig` in `reflowfy/destinations/schemas.py`**

Replace the `batch_requests` and `body` field lines in `ApiDestinationConfig`. Remove:

```python
    batch_requests: bool = Field(default=False, description="Send all records in one request")
```

and change the `body` field to accept dict or list:

```python
    body: Optional[Any] = Field(
        default=None, description="Request body sent verbatim (dict or list); None sends no body"
    )
```

(`Any` is already imported in that file.)

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_api_destination.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add reflowfy/destinations/api.py reflowfy/destinations/schemas.py tests/unit/test_api_destination.py
git commit -m "refactor: ApiDestination sends body verbatim, basic auth, drop wrapping/batch_requests"
```

---

## Task 6: Fix example pipeline and `id_based_pipeline` docstrings

**Files:**
- Modify: `pipelines/api_example_pipeline.py`
- Modify: `reflowfy/core/id_based_pipeline.py`

- [ ] **Step 1: Inspect the current example pipeline**

Run: `cat pipelines/api_example_pipeline.py`
Note the class name, `define_*` methods, and `define_parameters` entries (it currently references `paginated_api_source` and a `data_key` parameter).

- [ ] **Step 2: Rewrite `pipelines/api_example_pipeline.py`**

Replace the source construction so it uses `id_based_api_source` in per-ID mode. Keep the pipeline's existing class name, `name`, and structure; change the import and `define_source`. Example shape (adapt names to the existing file):

```python
from reflowfy.sources.api import id_based_api_source

# ... inside the pipeline class ...

    def define_source(self, runtime_params):
        return id_based_api_source(
            base_url=runtime_params.get("base_url", "https://jsonplaceholder.typicode.com"),
            endpoint_template="/posts/{id}",
            ids=runtime_params.get("ids", [1, 2, 3, 4, 5]),
            batch_size=runtime_params.get("batch_size", 2),
        )
```

Remove any `define_parameters` entry that exists solely to feed the old `data_key`/pagination arguments, and remove the `paginated_api_source` import. Ensure the pipeline still constructs (its `__init__` must not raise) so auto-registration works.

- [ ] **Step 3: Verify the example pipeline imports cleanly**

Run: `uv run python -c "import pipelines.api_example_pipeline"`
Expected: no error.

- [ ] **Step 4: Fix the two docstrings in `reflowfy/core/id_based_pipeline.py`**

Find the two docstring example blocks that call `paginated_api_source(...)` (around lines 15 and 175). Replace each with an `id_based_api_source` example, e.g.:

```python
    ...     def define_source(self, params):
    ...         return id_based_api_source(
    ...             base_url="https://api.example.com",
    ...             endpoint_template="/users/{id}",
    ...             ids=params["current_ids"],
    ...         )
```

- [ ] **Step 5: Verify no `paginated` references remain in `reflowfy/`**

Run: `grep -rn "paginated" reflowfy/ | grep -v __pycache__`
Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add pipelines/api_example_pipeline.py reflowfy/core/id_based_pipeline.py
git commit -m "docs: update example pipeline and id_based_pipeline docstrings to id_based_api_source"
```

---

## Task 7: Run the full unit suite + static checks

**Files:** none (verification gate before the Docker-only E2E work)

- [ ] **Step 1: Run the full unit suite**

Run: `uv run pytest tests/unit/ -v`
Expected: PASS (no references to removed names; new behavior covered).

- [ ] **Step 2: Lint, format, type-check**

Run:
```bash
uv run ruff check reflowfy/
uv run black --check reflowfy/
uv run mypy reflowfy/
```
Expected: clean. If `black --check` reports reformatting, run `uv run black reflowfy/` and re-run mypy/ruff.

- [ ] **Step 3: Commit any formatting**

```bash
git add -A
git commit -m "style: apply black/ruff to API connector changes" || echo "nothing to format"
```

---

## Task 8: E2E migration — source side

> E2E tests run only under the Docker stack via `./scripts/run_e2e_tests.sh`. These steps edit fixtures; final verification is in Task 10.

**Files:**
- Modify: `tests/e2e/test_pipelines/sources/__init__.py`
- Modify: `tests/e2e/test_pipelines/shared_sources.py`
- Delete: `tests/e2e/test_pipelines/api_source_test_pipeline.py`
- Modify: `tests/e2e/sources/test_api_source.py`
- Modify: `tests/e2e/sources/mock_api_server.py`
- Modify: `tests/e2e/test_pipelines/id_based_api_batch_pipeline_test.py`
- Modify: `tests/e2e/test_pipelines/id_based_api_advanced_pipeline_test.py`
- Modify: `tests/e2e/test_id_based_pipeline.py`

- [ ] **Step 1: Remove the paginated E2E source factory**

In `tests/e2e/test_pipelines/sources/__init__.py`, delete the `@source("e2e_paginated_api")` function (`e2e_paginated_api`) entirely.

Then migrate `e2e_id_based_api`: remove the `batch_id_key` parameter and argument, rename `data_key` → `response_key`, `request_body` → `body`, `query_params` → `params`. Result:

```python
@source("e2e_id_based_api")
def e2e_id_based_api(
    base_url: str = os.getenv("MOCK_API_URL", "http://localhost:8092"),
    endpoint_template: str = "/users/{id}",
    ids: Optional[List[Union[str, int]]] = None,
    method: str = "GET",
    batch_size: int = 2,
    response_key: Optional[str] = None,
    body: Optional[object] = None,
    params: Optional[dict] = None,
):
    """Pre-configured ID-based API source for E2E tests."""
    from reflowfy.sources.api import id_based_api_source

    return id_based_api_source(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids or [1, 2, 3, 4, 5],
        method=method,
        batch_size=batch_size,
        response_key=response_key,
        body=body,
        params=params,
    )
```

- [ ] **Step 2: Drop the paginated import in `shared_sources.py`**

In `tests/e2e/test_pipelines/shared_sources.py`, remove `e2e_paginated_api` from the import list (and any `__all__`/re-export of it).

- [ ] **Step 3: Delete the paginated test pipeline**

Run: `git rm tests/e2e/test_pipelines/api_source_test_pipeline.py`

If any other module imports it (e.g. a pipeline aggregator), remove that import. Find with:
`grep -rn "api_source_test_pipeline\|e2e_paginated_api" tests/ | grep -v __pycache__`
Resolve every hit.

- [ ] **Step 4: Remove the paginated E2E test class**

In `tests/e2e/sources/test_api_source.py`, delete `class TestPaginatedAPISourceE2E:` in full. Keep the ID-based E2E class(es).

- [ ] **Step 5: Migrate the batch pipeline test**

In `tests/e2e/test_pipelines/id_based_api_batch_pipeline_test.py`, the source is built with `batch_id_key="ids"` and `data_key="users"`. Read the file, then in its `define_source` build the body explicitly and use `response_key`:

```python
        return e2e_id_based_api(
            endpoint_template="/users/batch",
            method="POST",
            ids=ids,
            body={"ids": ids},
            response_key="users",
        )
```

(Use the same `ids` the method already resolves; if it uses `params["current_ids"]`, pass `body={"ids": params["current_ids"]}`.)

- [ ] **Step 6: Migrate the advanced pipeline test**

In `tests/e2e/test_pipelines/id_based_api_advanced_pipeline_test.py`, migrate each pipeline's `define_source` per the mapping (read the file; the three relevant call sites currently pass `batch_id_key=` + `data_key=`):

| old | new (build body from the resolved ids) |
|---|---|
| `batch_id_key=None, data_key="results"` | `body=ids, response_key="results"` |
| `batch_id_key="ids", data_key="updated", request_body={"active_only": ...}` | `body={"ids": ids, "active_only": ...}, response_key="updated"` |
| `batch_id_key="product_ids", data_key="items"` | `body={"product_ids": ids}, response_key="items"` |

Use whatever local variable the method already has for the IDs (e.g. `ids` or `params["current_ids"]`). Update the module docstring lines that describe `batch_id_key` to describe the `body=` shapes instead.

- [ ] **Step 7: Update the source mock server assertions**

In `tests/e2e/sources/mock_api_server.py`:
- Delete the paginated/cursor endpoints and their response models (the ones documented as "Paginated user list" / "cursor pagination", `PaginatedResponse`, `CursorPaginatedResponse`).
- For the batch endpoints, the request body is now exactly what the pipeline built. Where an endpoint previously read IDs from a fixed key, keep reading the key the migrated pipeline now sends (`ids`, `product_ids`) and support the raw-list body (request JSON is a list) for the `body=ids` case. Mirror the mapping in Step 6.

- [ ] **Step 8: Migrate remaining `batch_id_key` references**

Run: `grep -rn "batch_id_key\|data_key\|request_body\|query_params\|paginated\|Paginated" tests/e2e/ | grep -v __pycache__`
Resolve every hit (notably in `tests/e2e/test_id_based_pipeline.py` docstrings/asserts): replace with the `body=`/`response_key`/`params` model from Steps 5-6.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "test(e2e): migrate source fixtures to verbatim body + remove paginated"
```

---

## Task 9: E2E migration — destination side

**Files:**
- Modify: `tests/e2e/test_pipelines/destinations/__init__.py`
- Modify: `tests/e2e/test_pipelines/api_dest_test_pipeline.py`
- Modify: `tests/e2e/test_pipelines/elastic_routed_destinations_pipeline.py`
- Modify: `tests/e2e/destinations/mock_api_server.py`

- [ ] **Step 1: Migrate the `e2e_http` destination factories**

In `tests/e2e/test_pipelines/destinations/__init__.py`, remove the `batch_requests` parameter/argument from both `e2e_http` and `e2e_http_runtime_params`. Because the body is now user-authored, these factories take a `body` argument and pass it through. New `e2e_http`:

```python
@destination("e2e_http")
def e2e_http(
    url: str = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook"),
    method: str = "POST",
    headers: Optional[Dict[str, str]] = None,
    auth_type: str = "bearer",
    auth_token: str = "test-webhook-token",
    timeout: float = 30.0,
    body: Optional[object] = None,
):
    """Pre-configured API webhook destination for E2E tests."""
    return api_destination(
        url=url,
        method=method,
        headers=headers or {"Content-Type": "application/json"},
        auth_type=auth_type,
        auth_token=auth_token,
        timeout=timeout,
        body=body,
    )
```

For `e2e_http_runtime_params`, drop `batch_requests`, accept a `body` argument, and pass it through (it can still merge `runtime_params` into the body the caller provides if desired). Keep `_serialize_runtime_params` only if a migrated pipeline still uses it; otherwise delete it.

- [ ] **Step 2: Build the body in the destination pipelines**

In `tests/e2e/test_pipelines/api_dest_test_pipeline.py`, the `define_destination(self, records, runtime_params)` must now build the body. To preserve the previously-batched shape (`{"records": [...]}`) the mock server expects, do:

```python
        body = {"records": records, "runtime_params": runtime_params}
        return e2e_http(url=..., body=body)
```

(Adapt to the existing factory call; the file currently relies on `batch_requests=True` auto-wrapping — replace that with the explicit `body` above. If the test asserts a `runtime_params` key, include it as shown; if not, use `body={"records": records}`.)

- [ ] **Step 3: Drop `batch_requests` in the elastic-routed pipeline**

In `tests/e2e/test_pipelines/elastic_routed_destinations_pipeline.py`, remove the two `batch_requests=True` arguments and, where each `define_destination` has `records`, pass `body={"records": records}` to preserve the batched shape.

- [ ] **Step 4: Collapse the destination mock server to single-request**

In `tests/e2e/destinations/mock_api_server.py`, there are two endpoints — batched (`batch_requests=True` → `{"records": [...]}`) and individual (`batch_requests=False` → `{"record": {...}}`). Since the connector now sends one request with the user-built body:
- Keep the endpoint that receives `{"records": [...]}` (now fed by `body={"records": records}` from the pipelines) and assert on that shape.
- Remove or repurpose the per-record endpoint; if a test still posts to it, point that test's pipeline body at the kept endpoint instead.

Read both this file and the destination E2E test(s) that hit it (`grep -rn "mock_api_server\|/webhook\|records\b" tests/e2e/destinations/`) and make the endpoint set match what the migrated pipelines now send.

- [ ] **Step 5: Migrate remaining `batch_requests` references**

Run: `grep -rn "batch_requests" tests/e2e/ | grep -v __pycache__`
Resolve every hit.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test(e2e): migrate destination fixtures to user-built verbatim body"
```

---

## Task 10: Full verification

**Files:** none

- [ ] **Step 1: Confirm no removed names remain anywhere**

Run:
```bash
grep -rn "PaginatedAPISource\|paginated_api_source\|batch_id_key\|raw_body\|record_key\|records_key\|ids_source\|ids_field\|batch_requests" reflowfy/ tests/ pipelines/ | grep -v __pycache__
```
Expected: no output.

- [ ] **Step 2: Full unit suite + static checks**

Run:
```bash
uv run pytest tests/unit/ -v
uv run ruff check reflowfy/
uv run black --check reflowfy/
uv run mypy reflowfy/
```
Expected: all green.

- [ ] **Step 3: Run the E2E suites under Docker**

Run:
```bash
./scripts/run_e2e_tests.sh sources
./scripts/run_e2e_tests.sh destinations
```
Expected: PASS. Investigate and fix any fixture/mock mismatch surfaced here (re-edit the files from Tasks 8-9), then re-run.

- [ ] **Step 4: Refresh the graphify graph**

Run: `graphify update .`
(AST-only, no API cost — keeps `graphify-out/` current per `AGENTS.md`.)

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore: refresh graphify graph after API connector alignment" || echo "nothing to commit"
```

---

## Notes for the implementer

- `pytest` runs in `asyncio_mode = auto`; async tests need no decorator.
- The destination is reconstructed in workers via `ApiDestination(**config)` from the JSON-serialized `destination.config` (`reflowfy/worker/executor.py:323`). Everything in `config` must stay JSON-serializable — `body` is plain dict/list/None, which is why a callable body builder is out of scope.
- `define_destination` runs in the manager with the job's transformed records (`reflowfy/reflow_manager/pipeline_runner.py:472`), so a body built there from `records` is exactly what the worker sends.
- Keep edits within the API source/destination surface; do not touch other connectors.
