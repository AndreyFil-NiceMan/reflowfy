# API Source / Destination Alignment

**Date:** 2026-06-09
**Status:** Approved

## Goal

Remove `PaginatedAPISource`, leaving `IDBasedAPISource` as the only REST API
source. Align `IDBasedAPISource` and `ApiDestination` so they share identical
parameter names for shared concepts, give the destination explicit control over
the request body shape (dict *or* list, not forced wrapping), and make Basic
auth actually work in both.

## Motivation

`IDBasedAPISource` and `ApiDestination` drifted apart: the source used
`query_params` / `request_body` while the destination used `params` / `body`;
the destination always wrapped outgoing records in a `{record}` / `{records}`
object with no way to send a bare body; and `auth_type="basic"` was documented
on the destination but silently did nothing (no `Authorization` header was
ever set). `PaginatedAPISource` adds a second, differently-shaped API source
that the project no longer wants to maintain.

## Scope

### 1. Delete `PaginatedAPISource`

Remove the class and everything that references it:

- `reflowfy/sources/api.py` — delete `PaginatedAPISource` class and the
  `paginated_api_source` factory function.
- `reflowfy/sources/schemas.py` — delete `PaginatedAPISourceConfig`.
- `reflowfy/sources/__init__.py` — remove `PaginatedAPISource` and
  `paginated_api_source` from imports and `__all__`.
- `pipelines/api_example_pipeline.py` — rewrite to use `id_based_api_source`
  so the bundled example still runs.
- `reflowfy/core/id_based_pipeline.py` — fix the two docstring examples
  (lines ~15 and ~175) that call `paginated_api_source`.
- `tests/unit/sources/test_api_source.py` — remove `TestPaginatedAPISource`
  and the paginated factory test.
- `tests/e2e/sources/test_api_source.py` — remove `TestPaginatedAPISourceE2E`.
- `tests/e2e/test_pipelines/api_source_test_pipeline.py`,
  `tests/e2e/test_pipelines/sources/__init__.py`,
  `tests/e2e/test_pipelines/shared_sources.py` — remove the
  `e2e_paginated_api` source and its test pipeline.
- `tests/e2e/sources/mock_api_server.py` — remove the paginated/cursor
  endpoints and response models used only by the paginated source.

### 2. Sync shared parameters

After the cut, `IDBasedAPISource` and `ApiDestination` share these concepts.
They must use **identical names**:

`method`, `headers`, `auth_type`, `auth_token`, `timeout`, `params`, `body`,
`health_check_enabled`.

Change on the source (`IDBasedAPISource`, its factory, and
`IDBasedAPISourceConfig`):

- `query_params` → **`params`**
- `request_body` → **`body`**

**Hard rename — no backward-compatible aliases.** The old names are removed
entirely. All in-repo callers and tests are updated in the same change.

Parameters that stay different because they reflect each side's genuine job:

- Source-only: `endpoint_template` (needs `{id}` templating), `ids`, `ids_key`,
  `batch_size`, `response_key` (see §2a).
- Destination-only: `url`, `batch_requests`, `raw_body` (see §3),
  `retry_config`.

The source keeps `base_url` + `endpoint_template` while the destination keeps a
single `url`; this difference is intentional and documented in both docstrings.

### 2a. DX simplification of source-specific params

Beyond the shared-name sync, simplify the source's own params (all hard
changes, no aliases):

- **Remove `ids_source` and `ids_field`.** Confirmed unused across all
  pipelines, unit tests, and e2e suites. IDs now come only from the static
  `ids=[...]` list or `runtime_params["ids"]`. Delete the `_ids_source`
  instance attribute and the `ids_source` branch in `_get_all_ids`.
- **`batch_id_key` → `ids_key`** *(Optional[str], default `"ids"`)*. Same
  behavior: the request-body key wrapping the IDs list in batch mode; `None`
  sends the IDs as a raw JSON array `[1, 2, 3]`. Renamed for clarity
  (request-side).
- **`data_key` → `response_key`** *(Optional[str], default `None`)*. Same
  behavior: dotted key to extract the records list from the response envelope
  (e.g. `"data.users"`); `None` means the response itself is the list. Renamed
  for clarity (response-side).

Update `IDBasedAPISourceConfig` to drop `ids_field`, rename `batch_id_key` →
`ids_key` and `data_key` → `response_key`. (`ids_source` is a runtime object,
not part of the serialized config.)

### 3. Destination body control

`ApiDestination` gains one new constructor param:

- `raw_body: bool = False` — controls whether outgoing records are wrapped.

Behavior in `_build_payload` / `send`:

- `raw_body=False` (default): current behavior is preserved exactly — a single
  record is placed under `"record"`, the batch list under `"records"`, in an
  object that also carries the static `body` fields and `runtime_params`.
- `raw_body=True`: the bare record (dict) or bare record list is sent as the
  JSON body. The static `body` and `runtime_params` are **not** merged in
  (there is no wrapper object to merge them into); this is documented.

A single boolean was chosen over per-mode key params for the simplest DX; the
destination never exposed a customizable wrapper key before, so nothing is
lost.

Examples:

```python
# default — unchanged
api_destination(url=..., batch_requests=True)
# -> {"records": [...], ...body, "runtime_params": {...}}

# raw list body
api_destination(url=..., batch_requests=True, raw_body=True)
# -> [r1, r2, ...]

# raw single record
api_destination(url=..., raw_body=True)
# -> {...record...}
```

### 4. Basic auth in both connectors

`auth_type="basic"` with `auth_token="username:password"` produces
`Authorization: Basic base64(username:password)`.

To avoid duplicating auth logic across the source's sync `httpx.Client` and the
destination's async `httpx.AsyncClient`, add one pure helper:

```python
# reflowfy/http_auth.py (new, shared module)
def build_auth_headers(
    headers: dict[str, str],
    auth_type: str | None,
    auth_token: str | None,
) -> dict[str, str]:
    """Return a new headers dict with the auth header applied.

    bearer  -> Authorization: Bearer <token>
    apikey  -> X-API-Key: <token>
    basic   -> Authorization: Basic base64(<token>)   # token is "user:pass"
    """
```

Both `IDBasedAPISource._get_client` and `ApiDestination._get_client` call this
helper instead of inlining the `if auth_type == ...` chain.

`auth_type` Literal in `IDBasedAPISourceConfig` already allows `"basic"`; no
schema change needed there beyond the `params`/`body` rename.

## Components and data flow

```
build_auth_headers(headers, auth_type, auth_token)   # pure, shared
        ▲                              ▲
        │                              │
IDBasedAPISource._get_client    ApiDestination._get_client
   (httpx.Client, sync)            (httpx.AsyncClient, async)
        │                              │
   fetch / split_jobs            send (batch / per-record)
   params + body + ids_key       params + body + raw_body
   + response_key
```

## Error handling

- `build_auth_headers` with `auth_type="basic"` and an `auth_token` lacking a
  `:` separator: treat the whole token as the userinfo and base64-encode it as
  given (httpx/`b64encode` will not raise). No new exception type. Existing
  source/destination error wrapping (`SourceError` / `DestinationError`) is
  unchanged.
- Unknown `auth_type`: no header added (current behavior), unchanged.

## Testing

- **Unit** (`tests/unit/`):
  - `build_auth_headers`: bearer, apikey, basic (correct base64), None, unknown.
  - `IDBasedAPISource`: rename smoke (constructing with `params`/`body`/
    `ids_key`/`response_key` works; old `query_params`/`request_body`/
    `batch_id_key`/`data_key`/`ids_source`/`ids_field` raise `TypeError`),
    `ids_key=None` raw-list request body, basic-auth header set on the client.
  - `ApiDestination`: `raw_body=True` sends bare list (batch) and bare record
    (non-batch); default still wraps under `records`/`record`; basic-auth
    header set.
- **E2E** (`tests/e2e/`): existing ID-based source + API destination suites
  pass after the paginated removal. The e2e ID-based pipelines and shared
  source factory currently pass `batch_id_key=` and `data_key=` — update those
  call sites to `ids_key=` / `response_key=`. Add a raw-list destination case
  (`raw_body=True`) and a basic-auth case against the mock server.
- Lint/type: `uv run ruff check reflowfy/`, `uv run mypy reflowfy/`,
  `uv run black reflowfy/`.

## Out of scope

- Unifying `base_url`+`endpoint_template` (source) with `url` (destination).
- Callable/templated body builders on the destination.
- Any change to non-API sources/destinations.
