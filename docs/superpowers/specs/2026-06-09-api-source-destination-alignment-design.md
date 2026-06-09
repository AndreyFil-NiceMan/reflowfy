# API Source / Destination Alignment

**Date:** 2026-06-09
**Status:** Approved

## Goal

Remove `PaginatedAPISource`, leaving `IDBasedAPISource` as the only REST API
source. Align `IDBasedAPISource` and `ApiDestination` so they share identical
parameter names for shared concepts, make the request **`body` user-authored
and sent verbatim** on both (the user builds it from `records` /
`runtime_params` in the `define_*` methods — dict, list, or nothing), and make
Basic auth actually work in both.

## Motivation

`IDBasedAPISource` and `ApiDestination` drifted apart: the source used
`query_params` / `request_body` while the destination used `params` / `body`;
the destination always wrapped outgoing records in a `{record}` / `{records}`
object (plus an auto-injected `runtime_params` key) with no way to send the
body the user actually wanted; the source auto-injected the ID list under a
configurable `batch_id_key`; and `auth_type="basic"` was documented on the
destination but silently did nothing (no `Authorization` header was ever set).
`PaginatedAPISource` adds a second, differently-shaped API source that the
project no longer wants to maintain.

The unifying idea: the user already receives the data in the pipeline
`define_*` hooks — `define_destination(self, records, runtime_params)` and
`define_source(self, runtime_params)` (with `current_ids` injected for
`IdBasedPipeline`). So the connectors should stop guessing the body shape and
simply transmit the `body` the user constructs there. This works in
distributed mode because `define_destination` runs in the manager with the
job's transformed records (`pipeline_runner.py:472`), and only
`destination.config` (which now contains the finished `body`) is serialized to
the worker.

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

- Source-only: `endpoint_template` (needs `{id}` templating), `ids`,
  `batch_size`, `response_key` (see §2a).
- Destination-only: `url`, `retry_config`.

The source keeps `base_url` + `endpoint_template` while the destination keeps a
single `url`; this difference is intentional and documented in both docstrings.

### 2a. DX simplification of source-specific params

Beyond the shared-name sync, simplify the source's own params (all hard
changes, no aliases):

- **Remove `ids_source` and `ids_field`.** Confirmed unused across all
  pipelines, unit tests, and e2e suites. IDs come only from the static
  `ids=[...]` list or `runtime_params["ids"]` / `current_ids`. Delete the
  `_ids_source` instance attribute and the `ids_source` branch in
  `_get_all_ids`.
- **Remove `batch_id_key`.** In batch mode the source no longer auto-injects
  the ID list under a configurable key. Instead the user builds the request
  `body` in `define_source` from the IDs they already have in `runtime_params`
  (see §3). The body is sent verbatim.
- **`data_key` → `response_key`** *(Optional[str], default `None`)*. Same
  behavior: dotted key to extract the records list from the response envelope
  (e.g. `"data.users"`); `None` means the response itself is the list. Renamed
  for clarity (response-side).

Update `IDBasedAPISourceConfig` to drop `ids_field` and `batch_id_key`, rename
`request_body` → `body`, `query_params` → `params`, and `data_key` →
`response_key`. (`ids_source` is a runtime object, not part of the serialized
config.)

### 3. User-authored body, sent verbatim (both connectors)

The core change. Neither connector constructs or wraps the body. Whatever the
user passes as `body` is sent as the JSON request body, exactly as given.
`body` may be a dict, a list, or `None`.

**`ApiDestination`:**

- `send()` sends `config["body"]` verbatim as the JSON request body. Exactly
  **one HTTP request** per `send()` call.
- The user builds the body in `define_destination(self, records, runtime_params)`:
  ```python
  def define_destination(self, records, runtime_params):
      body = {"events": records, "tenant": runtime_params["tenant"]}  # or just `records`
      return api_destination(url=..., body=body, auth_type="basic", auth_token="u:p")
  ```
- **Remove** `batch_requests`, `record_key`/`records_key` (never shipped), the
  `_build_payload` / `_serialize_metadata` helpers, and the automatic
  `record` / `records` / `runtime_params` wrapping.
- `body=None` → the request is sent **with no body at all** (no `json=`
  argument passed to httpx).

**`IDBasedAPISource` (batch mode):**

- `_fetch_batch` sends `config["body"]` verbatim. The user builds it in
  `define_source` from the IDs in `runtime_params` / `current_ids`:
  ```python
  def define_source(self, params):
      body = {"ids": params["current_ids"]}        # was batch_id_key="ids"
      # body = {"product_ids": params["current_ids"]}  # custom key
      # body = params["current_ids"]                   # raw list
      return id_based_api_source(base_url=..., endpoint_template="/users/batch",
                                 method="POST", body=body)
  ```
- `body=None` in batch mode → request sent with no body.
- **Per-ID mode** (`{id}` in `endpoint_template`) is unchanged: `ids` still
  drives URL templating, and `body` is still sent per request with `{id}`
  substitution applied to its string values (`body=None` → no body, as today
  for GET).

Migration of the removed knobs:

| old | new |
|---|---|
| dest `batch_requests=True` + auto-wrap | `body={"records": records}` (user builds) |
| dest `batch_requests=False` (per-record) | dropped — one request with the user's body |
| dest auto `runtime_params` key | `body={..., "runtime_params": runtime_params}` if wanted |
| source `batch_id_key="ids"` | `body={"ids": params["current_ids"]}` |
| source `batch_id_key="product_ids"` | `body={"product_ids": params["current_ids"]}` |
| source `batch_id_key=None` (raw) | `body=params["current_ids"]` |

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
schema change needed there for auth. `reflowfy/destinations/schemas.py` must
drop its `batch_requests` field; `IDBasedAPISourceConfig` drops `batch_id_key`
and `ids_field` and renames `request_body`→`body`, `query_params`→`params`,
`data_key`→`response_key`.

## Components and data flow

```
build_auth_headers(headers, auth_type, auth_token)   # pure, shared
        ▲                              ▲
        │                              │
IDBasedAPISource._get_client    ApiDestination._get_client
   (httpx.Client, sync)            (httpx.AsyncClient, async)
        │                              │
   fetch / split_jobs            send (one request)
   params + response_key         params + body (verbatim)
   + body (verbatim, per-ID      body=None -> no json= sent
   or batch)
```

Body transmission rule (both connectors): `json=config["body"]` when `body`
is not `None`; otherwise the `json=` argument is omitted entirely so no request
body is sent.

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
    `response_key` works; old `query_params`/`request_body`/`batch_id_key`/
    `data_key`/`ids_source`/`ids_field` raise `TypeError`); batch mode sends
    `body` verbatim (dict, list, and `None` → no body); basic-auth header set
    on the client.
  - `ApiDestination`: `send()` issues exactly one request with `body` verbatim
    (dict body, list body, and `body=None` → no `json=` sent); no
    `record`/`records`/`runtime_params` wrapping anywhere; basic-auth header
    set. Remove the old `batch_requests` / payload-wrapping assertions in
    `tests/unit/test_api_destination.py`.
- **E2E** (`tests/e2e/`): existing ID-based source + API destination suites
  pass after the paginated removal. Migrate call sites:
  - source: `batch_id_key=...` / `data_key=...` → user-built `body={...}` /
    `response_key=...` in the e2e pipelines and shared source factory.
  - destination: `batch_requests=...` → user-built `body` in `define_destination`.
  - Update both mock servers (`tests/e2e/sources/mock_api_server.py`,
    `tests/e2e/destinations/mock_api_server.py`) to assert the user-built body
    shapes instead of the old wrapped/`batch_id_key` shapes; collapse the
    batched-vs-individual destination endpoints into the single-request model.
  - Add a basic-auth case against a mock server.
- Lint/type: `uv run ruff check reflowfy/`, `uv run mypy reflowfy/`,
  `uv run black reflowfy/`.

## Out of scope

- Unifying `base_url`+`endpoint_template` (source) with `url` (destination).
- Callable body builders (cannot cross the Kafka worker boundary; the
  user-authored-`body` model in §3 is the serializable equivalent).
- Any change to non-API sources/destinations.
