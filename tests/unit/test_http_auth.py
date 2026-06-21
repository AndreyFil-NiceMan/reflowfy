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
