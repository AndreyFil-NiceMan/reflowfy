"""Unit tests for ApiDestination."""

import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch

from reflowfy.destinations.api import ApiDestination, api_destination
from reflowfy.destinations.base import DestinationError
from reflowfy.destinations.schemas import ApiDestinationConfig


# ============================================================================
# Factory & config
# ============================================================================

class TestApiDestinationFactory:
    def test_factory_returns_instance(self):
        dest = api_destination(url="https://api.example.com/webhook")
        assert isinstance(dest, ApiDestination)

    def test_default_config(self):
        dest = api_destination(url="https://api.example.com/webhook")
        assert dest.config["method"] == "POST"
        assert dest.config["timeout"] == 30.0
        assert dest.config["batch_requests"] is False
        assert dest.config["params"] is None
        assert dest.config["body"] is None

    def test_custom_params_stored(self):
        dest = api_destination(
            url="https://api.example.com/webhook",
            params={"tenant": "acme", "env": "prod"},
        )
        assert dest.config["params"] == {"tenant": "acme", "env": "prod"}

    def test_custom_body_stored(self):
        dest = api_destination(
            url="https://api.example.com/webhook",
            body={"source": "reflowfy", "version": "2"},
        )
        assert dest.config["body"] == {"source": "reflowfy", "version": "2"}

    def test_method_uppercased(self):
        dest = api_destination(url="https://api.example.com/webhook", method="put")
        assert dest.config["method"] == "PUT"


# ============================================================================
# Schema validation
# ============================================================================

class TestApiDestinationConfig:
    def test_valid_config(self):
        cfg = ApiDestinationConfig(url="https://api.example.com/webhook")
        assert cfg.url == "https://api.example.com/webhook"
        assert cfg.method == "POST"
        assert cfg.params is None
        assert cfg.body is None

    def test_invalid_url_raises(self):
        with pytest.raises(Exception):
            ApiDestinationConfig(url="not-a-url")

    def test_params_and_body_accepted(self):
        cfg = ApiDestinationConfig(
            url="https://api.example.com/webhook",
            params={"key": "value"},
            body={"extra": "field"},
        )
        assert cfg.params == {"key": "value"}
        assert cfg.body == {"extra": "field"}

    def test_timeout_bounds(self):
        with pytest.raises(Exception):
            ApiDestinationConfig(url="https://api.example.com/webhook", timeout=0)
        with pytest.raises(Exception):
            ApiDestinationConfig(url="https://api.example.com/webhook", timeout=999)


# ============================================================================
# Payload building
# ============================================================================

class TestBuildPayload:
    def test_empty_body_returns_empty_dict(self):
        dest = api_destination(url="https://example.com")
        assert dest._build_payload([]) == {}

    def test_body_fields_copied(self):
        dest = api_destination(url="https://example.com", body={"source": "test", "v": 1})
        payload = dest._build_payload([])
        assert payload == {"source": "test", "v": 1}

    def test_build_payload_does_not_mutate_config(self):
        dest = api_destination(url="https://example.com", body={"k": "v"})
        p1 = dest._build_payload([])
        p1["injected"] = True
        p2 = dest._build_payload([])
        assert "injected" not in p2


# ============================================================================
# Send — batch mode
# ============================================================================

class TestSendBatch:
    @pytest.fixture
    def mock_response(self):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        return resp

    @pytest.fixture
    def dest(self):
        return api_destination(
            url="https://api.example.com/ingest",
            batch_requests=True,
            params={"tenant": "acme"},
            body={"source": "reflowfy"},
        )

    async def test_batch_sends_single_request(self, dest, mock_response):
        calls = []

        async def fake_request(method, url, *, json=None, params=None):
            calls.append({"method": method, "url": url, "json": json, "params": params})
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        await dest.send([{"id": 1}, {"id": 2}, {"id": 3}])

        assert len(calls) == 1

    async def test_batch_payload_contains_records(self, dest, mock_response):
        captured = {}

        async def fake_request(method, url, *, json=None, params=None):
            captured.update(json)
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        records = [{"id": i} for i in range(5)]
        await dest.send(records)

        assert captured["records"] == records

    async def test_batch_body_fields_merged(self, dest, mock_response):
        captured = {}

        async def fake_request(method, url, *, json=None, params=None):
            captured.update(json)
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        await dest.send([{"id": 1}])

        assert captured.get("source") == "reflowfy"

    async def test_batch_params_passed(self, dest, mock_response):
        captured_params = {}

        async def fake_request(method, url, *, json=None, params=None):
            if params:
                captured_params.update(params)
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        await dest.send([{"id": 1}])

        assert captured_params.get("tenant") == "acme"


# ============================================================================
# Send — individual mode
# ============================================================================

class TestSendIndividual:
    @pytest.fixture
    def mock_response(self):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        return resp

    @pytest.fixture
    def dest(self):
        return api_destination(
            url="https://api.example.com/ingest",
            batch_requests=False,
            body={"source": "test"},
        )

    async def test_individual_sends_one_request_per_record(self, dest, mock_response):
        calls = []

        async def fake_request(method, url, *, json=None, params=None):
            calls.append(json)
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        await dest.send([{"id": 1}, {"id": 2}, {"id": 3}])

        assert len(calls) == 3

    async def test_individual_payload_uses_record_key(self, dest, mock_response):
        payloads = []

        async def fake_request(method, url, *, json=None, params=None):
            payloads.append(json)
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        await dest.send([{"id": 42}])

        assert payloads[0]["record"] == {"id": 42}

    async def test_individual_body_fields_merged(self, dest, mock_response):
        payloads = []

        async def fake_request(method, url, *, json=None, params=None):
            payloads.append(json)
            return mock_response

        dest._client = MagicMock()
        dest._client.request = fake_request

        await dest.send([{"id": 1}, {"id": 2}])

        for p in payloads:
            assert p.get("source") == "test"


# ============================================================================
# Authentication
# ============================================================================

class TestAuthentication:
    async def _capture_headers(self, dest):
        captured = {}
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        with patch.object(httpx.AsyncClient, "request", new_callable=AsyncMock) as mock_req:
            mock_req.return_value = mock_response
            client = await dest._get_client()
            return dict(client.headers)

    async def test_bearer_auth_header(self):
        dest = api_destination(
            url="https://api.example.com",
            auth_type="bearer",
            auth_token="my-secret-token",
        )
        headers = await self._capture_headers(dest)
        assert headers.get("authorization") == "Bearer my-secret-token"
        await dest.close()

    async def test_apikey_auth_header(self):
        dest = api_destination(
            url="https://api.example.com",
            auth_type="apikey",
            auth_token="my-api-key",
        )
        headers = await self._capture_headers(dest)
        assert headers.get("x-api-key") == "my-api-key"
        await dest.close()

    async def test_no_auth_no_headers_added(self):
        dest = api_destination(url="https://api.example.com")
        headers = await self._capture_headers(dest)
        assert "authorization" not in headers
        assert "x-api-key" not in headers
        await dest.close()


# ============================================================================
# Error handling
# ============================================================================

class TestErrorHandling:
    @pytest.fixture
    def dest(self):
        return api_destination(url="https://api.example.com/ingest")

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

    async def test_http_5xx_raises_destination_error(self, dest):
        error_response = MagicMock()
        error_response.status_code = 503
        error_response.text = "Service Unavailable"
        http_error = httpx.HTTPStatusError("503", request=MagicMock(), response=error_response)
        error_response.raise_for_status = MagicMock(side_effect=http_error)

        dest._client = MagicMock()
        dest._client.request = AsyncMock(return_value=error_response)

        with pytest.raises(DestinationError) as exc_info:
            await dest.send([{"id": 1}])

        assert "503" in str(exc_info.value)

    async def test_network_error_raises_destination_error(self, dest):
        dest._client = MagicMock()
        dest._client.request = AsyncMock(
            side_effect=httpx.RequestError("Connection refused", request=MagicMock())
        )

        with pytest.raises(DestinationError) as exc_info:
            await dest.send([{"id": 1}])

        assert "Request failed" in str(exc_info.value)


# ============================================================================
# Health check
# ============================================================================

class TestHealthCheck:
    async def test_health_check_true_on_2xx(self):
        dest = api_destination(url="https://api.example.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 200

        dest._client = MagicMock()
        dest._client.head = AsyncMock(return_value=mock_resp)

        result = await dest.health_check()
        assert result is True

    async def test_health_check_false_on_5xx(self):
        dest = api_destination(url="https://api.example.com")
        mock_resp = MagicMock()
        mock_resp.status_code = 503

        dest._client = MagicMock()
        dest._client.head = AsyncMock(return_value=mock_resp)

        result = await dest.health_check()
        assert result is False

    async def test_health_check_false_on_exception(self):
        dest = api_destination(url="https://api.example.com")
        dest._client = MagicMock()
        dest._client.head = AsyncMock(side_effect=Exception("unreachable"))

        result = await dest.health_check()
        assert result is False

    async def test_close_clears_client(self):
        dest = api_destination(url="https://api.example.com")
        dest._client = MagicMock()
        dest._client.aclose = AsyncMock()

        await dest.close()

        dest._client.aclose.assert_awaited_once()
        assert dest._client is None
