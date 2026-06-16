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

        async def fake_request(method, url, *, json=None, content=None, params=None):
            calls.append(
                {"method": method, "url": url, "json": json, "content": content, "params": params}
            )
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
        assert calls[0]["content"] is None

    async def test_str_body_sent_as_raw_content(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/ingest",
            body="<events><id>1</id></events>",
            headers={"Content-Type": "application/xml"},
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1}])
        assert len(calls) == 1
        assert calls[0]["content"] == "<events><id>1</id></events>"
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
