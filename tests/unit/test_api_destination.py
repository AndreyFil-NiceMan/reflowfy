"""Unit tests for ApiDestination."""

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from reflowfy.destinations.api import ApiDestination, api_destination
from reflowfy.destinations.base import DestinationError, PermanentDestinationError


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
        error_response = httpx.Response(
            401,
            request=httpx.Request("POST", "https://api.example.com/ingest"),
            text="Unauthorized",
        )
        dest._client = MagicMock()
        dest._client.request = AsyncMock(return_value=error_response)
        with pytest.raises(PermanentDestinationError) as exc_info:
            await dest.send([{"id": 1}])
        assert "HTTP 401 Unauthorized (text/plain, 12 bytes)" in str(exc_info.value)
        assert isinstance(exc_info.value, DestinationError)

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


class TestPerRecord:
    """per_record=True: one request per record, like kafka_destination."""

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

    async def test_one_request_per_record_with_record_as_body(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/ingest", per_record=True, params={"t": "acme"}
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1}, {"id": 2}, {"id": 3}])
        assert [c["json"] for c in calls] == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert all(c["url"] == "https://api.example.com/ingest" for c in calls)
        assert all(c["params"] == {"t": "acme"} for c in calls)

    async def test_url_placeholders_filled_from_each_record_and_encoded(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/tenants/{tenant}/users/{user_id}",
            method="PUT",
            per_record=True,
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"tenant": "a", "user_id": 7}, {"tenant": "b c/d", "user_id": "x?y"}])
        assert [c["url"] for c in calls] == [
            "https://api.example.com/tenants/a/users/7",
            "https://api.example.com/tenants/b%20c%2Fd/users/x%3Fy",
        ]
        assert all(c["method"] == "PUT" for c in calls)

    async def test_body_field_sends_only_that_field(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/users/{id}", per_record=True, body_field="payload"
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1, "payload": {"name": "a"}}, {"id": 2, "name": "b"}])
        # Missing field → whole record, as kafka_destination does.
        assert [c["json"] for c in calls] == [{"name": "a"}, {"id": 2, "name": "b"}]
        assert [c["url"] for c in calls] == [
            "https://api.example.com/users/1",
            "https://api.example.com/users/2",
        ]

    async def test_str_payload_sent_raw(self, mock_response):
        dest = api_destination(url="https://api.example.com", per_record=True, body_field="xml")
        calls = await self._capture(dest, mock_response)
        await dest.send([{"xml": "<a/>"}])
        assert calls[0]["content"] == "<a/>"
        assert calls[0]["json"] is None

    async def test_empty_records_send_nothing(self, mock_response):
        dest = api_destination(url="https://api.example.com", per_record=True)
        calls = await self._capture(dest, mock_response)
        await dest.send([])
        assert calls == []

    async def test_missing_placeholder_field_fails_without_request(self, mock_response):
        dest = api_destination(url="https://api.example.com/users/{user_id}", per_record=True)
        calls = await self._capture(dest, mock_response)
        with pytest.raises(ValueError, match=r"\{user_id\}.*record 2 of 2"):
            await dest.send([{"user_id": 1}, {"name": "no id"}])
        assert calls == []  # every request is built before the first is sent

    async def test_placeholder_on_non_dict_record_fails(self, mock_response):
        dest = api_destination(url="https://api.example.com/{id}", per_record=True)
        await self._capture(dest, mock_response)
        with pytest.raises(ValueError):
            await dest.send(["raw line"])

    async def test_failure_names_the_record_and_stops(self, monkeypatch):
        monkeypatch.setenv("TRANSFORM_CONCURRENCY", "1")  # one at a time: the stop is visible
        ok = MagicMock()
        ok.raise_for_status = MagicMock()
        bad = httpx.Response(
            404,
            request=httpx.Request("PUT", "https://api.example.com/2"),
            json={"error": "no such user", "id": 2},
        )
        dest = api_destination(url="https://api.example.com/{id}", per_record=True)
        dest._client = MagicMock()
        dest._client.request = AsyncMock(side_effect=[ok, bad, ok])
        with pytest.raises(PermanentDestinationError) as exc_info:
            await dest.send([{"id": 1}, {"id": 2}, {"id": 3}])
        assert str(exc_info.value).startswith(
            "Destination 'api' error: record 2 of 3: HTTP 404 Not Found (application/json, 31 bytes)"
        )
        assert "no such user" not in str(exc_info.value)  # the body stays out of the message
        assert dest._client.request.await_count == 2

    async def test_health_check_probes_url_before_first_placeholder(self):
        dest = api_destination(url="https://api.example.com/users/{id}", per_record=True)
        resp = MagicMock()
        resp.status_code = 200
        dest._client = MagicMock()
        dest._client.head = AsyncMock(return_value=resp)
        assert await dest.health_check() is True
        assert dest._client.head.await_args.args[0] == "https://api.example.com/users/"

    async def test_placeholders_untouched_without_per_record(self, mock_response):
        dest = api_destination(url="https://api.example.com/{id}", body={"a": 1})
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1}, {"id": 2}])
        assert len(calls) == 1
        assert calls[0]["url"] == "https://api.example.com/{id}"
        assert calls[0]["json"] == {"a": 1}

    async def test_param_placeholders_filled_from_each_record(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/users/{id}",
            per_record=True,
            params={"tenant": "acme", "v": "{version}", "q": "id:{id}", "tags": "{tags}"},
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1, "version": 3, "tags": ["a", "b"]}])
        # "{field}" alone keeps the value's type; mixed text is formatted.
        assert calls[0]["params"] == {"tenant": "acme", "v": 3, "q": "id:1", "tags": ["a", "b"]}
        # The record is still the body, unchanged.
        assert calls[0]["json"] == {"id": 1, "version": 3, "tags": ["a", "b"]}

    async def test_param_placeholders_reach_the_wire_encoded(self):
        seen = []
        transport = httpx.MockTransport(lambda req: (seen.append(req.url), httpx.Response(200))[1])
        dest = api_destination(
            url="https://api.example.com/x", per_record=True, params={"q": "{q}", "t": "{t}"}
        )
        dest._client = httpx.AsyncClient(transport=transport)
        await dest.send([{"q": "a b&c", "t": ["1", "2"]}])
        await dest.close()
        assert str(seen[0]) == "https://api.example.com/x?q=a+b%26c&t=1&t=2"

    async def test_missing_param_placeholder_field_fails_without_request(self, mock_response):
        dest = api_destination(
            url="https://api.example.com", per_record=True, params={"v": "{version}"}
        )
        calls = await self._capture(dest, mock_response)
        with pytest.raises(ValueError, match=r"params\['v'\].*\{version\}.*record 2 of 2"):
            await dest.send([{"version": 1}, {"id": 2}])
        assert calls == []

    async def test_param_placeholders_untouched_without_per_record(self, mock_response):
        dest = api_destination(url="https://api.example.com", params={"v": "{version}"})
        calls = await self._capture(dest, mock_response)
        await dest.send([{"version": 1}])
        assert calls[0]["params"] == {"v": "{version}"}

    async def test_params_field_merged_over_params_and_left_out_of_body(self, mock_response):
        dest = api_destination(
            url="https://api.example.com/users/{id}",
            per_record=True,
            params={"tenant": "acme", "mode": "full"},
            params_field="query",
        )
        calls = await self._capture(dest, mock_response)
        await dest.send(
            [
                {"id": 1, "name": "a", "query": {"mode": "patch", "dry_run": "true"}},
                {"id": 2, "name": "b"},
                {"id": 3, "query": {"tenant": None}},
                {"id": 4, "query": None},
            ]
        )
        assert [c["params"] for c in calls] == [
            {"tenant": "acme", "mode": "patch", "dry_run": "true"},
            {"tenant": "acme", "mode": "full"},
            {"mode": "full"},
            {"tenant": "acme", "mode": "full"},
        ]
        assert [c["json"] for c in calls] == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
            {"id": 3},
            {"id": 4},
        ]

    async def test_params_field_without_static_params(self, mock_response):
        dest = api_destination(url="https://api.example.com", per_record=True, params_field="q")
        calls = await self._capture(dest, mock_response)
        await dest.send([{"id": 1, "q": {"a": 1}}, {"id": 2}, "raw line"])
        assert [c["params"] for c in calls] == [{"a": 1}, None, None]
        assert calls[2]["content"] == "raw line"

    async def test_params_field_with_body_field(self, mock_response):
        dest = api_destination(
            url="https://api.example.com",
            per_record=True,
            body_field="payload",
            params_field="query",
        )
        calls = await self._capture(dest, mock_response)
        await dest.send([{"payload": {"x": 1}, "query": {"a": "1"}}])
        assert calls[0]["json"] == {"x": 1}
        assert calls[0]["params"] == {"a": "1"}

    async def test_params_field_not_a_dict_fails_without_request(self, mock_response):
        dest = api_destination(url="https://api.example.com", per_record=True, params_field="q")
        calls = await self._capture(dest, mock_response)
        with pytest.raises(ValueError, match=r"'q' on record 1 of 1 must be a dict.*str"):
            await dest.send([{"q": "a=1"}])
        assert calls == []

    def test_params_field_without_per_record_rejected(self):
        with pytest.raises(ValueError, match="per_record"):
            api_destination(url="https://api.example.com", params_field="q")

    def test_params_field_same_as_body_field_rejected(self):
        with pytest.raises(ValueError, match="must differ"):
            api_destination(
                url="https://api.example.com", per_record=True, body_field="p", params_field="p"
            )

    def test_body_with_per_record_rejected(self):
        with pytest.raises(ValueError, match="per_record"):
            api_destination(url="https://api.example.com", per_record=True, body={"a": 1})

    def test_body_field_without_per_record_rejected(self):
        with pytest.raises(ValueError, match="per_record"):
            api_destination(url="https://api.example.com", body_field="payload")


