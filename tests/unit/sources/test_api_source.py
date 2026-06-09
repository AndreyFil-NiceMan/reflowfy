"""Unit tests for API sources."""

from unittest.mock import MagicMock, patch

import pytest

from reflowfy.sources.api import IDBasedAPISource, id_based_api_source


class TestIDBasedAPISource:
    """Tests for IDBasedAPISource class."""

    def test_init(self):
        """Test IDBasedAPISource initialization."""
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            ids=[1, 2, 3],
        )

        assert source.config["base_url"] == "https://api.example.com"
        assert source.config["endpoint_template"] == "/users/{id}"
        assert source.config["ids"] == [1, 2, 3]

    def test_factory_function(self):
        """Test id_based_api_source factory function."""
        source = id_based_api_source(
            base_url="https://api.example.com",
            endpoint_template="/products/{id}",
            ids=["a", "b", "c"],
            batch_size=2,
        )

        assert isinstance(source, IDBasedAPISource)
        assert source.config["batch_size"] == 2

    def test_get_all_ids_from_config(self):
        """Test getting IDs from config."""
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            ids=[1, 2, 3],
        )

        ids = source._get_all_ids({})
        assert ids == [1, 2, 3]

    def test_get_all_ids_from_runtime_params(self):
        """Test getting IDs from runtime parameters."""
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
        )

        ids = source._get_all_ids({"ids": [4, 5, 6]})
        assert ids == [4, 5, 6]

    @patch("httpx.Client")
    def test_fetch_by_id(self, mock_client_class):
        """Test fetching a single resource by ID."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": 1, "name": "Test"}
        mock_client.request.return_value = mock_response

        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            ids=[1],
        )

        record = source._fetch_by_id(1)
        assert record["id"] == 1
        assert record["name"] == "Test"

    @patch("httpx.Client")
    def test_fetch_by_id_not_found(self, mock_client_class):
        """Test handling 404 response."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client.request.return_value = mock_response

        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
        )

        record = source._fetch_by_id(999)
        assert record is None

    @patch("httpx.Client")
    def test_split_jobs_batching(self, mock_client_class):
        """Test job splitting with batching."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": 1, "name": "Test"}
        mock_client.request.return_value = mock_response

        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            ids=[1, 2, 3, 4, 5],
            batch_size=2,
        )

        jobs = list(source.split_jobs({}))

        # 5 IDs with batch_size=2 = 3 batches
        assert len(jobs) == 3
        assert jobs[0].metadata["id_count"] == 2
        assert jobs[1].metadata["id_count"] == 2
        assert jobs[2].metadata["id_count"] == 1

    @patch("httpx.Client")
    def test_health_check_disabled_skips_requests(self, mock_client_class):
        """Health check can be disabled via config."""
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            health_check_enabled=False,
        )

        assert source.health_check() is True
        mock_client_class.assert_not_called()

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


class TestAuthenticationHeaders:
    """Test authentication handling."""

    @patch("httpx.Client")
    def test_bearer_auth(self, mock_client_class):
        """Test bearer token authentication."""
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            auth_type="bearer",
            auth_token="secret-token",
        )

        source._get_client()

        # Verify httpx.Client was called with correct headers
        call_kwargs = mock_client_class.call_args[1]
        assert "Authorization" in call_kwargs["headers"]
        assert call_kwargs["headers"]["Authorization"] == "Bearer secret-token"

    @patch("httpx.Client")
    def test_apikey_auth(self, mock_client_class):
        """Test API key authentication."""
        source = IDBasedAPISource(
            base_url="https://api.example.com",
            endpoint_template="/users/{id}",
            auth_type="apikey",
            auth_token="my-api-key",
        )

        source._get_client()

        call_kwargs = mock_client_class.call_args[1]
        assert "X-API-Key" in call_kwargs["headers"]
        assert call_kwargs["headers"]["X-API-Key"] == "my-api-key"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
