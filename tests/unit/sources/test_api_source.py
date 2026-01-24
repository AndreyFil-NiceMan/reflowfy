"""Unit tests for API sources."""

import pytest
from unittest.mock import MagicMock, patch
import httpx

from reflowfy.sources.api import (
    PaginatedAPISource,
    IDBasedAPISource,
    paginated_api_source,
    id_based_api_source,
)


class TestPaginatedAPISource:
    """Tests for PaginatedAPISource class."""
    
    def test_init(self):
        """Test PaginatedAPISource initialization."""
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            pagination_type="offset",
            page_size=100,
        )
        
        assert source.config["base_url"] == "https://api.example.com"
        assert source.config["endpoint"] == "/users"
        assert source.config["pagination_type"] == "offset"
        assert source.config["page_size"] == 100
    
    def test_factory_function(self):
        """Test paginated_api_source factory function."""
        source = paginated_api_source(
            base_url="https://api.example.com",
            endpoint="/users",
            pagination_type="cursor",
            page_size=50,
        )
        
        assert isinstance(source, PaginatedAPISource)
        assert source.config["pagination_type"] == "cursor"
        assert source.config["page_size"] == 50
    
    def test_extract_data_with_key(self):
        """Test extracting data from response with data key."""
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            data_key="data",
        )
        
        response_data = {
            "data": [{"id": 1}, {"id": 2}],
            "meta": {"total": 100}
        }
        
        records = source._extract_data(response_data)
        assert len(records) == 2
        assert records[0]["id"] == 1
    
    def test_extract_data_nested_key(self):
        """Test extracting data with nested key."""
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            data_key="response.results",
        )
        
        response_data = {
            "response": {
                "results": [{"id": 1}],
                "total": 10
            }
        }
        
        records = source._extract_data(response_data)
        assert len(records) == 1
    
    def test_extract_data_no_key(self):
        """Test extracting data when response is the array."""
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            data_key="",
        )
        
        response_data = [{"id": 1}, {"id": 2}]
        records = source._extract_data(response_data)
        assert len(records) == 2
    
    def test_get_next_cursor(self):
        """Test extracting cursor from response."""
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            pagination_type="cursor",
            cursor_response_key="meta.next_cursor",
        )
        
        response_data = {
            "data": [],
            "meta": {"next_cursor": "abc123"}
        }
        
        cursor = source._get_next_cursor(response_data)
        assert cursor == "abc123"
    
    @patch("httpx.Client")
    def test_fetch_offset_pagination(self, mock_client_class):
        """Test fetching with offset pagination."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": 1}, {"id": 2}]
        }
        mock_client.request.return_value = mock_response
        
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            pagination_type="offset",
        )
        
        records = source.fetch({})
        
        assert len(records) == 2
        mock_client.request.assert_called_once()
    
    @patch("httpx.Client")
    def test_split_jobs_offset(self, mock_client_class):
        """Test job splitting with offset pagination."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Return 2 pages, then empty
        responses = [
            {"data": [{"id": i} for i in range(10)]},
            {"data": [{"id": i} for i in range(10, 15)]},  # Partial page = last
        ]
        
        mock_response = MagicMock()
        mock_response.json.side_effect = responses
        mock_client.request.return_value = mock_response
        
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            pagination_type="offset",
            page_size=10,
        )
        
        jobs = list(source.split_jobs({}))
        
        assert len(jobs) == 2
        assert jobs[0].metadata["page_num"] == 0
        assert jobs[1].metadata["page_num"] == 1
    
    def test_health_check_success(self):
        """Test health check success."""
        with patch("httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_client.request.return_value = mock_response
            
            source = PaginatedAPISource(
                base_url="https://api.example.com",
                endpoint="/users",
            )
            
            assert source.health_check() is True


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


class TestAuthenticationHeaders:
    """Test authentication handling."""
    
    @patch("httpx.Client")
    def test_bearer_auth(self, mock_client_class):
        """Test bearer token authentication."""
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
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
        source = PaginatedAPISource(
            base_url="https://api.example.com",
            endpoint="/users",
            auth_type="apikey",
            auth_token="my-api-key",
        )
        
        source._get_client()
        
        call_kwargs = mock_client_class.call_args[1]
        assert "X-API-Key" in call_kwargs["headers"]
        assert call_kwargs["headers"]["X-API-Key"] == "my-api-key"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
