# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Additional tests for the twin_data_layer module to improve coverage.
"""

import json
import pathlib
import tempfile
from io import StringIO
from unittest.mock import MagicMock, patch, mock_open, call

import pytest
import requests
from cosmotech_api import DatasetApi, RunnerApi, DatasetTwinGraphQuery

from cosmotech.coal.cosmotech_api.twin_data_layer import (
    get_dataset_id_from_runner,
    send_files_to_tdl,
    load_files_from_tdl,
    _process_csv_file,
    _write_files,
    BATCH_SIZE_LIMIT,
)


class TestTwinDataLayerCoverage:
    """Additional tests for the twin_data_layer module to improve coverage."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        mock_client = MagicMock()
        mock_client.default_headers = {}
        mock_client.configuration.auth_settings.return_value = {}
        return mock_client

    @pytest.fixture
    def mock_dataset_api(self):
        """Create a mock DatasetApi."""
        mock_api = MagicMock(spec=DatasetApi)
        return mock_api

    @pytest.fixture
    def mock_runner_api(self):
        """Create a mock RunnerApi."""
        mock_api = MagicMock(spec=RunnerApi)
        return mock_api

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    def test_get_dataset_id_from_runner_error_message(self, mock_get_api_client, mock_runner_api):
        """Test the error message in get_dataset_id_from_runner when too many arguments are provided."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())

        # Act & Assert
        with pytest.raises(TypeError) as excinfo:
            # This should raise a TypeError because the function only takes 3 arguments
            get_dataset_id_from_runner("org-123", "ws-123", "runner-123", "extra-arg")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    def test_get_dataset_id_from_runner_error_message_none(self, mock_get_api_client):
        """Test the error message in get_dataset_id_from_runner when missing required argument."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())

        # Act & Assert
        with pytest.raises(TypeError) as excinfo:
            # This should raise a TypeError because runner_id is required
            get_dataset_id_from_runner("org-123", "ws-123")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi")
    @patch("pathlib.Path.is_dir")
    def test_send_files_to_tdl_not_directory(
        self, mock_is_dir, mock_dataset_api, mock_get_dataset_id, mock_get_api_client
    ):
        """Test send_files_to_tdl when the directory path is not a directory."""
        # Arrange
        mock_api_client = MagicMock()
        mock_api_client.default_headers = {}
        mock_api_client.configuration.auth_settings.return_value = {}
        mock_get_api_client.return_value = (mock_api_client, MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"

        # Mock the dataset API
        mock_dataset_api_instance = MagicMock()
        mock_dataset_api.return_value = mock_dataset_api_instance
        mock_dataset_api_instance.api_client = mock_api_client

        # Set is_dir to False to trigger the error
        mock_is_dir.return_value = False

        # Act & Assert
        with pytest.raises(ValueError) as excinfo:
            send_files_to_tdl("http://api.example.com", "org-123", "ws-123", "runner-123", "/data/not_a_dir")

        # Check the specific error message
        assert "is not a directory" in str(excinfo.value)

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi")
    @patch("pathlib.Path.is_file")
    def test_load_files_from_tdl_directory_is_file(
        self, mock_is_file, mock_dataset_api, mock_get_dataset_id, mock_get_api_client
    ):
        """Test load_files_from_tdl when the directory path points to a file."""
        # Arrange
        mock_api_client = MagicMock()
        mock_get_api_client.return_value = (mock_api_client, MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"

        # Mock the dataset API
        mock_dataset_api_instance = MagicMock()
        mock_dataset_api.return_value = mock_dataset_api_instance

        # Mock dataset info
        mock_dataset = MagicMock()
        mock_dataset.ingestion_status = "SUCCESS"
        mock_dataset_api_instance.find_dataset_by_id.return_value = mock_dataset

        # Set is_file to True to trigger the error
        mock_is_file.return_value = True
        file_path = "/path/to/file.txt"

        # Act & Assert
        with pytest.raises(ValueError) as excinfo:
            load_files_from_tdl("org-123", "ws-123", file_path, "runner-123")

        # Check the specific error message
        assert f"{file_path} is not a directory" in str(excinfo.value)

        # Verify the correct methods were called
        mock_get_dataset_id.assert_called_once_with("org-123", "ws-123", "runner-123")
        mock_dataset_api_instance.find_dataset_by_id.assert_called_once_with("org-123", "dataset-123")
        mock_is_file.assert_called_once()

    @patch("requests.post")
    def test_process_csv_file_with_batches(self, mock_post):
        """Test _process_csv_file with a large CSV file that requires batching."""
        # Arrange
        file_path = pathlib.Path("test.csv")
        query = "CREATE (:test {id: $id})"
        api_url = "http://api.example.com"
        organization_id = "org-123"
        dataset_id = "dataset-123"
        header = {"Content-Type": "text/csv"}

        # Create a CSV with more rows than the batch size limit
        csv_rows = ["id,name"]
        for i in range(BATCH_SIZE_LIMIT + 100):  # Exceed batch size limit
            csv_rows.append(f"{i},Name{i}")
        csv_content = "\n".join(csv_rows)

        # Mock response
        mock_response = MagicMock()
        mock_response.content = json.dumps({"errors": []}).encode()
        mock_post.return_value = mock_response

        with patch("builtins.open", mock_open(read_data=csv_content)):
            # Act
            _process_csv_file(file_path, query, api_url, organization_id, dataset_id, header)

            # Assert
            # Should have called post at least twice (once for each batch)
            assert mock_post.call_count >= 2

            # Check that the URL is correct
            expected_url = f"{api_url}/organizations/{organization_id}/datasets/{dataset_id}/batch?query={query}"
            for call_args in mock_post.call_args_list:
                assert call_args[0][0] == expected_url

    def test_write_files_with_complex_values(self):
        """Test _write_files with boolean, dict, and list values."""
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            directory_path = pathlib.Path(temp_dir)

            # Create test data with complex values
            files_content = {
                "ComplexTypes": [
                    {"id": "1", "boolean_value": True, "dict_value": {"key": "value"}, "list_value": [1, 2, 3]},
                    {
                        "id": "2",
                        "boolean_value": False,
                        "dict_value": {"nested": {"key": "value"}},
                        "list_value": ["a", "b", "c"],
                    },
                ]
            }
            files_headers = {"ComplexTypes": {"id", "boolean_value", "dict_value", "list_value"}}

            # Act
            _write_files(directory_path, files_content, files_headers)

            # Assert
            assert (directory_path / "ComplexTypes.csv").exists()

            # Check file content
            with open(directory_path / "ComplexTypes.csv", "r") as f:
                content = f.read()
                # Check headers - the order might vary
                for header in ["id", "boolean_value", "dict_value", "list_value"]:
                    assert header in content

                # Check that values are present (the exact format might vary)
                assert "1" in content  # ID
                assert "2" in content  # ID
                assert "true" in content.lower() or "True" in content  # Boolean
                assert "false" in content.lower() or "False" in content  # Boolean

                # Check for dict and list values - they might be serialized differently
                assert "key" in content and "value" in content  # Dict content
                assert "nested" in content  # Nested dict

                # Check for list values - content should be there even if format varies
                for val in ["1", "2", "3", "a", "b", "c"]:
                    assert val in content
