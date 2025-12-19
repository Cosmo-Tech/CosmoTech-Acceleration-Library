# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.cosmotech_api.apis.runner import RunnerApi
from cosmotech.coal.utils.configuration import Configuration


@pytest.fixture
def base_runner_config():
    return Configuration(
        {
            "cosmotech": {
                "organization_id": "org-123",
                "workspace_id": "ws-456",
                "runner_id": "runner-789",
                "parameters_absolute_path": "/tmp/params",
                "datasets_absolute_path": "/tmp/datasets",
            }
        }
    )


class TestRunnerApi:
    """Tests for the RunnerApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_runner_api_initialization(self, mock_api_client, base_runner_config):
        """Test RunnerApi initialization."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        api = RunnerApi(configuration=base_runner_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == base_runner_config

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_get_runner_metadata_default_config(self, mock_api_client, base_runner_config):
        """Test getting runner metadata."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner object
        mock_runner = MagicMock()
        mock_runner.model_dump.return_value = {
            "id": "runner-123",
            "name": "Test Runner",
            "description": "Test Description",
        }

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner)

        result = api.get_runner_metadata()

        assert result == {"id": "runner-123", "name": "Test Runner", "description": "Test Description"}
        api.get_runner.assert_called_once_with("org-123", "ws-456", "runner-789")
        mock_runner.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True, include=None, exclude=None, mode="json"
        )

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_get_runner_metadata(self, mock_api_client, base_runner_config):
        """Test getting runner metadata."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner object
        mock_runner = MagicMock()
        mock_runner.model_dump.return_value = {
            "id": "runner-123",
            "name": "Test Runner",
            "description": "Test Description",
        }

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner)

        result = api.get_runner_metadata("runner-1000")

        assert result == {"id": "runner-123", "name": "Test Runner", "description": "Test Description"}
        api.get_runner.assert_called_once_with("org-123", "ws-456", "runner-1000")
        mock_runner.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True, include=None, exclude=None, mode="json"
        )

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_get_runner_metadata_with_include(self, mock_api_client, base_runner_config):
        """Test getting runner metadata with include filter."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner object
        mock_runner = MagicMock()
        mock_runner.model_dump.return_value = {"id": "runner-123", "name": "Test Runner"}

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner)

        result = api.get_runner_metadata(include=["id", "name"])

        assert result == {"id": "runner-123", "name": "Test Runner"}
        mock_runner.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True, include=["id", "name"], exclude=None, mode="json"
        )

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_get_runner_metadata_with_exclude(self, mock_api_client, base_runner_config):
        """Test getting runner metadata with exclude filter."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner object
        mock_runner = MagicMock()
        mock_runner.model_dump.return_value = {"id": "runner-123", "name": "Test Runner"}

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner)

        result = api.get_runner_metadata(exclude=["description"])

        assert result == {"id": "runner-123", "name": "Test Runner"}
        mock_runner.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True, include=None, exclude=["description"], mode="json"
        )

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech.coal.cosmotech_api.apis.runner.Parameters")
    def test_download_runner_data_with_parameters(self, mock_parameters_class, mock_api_client, base_runner_config):
        """Test downloading runner data with parameters."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner data with parameters
        mock_runner_data = MagicMock()
        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        mock_runner_data.parameters_values = [param1]
        mock_runner_data.datasets.bases = []

        # Mock Parameters instance
        mock_parameters_instance = MagicMock()
        mock_parameters_class.return_value = mock_parameters_instance

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner_data)

        api.download_runner_data()

        api.get_runner.assert_called_once_with("org-123", "ws-456", "runner-789")
        mock_parameters_class.assert_called_once_with(mock_runner_data)
        mock_parameters_instance.write_parameters_to_json.assert_called_once_with("/tmp/params")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_download_runner_data_no_parameters(self, mock_api_client, base_runner_config):
        """Test downloading runner data without parameters."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner data without parameters
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = None
        mock_runner_data.datasets.bases = []

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner_data)

        # Should not raise an exception
        api.download_runner_data()

        api.get_runner.assert_called_once_with("org-123", "ws-456", "runner-789")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech.coal.cosmotech_api.apis.runner.DatasetApi")
    def test_download_runner_data_with_datasets(self, mock_dataset_api_class, mock_api_client, base_runner_config):
        """Test downloading runner data with datasets."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner data with datasets
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = None
        mock_runner_data.datasets.bases = ["dataset-1", "dataset-2"]

        # Mock DatasetApi instance
        mock_dataset_api_instance = MagicMock()
        mock_dataset_api_class.return_value = mock_dataset_api_instance

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner_data)

        api.download_runner_data("/tmp/datasets")

        api.get_runner.assert_called_once_with("org-123", "ws-456", "runner-789")
        mock_dataset_api_class.assert_called_once_with(base_runner_config)
        assert mock_dataset_api_instance.download_dataset.call_count == 2
        mock_dataset_api_instance.download_dataset.assert_any_call("dataset-1")
        mock_dataset_api_instance.download_dataset.assert_any_call("dataset-2")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    def test_download_runner_data_no_datasets_without_folder(self, mock_api_client, base_runner_config):
        """Test downloading runner data without datasets when dataset_folder is None."""
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance

        # Mock runner data with datasets
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = None
        mock_runner_data.datasets.bases = ["dataset-1"]

        api = RunnerApi(configuration=base_runner_config)
        api.get_runner = MagicMock(return_value=mock_runner_data)

        # Should not try to download datasets when dataset_folder is None
        api.download_runner_data(dataset_folder=None)

        api.get_runner.assert_called_once_with("org-123", "ws-456", "runner-789")
