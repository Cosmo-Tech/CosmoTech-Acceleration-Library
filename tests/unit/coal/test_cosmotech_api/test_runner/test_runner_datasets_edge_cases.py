# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from pathlib import Path
from unittest.mock import MagicMock, patch, ANY

import pytest
from azure.identity import DefaultAzureCredential
from cosmotech_api import DatasetApi

from cosmotech.coal.cosmotech_api.runner.datasets import (
    download_dataset,
    download_datasets_parallel,
    download_datasets_sequential,
    download_datasets,
    dataset_to_file,
)
from cosmotech.coal.utils.semver import semver_of


class TestDatasetsEdgeCases:
    """Tests for edge cases in the datasets module."""

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_adt_dataset")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major >= 5, reason='not supported in version 5'
    )
    def test_download_dataset_adt_pass_credentials(self, mock_download_adt, mock_get_api_client):
        """Test that download_dataset passes credentials to download_adt_dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset = MagicMock()
        mock_dataset.name = "test-dataset"
        mock_dataset.connector = MagicMock()
        mock_dataset.connector.parameters_values = {"AZURE_DIGITAL_TWINS_URL": "https://adt.example.com"}
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock ADT download
        mock_content = {"nodes": [], "edges": []}
        mock_folder_path = Path("/tmp/adt")
        mock_download_adt.return_value = (mock_content, mock_folder_path)

        # Create a mock credential
        mock_credential = MagicMock(spec=DefaultAzureCredential)

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_download_adt.assert_called_once_with(
                adt_address="https://adt.example.com",
                credentials=ANY,
            )
            assert result["type"] == "adt"

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("multiprocessing.Process")
    @patch("multiprocessing.Manager")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_parallel_error(
        self, mock_get_api_client, mock_manager, mock_process, mock_download_dataset
    ):
        """Test the download_datasets_parallel function with an error."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock multiprocessing
        mock_return_dict = {}
        mock_error_dict = {"dataset-2": "ValueError: Test error"}
        mock_manager_instance = MagicMock()
        mock_manager_instance.dict.side_effect = [mock_return_dict, mock_error_dict]
        mock_manager.return_value = mock_manager_instance

        # Mock processes
        mock_process_instance1 = MagicMock()
        mock_process_instance1.exitcode = 0
        mock_process_instance2 = MagicMock()
        mock_process_instance2.exitcode = 1  # Error exit code
        mock_process.side_effect = [mock_process_instance1, mock_process_instance2]

        # Mock dataset download results
        mock_return_dict["dataset-1"] = {"type": "csv", "content": {}, "name": "dataset-1"}

        # Act & Assert
        with pytest.raises(ChildProcessError) as excinfo:
            download_datasets_parallel(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )

        assert "Failed to download dataset 'dataset-2'" in str(excinfo.value)
        assert "ValueError: Test error" in str(excinfo.value)

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_sequential_error(self, mock_get_api_client, mock_download_dataset):
        """Test the download_datasets_sequential function with an error."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset download results
        mock_download_dataset.side_effect = [
            {"type": "csv", "content": {}, "name": "dataset-1"},
            ValueError("Test error"),
        ]

        # Act & Assert
        with pytest.raises(ChildProcessError) as excinfo:
            download_datasets_sequential(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )

        assert "Failed to download dataset 'dataset-2'" in str(excinfo.value)

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_empty(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with empty dataset IDs."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = []

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )

        # Assert
        mock_sequential.assert_not_called()
        mock_parallel.assert_not_called()
        assert result == {}

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.convert_graph_dataset_to_files")
    @patch("tempfile.mkdtemp")
    def test_dataset_to_file_no_folder_path(self, mock_mkdtemp, mock_convert):
        """Test the dataset_to_file function with no folder path."""
        # Arrange
        dataset_info = {
            "type": "unknown",
            "content": {},
            "name": "test-dataset",
            # No folder_path
        }

        # Mock temp dir
        mock_mkdtemp.return_value = "/tmp/temp-dir"

        # Act
        result = dataset_to_file(dataset_info)

        # Assert
        mock_mkdtemp.assert_called_once()
        assert result == "/tmp/temp-dir"
