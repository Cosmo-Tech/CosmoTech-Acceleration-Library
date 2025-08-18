# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.cosmotech_api.runner.datasets import (
    download_datasets_parallel,
    download_datasets_sequential,
    dataset_to_file,
)


class TestDatasetsCoverage:
    """Additional tests for the datasets module to improve coverage."""

    @patch("multiprocessing.Process")
    @patch("multiprocessing.Manager")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_parallel_with_error(self, mock_get_api_client, mock_manager, mock_process):
        """Test the download_datasets_parallel function with an error in one of the processes."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (
            mock_api_client,
            "Azure Entra Connection",
        )

        # Mock multiprocessing
        mock_return_dict = {}
        mock_error_dict = {"dataset-2": "ValueError: Failed to download dataset"}
        mock_manager_instance = MagicMock()
        mock_manager_instance.dict.side_effect = [mock_return_dict, mock_error_dict]
        mock_manager.return_value = mock_manager_instance

        # Mock processes
        mock_process_instance1 = MagicMock()
        mock_process_instance1.exitcode = 0
        mock_process_instance2 = MagicMock()
        mock_process_instance2.exitcode = 1  # Error exit code
        mock_process.side_effect = [mock_process_instance1, mock_process_instance2]

        # Act & Assert
        with pytest.raises(ChildProcessError) as excinfo:
            download_datasets_parallel(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )

        # Verify the error message
        assert "Failed to download dataset 'dataset-2'" in str(excinfo.value)
        assert "ValueError: Failed to download dataset" in str(excinfo.value)

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_sequential_with_error(self, mock_get_api_client, mock_download_dataset):
        """Test the download_datasets_sequential function with an error."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock API client to return a non-Azure connection type
        mock_api_client = MagicMock()
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock download_dataset to succeed for the first dataset and fail for the second
        mock_download_dataset.side_effect = [
            {"type": "csv", "content": {}, "name": "dataset-1"},  # First call succeeds
            ValueError("Failed to download dataset"),  # Second call raises an exception
        ]

        # Act & Assert
        with pytest.raises(ChildProcessError) as excinfo:
            download_datasets_sequential(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )

        # Verify the error message
        assert "Failed to download dataset 'dataset-2'" in str(excinfo.value)

        # Verify download_dataset was called for both datasets
        assert mock_download_dataset.call_count == 2
        mock_download_dataset.assert_any_call(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id="dataset-1",
            read_files=True,
        )
        mock_download_dataset.assert_any_call(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id="dataset-2",
            read_files=True,
        )

    @patch("tempfile.mkdtemp")
    def test_dataset_to_file_with_file_dataset_and_folder_path(self, mock_mkdtemp):
        """Test the dataset_to_file function with a file dataset that has a folder_path."""
        # Arrange
        dataset_info = {
            "type": "csv",
            "content": {"data": "test data"},
            "name": "test-dataset",
            "folder_path": "/tmp/dataset",
            "dataset_id": "dataset-123",
            "file_name": "data.csv",
        }

        # Act
        result = dataset_to_file(dataset_info)

        # Assert
        assert result == "/tmp/dataset"
        mock_mkdtemp.assert_not_called()  # Should not create a temp dir since folder_path is provided

    @patch("tempfile.mkdtemp")
    def test_dataset_to_file_fallback_to_temp_dir(self, mock_mkdtemp):
        """Test the dataset_to_file function fallback to creating a temp directory."""
        # Arrange
        dataset_info = {
            "type": "unknown",  # Not a graph dataset
            "content": {"data": "test data"},
            "name": "test-dataset",
            # No folder_path provided
        }

        # Mock tempfile.mkdtemp to return a specific path
        mock_mkdtemp.return_value = "/tmp/tempdir"

        # Act
        result = dataset_to_file(dataset_info)

        # Assert
        assert result == "/tmp/tempdir"
        mock_mkdtemp.assert_called_once()  # Should create a temp dir since no folder_path is provided

    @patch("tempfile.mkdtemp")
    def test_dataset_to_file_with_target_folder_non_graph(self, mock_mkdtemp):
        """Test the dataset_to_file function with a target folder for a non-graph dataset."""
        # Arrange
        dataset_info = {
            "type": "csv",  # Not a graph dataset
            "content": {"data": "test data"},
            "name": "test-dataset",
            # No folder_path provided
        }
        target_folder = "/tmp/target"

        # Act
        result = dataset_to_file(dataset_info, target_folder)

        # Assert
        assert result == "/tmp/target"
        mock_mkdtemp.assert_not_called()  # Should not create a temp dir since target_folder is provided
