# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import patch

import pytest

from cosmotech.coal.cosmotech_api.runner.datasets import download_dataset_process


class TestRunnerDatasetsProcess:
    """Tests for the download_dataset_process function."""

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    def test_download_dataset_process_success(self, mock_download_dataset):
        """Test the download_dataset_process function with successful download."""
        # Arrange
        dataset_id = "dataset-123"
        organization_id = "org-123"
        workspace_id = "ws-123"
        read_files = True

        # Create shared dictionaries
        return_dict = {}
        error_dict = {}

        # Mock download_dataset to return dataset info
        mock_dataset_info = {
            "type": "csv",
            "content": {"data": "test data"},
            "name": "test-dataset",
            "folder_path": "/tmp/dataset",
            "dataset_id": dataset_id,
        }
        mock_download_dataset.return_value = mock_dataset_info

        # Act
        download_dataset_process(dataset_id, organization_id, workspace_id, read_files, return_dict, error_dict)

        # Assert
        mock_download_dataset.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            read_files=read_files,
        )
        assert dataset_id in return_dict
        assert return_dict[dataset_id] == mock_dataset_info
        assert len(error_dict) == 0

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    def test_download_dataset_process_error(self, mock_download_dataset):
        """Test the download_dataset_process function with an error."""
        # Arrange
        dataset_id = "dataset-123"
        organization_id = "org-123"
        workspace_id = "ws-123"
        read_files = True

        # Create shared dictionaries
        return_dict = {}
        error_dict = {}

        # Mock download_dataset to raise an exception
        mock_error = ValueError("Failed to download dataset")
        mock_download_dataset.side_effect = mock_error

        # Act & Assert
        with pytest.raises(ValueError) as excinfo:
            download_dataset_process(dataset_id, organization_id, workspace_id, read_files, return_dict, error_dict)

        # Verify the error was re-raised
        assert str(excinfo.value) == "Failed to download dataset"

        # Verify the error was stored in the error dictionary
        assert dataset_id in error_dict
        assert error_dict[dataset_id] == "ValueError: Failed to download dataset"

        # Verify the return dictionary is empty
        assert len(return_dict) == 0
