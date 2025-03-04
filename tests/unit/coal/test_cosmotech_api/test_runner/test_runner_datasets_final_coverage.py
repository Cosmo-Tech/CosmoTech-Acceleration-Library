# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import multiprocessing
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from azure.identity import DefaultAzureCredential
from cosmotech_api import DatasetApi

from cosmotech.coal.cosmotech_api.runner.datasets import (
    download_dataset,
    download_datasets_parallel,
    download_datasets_sequential,
    download_datasets,
    dataset_to_file,
    get_dataset_ids_from_runner,
)


class TestRunnerDatasetsFinalCoverage:
    """Final tests for the datasets module to improve coverage."""

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    def test_download_datasets_sequential_pass_credentials(self, mock_download_dataset):
        """Test that download_datasets_sequential passes credentials to download_dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock credentials
        mock_credentials = MagicMock(spec=DefaultAzureCredential)

        # Mock download_dataset to return dataset info
        mock_download_dataset.side_effect = [
            {"type": "csv", "content": {}, "name": "dataset-1"},
            {"type": "csv", "content": {}, "name": "dataset-2"},
        ]

        # Act
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client") as mock_get_api_client:
            mock_api_client = MagicMock()
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            result = download_datasets_sequential(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
                credentials=mock_credentials,
            )

        # Assert
        assert len(result) == 2
        assert "dataset-1" in result
        assert "dataset-2" in result

        # Verify that download_dataset was called with the credentials
        for dataset_id in dataset_ids:
            mock_download_dataset.assert_any_call(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
                read_files=True,
                credentials=mock_credentials,
            )

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_with_parallel_true(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with parallel=True."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock parallel download result
        expected_result = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
            "dataset-2": {"type": "csv", "content": {}, "name": "dataset-2"},
        }
        mock_parallel.return_value = expected_result

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            parallel=True,
        )

        # Assert
        assert result == expected_result
        mock_parallel.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=True,
            credentials=None,
        )
        mock_sequential.assert_not_called()

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_with_parallel_false(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with parallel=False."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock sequential download result
        expected_result = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
            "dataset-2": {"type": "csv", "content": {}, "name": "dataset-2"},
        }
        mock_sequential.return_value = expected_result

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            parallel=False,
        )

        # Assert
        assert result == expected_result
        mock_sequential.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=True,
            credentials=None,
        )
        mock_parallel.assert_not_called()
