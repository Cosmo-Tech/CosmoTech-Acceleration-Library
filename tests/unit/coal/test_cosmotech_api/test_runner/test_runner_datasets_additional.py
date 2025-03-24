# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import multiprocessing
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from azure.identity import DefaultAzureCredential
from cosmotech_api import DatasetApi

from cosmotech.coal.cosmotech_api.runner.datasets import (
    download_dataset,
    download_datasets_parallel,
    download_datasets,
    dataset_to_file,
)


class TestDatasetsAdditional:
    """Additional tests for the datasets module to improve coverage."""

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_dataset_no_connector(self, mock_get_api_client):
        """Test the download_dataset function with a dataset that has no connector."""
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
        mock_dataset.connector = None  # No connector
        mock_dataset.tags = None
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock twin graph download
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            with patch(
                "cosmotech.coal.cosmotech_api.runner.datasets.download_twingraph_dataset"
            ) as mock_download_twingraph:
                mock_content = {"nodes": [], "edges": []}
                mock_folder_path = "/tmp/twingraph"
                mock_download_twingraph.return_value = (mock_content, mock_folder_path)

                # Act
                result = download_dataset(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_id=dataset_id,
                )

                # Assert
                mock_dataset_api.find_dataset_by_id.assert_called_once_with(
                    organization_id=organization_id, dataset_id=dataset_id
                )
                mock_download_twingraph.assert_called_once_with(organization_id=organization_id, dataset_id=dataset_id)
                assert result["type"] == "twincache"
                assert result["content"] == mock_content
                assert result["name"] == "test-dataset"
                assert result["folder_path"] == str(mock_folder_path)
                assert result["dataset_id"] == dataset_id

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("multiprocessing.Process")
    @patch("multiprocessing.Manager")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_parallel_process_error_no_message(
        self, mock_get_api_client, mock_manager, mock_process, mock_download_dataset
    ):
        """Test the download_datasets_parallel function with a process error but no error message."""
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
        mock_error_dict = {}  # No error message
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

        # Act
        # This should not raise an exception because we're handling the case where exitcode != 0 but no error message
        result = download_datasets_parallel(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )

        # Assert
        assert len(result) == 1
        assert "dataset-1" in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_empty_list(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with an empty list."""
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

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_none(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with None."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = None

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

    @patch("multiprocessing.Process")
    @patch("multiprocessing.Manager")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_parallel_start_join(self, mock_get_api_client, mock_manager, mock_process):
        """Test the start and join operations in download_datasets_parallel."""
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
        mock_error_dict = {}
        mock_manager_instance = MagicMock()
        mock_manager_instance.dict.side_effect = [mock_return_dict, mock_error_dict]
        mock_manager.return_value = mock_manager_instance

        # Mock processes
        mock_process_instance1 = MagicMock()
        mock_process_instance2 = MagicMock()
        mock_process.side_effect = [mock_process_instance1, mock_process_instance2]

        # Act
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset") as mock_download_dataset:
            download_datasets_parallel(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )

        # Assert
        # Check that start and join were called for each process
        mock_process_instance1.start.assert_called_once()
        mock_process_instance2.start.assert_called_once()
        mock_process_instance1.join.assert_called_once()
        mock_process_instance2.join.assert_called_once()

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_single_dataset_with_parallel_true(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with a single dataset and parallel=True."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1"]  # Single dataset

        # Mock sequential download result
        mock_sequential.return_value = {"dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"}}

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            parallel=True,  # Even though parallel is True, it should use sequential for a single dataset
        )

        # Assert
        mock_sequential.assert_called_once()
        mock_parallel.assert_not_called()
        assert "dataset-1" in result

    @patch("tempfile.mkdtemp")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.convert_graph_dataset_to_files")
    def test_dataset_to_file_with_target_folder(self, mock_convert, mock_mkdtemp):
        """Test the dataset_to_file function with a target folder."""
        # Arrange
        dataset_info = {
            "type": "adt",
            "content": {"nodes": [], "edges": []},
            "name": "test-dataset",
        }
        target_folder = "/tmp/target"

        # Mock conversion
        mock_convert.return_value = Path("/tmp/target/converted")

        # Act
        result = dataset_to_file(dataset_info, target_folder)

        # Assert
        mock_convert.assert_called_once_with(dataset_info["content"], target_folder)
        mock_mkdtemp.assert_not_called()
        assert result == "/tmp/target/converted"
