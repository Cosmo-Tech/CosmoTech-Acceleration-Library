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


class TestRunnerDatasetsAdditionalCoverage:
    """Additional tests for the datasets module to improve coverage."""

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("multiprocessing.Process")
    @patch("multiprocessing.Manager")
    def test_download_datasets_parallel_process_error_with_nonzero_exitcode(
        self, mock_manager, mock_process, mock_download_dataset
    ):
        """Test the download_datasets_parallel function with a process that has nonzero exitcode and is in error_dict."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock multiprocessing
        mock_return_dict = {"dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"}}
        mock_error_dict = {"dataset-2": "ValueError: Failed to download dataset"}  # Error message exists
        mock_manager_instance = MagicMock()
        mock_manager_instance.dict.side_effect = [mock_return_dict, mock_error_dict]
        mock_manager.return_value = mock_manager_instance

        # Mock processes
        mock_process_instance1 = MagicMock()
        mock_process_instance1.exitcode = 0
        mock_process_instance2 = MagicMock()
        mock_process_instance2.exitcode = 1  # Nonzero exitcode and there's an error message
        mock_process.side_effect = [mock_process_instance1, mock_process_instance2]

        # Act & Assert
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client") as mock_get_api_client:
            mock_api_client = MagicMock()
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # This should raise an exception because there's a nonzero exitcode and an error message in error_dict
            with pytest.raises(ChildProcessError) as excinfo:
                download_datasets_parallel(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_ids=dataset_ids,
                )

            # Verify the error message
            assert "Failed to download dataset 'dataset-2'" in str(excinfo.value)
            assert "ValueError: Failed to download dataset" in str(excinfo.value)

    def test_get_dataset_ids_from_runner_with_parameters(self):
        """Test the get_dataset_ids_from_runner function with dataset parameters."""
        # Arrange
        # Create a mock runner data object with dataset_list and parameters_values
        runner_data = MagicMock()
        runner_data.dataset_list = ["dataset-1", "dataset-2"]

        # Create parameter values with a dataset ID parameter
        param1 = MagicMock()
        param1.var_type = "%DATASETID%"
        param1.value = "dataset-3"

        param2 = MagicMock()
        param2.var_type = "string"
        param2.value = "not-a-dataset"

        param3 = MagicMock()
        param3.var_type = "%DATASETID%"
        param3.value = ""  # Empty value should be ignored

        runner_data.parameters_values = [param1, param2, param3]

        # Act
        result = get_dataset_ids_from_runner(runner_data)

        # Assert
        assert len(result) == 3
        assert "dataset-1" in result
        assert "dataset-2" in result
        assert "dataset-3" in result
        assert "not-a-dataset" not in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.convert_graph_dataset_to_files")
    def test_dataset_to_file_with_graph_dataset_and_target_folder(self, mock_convert):
        """Test the dataset_to_file function with a graph dataset and target folder."""
        # Arrange
        dataset_info = {
            "type": "twincache",  # Graph dataset
            "content": {"nodes": [], "edges": []},
            "name": "test-dataset",
            "folder_path": "/tmp/original",
            "dataset_id": "dataset-123",
        }
        target_folder = "/tmp/target"

        # Mock the conversion function
        mock_convert.return_value = Path("/tmp/target/converted")

        # Act
        result = dataset_to_file(dataset_info, target_folder)

        # Assert
        assert result == "/tmp/target/converted"
        mock_convert.assert_called_once_with(dataset_info["content"], target_folder)
