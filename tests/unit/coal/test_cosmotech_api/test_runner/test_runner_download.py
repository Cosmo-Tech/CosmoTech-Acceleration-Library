# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import pathlib
import shutil
import tempfile
from unittest.mock import MagicMock, patch, call

import pytest
from azure.identity import DefaultAzureCredential
from cosmotech_api import RunnerApi, ScenarioApi
from cosmotech_api.exceptions import ApiException

from cosmotech.coal.cosmotech_api.runner.download import download_runner_data


class TestDownloadFunctions:
    """Tests for top-level functions in the download module."""


    @patch("cosmotech.coal.cosmotech_api.runner.download.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_runner_data")
    @patch("cosmotech.coal.cosmotech_api.runner.download.format_parameters_list")
    @patch("cosmotech.coal.cosmotech_api.runner.download.write_parameters")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_dataset_ids_from_runner")
    @patch("cosmotech.coal.cosmotech_api.runner.download.download_datasets")
    @patch("cosmotech.coal.cosmotech_api.runner.download.dataset_to_file")
    @patch("pathlib.Path.mkdir")
    @patch("shutil.copytree")
    def test_download_runner_data_with_datasets(
        self,
        mock_copytree,
        mock_mkdir,
        mock_dataset_to_file,
        mock_download_datasets,
        mock_get_dataset_ids,
        mock_write_parameters,
        mock_format_parameters,
        mock_get_runner_data,
        mock_get_api_client,
    ):
        """Test the download_runner_data function with datasets."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"
        dataset_folder = "/tmp/datasets"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock runner data
        mock_runner_data = MagicMock()
        mock_runner_data.dataset_list = ["dataset-1", "dataset-2"]
        mock_runner_data.parameters_values = [
            MagicMock(var_type="%DATASETID%", value="dataset-3", parameter_id="param1"),
            MagicMock(var_type="string", value="value1", parameter_id="param2"),
        ]
        mock_get_runner_data.return_value = mock_runner_data

        # Mock parameters
        mock_parameters = [
            {"parameterId": "param1", "value": "dataset-3"},
            {"parameterId": "param2", "value": "value1"},
        ]
        mock_format_parameters.return_value = mock_parameters

        # Mock dataset IDs
        mock_get_dataset_ids.return_value = ["dataset-1", "dataset-2", "dataset-3"]

        # Mock datasets
        mock_datasets = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
            "dataset-2": {"type": "json", "content": {}, "name": "dataset-2"},
            "dataset-3": {"type": "twincache", "content": {}, "name": "dataset-3"},
        }
        mock_download_datasets.return_value = mock_datasets

        # Mock dataset to file
        mock_dataset_to_file.return_value = "/tmp/dataset_files"

        # Act
        result = download_runner_data(
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
            parameter_folder=parameter_folder,
            dataset_folder=dataset_folder,
            fetch_dataset=True,
        )

        # Assert
        mock_get_runner_data.assert_called_once_with(organization_id, workspace_id, runner_id)
        mock_format_parameters.assert_called_once_with(mock_runner_data)
        mock_get_dataset_ids.assert_called_once_with(mock_runner_data)
        mock_download_datasets.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=["dataset-1", "dataset-2", "dataset-3"],
            read_files=False,
            parallel=True,
            credentials=None,
        )
        # The dataset_to_file function is called for each dataset in the dataset_list (2) and for the dataset referenced by a parameter (1)
        assert mock_dataset_to_file.call_count == 3
        assert mock_copytree.call_count == 3
        mock_write_parameters.assert_called_once_with(parameter_folder, mock_parameters, False, True)

        assert result["runner_data"] == mock_runner_data
        assert result["datasets"] == mock_datasets
        assert result["parameters"] == {"param1": "dataset-3", "param2": "value1"}

    @patch("cosmotech.coal.cosmotech_api.runner.download.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_runner_data")
    def test_download_runner_data_no_parameters(self, mock_get_runner_data, mock_get_api_client):
        """Test the download_runner_data function with no parameters."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock runner data with no parameters
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = None
        mock_get_runner_data.return_value = mock_runner_data

        # Act
        result = download_runner_data(
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
            parameter_folder=parameter_folder,
        )

        # Assert
        mock_get_runner_data.assert_called_once_with(organization_id, workspace_id, runner_id)

        assert result["runner_data"] == mock_runner_data
        assert result["datasets"] == {}
        assert result["parameters"] == {}

    @patch("cosmotech.coal.cosmotech_api.runner.download.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_runner_data")
    @patch("cosmotech.coal.cosmotech_api.runner.download.format_parameters_list")
    @patch("cosmotech.coal.cosmotech_api.runner.download.write_parameters")
    def test_download_runner_data_no_datasets(
        self,
        mock_write_parameters,
        mock_format_parameters,
        mock_get_runner_data,
        mock_get_api_client,
    ):
        """Test the download_runner_data function without datasets."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock runner data
        mock_runner_data = MagicMock()
        mock_runner_data.dataset_list = []
        mock_runner_data.parameters_values = [
            MagicMock(var_type="string", value="value1", parameter_id="param1"),
            MagicMock(var_type="string", value="value2", parameter_id="param2"),
        ]
        mock_get_runner_data.return_value = mock_runner_data

        # Mock parameters
        mock_parameters = [
            {"parameterId": "param1", "value": "value1"},
            {"parameterId": "param2", "value": "value2"},
        ]
        mock_format_parameters.return_value = mock_parameters

        # Act
        result = download_runner_data(
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
            parameter_folder=parameter_folder,
            fetch_dataset=False,
        )

        # Assert
        mock_get_runner_data.assert_called_once_with(organization_id, workspace_id, runner_id)
        mock_format_parameters.assert_called_once_with(mock_runner_data)
        mock_write_parameters.assert_called_once_with(parameter_folder, mock_parameters, False, True)

        assert result["runner_data"] == mock_runner_data
        assert result["datasets"] == {}
        assert result["parameters"] == {"param1": "value1", "param2": "value2"}
