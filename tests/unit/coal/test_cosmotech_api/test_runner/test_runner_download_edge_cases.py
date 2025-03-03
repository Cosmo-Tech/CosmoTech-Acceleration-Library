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

from cosmotech.coal.cosmotech_api.runner.download import download_run_data, download_runner_data


class TestDownloadEdgeCases:
    """Tests for edge cases in the download module."""

    @patch("cosmotech.coal.cosmotech_api.runner.download.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.download.DefaultAzureCredential")
    @patch("cosmotech.coal.cosmotech_api.runner.download.format_parameters_list")
    @patch("cosmotech.coal.cosmotech_api.runner.download.write_parameters")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_dataset_ids_from_runner")
    @patch("cosmotech.coal.cosmotech_api.runner.download.download_datasets")
    def test_download_run_data_azure_credentials(
        self,
        mock_download_datasets,
        mock_get_dataset_ids,
        mock_write_parameters,
        mock_format_parameters,
        mock_default_credential,
        mock_get_api_client,
    ):
        """Test the download_run_data function with Azure credentials."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"

        # Mock API client with Azure Entra Connection
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock DefaultAzureCredential
        mock_credential = MagicMock(spec=DefaultAzureCredential)
        mock_default_credential.return_value = mock_credential

        # Mock runner API
        mock_runner_api = MagicMock(spec=RunnerApi)
        mock_runner_data = MagicMock()
        mock_runner_data.dataset_list = ["dataset-1"]
        mock_runner_data.parameters_values = [
            MagicMock(var_type="%DATASETID%", value="dataset-1", parameter_id="param1"),
        ]
        mock_runner_api.get_runner.return_value = mock_runner_data

        # Mock parameters
        mock_parameters = [
            {"parameterId": "param1", "value": "dataset-1"},
        ]
        mock_format_parameters.return_value = mock_parameters

        # Mock dataset IDs
        mock_get_dataset_ids.return_value = ["dataset-1"]

        # Mock datasets
        mock_datasets = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
        }
        mock_download_datasets.return_value = mock_datasets

        with patch("cosmotech.coal.cosmotech_api.runner.download.RunnerApi", return_value=mock_runner_api):
            # Act
            result = download_run_data(
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                parameter_folder=parameter_folder,
                fetch_dataset=True,
            )

            # Assert
            mock_default_credential.assert_called_once()
            mock_download_datasets.assert_called_once_with(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_ids=["dataset-1"],
                read_files=False,
                parallel=True,
                credentials=mock_credential,
            )

    @patch("cosmotech.coal.cosmotech_api.runner.download.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.download.format_parameters_list")
    @patch("cosmotech.coal.cosmotech_api.runner.download.write_parameters")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_dataset_ids_from_runner")
    @patch("cosmotech.coal.cosmotech_api.runner.download.download_datasets")
    @patch("cosmotech.coal.cosmotech_api.runner.download.dataset_to_file")
    @patch("pathlib.Path.mkdir")
    @patch("shutil.copytree")
    def test_download_run_data_no_dataset_folder(
        self,
        mock_copytree,
        mock_mkdir,
        mock_dataset_to_file,
        mock_download_datasets,
        mock_get_dataset_ids,
        mock_write_parameters,
        mock_format_parameters,
        mock_get_api_client,
    ):
        """Test the download_run_data function with no dataset folder."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock runner API
        mock_runner_api = MagicMock(spec=RunnerApi)
        mock_runner_data = MagicMock()
        mock_runner_data.dataset_list = ["dataset-1", "dataset-2"]
        mock_runner_data.parameters_values = [
            MagicMock(var_type="%DATASETID%", value="dataset-3", parameter_id="param1"),
            MagicMock(var_type="string", value="value1", parameter_id="param2"),
        ]
        mock_runner_api.get_runner.return_value = mock_runner_data

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

        with patch("cosmotech.coal.cosmotech_api.runner.download.RunnerApi", return_value=mock_runner_api):
            # Act
            result = download_run_data(
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                parameter_folder=parameter_folder,
                dataset_folder=None,  # No dataset folder
                fetch_dataset=True,
            )

            # Assert
            mock_runner_api.get_runner.assert_called_once_with(
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
            )

            # Should only copy to parameter folder, not to dataset folder
            assert mock_dataset_to_file.call_count == 1
            assert mock_copytree.call_count == 1

            # Should only copy dataset-3 (referenced by parameter)
            mock_dataset_to_file.assert_called_once_with(mock_datasets["dataset-3"])
            mock_copytree.assert_called_once_with(
                "/tmp/dataset_files", os.path.join(parameter_folder, "param1"), dirs_exist_ok=True
            )

    @patch("cosmotech.coal.cosmotech_api.runner.download.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.download.DefaultAzureCredential")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_runner_data")
    @patch("cosmotech.coal.cosmotech_api.runner.download.format_parameters_list")
    @patch("cosmotech.coal.cosmotech_api.runner.download.write_parameters")
    @patch("cosmotech.coal.cosmotech_api.runner.download.get_dataset_ids_from_runner")
    @patch("cosmotech.coal.cosmotech_api.runner.download.download_datasets")
    def test_download_runner_data_azure_credentials(
        self,
        mock_download_datasets,
        mock_get_dataset_ids,
        mock_write_parameters,
        mock_format_parameters,
        mock_get_runner_data,
        mock_default_credential,
        mock_get_api_client,
    ):
        """Test the download_runner_data function with Azure credentials."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"

        # Mock API client with Azure Entra Connection
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock DefaultAzureCredential
        mock_credential = MagicMock(spec=DefaultAzureCredential)
        mock_default_credential.return_value = mock_credential

        # Mock runner data
        mock_runner_data = MagicMock()
        mock_runner_data.dataset_list = ["dataset-1"]
        mock_runner_data.parameters_values = [
            MagicMock(var_type="%DATASETID%", value="dataset-1", parameter_id="param1"),
        ]
        mock_get_runner_data.return_value = mock_runner_data

        # Mock parameters
        mock_parameters = [
            {"parameterId": "param1", "value": "dataset-1"},
        ]
        mock_format_parameters.return_value = mock_parameters

        # Mock dataset IDs
        mock_get_dataset_ids.return_value = ["dataset-1"]

        # Mock datasets
        mock_datasets = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
        }
        mock_download_datasets.return_value = mock_datasets

        # Act
        result = download_runner_data(
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
            parameter_folder=parameter_folder,
            fetch_dataset=True,
        )

        # Assert
        mock_default_credential.assert_called_once()
        mock_download_datasets.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=["dataset-1"],
            read_files=False,
            parallel=True,
            credentials=mock_credential,
        )
