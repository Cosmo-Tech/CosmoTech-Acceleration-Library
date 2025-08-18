# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

from cosmotech.coal.cosmotech_api.runner.download import download_runner_data


class TestDownloadEdgeCases:
    """Tests for edge cases in the download module."""

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
    ):
        """Test the download_runner_data function with Azure credentials."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        parameter_folder = "/tmp/params"

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
        mock_download_datasets.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=["dataset-1"],
            read_files=False,
            parallel=True,
        )
