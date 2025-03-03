# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import sys
import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
from azure.identity import DefaultAzureCredential


class TestDatasetsFunctions:
    """Tests for top-level functions in the datasets module."""

    def test_get_dataset_ids_from_scenario(self):
        """Test the get_dataset_ids_from_scenario function."""
        # Arrange
        mock_scenario_data = MagicMock()
        expected_dataset_ids = ["dataset-1", "dataset-2"]

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.get_dataset_ids_from_runner") as mock_get_dataset_ids:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_get_dataset_ids.return_value = expected_dataset_ids

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.datasets" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.datasets"]

                # Import the module
                from cosmotech.coal.scenario.datasets import get_dataset_ids_from_scenario

                # Act
                result = get_dataset_ids_from_scenario(mock_scenario_data)

                # Assert
                mock_get_dataset_ids.assert_called_once_with(mock_scenario_data)
                assert result == expected_dataset_ids

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "get_dataset_ids_from_scenario" in warning_message
                assert "get_dataset_ids_from_runner" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2

    def test_download_dataset(self):
        """Test the download_dataset function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        read_files = True
        credentials = MagicMock(spec=DefaultAzureCredential)

        expected_result = {"id": dataset_id, "data": "some data", "type": "csv"}

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset") as mock_download_dataset_func:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_download_dataset_func.return_value = expected_result

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.datasets" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.datasets"]

                # Import the module
                from cosmotech.coal.scenario.datasets import download_dataset

                # Act
                result = download_dataset(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_id=dataset_id,
                    read_files=read_files,
                    credentials=credentials,
                )

                # Assert
                mock_download_dataset_func.assert_called_once_with(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_id=dataset_id,
                    read_files=read_files,
                    credentials=credentials,
                )
                assert result == expected_result

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "download_dataset" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2

    def test_download_datasets(self):
        """Test the download_datasets function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]
        read_files = True
        parallel = True
        credentials = MagicMock(spec=DefaultAzureCredential)

        expected_result = {
            "dataset-1": {"id": "dataset-1", "data": "data 1", "type": "csv"},
            "dataset-2": {"id": "dataset-2", "data": "data 2", "type": "csv"},
        }

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets") as mock_download_datasets_func:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_download_datasets_func.return_value = expected_result

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.datasets" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.datasets"]

                # Import the module
                from cosmotech.coal.scenario.datasets import download_datasets

                # Act
                result = download_datasets(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_ids=dataset_ids,
                    read_files=read_files,
                    parallel=parallel,
                    credentials=credentials,
                )

                # Assert
                mock_download_datasets_func.assert_called_once_with(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_ids=dataset_ids,
                    read_files=read_files,
                    parallel=parallel,
                    credentials=credentials,
                )
                assert result == expected_result

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "download_datasets" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2

    def test_dataset_to_file(self):
        """Test the dataset_to_file function."""
        # Arrange
        dataset_info = {"id": "dataset-123", "data": "some data", "type": "csv"}
        target_folder = Path("/path/to/folder")

        expected_result = "/path/to/folder/dataset-123"

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.dataset_to_file") as mock_dataset_to_file_func:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_dataset_to_file_func.return_value = expected_result

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.datasets" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.datasets"]

                # Import the module
                from cosmotech.coal.scenario.datasets import dataset_to_file

                # Act
                result = dataset_to_file(dataset_info=dataset_info, target_folder=target_folder)

                # Assert
                mock_dataset_to_file_func.assert_called_once_with(dataset_info, target_folder)
                assert result == expected_result

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "dataset_to_file" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2

    def test_dataset_to_file_no_target_folder(self):
        """Test the dataset_to_file function with no target folder."""
        # Arrange
        dataset_info = {"id": "dataset-123", "data": "some data", "type": "csv"}

        expected_result = "/tmp/dataset-123"

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.datasets.dataset_to_file") as mock_dataset_to_file_func:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_dataset_to_file_func.return_value = expected_result

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.datasets" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.datasets"]

                # Import the module
                from cosmotech.coal.scenario.datasets import dataset_to_file

                # Act
                result = dataset_to_file(dataset_info=dataset_info)

                # Assert
                mock_dataset_to_file_func.assert_called_once_with(dataset_info, None)
                assert result == expected_result

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
