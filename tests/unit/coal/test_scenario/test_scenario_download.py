# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import sys
import warnings
from unittest.mock import MagicMock, patch

import pytest


class TestDownloadFunctions:
    """Tests for top-level functions in the download module."""

    def test_download_scenario_data(self):
        """Test the download_scenario_data function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        scenario_id = "scenario-123"
        parameter_folder = "/tmp/params"
        dataset_folder = "/tmp/datasets"
        read_files = False
        parallel = True
        write_json = False
        write_csv = True
        fetch_dataset = True

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.download.download_run_data") as mock_download_run_data:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_download_run_data.return_value = {
                    "runner_data": {"id": scenario_id, "name": "Test Scenario"},
                    "datasets": {"dataset-1": {"id": "dataset-1", "data": "data 1"}},
                    "parameters": {"param1": "value1"},
                }

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.download" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.download"]

                # Import the module
                from cosmotech.coal.scenario.download import download_scenario_data

                # Act
                result = download_scenario_data(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    scenario_id=scenario_id,
                    parameter_folder=parameter_folder,
                    dataset_folder=dataset_folder,
                    read_files=read_files,
                    parallel=parallel,
                    write_json=write_json,
                    write_csv=write_csv,
                    fetch_dataset=fetch_dataset,
                )

                # Assert
                # Verify that download_run_data was called with the correct parameters
                mock_download_run_data.assert_called_once_with(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=scenario_id,
                    parameter_folder=parameter_folder,
                    dataset_folder=dataset_folder,
                    read_files=read_files,
                    parallel=parallel,
                    write_json=write_json,
                    write_csv=write_csv,
                    fetch_dataset=fetch_dataset,
                )

                # Verify that the result has the expected structure
                assert "scenario_data" in result
                assert "datasets" in result
                assert "parameters" in result

                # Verify that runner_data was renamed to scenario_data
                assert result["scenario_data"] == {"id": scenario_id, "name": "Test Scenario"}
                assert result["datasets"] == {"dataset-1": {"id": "dataset-1", "data": "data 1"}}
                assert result["parameters"] == {"param1": "value1"}

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "download_scenario_data" in warning_message
                assert "download_run_data" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2

    def test_download_scenario_data_no_runner_data(self):
        """Test the download_scenario_data function when runner_data is not in the result."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        scenario_id = "scenario-123"
        parameter_folder = "/tmp/params"

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.download.download_run_data") as mock_download_run_data:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_download_run_data.return_value = {"datasets": {}, "parameters": {}}

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.download" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.download"]

                # Import the module
                from cosmotech.coal.scenario.download import download_scenario_data

                # Act
                result = download_scenario_data(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    scenario_id=scenario_id,
                    parameter_folder=parameter_folder,
                )

                # Assert
                # Verify that download_run_data was called with the correct parameters
                mock_download_run_data.assert_called_once()

                # Verify that the result has the expected structure
                assert "datasets" in result
                assert "parameters" in result
                assert "scenario_data" not in result

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
