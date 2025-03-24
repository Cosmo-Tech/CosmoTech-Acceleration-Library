# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

from cosmotech.coal.cosmotech_api.parameters import write_parameters


class TestParametersFunctions:
    """Tests for top-level functions in the parameters module."""

    @patch("cosmotech.coal.cosmotech_api.parameters.open")
    @patch("cosmotech.coal.cosmotech_api.parameters.DictWriter")
    @patch("cosmotech.coal.cosmotech_api.parameters.json.dump")
    @patch("cosmotech.coal.cosmotech_api.parameters.os.path.join")
    @patch("cosmotech.coal.cosmotech_api.parameters.LOGGER")
    def test_write_parameters_csv_and_json(self, mock_logger, mock_join, mock_json_dump, mock_dict_writer, mock_open):
        """Test the write_parameters function with both CSV and JSON output."""
        # Arrange
        parameter_folder = "/path/to/parameters"
        parameters = [
            {"parameterId": "param1", "value": "value1", "varType": "string", "isInherited": False},
            {"parameterId": "param2", "value": 42, "varType": "int", "isInherited": True},
        ]

        # Mock file paths
        mock_join.side_effect = [
            "/path/to/parameters/parameters.csv",
            "/path/to/parameters/parameters.json",
        ]

        # Mock file handlers
        mock_file_csv = MagicMock()
        mock_file_json = MagicMock()
        mock_open.return_value.__enter__.side_effect = [mock_file_csv, mock_file_json]

        # Mock CSV writer
        mock_writer = MagicMock()
        mock_dict_writer.return_value = mock_writer

        # Act
        write_parameters(parameter_folder, parameters, write_csv=True, write_json=True)

        # Assert
        # Check that paths were joined correctly
        mock_join.assert_any_call(parameter_folder, "parameters.csv")
        mock_join.assert_any_call(parameter_folder, "parameters.json")

        # Check that files were opened correctly
        mock_open.assert_any_call("/path/to/parameters/parameters.csv", "w")
        mock_open.assert_any_call("/path/to/parameters/parameters.json", "w")

        # Check CSV writer was initialized and used correctly
        mock_dict_writer.assert_called_once_with(
            mock_file_csv, fieldnames=["parameterId", "value", "varType", "isInherited"]
        )
        mock_writer.writeheader.assert_called_once()
        mock_writer.writerows.assert_called_once_with(parameters)

        # Check JSON dump was called correctly
        mock_json_dump.assert_called_once_with(parameters, mock_file_json, indent=2)

        # Check logging
        mock_logger.info.assert_any_call("Generating /path/to/parameters/parameters.csv")
        mock_logger.info.assert_any_call("Generating /path/to/parameters/parameters.json")

    @patch("cosmotech.coal.cosmotech_api.parameters.open")
    @patch("cosmotech.coal.cosmotech_api.parameters.DictWriter")
    @patch("cosmotech.coal.cosmotech_api.parameters.json.dump")
    @patch("cosmotech.coal.cosmotech_api.parameters.os.path.join")
    @patch("cosmotech.coal.cosmotech_api.parameters.LOGGER")
    def test_write_parameters_csv_only(self, mock_logger, mock_join, mock_json_dump, mock_dict_writer, mock_open):
        """Test the write_parameters function with CSV output only."""
        # Arrange
        parameter_folder = "/path/to/parameters"
        parameters = [
            {"parameterId": "param1", "value": "value1", "varType": "string", "isInherited": False},
        ]

        # Mock file path
        mock_join.return_value = "/path/to/parameters/parameters.csv"

        # Mock file handler
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock CSV writer
        mock_writer = MagicMock()
        mock_dict_writer.return_value = mock_writer

        # Act
        write_parameters(parameter_folder, parameters, write_csv=True, write_json=False)

        # Assert
        # Check that path was joined correctly
        mock_join.assert_called_once_with(parameter_folder, "parameters.csv")

        # Check that file was opened correctly
        mock_open.assert_called_once_with("/path/to/parameters/parameters.csv", "w")

        # Check CSV writer was initialized and used correctly
        mock_dict_writer.assert_called_once_with(
            mock_file, fieldnames=["parameterId", "value", "varType", "isInherited"]
        )
        mock_writer.writeheader.assert_called_once()
        mock_writer.writerows.assert_called_once_with(parameters)

        # Check JSON dump was not called
        mock_json_dump.assert_not_called()

        # Check logging
        mock_logger.info.assert_called_once_with("Generating /path/to/parameters/parameters.csv")

    @patch("cosmotech.coal.cosmotech_api.parameters.open")
    @patch("cosmotech.coal.cosmotech_api.parameters.DictWriter")
    @patch("cosmotech.coal.cosmotech_api.parameters.json.dump")
    @patch("cosmotech.coal.cosmotech_api.parameters.os.path.join")
    @patch("cosmotech.coal.cosmotech_api.parameters.LOGGER")
    def test_write_parameters_json_only(self, mock_logger, mock_join, mock_json_dump, mock_dict_writer, mock_open):
        """Test the write_parameters function with JSON output only."""
        # Arrange
        parameter_folder = "/path/to/parameters"
        parameters = [
            {"parameterId": "param1", "value": "value1", "varType": "string", "isInherited": False},
        ]

        # Mock file path
        mock_join.return_value = "/path/to/parameters/parameters.json"

        # Mock file handler
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        # Act
        write_parameters(parameter_folder, parameters, write_csv=False, write_json=True)

        # Assert
        # Check that path was joined correctly
        mock_join.assert_called_once_with(parameter_folder, "parameters.json")

        # Check that file was opened correctly
        mock_open.assert_called_once_with("/path/to/parameters/parameters.json", "w")

        # Check CSV writer was not initialized
        mock_dict_writer.assert_not_called()

        # Check JSON dump was called correctly
        mock_json_dump.assert_called_once_with(parameters, mock_file, indent=2)

        # Check logging
        mock_logger.info.assert_called_once_with("Generating /path/to/parameters/parameters.json")

    @patch("cosmotech.coal.cosmotech_api.parameters.open")
    @patch("cosmotech.coal.cosmotech_api.parameters.DictWriter")
    @patch("cosmotech.coal.cosmotech_api.parameters.json.dump")
    @patch("cosmotech.coal.cosmotech_api.parameters.os.path.join")
    @patch("cosmotech.coal.cosmotech_api.parameters.LOGGER")
    def test_write_parameters_no_output(self, mock_logger, mock_join, mock_json_dump, mock_dict_writer, mock_open):
        """Test the write_parameters function with no output."""
        # Arrange
        parameter_folder = "/path/to/parameters"
        parameters = [
            {"parameterId": "param1", "value": "value1", "varType": "string", "isInherited": False},
        ]

        # Act
        write_parameters(parameter_folder, parameters, write_csv=False, write_json=False)

        # Assert
        # Check that no files were created
        mock_join.assert_not_called()
        mock_open.assert_not_called()
        mock_dict_writer.assert_not_called()
        mock_json_dump.assert_not_called()
        mock_logger.info.assert_not_called()
