# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import tempfile
from csv import DictReader
from unittest.mock import MagicMock, patch, mock_open

import pytest

from cosmotech.coal.cosmotech_api.runner.parameters import (
    get_runner_parameters,
    format_parameters_list,
    write_parameters_to_json,
    write_parameters_to_csv,
    write_parameters,
)


class TestParametersFunctions:
    """Tests for top-level functions in the parameters module."""

    def test_get_runner_parameters(self):
        """Test the get_runner_parameters function."""
        # Arrange
        mock_runner_data = MagicMock()
        mock_param1 = MagicMock()
        mock_param1.parameter_id = "param1"
        mock_param1.value = "value1"

        mock_param2 = MagicMock()
        mock_param2.parameter_id = "param2"
        mock_param2.value = "value2"

        mock_runner_data.parameters_values = [mock_param1, mock_param2]

        # Act
        result = get_runner_parameters(mock_runner_data)

        # Assert
        assert result == {"param1": "value1", "param2": "value2"}

    def test_format_parameters_list(self):
        """Test the format_parameters_list function."""
        # Arrange
        mock_runner_data = MagicMock()
        mock_param1 = MagicMock()
        mock_param1.parameter_id = "param1"
        mock_param1.value = "value1"
        mock_param1.var_type = "string"
        mock_param1.is_inherited = False

        mock_param2 = MagicMock()
        mock_param2.parameter_id = "param2"
        mock_param2.value = "value2"
        mock_param2.var_type = "number"
        mock_param2.is_inherited = True

        mock_runner_data.parameters_values = [mock_param1, mock_param2]

        # Act
        result = format_parameters_list(mock_runner_data)

        # Assert
        expected_result = [
            {
                "parameterId": "param1",
                "value": "value1",
                "varType": "string",
                "isInherited": False,
            },
            {
                "parameterId": "param2",
                "value": "value2",
                "varType": "number",
                "isInherited": True,
            },
        ]
        assert result == expected_result

    def test_format_parameters_list_empty(self):
        """Test the format_parameters_list function with empty parameters."""
        # Arrange
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = []

        # Act
        result = format_parameters_list(mock_runner_data)

        # Assert
        assert result == []

    def test_format_parameters_list_none(self):
        """Test the format_parameters_list function with None parameters."""
        # Arrange
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = None

        # Act
        result = format_parameters_list(mock_runner_data)

        # Assert
        assert result == []

    @patch("pathlib.Path.mkdir")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_write_parameters_to_json(self, mock_json_dump, mock_file_open, mock_mkdir):
        """Test the write_parameters_to_json function."""
        # Arrange
        parameter_folder = "/tmp/params"
        parameters = [
            {
                "parameterId": "param1",
                "value": "value1",
                "varType": "string",
                "isInherited": False,
            },
            {
                "parameterId": "param2",
                "value": "value2",
                "varType": "number",
                "isInherited": True,
            },
        ]

        # Act
        result = write_parameters_to_json(parameter_folder, parameters)

        # Assert
        mock_mkdir.assert_called_once_with(exist_ok=True, parents=True)
        mock_file_open.assert_called_once_with(os.path.join(parameter_folder, "parameters.json"), "w")
        mock_json_dump.assert_called_once_with(parameters, mock_file_open(), indent=2)
        assert result == os.path.join(parameter_folder, "parameters.json")

    # We're using the integration test for write_parameters_to_csv instead of a unit test
    # because it's more reliable and provides better coverage

    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_csv")
    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_json")
    def test_write_parameters_both_formats(self, mock_write_json, mock_write_csv):
        """Test the write_parameters function with both CSV and JSON formats."""
        # Arrange
        parameter_folder = "/tmp/params"
        parameters = [
            {
                "parameterId": "param1",
                "value": "value1",
                "varType": "string",
                "isInherited": False,
            },
        ]

        # Mock return values
        mock_write_csv.return_value = os.path.join(parameter_folder, "parameters.csv")
        mock_write_json.return_value = os.path.join(parameter_folder, "parameters.json")

        # Act
        result = write_parameters(
            parameter_folder=parameter_folder,
            parameters=parameters,
            write_csv=True,
            write_json=True,
        )

        # Assert
        mock_write_csv.assert_called_once_with(parameter_folder, parameters)
        mock_write_json.assert_called_once_with(parameter_folder, parameters)
        assert result == {
            "csv": os.path.join(parameter_folder, "parameters.csv"),
            "json": os.path.join(parameter_folder, "parameters.json"),
        }

    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_csv")
    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_json")
    def test_write_parameters_csv_only(self, mock_write_json, mock_write_csv):
        """Test the write_parameters function with CSV format only."""
        # Arrange
        parameter_folder = "/tmp/params"
        parameters = [
            {
                "parameterId": "param1",
                "value": "value1",
                "varType": "string",
                "isInherited": False,
            },
        ]

        # Mock return values
        mock_write_csv.return_value = os.path.join(parameter_folder, "parameters.csv")

        # Act
        result = write_parameters(
            parameter_folder=parameter_folder,
            parameters=parameters,
            write_csv=True,
            write_json=False,
        )

        # Assert
        mock_write_csv.assert_called_once_with(parameter_folder, parameters)
        mock_write_json.assert_not_called()
        assert result == {
            "csv": os.path.join(parameter_folder, "parameters.csv"),
        }

    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_csv")
    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_json")
    def test_write_parameters_json_only(self, mock_write_json, mock_write_csv):
        """Test the write_parameters function with JSON format only."""
        # Arrange
        parameter_folder = "/tmp/params"
        parameters = [
            {
                "parameterId": "param1",
                "value": "value1",
                "varType": "string",
                "isInherited": False,
            },
        ]

        # Mock return values
        mock_write_json.return_value = os.path.join(parameter_folder, "parameters.json")

        # Act
        result = write_parameters(
            parameter_folder=parameter_folder,
            parameters=parameters,
            write_csv=False,
            write_json=True,
        )

        # Assert
        mock_write_csv.assert_not_called()
        mock_write_json.assert_called_once_with(parameter_folder, parameters)
        assert result == {
            "json": os.path.join(parameter_folder, "parameters.json"),
        }

    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_csv")
    @patch("cosmotech.coal.cosmotech_api.runner.parameters.write_parameters_to_json")
    def test_write_parameters_no_formats(self, mock_write_json, mock_write_csv):
        """Test the write_parameters function with no formats specified."""
        # Arrange
        parameter_folder = "/tmp/params"
        parameters = [
            {
                "parameterId": "param1",
                "value": "value1",
                "varType": "string",
                "isInherited": False,
            },
        ]

        # Act
        result = write_parameters(
            parameter_folder=parameter_folder,
            parameters=parameters,
            write_csv=False,
            write_json=False,
        )

        # Assert
        mock_write_csv.assert_not_called()
        mock_write_json.assert_not_called()
        assert result == {}

    def test_integration_write_parameters_to_json(self):
        """Integration test for writing parameters to JSON."""
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            parameters = [
                {
                    "parameterId": "param1",
                    "value": "value1",
                    "varType": "string",
                    "isInherited": False,
                },
                {
                    "parameterId": "param2",
                    "value": "value2",
                    "varType": "number",
                    "isInherited": True,
                },
            ]

            # Act
            result = write_parameters_to_json(temp_dir, parameters)

            # Assert
            assert os.path.exists(result)
            with open(result, "r") as f:
                loaded_data = json.load(f)
                assert loaded_data == parameters

    def test_integration_write_parameters_to_csv(self):
        """Integration test for writing parameters to CSV."""
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            parameters = [
                {
                    "parameterId": "param1",
                    "value": "value1",
                    "varType": "string",
                    "isInherited": "False",
                },
                {
                    "parameterId": "param2",
                    "value": "value2",
                    "varType": "number",
                    "isInherited": "True",
                },
            ]

            # Act
            result = write_parameters_to_csv(temp_dir, parameters)

            # Assert
            assert os.path.exists(result)
            with open(result, "r") as f:
                reader = DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                assert rows[0]["parameterId"] == "param1"
                assert rows[0]["value"] == "value1"
                assert rows[0]["varType"] == "string"
                assert rows[0]["isInherited"] == "False"
                assert rows[1]["parameterId"] == "param2"
                assert rows[1]["value"] == "value2"
                assert rows[1]["varType"] == "number"
                assert rows[1]["isInherited"] == "True"
