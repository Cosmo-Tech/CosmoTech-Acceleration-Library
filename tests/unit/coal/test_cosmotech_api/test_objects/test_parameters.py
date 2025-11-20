# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.cosmotech_api.objects.parameters import Parameters


class TestParameters:
    """Tests for the Parameters class."""

    def test_parameters_initialization_empty(self):
        """Test Parameters initialization with no parameters."""
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = []

        params = Parameters(mock_runner_data)

        assert params.values == {}
        assert params.parameters_list == []

    def test_parameters_initialization_with_values(self):
        """Test Parameters initialization with parameter values."""
        mock_runner_data = MagicMock()

        # Mock parameter values
        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        param2 = MagicMock()
        param2.parameter_id = "param2"
        param2.value = 42
        param2.var_type = "int"
        param2.is_inherited = True

        mock_runner_data.parameters_values = [param1, param2]

        params = Parameters(mock_runner_data)

        assert params.values == {"param1": "value1", "param2": 42}
        assert len(params.parameters_list) == 2
        assert params.parameters_list[0]["parameterId"] == "param1"
        assert params.parameters_list[0]["value"] == "value1"
        assert params.parameters_list[0]["varType"] == "string"
        assert params.parameters_list[0]["isInherited"] is False
        assert params.parameters_list[1]["parameterId"] == "param2"
        assert params.parameters_list[1]["value"] == 42
        assert params.parameters_list[1]["varType"] == "int"
        assert params.parameters_list[1]["isInherited"] is True

    def test_format_parameters_list_empty(self):
        """Test format_parameters_list with no parameters."""
        mock_runner_data = MagicMock()
        mock_runner_data.parameters_values = None

        result = Parameters.format_parameters_list(mock_runner_data)

        assert result == []

    def test_format_parameters_list_with_parameters(self):
        """Test format_parameters_list with multiple parameters."""
        mock_runner_data = MagicMock()

        # Mock parameter values
        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        param2 = MagicMock()
        param2.parameter_id = "param2"
        param2.value = 42
        param2.var_type = "int"
        param2.is_inherited = True

        mock_runner_data.parameters_values = [param1, param2]

        result = Parameters.format_parameters_list(mock_runner_data)

        assert len(result) == 2
        assert result[0] == {
            "parameterId": "param1",
            "value": "value1",
            "varType": "string",
            "isInherited": False,
        }
        assert result[1] == {
            "parameterId": "param2",
            "value": 42,
            "varType": "int",
            "isInherited": True,
        }

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_to_json(self, mock_logger):
        """Test writing parameters to JSON file."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = params.write_parameters_to_json(tmpdir)

            expected_path = os.path.join(tmpdir, "parameters.json")
            assert result_path == expected_path
            assert os.path.exists(result_path)

            # Verify JSON content
            with open(result_path, "r") as f:
                content = json.load(f)

            assert len(content) == 1
            assert content[0]["parameterId"] == "param1"
            assert content[0]["value"] == "value1"
            assert content[0]["varType"] == "string"
            assert content[0]["isInherited"] is False

            mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_to_json_creates_directory(self, mock_logger):
        """Test that write_parameters_to_json creates directory if it doesn't exist."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "nested", "path")
            result_path = params.write_parameters_to_json(nested_dir)

            assert os.path.exists(result_path)
            assert os.path.isfile(result_path)

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_to_csv(self, mock_logger):
        """Test writing parameters to CSV file."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        param2 = MagicMock()
        param2.parameter_id = "param2"
        param2.value = 42
        param2.var_type = "int"
        param2.is_inherited = True

        mock_runner_data.parameters_values = [param1, param2]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            result_path = params.write_parameters_to_csv(tmpdir)

            expected_path = os.path.join(tmpdir, "parameters.csv")
            assert result_path == expected_path
            assert os.path.exists(result_path)

            # Verify CSV content
            with open(result_path, "r") as f:
                lines = f.readlines()

            assert len(lines) == 3  # Header + 2 data rows
            assert "parameterId,value,varType,isInherited" in lines[0]
            assert "param1,value1,string,False" in lines[1]
            assert "param2,42,int,True" in lines[2]

            mock_logger.info.assert_called_once()

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_to_csv_creates_directory(self, mock_logger):
        """Test that write_parameters_to_csv creates directory if it doesn't exist."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            nested_dir = os.path.join(tmpdir, "nested", "path")
            result_path = params.write_parameters_to_csv(nested_dir)

            assert os.path.exists(result_path)
            assert os.path.isfile(result_path)

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_default_csv_only(self, mock_logger):
        """Test write_parameters with default settings (CSV only)."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = params.write_parameters(tmpdir)

            assert "csv" in result
            assert "json" not in result
            assert os.path.exists(result["csv"])
            assert result["csv"].endswith("parameters.csv")

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_csv_and_json(self, mock_logger):
        """Test write_parameters with both CSV and JSON."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = params.write_parameters(tmpdir, write_csv=True, write_json=True)

            assert "csv" in result
            assert "json" in result
            assert os.path.exists(result["csv"])
            assert os.path.exists(result["json"])
            assert result["csv"].endswith("parameters.csv")
            assert result["json"].endswith("parameters.json")

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_json_only(self, mock_logger):
        """Test write_parameters with JSON only."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = params.write_parameters(tmpdir, write_csv=False, write_json=True)

            assert "csv" not in result
            assert "json" in result
            assert os.path.exists(result["json"])

    @patch("cosmotech.coal.cosmotech_api.objects.parameters.LOGGER")
    def test_write_parameters_none(self, mock_logger):
        """Test write_parameters with both CSV and JSON disabled."""
        mock_runner_data = MagicMock()

        param1 = MagicMock()
        param1.parameter_id = "param1"
        param1.value = "value1"
        param1.var_type = "string"
        param1.is_inherited = False

        mock_runner_data.parameters_values = [param1]

        params = Parameters(mock_runner_data)

        with tempfile.TemporaryDirectory() as tmpdir:
            result = params.write_parameters(tmpdir, write_csv=False, write_json=False)

            assert result == {}
