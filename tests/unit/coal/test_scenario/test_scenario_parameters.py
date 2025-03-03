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


class TestParametersFunctions:
    """Tests for top-level functions in the parameters module."""

    def test_get_scenario_parameters(self):
        """Test the get_scenario_parameters function."""
        # Arrange
        mock_scenario_data = MagicMock()
        expected_parameters = {"param1": "value1", "param2": 42, "param3": True}

        # Create patches
        with patch(
            "cosmotech.coal.cosmotech_api.runner.parameters.get_runner_parameters"
        ) as mock_get_runner_parameters:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_get_runner_parameters.return_value = expected_parameters

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.parameters" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.parameters"]

                # Import the module
                from cosmotech.coal.scenario.parameters import get_scenario_parameters

                # Act
                result = get_scenario_parameters(mock_scenario_data)

                # Assert
                # Verify that get_runner_parameters was called with the correct parameters
                mock_get_runner_parameters.assert_called_once_with(mock_scenario_data)

                # Verify that the result is the expected parameters dictionary
                assert result == expected_parameters

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "get_scenario_parameters" in warning_message
                assert "get_runner_parameters" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2
