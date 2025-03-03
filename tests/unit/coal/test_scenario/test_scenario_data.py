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


class TestDataFunctions:
    """Tests for top-level functions in the data module."""

    def test_get_scenario_data(self):
        """Test the get_scenario_data function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        scenario_id = "scenario-123"

        expected_data = MagicMock()
        expected_data.id = scenario_id
        expected_data.name = "Test Scenario"

        # Create patches
        with patch("cosmotech.coal.cosmotech_api.runner.data.get_runner_data") as mock_get_runner_data:
            with patch("warnings.warn") as mock_warn:
                # Set up the mock return value
                mock_get_runner_data.return_value = expected_data

                # Remove the module from sys.modules if it's already imported
                if "cosmotech.coal.scenario.data" in sys.modules:
                    del sys.modules["cosmotech.coal.scenario.data"]

                # Import the module
                from cosmotech.coal.scenario.data import get_scenario_data

                # Reset the mock to clear the warning from the import
                mock_warn.reset_mock()

                # Act
                result = get_scenario_data(
                    organization_id=organization_id, workspace_id=workspace_id, scenario_id=scenario_id
                )

                # Assert
                # Verify that get_runner_data was called with the correct parameters
                mock_get_runner_data.assert_called_once_with(organization_id, workspace_id, scenario_id)

                # Verify that the result is the expected scenario data
                assert result == expected_data
                assert result.id == scenario_id
                assert result.name == "Test Scenario"

                # Verify that a deprecation warning was issued
                mock_warn.assert_called_once()
                warning_message = mock_warn.call_args[0][0]
                assert "deprecated" in warning_message.lower()
                assert "get_scenario_data" in warning_message
                assert "get_runner_data" in warning_message
                assert mock_warn.call_args[0][1] is DeprecationWarning
                assert mock_warn.call_args[1]["stacklevel"] == 2
