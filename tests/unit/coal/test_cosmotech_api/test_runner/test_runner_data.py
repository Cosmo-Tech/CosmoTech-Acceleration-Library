# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

from cosmotech_api.api.runner_api import RunnerApi
from cosmotech.coal.cosmotech_api.runner.data import get_runner_data


class TestDataFunctions:
    """Tests for top-level functions in the data module."""

    @patch("cosmotech.coal.cosmotech_api.runner.data.get_api_client")
    def test_get_runner_data(self, mock_get_api_client):
        """Test the get_runner_data function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock runner API
        mock_runner_api = MagicMock(spec=RunnerApi)
        mock_runner_data = MagicMock()
        mock_runner_data.id = runner_id
        mock_runner_data.name = "Test Runner"
        mock_runner_api.get_runner.return_value = mock_runner_data

        with patch("cosmotech.coal.cosmotech_api.runner.data.RunnerApi", return_value=mock_runner_api):
            # Act
            result = get_runner_data(
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
            )

            # Assert
            mock_runner_api.get_runner.assert_called_once_with(
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
            )
            assert result == mock_runner_data
            assert result.id == runner_id
            assert result.name == "Test Runner"
