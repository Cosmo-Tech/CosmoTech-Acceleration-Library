# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

import cosmotech_api
from cosmotech.coal.cosmotech_api.run import get_run_metadata


class TestRunFunctions:
    """Tests for top-level functions in the run module."""

    def test_get_run_metadata(self):
        """Test the get_run_metadata function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)

        # Mock run API
        mock_run_api = MagicMock(spec=cosmotech_api.RunApi)
        mock_run = MagicMock(spec=cosmotech_api.Run)
        mock_run.id = run_id
        mock_run.state = "Running"
        mock_run.model_dump.return_value = {
            "id": run_id,
            "state": "Running",
            "workspaceId": workspace_id,
            "runnerId": runner_id,
        }
        mock_run_api.get_run.return_value = mock_run

        with patch("cosmotech_api.RunApi", return_value=mock_run_api):
            # Act
            result = get_run_metadata(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                run_id=run_id,
            )

            # Assert
            mock_run_api.get_run.assert_called_once_with(organization_id, workspace_id, runner_id, run_id)
            mock_run.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True, include=None, exclude=None, mode="json"
            )
            assert result == {
                "id": run_id,
                "state": "Running",
                "workspaceId": workspace_id,
                "runnerId": runner_id,
            }

    def test_get_run_metadata_with_include(self):
        """Test the get_run_metadata function with include parameter."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        include = ["id", "state"]

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)

        # Mock run API
        mock_run_api = MagicMock(spec=cosmotech_api.RunApi)
        mock_run = MagicMock(spec=cosmotech_api.Run)
        mock_run.id = run_id
        mock_run.state = "Running"
        mock_run.model_dump.return_value = {
            "id": run_id,
            "state": "Running",
        }
        mock_run_api.get_run.return_value = mock_run

        with patch("cosmotech_api.RunApi", return_value=mock_run_api):
            # Act
            result = get_run_metadata(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                run_id=run_id,
                include=include,
            )

            # Assert
            mock_run_api.get_run.assert_called_once_with(organization_id, workspace_id, runner_id, run_id)
            mock_run.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True, include=include, exclude=None, mode="json"
            )
            assert result == {
                "id": run_id,
                "state": "Running",
            }

    def test_get_run_metadata_with_exclude(self):
        """Test the get_run_metadata function with exclude parameter."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        exclude = ["workspaceId", "runnerId"]

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)

        # Mock run API
        mock_run_api = MagicMock(spec=cosmotech_api.RunApi)
        mock_run = MagicMock(spec=cosmotech_api.Run)
        mock_run.id = run_id
        mock_run.state = "Running"
        mock_run.model_dump.return_value = {
            "id": run_id,
            "state": "Running",
        }
        mock_run_api.get_run.return_value = mock_run

        with patch("cosmotech_api.RunApi", return_value=mock_run_api):
            # Act
            result = get_run_metadata(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                run_id=run_id,
                exclude=exclude,
            )

            # Assert
            mock_run_api.get_run.assert_called_once_with(organization_id, workspace_id, runner_id, run_id)
            mock_run.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True, include=None, exclude=exclude, mode="json"
            )
            assert result == {
                "id": run_id,
                "state": "Running",
            }
