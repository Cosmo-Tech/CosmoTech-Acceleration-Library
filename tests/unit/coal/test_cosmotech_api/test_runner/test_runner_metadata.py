# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

import cosmotech_api
from cosmotech.coal.cosmotech_api.runner.metadata import get_runner_metadata


class TestMetadataFunctions:
    """Tests for top-level functions in the metadata module."""

    def test_get_runner_metadata(self):
        """Test the get_runner_metadata function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)

        # Mock runner API
        mock_runner_api = MagicMock(spec=cosmotech_api.RunnerApi)
        mock_runner = MagicMock(spec=cosmotech_api.Runner)
        mock_runner.id = runner_id
        mock_runner.name = "Test Runner"
        mock_runner.model_dump.return_value = {
            "id": runner_id,
            "name": "Test Runner",
            "description": "Test runner description",
            "tags": ["test", "runner"],
        }
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech_api.RunnerApi", return_value=mock_runner_api):
            # Act
            result = get_runner_metadata(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
            )

            # Assert
            mock_runner_api.get_runner.assert_called_once_with(organization_id, workspace_id, runner_id)
            mock_runner.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True, include=None, exclude=None, mode="json"
            )
            assert result == {
                "id": runner_id,
                "name": "Test Runner",
                "description": "Test runner description",
                "tags": ["test", "runner"],
            }

    def test_get_runner_metadata_with_include(self):
        """Test the get_runner_metadata function with include parameter."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        include = ["id", "name"]

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)

        # Mock runner API
        mock_runner_api = MagicMock(spec=cosmotech_api.RunnerApi)
        mock_runner = MagicMock(spec=cosmotech_api.Runner)
        mock_runner.id = runner_id
        mock_runner.name = "Test Runner"
        mock_runner.model_dump.return_value = {
            "id": runner_id,
            "name": "Test Runner",
        }
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech_api.RunnerApi", return_value=mock_runner_api):
            # Act
            result = get_runner_metadata(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                include=include,
            )

            # Assert
            mock_runner_api.get_runner.assert_called_once_with(organization_id, workspace_id, runner_id)
            mock_runner.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True, include=include, exclude=None, mode="json"
            )
            assert result == {
                "id": runner_id,
                "name": "Test Runner",
            }

    def test_get_runner_metadata_with_exclude(self):
        """Test the get_runner_metadata function with exclude parameter."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        exclude = ["description", "tags"]

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)

        # Mock runner API
        mock_runner_api = MagicMock(spec=cosmotech_api.RunnerApi)
        mock_runner = MagicMock(spec=cosmotech_api.Runner)
        mock_runner.id = runner_id
        mock_runner.name = "Test Runner"
        mock_runner.model_dump.return_value = {
            "id": runner_id,
            "name": "Test Runner",
        }
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech_api.RunnerApi", return_value=mock_runner_api):
            # Act
            result = get_runner_metadata(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                exclude=exclude,
            )

            # Assert
            mock_runner_api.get_runner.assert_called_once_with(organization_id, workspace_id, runner_id)
            mock_runner.model_dump.assert_called_once_with(
                by_alias=True, exclude_none=True, include=None, exclude=exclude, mode="json"
            )
            assert result == {
                "id": runner_id,
                "name": "Test Runner",
            }
