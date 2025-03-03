# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import pathlib
from unittest.mock import MagicMock, patch, mock_open

import pytest
import yaml
from cosmotech_api import Solution, Workspace
from cosmotech_api.exceptions import ServiceException

# Mock the dependencies to avoid circular imports
import sys

sys.modules["cosmotech.coal.cosmotech_api.connection"] = MagicMock()
sys.modules["cosmotech.coal.cosmotech_api.orchestrator"] = MagicMock()

# Now we can import the functions
from cosmotech.coal.utils.api import read_solution_file, get_solution


class TestApiFunctions:
    """Tests for top-level functions in the api module."""

    @patch("pathlib.Path")
    @patch("json.load")
    def test_read_solution_file_json(self, mock_json_load, mock_path_class):
        """Test the read_solution_file function with a JSON file."""
        # Arrange
        solution_file = "solution.json"
        solution_content = {
            "name": "Test Solution",
            "version": "1.0.0",
            "parameters": [{"id": "param1", "name": "Parameter 1"}],
        }
        mock_json_load.return_value = solution_content

        # Mock Path instance
        mock_path = MagicMock()
        mock_path.suffix = ".json"
        mock_path.open.return_value.__enter__.return_value = MagicMock()
        mock_path_class.return_value = mock_path

        # Act
        result = read_solution_file(solution_file)

        # Assert
        mock_path.open.assert_called_once()
        mock_json_load.assert_called_once()
        assert result is not None
        assert result.name == "Test Solution"
        assert result.version == "1.0.0"
        assert len(result.parameters) == 1
        assert result.parameters[0].id == "param1"

    @patch("pathlib.Path")
    @patch("yaml.safe_load")
    def test_read_solution_file_yaml(self, mock_yaml_load, mock_path_class):
        """Test the read_solution_file function with a YAML file."""
        # Arrange
        solution_file = "solution.yaml"
        solution_content = {
            "name": "Test Solution",
            "version": "1.0.0",
            "parameters": [{"id": "param1", "name": "Parameter 1"}],
        }
        mock_yaml_load.return_value = solution_content

        # Mock Path instance
        mock_path = MagicMock()
        mock_path.suffix = ".yaml"
        mock_path.open.return_value.__enter__.return_value = MagicMock()
        mock_path_class.return_value = mock_path

        # Act
        result = read_solution_file(solution_file)

        # Assert
        mock_path.open.assert_called_once()
        mock_yaml_load.assert_called_once()
        assert result is not None
        assert result.name == "Test Solution"
        assert result.version == "1.0.0"
        assert len(result.parameters) == 1
        assert result.parameters[0].id == "param1"

    @patch("pathlib.Path")
    def test_read_solution_file_invalid_extension(self, mock_path_class):
        """Test the read_solution_file function with an invalid file extension."""
        # Arrange
        solution_file = "solution.txt"

        # Mock Path instance
        mock_path = MagicMock()
        mock_path.suffix = ".txt"
        mock_path_class.return_value = mock_path

        # Act
        result = read_solution_file(solution_file)

        # Assert
        assert result is None

    @patch("cosmotech.coal.utils.api.get_api_client")
    def test_get_solution_success(self, mock_get_api_client):
        """Test the get_solution function with successful API calls."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        solution_id = "sol-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock workspace API
        mock_workspace_api = MagicMock()
        mock_workspace = MagicMock()
        # Create a solution attribute with a solution_id
        mock_solution = MagicMock()
        mock_solution.solution_id = solution_id
        mock_workspace.solution = mock_solution
        mock_workspace_api.find_workspace_by_id.return_value = mock_workspace

        # Mock solution API
        mock_solution_api = MagicMock()
        mock_solution = MagicMock(spec=Solution)
        mock_solution.name = "Test Solution"
        mock_solution_api.find_solution_by_id.return_value = mock_solution

        with patch("cosmotech.coal.utils.api.WorkspaceApi", return_value=mock_workspace_api):
            with patch("cosmotech.coal.utils.api.SolutionApi", return_value=mock_solution_api):
                # Act
                result = get_solution(organization_id, workspace_id)

        # Assert
        mock_workspace_api.find_workspace_by_id.assert_called_once_with(
            organization_id=organization_id, workspace_id=workspace_id
        )
        mock_solution_api.find_solution_by_id.assert_called_once_with(
            organization_id=organization_id, solution_id=solution_id
        )
        assert result == mock_solution

    @patch("cosmotech.coal.utils.api.get_api_client")
    def test_get_solution_workspace_not_found(self, mock_get_api_client):
        """Test the get_solution function when workspace is not found."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock workspace API to raise exception
        mock_workspace_api = MagicMock()
        mock_workspace_api.find_workspace_by_id.side_effect = ServiceException(
            status=404, reason="Not Found", body="Workspace not found"
        )

        with patch("cosmotech.coal.utils.api.WorkspaceApi", return_value=mock_workspace_api):
            # Act
            result = get_solution(organization_id, workspace_id)

        # Assert
        mock_workspace_api.find_workspace_by_id.assert_called_once_with(
            organization_id=organization_id, workspace_id=workspace_id
        )
        assert result is None
