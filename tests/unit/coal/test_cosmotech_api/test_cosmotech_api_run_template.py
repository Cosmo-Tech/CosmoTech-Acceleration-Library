# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
import pytest
from unittest.mock import MagicMock, patch, mock_open
from io import BytesIO
from zipfile import BadZipfile, ZipFile

import cosmotech_api
from cosmotech_api.api.solution_api import SolutionApi
from cosmotech_api.api.workspace_api import Workspace, WorkspaceApi
from cosmotech_api.exceptions import ServiceException

from cosmotech.coal.cosmotech_api.run_template import load_run_template_handlers


class TestRunTemplateFunctions:
    """Tests for top-level functions in the run_template module."""

    def test_load_run_template_handlers_success(self):
        """Test the load_run_template_handlers function with successful download and extraction."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        run_template_id = "rt-123"
        handler_list = "parameters_handler,validator"
        solution_id = "sol-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)
        mock_api_client_context = MagicMock()
        mock_api_client_context.__enter__.return_value = mock_api_client

        # Mock workspace API
        mock_workspace_api = MagicMock(spec=WorkspaceApi)
        mock_workspace = MagicMock(spec=Workspace)
        mock_solution = MagicMock()
        mock_solution.solution_id = solution_id
        mock_workspace.solution = mock_solution
        mock_workspace_api.find_workspace_by_id.return_value = mock_workspace

        # Mock solution API
        mock_solution_api = MagicMock(spec=SolutionApi)
        mock_solution_api.download_run_template_handler.return_value = b"zip_content"

        # Mock ZipFile
        mock_zipfile = MagicMock(spec=ZipFile)
        mock_zipfile_context = MagicMock()
        mock_zipfile_context.__enter__.return_value = mock_zipfile
        mock_zipfile.return_value = mock_zipfile_context

        # Mock Path
        mock_path = MagicMock(spec=pathlib.Path)
        mock_path.absolute.return_value = "/path/to/handler"

        with patch(
            "cosmotech.coal.cosmotech_api.run_template.get_api_client",
            return_value=(mock_api_client_context, "API Key"),
        ), patch("cosmotech.coal.cosmotech_api.run_template.WorkspaceApi", return_value=mock_workspace_api), patch(
            "cosmotech.coal.cosmotech_api.run_template.SolutionApi", return_value=mock_solution_api
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.ZipFile", return_value=mock_zipfile_context
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.BytesIO"
        ) as mock_bytesio, patch(
            "cosmotech.coal.cosmotech_api.run_template.pathlib.Path"
        ) as mock_path_class:
            mock_path_class.return_value = mock_path
            mock_path.mkdir.return_value = None
            mock_path.__truediv__.return_value = mock_path

            # Act
            result = load_run_template_handlers(
                organization_id=organization_id,
                workspace_id=workspace_id,
                run_template_id=run_template_id,
                handler_list=handler_list,
            )

            # Assert
            assert result is True
            mock_workspace_api.find_workspace_by_id.assert_called_once_with(
                organization_id=organization_id, workspace_id=workspace_id
            )
            assert mock_solution_api.download_run_template_handler.call_count == 2
            mock_solution_api.download_run_template_handler.assert_any_call(
                organization_id=organization_id,
                solution_id=solution_id,
                run_template_id=run_template_id,
                handler_id="parameters_handler",
            )
            mock_solution_api.download_run_template_handler.assert_any_call(
                organization_id=organization_id,
                solution_id=solution_id,
                run_template_id=run_template_id,
                handler_id="validator",
            )
            assert mock_path.mkdir.call_count == 2
            assert mock_zipfile.extractall.call_count == 2

    def test_load_run_template_handlers_workspace_not_found(self):
        """Test the load_run_template_handlers function when workspace is not found."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        run_template_id = "rt-123"
        handler_list = "parameters_handler"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)
        mock_api_client_context = MagicMock()
        mock_api_client_context.__enter__.return_value = mock_api_client

        # Mock workspace API with exception
        mock_workspace_api = MagicMock(spec=WorkspaceApi)
        mock_exception = ServiceException(http_resp=MagicMock(status=404, data=b'{"message": "Workspace not found"}'))
        mock_workspace_api.find_workspace_by_id.side_effect = mock_exception

        with patch(
            "cosmotech.coal.cosmotech_api.run_template.get_api_client",
            return_value=(mock_api_client_context, "API Key"),
        ), patch("cosmotech.coal.cosmotech_api.run_template.WorkspaceApi", return_value=mock_workspace_api):
            # Act & Assert
            with pytest.raises(
                ValueError, match=f"Workspace {workspace_id} not found in organization {organization_id}"
            ):
                load_run_template_handlers(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    run_template_id=run_template_id,
                    handler_list=handler_list,
                )

            mock_workspace_api.find_workspace_by_id.assert_called_once_with(
                organization_id=organization_id, workspace_id=workspace_id
            )

    def test_load_run_template_handlers_handler_not_found(self):
        """Test the load_run_template_handlers function when handler is not found."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        run_template_id = "rt-123"
        handler_list = "parameters_handler"
        solution_id = "sol-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)
        mock_api_client_context = MagicMock()
        mock_api_client_context.__enter__.return_value = mock_api_client

        # Mock workspace API
        mock_workspace_api = MagicMock(spec=WorkspaceApi)
        mock_workspace = MagicMock(spec=Workspace)
        mock_solution = MagicMock()
        mock_solution.solution_id = solution_id
        mock_workspace.solution = mock_solution
        mock_workspace_api.find_workspace_by_id.return_value = mock_workspace

        # Mock solution API with exception
        mock_solution_api = MagicMock(spec=SolutionApi)
        mock_exception = ServiceException(http_resp=MagicMock(status=404, data=b'{"message": "Handler not found"}'))
        mock_solution_api.download_run_template_handler.side_effect = mock_exception

        # Mock Path
        mock_path = MagicMock(spec=pathlib.Path)
        mock_path.absolute.return_value = "/path/to/handler"

        with patch(
            "cosmotech.coal.cosmotech_api.run_template.get_api_client",
            return_value=(mock_api_client_context, "API Key"),
        ), patch("cosmotech.coal.cosmotech_api.run_template.WorkspaceApi", return_value=mock_workspace_api), patch(
            "cosmotech.coal.cosmotech_api.run_template.SolutionApi", return_value=mock_solution_api
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.pathlib.Path"
        ) as mock_path_class:
            mock_path_class.return_value = mock_path
            mock_path.mkdir.return_value = None
            mock_path.__truediv__.return_value = mock_path

            # Act
            result = load_run_template_handlers(
                organization_id=organization_id,
                workspace_id=workspace_id,
                run_template_id=run_template_id,
                handler_list=handler_list,
            )

            # Assert
            assert result is False
            mock_workspace_api.find_workspace_by_id.assert_called_once_with(
                organization_id=organization_id, workspace_id=workspace_id
            )
            mock_solution_api.download_run_template_handler.assert_called_once_with(
                organization_id=organization_id,
                solution_id=solution_id,
                run_template_id=run_template_id,
                handler_id="parameters_handler",
            )

    def test_load_run_template_handlers_bad_zip_file(self):
        """Test the load_run_template_handlers function when the handler is not a valid zip file."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        run_template_id = "rt-123"
        handler_list = "parameters_handler"
        solution_id = "sol-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)
        mock_api_client_context = MagicMock()
        mock_api_client_context.__enter__.return_value = mock_api_client

        # Mock workspace API
        mock_workspace_api = MagicMock(spec=WorkspaceApi)
        mock_workspace = MagicMock(spec=Workspace)
        mock_solution = MagicMock()
        mock_solution.solution_id = solution_id
        mock_workspace.solution = mock_solution
        mock_workspace_api.find_workspace_by_id.return_value = mock_workspace

        # Mock solution API
        mock_solution_api = MagicMock(spec=SolutionApi)
        mock_solution_api.download_run_template_handler.return_value = b"not_a_zip_file"

        # Mock Path
        mock_path = MagicMock(spec=pathlib.Path)
        mock_path.absolute.return_value = "/path/to/handler"

        with patch(
            "cosmotech.coal.cosmotech_api.run_template.get_api_client",
            return_value=(mock_api_client_context, "API Key"),
        ), patch("cosmotech.coal.cosmotech_api.run_template.WorkspaceApi", return_value=mock_workspace_api), patch(
            "cosmotech.coal.cosmotech_api.run_template.SolutionApi", return_value=mock_solution_api
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.ZipFile"
        ) as mock_zipfile, patch(
            "cosmotech.coal.cosmotech_api.run_template.BytesIO"
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.pathlib.Path"
        ) as mock_path_class:
            mock_path_class.return_value = mock_path
            mock_path.mkdir.return_value = None
            mock_path.__truediv__.return_value = mock_path
            mock_zipfile.side_effect = BadZipfile("Not a zip file")

            # Act
            result = load_run_template_handlers(
                organization_id=organization_id,
                workspace_id=workspace_id,
                run_template_id=run_template_id,
                handler_list=handler_list,
            )

            # Assert
            assert result is False
            mock_workspace_api.find_workspace_by_id.assert_called_once_with(
                organization_id=organization_id, workspace_id=workspace_id
            )
            mock_solution_api.download_run_template_handler.assert_called_once_with(
                organization_id=organization_id,
                solution_id=solution_id,
                run_template_id=run_template_id,
                handler_id="parameters_handler",
            )

    def test_load_run_template_handlers_handle_parameters_conversion(self):
        """Test that 'handle-parameters' is converted to 'parameters_handler'."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        run_template_id = "rt-123"
        handler_list = "handle-parameters"  # This should be converted to parameters_handler
        solution_id = "sol-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)
        mock_api_client_context = MagicMock()
        mock_api_client_context.__enter__.return_value = mock_api_client

        # Mock workspace API
        mock_workspace_api = MagicMock(spec=WorkspaceApi)
        mock_workspace = MagicMock(spec=Workspace)
        mock_solution = MagicMock()
        mock_solution.solution_id = solution_id
        mock_workspace.solution = mock_solution
        mock_workspace_api.find_workspace_by_id.return_value = mock_workspace

        # Mock solution API
        mock_solution_api = MagicMock(spec=SolutionApi)
        mock_solution_api.download_run_template_handler.return_value = b"zip_content"

        # Mock ZipFile
        mock_zipfile = MagicMock(spec=ZipFile)
        mock_zipfile_context = MagicMock()
        mock_zipfile_context.__enter__.return_value = mock_zipfile
        mock_zipfile.return_value = mock_zipfile_context

        # Mock Path
        mock_path = MagicMock(spec=pathlib.Path)
        mock_path.absolute.return_value = "/path/to/handler"

        with patch(
            "cosmotech.coal.cosmotech_api.run_template.get_api_client",
            return_value=(mock_api_client_context, "API Key"),
        ), patch("cosmotech.coal.cosmotech_api.run_template.WorkspaceApi", return_value=mock_workspace_api), patch(
            "cosmotech.coal.cosmotech_api.run_template.SolutionApi", return_value=mock_solution_api
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.ZipFile", return_value=mock_zipfile_context
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.BytesIO"
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.pathlib.Path"
        ) as mock_path_class:
            mock_path_class.return_value = mock_path
            mock_path.mkdir.return_value = None
            mock_path.__truediv__.return_value = mock_path

            # Act
            result = load_run_template_handlers(
                organization_id=organization_id,
                workspace_id=workspace_id,
                run_template_id=run_template_id,
                handler_list=handler_list,
            )

            # Assert
            assert result is True
            mock_solution_api.download_run_template_handler.assert_called_once_with(
                organization_id=organization_id,
                solution_id=solution_id,
                run_template_id=run_template_id,
                handler_id="parameters_handler",  # Should be converted from handle-parameters
            )

    def test_load_run_template_handlers_multiple_handlers_partial_failure(self):
        """Test the load_run_template_handlers function with multiple handlers where some fail."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        run_template_id = "rt-123"
        handler_list = "parameters_handler,validator,missing_handler"
        solution_id = "sol-123"

        # Mock API client
        mock_api_client = MagicMock(spec=cosmotech_api.ApiClient)
        mock_api_client_context = MagicMock()
        mock_api_client_context.__enter__.return_value = mock_api_client

        # Mock workspace API
        mock_workspace_api = MagicMock(spec=WorkspaceApi)
        mock_workspace = MagicMock(spec=Workspace)
        mock_solution = MagicMock()
        mock_solution.solution_id = solution_id
        mock_workspace.solution = mock_solution
        mock_workspace_api.find_workspace_by_id.return_value = mock_workspace

        # Mock solution API with conditional behavior
        mock_solution_api = MagicMock(spec=SolutionApi)

        def download_handler_side_effect(organization_id, solution_id, run_template_id, handler_id):
            if handler_id == "missing_handler":
                raise ServiceException(http_resp=MagicMock(status=404, data=b'{"message": "Handler not found"}'))
            return b"zip_content"

        mock_solution_api.download_run_template_handler.side_effect = download_handler_side_effect

        # Mock ZipFile
        mock_zipfile = MagicMock(spec=ZipFile)
        mock_zipfile_context = MagicMock()
        mock_zipfile_context.__enter__.return_value = mock_zipfile
        mock_zipfile.return_value = mock_zipfile_context

        # Mock Path
        mock_path = MagicMock(spec=pathlib.Path)
        mock_path.absolute.return_value = "/path/to/handler"

        with patch(
            "cosmotech.coal.cosmotech_api.run_template.get_api_client",
            return_value=(mock_api_client_context, "API Key"),
        ), patch("cosmotech.coal.cosmotech_api.run_template.WorkspaceApi", return_value=mock_workspace_api), patch(
            "cosmotech.coal.cosmotech_api.run_template.SolutionApi", return_value=mock_solution_api
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.ZipFile", return_value=mock_zipfile_context
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.BytesIO"
        ), patch(
            "cosmotech.coal.cosmotech_api.run_template.pathlib.Path"
        ) as mock_path_class:
            mock_path_class.return_value = mock_path
            mock_path.mkdir.return_value = None
            mock_path.__truediv__.return_value = mock_path

            # Act
            result = load_run_template_handlers(
                organization_id=organization_id,
                workspace_id=workspace_id,
                run_template_id=run_template_id,
                handler_list=handler_list,
            )

            # Assert
            assert result is False  # Should return False because one handler failed
            assert mock_solution_api.download_run_template_handler.call_count == 3
            assert mock_zipfile.extractall.call_count == 2  # Only two successful extractions
