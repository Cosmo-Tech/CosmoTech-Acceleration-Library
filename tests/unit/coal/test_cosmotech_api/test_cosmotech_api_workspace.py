# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from unittest.mock import MagicMock, patch, mock_open

import pytest
import cosmotech_api
from cosmotech_api.models.workspace_file import WorkspaceFile

from cosmotech.coal.cosmotech_api.workspace import list_workspace_files, download_workspace_file, upload_workspace_file


class TestWorkspaceFunctions:
    """Tests for top-level functions in the workspace module."""

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    def test_list_workspace_files(self, mock_workspace_api):
        """Test the list_workspace_files function."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_prefix = "data_"

        # Create mock workspace files
        mock_file1 = MagicMock()
        mock_file1.file_name = "data_file1.csv"
        mock_file2 = MagicMock()
        mock_file2.file_name = "data_file2.csv"
        mock_file3 = MagicMock()
        mock_file3.file_name = "other_file.csv"

        # Set up the mock API response
        mock_api_instance = MagicMock()
        mock_workspace_api.return_value = mock_api_instance
        mock_api_instance.find_all_workspace_files.return_value = [mock_file1, mock_file2, mock_file3]

        # Act
        result = list_workspace_files(
            api_client=mock_api_client,
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_prefix=file_prefix,
        )

        # Assert
        mock_workspace_api.assert_called_once_with(mock_api_client)
        mock_api_instance.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        assert result == ["data_file1.csv", "data_file2.csv"]
        assert "other_file.csv" not in result

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    def test_list_workspace_files_empty(self, mock_workspace_api):
        """Test the list_workspace_files function."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_prefix = "data_"

        # Set up the mock API response
        mock_api_instance = MagicMock()
        mock_workspace_api.return_value = mock_api_instance
        mock_api_instance.find_all_workspace_files.return_value = []

        # Act

        with pytest.raises(ValueError) as excinfo:
            list_workspace_files(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_prefix=file_prefix,
            )

        # Assert
        mock_workspace_api.assert_called_once_with(mock_api_client)
        mock_api_instance.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    @patch("pathlib.Path.mkdir")
    @patch("builtins.open", new_callable=mock_open)
    def test_download_workspace_file(self, mock_file, mock_mkdir, mock_workspace_api):
        """Test the download_workspace_file function."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "data/file1.csv"
        target_dir = pathlib.Path("/tmp/download")

        # Mock the target_dir.is_file() method to return False
        mock_target_dir = MagicMock()
        mock_target_dir.is_file.return_value = False
        mock_target_dir.__truediv__ = lambda self, other: pathlib.Path(f"{self}/{other}")

        # Set up the mock API response
        mock_api_instance = MagicMock()
        mock_workspace_api.return_value = mock_api_instance
        mock_api_instance.download_workspace_file.return_value = b"file content"

        # Act
        with patch("pathlib.Path") as mock_path:
            mock_path.return_value = mock_target_dir
            result = download_workspace_file(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=file_name,
                target_dir=target_dir,
            )

        # Assert
        mock_workspace_api.assert_called_once_with(mock_api_client)
        mock_api_instance.download_workspace_file.assert_called_once_with(organization_id, workspace_id, file_name)
        mock_file.assert_called_once()
        mock_file().write.assert_called_once_with(b"file content")

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    def test_download_workspace_file_target_is_file(self, mock_workspace_api):
        """Test the download_workspace_file function when target_dir is a file."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "file1.csv"

        # Mock the target_dir to be a file
        mock_target_dir = MagicMock()
        mock_target_dir.is_file.return_value = True

        # Act & Assert
        with pytest.raises(ValueError) as excinfo:
            download_workspace_file(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=file_name,
                target_dir=mock_target_dir,
            )

        # Just check that an error is raised
        assert "file" in str(excinfo.value).lower() and "directory" in str(excinfo.value).lower()

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_file")
    def test_upload_workspace_file(self, mock_is_file, mock_exists, mock_workspace_api):
        """Test the upload_workspace_file function."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_path = "/tmp/upload/file1.csv"
        workspace_path = "data/"

        # Mock the file existence checks
        mock_exists.return_value = True
        mock_is_file.return_value = True

        # Set up the mock API response
        mock_api_instance = MagicMock()
        mock_workspace_api.return_value = mock_api_instance
        mock_workspace_file = WorkspaceFile(file_name="data/file1.csv")
        mock_api_instance.upload_workspace_file.return_value = mock_workspace_file

        # Act
        result = upload_workspace_file(
            api_client=mock_api_client,
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_path=file_path,
            workspace_path=workspace_path,
        )

        # Assert
        mock_workspace_api.assert_called_once_with(mock_api_client)
        mock_api_instance.upload_workspace_file.assert_called_once_with(
            organization_id, workspace_id, file_path, True, destination="data/file1.csv"
        )
        assert result == "data/file1.csv"

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    @patch("pathlib.Path.exists")
    def test_upload_workspace_file_file_not_exists(self, mock_exists, mock_workspace_api):
        """Test the upload_workspace_file function when the file doesn't exist."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_path = "/tmp/upload/nonexistent.csv"
        workspace_path = "data/"

        # Mock the file existence check
        mock_exists.return_value = False

        # Act & Assert
        with pytest.raises(ValueError) as excinfo:
            upload_workspace_file(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_path=file_path,
                workspace_path=workspace_path,
            )

        # Just check that the error message contains the file path
        assert file_path in str(excinfo.value)
        assert "not exists" in str(excinfo.value).lower() or "not found" in str(excinfo.value).lower()

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_file")
    def test_upload_workspace_file_not_a_file(self, mock_is_file, mock_exists, mock_workspace_api):
        """Test the upload_workspace_file function when the path is not a file."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_path = "/tmp/upload/directory"
        workspace_path = "data/"

        # Mock the file existence and type checks
        mock_exists.return_value = True
        mock_is_file.return_value = False

        # Act & Assert
        with pytest.raises(ValueError) as excinfo:
            upload_workspace_file(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_path=file_path,
                workspace_path=workspace_path,
            )

        # Just check that the error message contains the file path and indicates it's not a file
        assert file_path in str(excinfo.value)
        assert "not a" in str(excinfo.value).lower() or "not single" in str(excinfo.value).lower()

    @patch("cosmotech_api.api.workspace_api.WorkspaceApi")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.is_file")
    def test_upload_workspace_file_api_exception(self, mock_is_file, mock_exists, mock_workspace_api):
        """Test the upload_workspace_file function when the API raises an exception."""
        # Arrange
        mock_api_client = MagicMock()
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_path = "/tmp/upload/file1.csv"
        workspace_path = "data/"

        # Mock the file existence and type checks
        mock_exists.return_value = True
        mock_is_file.return_value = True

        # Set up the mock API to raise an exception
        mock_api_instance = MagicMock()
        mock_workspace_api.return_value = mock_api_instance
        mock_api_instance.upload_workspace_file.side_effect = cosmotech_api.exceptions.ApiException(status=409)

        # Act & Assert
        with pytest.raises(cosmotech_api.exceptions.ApiException):
            upload_workspace_file(
                api_client=mock_api_client,
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_path=file_path,
                workspace_path=workspace_path,
            )
