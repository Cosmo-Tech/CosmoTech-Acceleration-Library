# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cosmotech_api.exceptions import ApiException

from cosmotech.coal.cosmotech_api.apis.workspace import WorkspaceApi
from cosmotech.coal.utils.configuration import Configuration


class TestWorkspaceApi:
    """Tests for the WorkspaceApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_workspace_api_initialization(self, mock_cosmotech_config, mock_api_client):
        """Test WorkspaceApi initialization."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == mock_config

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_list_filtered_workspace_files(self, mock_cosmotech_config, mock_api_client):
        """Test listing filtered workspace files."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock workspace files
        mock_file1 = MagicMock()
        mock_file1.file_name = "data/file1.csv"
        mock_file2 = MagicMock()
        mock_file2.file_name = "data/file2.csv"
        mock_file3 = MagicMock()
        mock_file3.file_name = "other/file3.csv"

        api = WorkspaceApi(configuration=mock_config)
        api.list_workspace_files = MagicMock(return_value=[mock_file1, mock_file2, mock_file3])

        result = api.list_filtered_workspace_files("org-123", "ws-456", "data/")

        assert len(result) == 2
        assert "data/file1.csv" in result
        assert "data/file2.csv" in result
        assert "other/file3.csv" not in result

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_list_filtered_workspace_files_no_matches(self, mock_cosmotech_config, mock_api_client):
        """Test listing filtered workspace files with no matches."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock workspace files
        mock_file1 = MagicMock()
        mock_file1.file_name = "other/file1.csv"

        api = WorkspaceApi(configuration=mock_config)
        api.list_workspace_files = MagicMock(return_value=[mock_file1])

        with pytest.raises(ValueError):
            api.list_filtered_workspace_files("org-123", "ws-456", "data/")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_download_workspace_file(self, mock_cosmotech_config, mock_api_client):
        """Test downloading a workspace file."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)
        api.get_workspace_file = MagicMock(return_value=b"file content")

        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir)
            result = api.download_workspace_file("org-123", "ws-456", "data.csv", target_dir)

            expected_path = target_dir / "data.csv"
            assert result == expected_path
            assert result.exists()
            assert result.read_bytes() == b"file content"

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_download_workspace_file_nested_path(self, mock_cosmotech_config, mock_api_client):
        """Test downloading a workspace file to a nested directory."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)
        api.get_workspace_file = MagicMock(return_value=b"file content")

        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir)
            result = api.download_workspace_file("org-123", "ws-456", "subdir/data.csv", target_dir)

            expected_path = target_dir / "subdir" / "data.csv"
            assert result == expected_path
            assert result.exists()
            assert result.read_bytes() == b"file content"

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_download_workspace_file_target_is_file_raises_error(self, mock_cosmotech_config, mock_api_client):
        """Test that download_workspace_file raises error if target is a file."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)

        with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
            try:
                with pytest.raises(ValueError):
                    api.download_workspace_file("org-123", "ws-456", "data.csv", Path(tmpfile.name))
            finally:
                os.unlink(tmpfile.name)

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_workspace_file(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a workspace file."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        mock_file_response = MagicMock()
        mock_file_response.file_name = "data.csv"

        api = WorkspaceApi(configuration=mock_config)
        api.create_workspace_file = MagicMock(return_value=mock_file_response)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            tmpfile.write(b"test data")
            tmpfile_path = tmpfile.name

        try:
            result = api.upload_workspace_file("org-123", "ws-456", tmpfile_path, "data.csv")

            assert result == "data.csv"
            api.create_workspace_file.assert_called_once_with(
                "org-123", "ws-456", tmpfile_path, True, destination="data.csv"
            )
        finally:
            os.unlink(tmpfile_path)

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_workspace_file_with_trailing_slash(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a workspace file with workspace_path ending in slash."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        mock_file_response = MagicMock()
        mock_file_response.file_name = "subdir/data.csv"

        api = WorkspaceApi(configuration=mock_config)
        api.create_workspace_file = MagicMock(return_value=mock_file_response)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            tmpfile.write(b"test data")
            tmpfile_path = tmpfile.name
            file_name = Path(tmpfile_path).name

        try:
            result = api.upload_workspace_file("org-123", "ws-456", tmpfile_path, "subdir/")

            assert result == "subdir/data.csv"
            api.create_workspace_file.assert_called_once()
            call_args = api.create_workspace_file.call_args
            assert call_args[1]["destination"] == f"subdir/{file_name}"
        finally:
            os.unlink(tmpfile_path)

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_workspace_file_no_overwrite(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a workspace file without overwrite."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        mock_file_response = MagicMock()
        mock_file_response.file_name = "data.csv"

        api = WorkspaceApi(configuration=mock_config)
        api.create_workspace_file = MagicMock(return_value=mock_file_response)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            tmpfile.write(b"test data")
            tmpfile_path = tmpfile.name

        try:
            result = api.upload_workspace_file("org-123", "ws-456", tmpfile_path, "data.csv", overwrite=False)

            assert result == "data.csv"
            api.create_workspace_file.assert_called_once_with(
                "org-123", "ws-456", tmpfile_path, False, destination="data.csv"
            )
        finally:
            os.unlink(tmpfile_path)

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_workspace_file_nonexistent_raises_error(self, mock_cosmotech_config, mock_api_client):
        """Test that upload_workspace_file raises error for non-existent file."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)

        with pytest.raises(ValueError):
            api.upload_workspace_file("org-123", "ws-456", "/nonexistent/file.csv", "data.csv")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_workspace_file_directory_raises_error(self, mock_cosmotech_config, mock_api_client):
        """Test that upload_workspace_file raises error for directory."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError):
                api.upload_workspace_file("org-123", "ws-456", tmpdir, "data.csv")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_workspace_file_api_exception(self, mock_cosmotech_config, mock_api_client):
        """Test that upload_workspace_file propagates ApiException."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = WorkspaceApi(configuration=mock_config)
        api.create_workspace_file = MagicMock(side_effect=ApiException("File already exists"))

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            tmpfile.write(b"test data")
            tmpfile_path = tmpfile.name

        try:
            with pytest.raises(ApiException):
                api.upload_workspace_file("org-123", "ws-456", tmpfile_path, "data.csv")
        finally:
            os.unlink(tmpfile_path)
