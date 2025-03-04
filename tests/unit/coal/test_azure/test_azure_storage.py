# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import pathlib
import pytest
from unittest.mock import MagicMock, patch, mock_open

from cosmotech.coal.azure.storage import upload_file, upload_folder


class TestStorageFunctions:
    """Tests for top-level functions in the storage module."""

    @patch("cosmotech.coal.azure.storage.ContainerClient")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_file(self, mock_logger, mock_container_client):
        """Test the upload_file function."""
        # Arrange
        mock_container_client_instance = MagicMock()
        mock_container_client.from_container_url.return_value = mock_container_client_instance

        # Create a mock file path with a mock open method
        file_path = MagicMock(spec=pathlib.Path)
        file_path.name = "test_file.txt"
        mock_file = MagicMock()
        file_path.open.return_value = mock_file

        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"
        file_prefix = "prefix_"

        # Act
        upload_file(file_path, blob_name, sas_url, file_prefix)

        # Assert
        mock_container_client.from_container_url.assert_called_once_with(sas_url)
        mock_container_client_instance.upload_blob.assert_called_once_with(
            "test-blob/prefix_test_file.txt", mock_file, overwrite=True
        )
        mock_logger.info.assert_called_once()
        file_path.open.assert_called_once_with("rb")

    @patch("cosmotech.coal.azure.storage.ContainerClient")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_file_without_prefix(self, mock_logger, mock_container_client):
        """Test the upload_file function without a prefix."""
        # Arrange
        mock_container_client_instance = MagicMock()
        mock_container_client.from_container_url.return_value = mock_container_client_instance

        # Create a mock file path with a mock open method
        file_path = MagicMock(spec=pathlib.Path)
        file_path.name = "test_file.txt"
        mock_file = MagicMock()
        file_path.open.return_value = mock_file

        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"

        # Act
        upload_file(file_path, blob_name, sas_url)

        # Assert
        mock_container_client.from_container_url.assert_called_once_with(sas_url)
        mock_container_client_instance.upload_blob.assert_called_once_with(
            "test-blob/test_file.txt", mock_file, overwrite=True
        )
        mock_logger.info.assert_called_once()
        file_path.open.assert_called_once_with("rb")

    @patch("cosmotech.coal.azure.storage.upload_file")
    @patch("cosmotech.coal.azure.storage.pathlib.Path")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_folder_recursive(self, mock_logger, mock_path, mock_upload_file):
        """Test the upload_folder function with recursive=True."""
        # Arrange
        source_folder = "/path/to/folder"
        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"
        file_prefix = "prefix_"

        # Setup mock Path
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_path_instance.exists.return_value = True
        mock_path_instance.is_dir.return_value = True

        # Create mock files
        file1 = MagicMock()
        file1.is_file.return_value = True
        file1.name = "file1.txt"
        file2 = MagicMock()
        file2.is_file.return_value = True
        file2.name = "file2.txt"
        dir1 = MagicMock()
        dir1.is_file.return_value = False

        # Setup glob to return our mock files
        mock_path_instance.glob.return_value = [file1, file2, dir1]

        # Setup str representation for source path
        mock_path_instance.__str__.return_value = source_folder

        # Setup str representation for file paths
        file1.__str__.return_value = f"{source_folder}/file1.txt"
        file2.__str__.return_value = f"{source_folder}/file2.txt"

        # Act
        upload_folder(source_folder, blob_name, sas_url, file_prefix, recursive=True)

        # Assert
        mock_path.assert_called_once_with(source_folder)
        mock_path_instance.exists.assert_called_once()
        mock_path_instance.is_dir.assert_called_once()
        mock_path_instance.glob.assert_called_once_with("**/*")

        # Should call upload_file twice (once for each file)
        assert mock_upload_file.call_count == 2
        mock_upload_file.assert_any_call(file1, blob_name, sas_url, file_prefix)
        mock_upload_file.assert_any_call(file2, blob_name, sas_url, file_prefix)

    @patch("cosmotech.coal.azure.storage.upload_file")
    @patch("cosmotech.coal.azure.storage.pathlib.Path")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_folder_non_recursive(self, mock_logger, mock_path, mock_upload_file):
        """Test the upload_folder function with recursive=False."""
        # Arrange
        source_folder = "/path/to/folder"
        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"

        # Setup mock Path
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_path_instance.exists.return_value = True
        mock_path_instance.is_dir.return_value = True

        # Create mock files
        file1 = MagicMock()
        file1.is_file.return_value = True
        file1.name = "file1.txt"

        # Setup glob to return our mock files
        mock_path_instance.glob.return_value = [file1]

        # Setup str representation for source path
        mock_path_instance.__str__.return_value = source_folder

        # Setup str representation for file paths
        file1.__str__.return_value = f"{source_folder}/file1.txt"

        # Act
        upload_folder(source_folder, blob_name, sas_url)

        # Assert
        mock_path.assert_called_once_with(source_folder)
        mock_path_instance.exists.assert_called_once()
        mock_path_instance.is_dir.assert_called_once()
        mock_path_instance.glob.assert_called_once_with("*")

        # Should call upload_file once
        mock_upload_file.assert_called_once_with(file1, blob_name, sas_url, "")

    @patch("cosmotech.coal.azure.storage.upload_file")
    @patch("cosmotech.coal.azure.storage.pathlib.Path")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_folder_source_is_file(self, mock_logger, mock_path, mock_upload_file):
        """Test the upload_folder function when source is a file."""
        # Arrange
        source_file = "/path/to/file.txt"
        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"
        file_prefix = "prefix_"

        # Setup mock Path
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_path_instance.exists.return_value = True
        mock_path_instance.is_dir.return_value = False

        # Act
        upload_folder(source_file, blob_name, sas_url, file_prefix)

        # Assert
        mock_path.assert_called_once_with(source_file)
        mock_path_instance.exists.assert_called_once()
        mock_path_instance.is_dir.assert_called_once()

        # Should call upload_file once with the file path
        mock_upload_file.assert_called_once_with(mock_path_instance, blob_name, sas_url, file_prefix)

    @patch("cosmotech.coal.azure.storage.pathlib.Path")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_folder_source_not_found(self, mock_logger, mock_path):
        """Test the upload_folder function when source folder doesn't exist."""
        # Arrange
        source_folder = "/path/to/nonexistent_folder"
        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"

        # Setup mock Path
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_path_instance.exists.return_value = False

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            upload_folder(source_folder, blob_name, sas_url)

        mock_path.assert_called_once_with(source_folder)
        mock_path_instance.exists.assert_called_once()
        mock_logger.error.assert_called_once()

    @patch("cosmotech.coal.azure.storage.upload_file")
    @patch("cosmotech.coal.azure.storage.pathlib.Path")
    @patch("cosmotech.coal.azure.storage.LOGGER")
    def test_upload_folder_empty_folder(self, mock_logger, mock_path, mock_upload_file):
        """Test the upload_folder function with an empty folder."""
        # Arrange
        source_folder = "/path/to/empty_folder"
        blob_name = "test-blob"
        sas_url = "https://test-storage.blob.core.windows.net/container?sas-token"

        # Setup mock Path
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_path_instance.exists.return_value = True
        mock_path_instance.is_dir.return_value = True

        # Setup glob to return empty list (no files)
        mock_path_instance.glob.return_value = []

        # Act
        upload_folder(source_folder, blob_name, sas_url)

        # Assert
        mock_path.assert_called_once_with(source_folder)
        mock_path_instance.exists.assert_called_once()
        mock_path_instance.is_dir.assert_called_once()
        mock_path_instance.glob.assert_called_once_with("*")

        # Should not call upload_file
        mock_upload_file.assert_not_called()
