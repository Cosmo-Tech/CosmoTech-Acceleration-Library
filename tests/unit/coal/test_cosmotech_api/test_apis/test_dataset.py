# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from cosmotech_api.models.dataset import Dataset
from cosmotech_api.models.dataset_part_type_enum import DatasetPartTypeEnum

from cosmotech.coal.cosmotech_api.apis.dataset import DatasetApi
from cosmotech.coal.utils.configuration import Configuration


class TestDatasetApi:
    """Tests for the DatasetApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_dataset_api_initialization(self, mock_cosmotech_config, mock_api_client):
        """Test DatasetApi initialization."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = DatasetApi(configuration=mock_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == mock_config

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    @patch("builtins.open", new_callable=mock_open)
    @patch("pathlib.Path.mkdir")
    def test_download_dataset(self, mock_mkdir, mock_file, mock_cosmotech_config, mock_api_client):
        """Test downloading a dataset."""
        mock_config = MagicMock()
        mock_config.cosmotech.organization_id = "org-123"
        mock_config.cosmotech.workspace_id = "ws-456"
        mock_config.cosmotech.dataset_absolute_path = "/tmp/datasets"

        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock dataset with parts
        mock_dataset = MagicMock(spec=Dataset)
        mock_part1 = MagicMock()
        mock_part1.id = "part-1"
        mock_part1.source_name = "file1.csv"
        mock_part2 = MagicMock()
        mock_part2.id = "part-2"
        mock_part2.source_name = "subdir/file2.csv"
        mock_dataset.parts = [mock_part1, mock_part2]

        api = DatasetApi(configuration=mock_config)
        api.get_dataset = MagicMock(return_value=mock_dataset)
        api.download_dataset_part = MagicMock(side_effect=[b"data1", b"data2"])

        result = api.download_dataset("dataset-789")

        assert result == mock_dataset
        api.get_dataset.assert_called_once_with(
            organization_id="org-123", workspace_id="ws-456", dataset_id="dataset-789"
        )
        assert api.download_dataset_part.call_count == 2
        assert mock_file.call_count == 2

    def test_path_to_parts_single_file(self):
        """Test path_to_parts with a single file."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            tmpfile.write(b"test data")
            tmpfile_path = tmpfile.name

        try:
            result = DatasetApi.path_to_parts(tmpfile_path, DatasetPartTypeEnum.FILE)

            assert len(result) == 1
            assert result[0][0] == Path(tmpfile_path).name
            assert result[0][1] == Path(tmpfile_path)
            assert result[0][2] == DatasetPartTypeEnum.FILE
        finally:
            os.unlink(tmpfile_path)

    def test_path_to_parts_directory(self):
        """Test path_to_parts with a directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test files
            file1 = Path(tmpdir) / "file1.csv"
            file1.write_text("data1")
            file2 = Path(tmpdir) / "subdir" / "file2.csv"
            file2.parent.mkdir(parents=True, exist_ok=True)
            file2.write_text("data2")

            result = DatasetApi.path_to_parts(tmpdir, DatasetPartTypeEnum.FILE)

            assert len(result) == 2
            # Check that all returned items are tuples with proper structure
            for item in result:
                assert len(item) == 3
                assert isinstance(item[0], str)  # relative path
                assert isinstance(item[1], Path)  # absolute path
                assert item[2] == DatasetPartTypeEnum.FILE

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_dataset_with_files(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a dataset with files."""
        mock_config = MagicMock()
        mock_config.cosmotech.organization_id = "org-123"
        mock_config.cosmotech.workspace_id = "ws-456"

        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock created dataset
        mock_dataset = MagicMock(spec=Dataset)
        mock_dataset.id = "new-dataset-123"

        api = DatasetApi(configuration=mock_config)
        api.create_dataset = MagicMock(return_value=mock_dataset)

        with tempfile.TemporaryDirectory() as tmpdir:
            file1 = Path(tmpdir) / "file1.csv"
            file1.write_text("data1")

            result = api.upload_dataset("Test Dataset", as_files=[str(file1)])

            assert result == mock_dataset
            api.create_dataset.assert_called_once()
            call_args = api.create_dataset.call_args
            assert call_args[0][0] == "org-123"
            assert call_args[0][1] == "ws-456"

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_dataset_with_db_files(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a dataset with database files."""
        mock_config = MagicMock()
        mock_config.cosmotech.organization_id = "org-123"
        mock_config.cosmotech.workspace_id = "ws-456"

        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock created dataset
        mock_dataset = MagicMock(spec=Dataset)
        mock_dataset.id = "new-dataset-123"

        api = DatasetApi(configuration=mock_config)
        api.create_dataset = MagicMock(return_value=mock_dataset)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = Path(tmpdir) / "data.db"
            db_file.write_text("database content")

            result = api.upload_dataset("Test Dataset", as_db=[str(db_file)])

            assert result == mock_dataset
            api.create_dataset.assert_called_once()

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_dataset_mixed_files(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a dataset with both file and database types."""
        mock_config = MagicMock()
        mock_config.cosmotech.organization_id = "org-123"
        mock_config.cosmotech.workspace_id = "ws-456"

        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock created dataset
        mock_dataset = MagicMock(spec=Dataset)
        mock_dataset.id = "new-dataset-123"

        api = DatasetApi(configuration=mock_config)
        api.create_dataset = MagicMock(return_value=mock_dataset)

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_file = Path(tmpdir) / "data.csv"
            csv_file.write_text("csv content")
            db_file = Path(tmpdir) / "data.db"
            db_file.write_text("database content")

            result = api.upload_dataset("Test Dataset", as_files=[str(csv_file)], as_db=[str(db_file)])

            assert result == mock_dataset
            api.create_dataset.assert_called_once()

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_upload_dataset_empty(self, mock_cosmotech_config, mock_api_client):
        """Test uploading a dataset with no files."""
        mock_config = MagicMock()
        mock_config.cosmotech.organization_id = "org-123"
        mock_config.cosmotech.workspace_id = "ws-456"

        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock created dataset
        mock_dataset = MagicMock(spec=Dataset)
        mock_dataset.id = "new-dataset-123"

        api = DatasetApi(configuration=mock_config)
        api.create_dataset = MagicMock(return_value=mock_dataset)

        result = api.upload_dataset("Empty Dataset")

        assert result == mock_dataset
        api.create_dataset.assert_called_once()
        call_args = api.create_dataset.call_args
        # Verify the request has an empty parts list
        assert len(call_args[0][2].parts) == 0
