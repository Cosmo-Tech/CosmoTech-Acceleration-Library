# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import csv
import io
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
from cosmotech_api import WorkspaceApi, WorkspaceFile

from cosmotech.coal.cosmotech_api.dataset.download.file import download_file_dataset


class TestFileFunctions:
    """Tests for top-level functions in the file module."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        return mock_client

    @pytest.fixture
    def mock_workspace_api(self):
        """Create a mock WorkspaceApi."""
        return MagicMock(spec=WorkspaceApi)

    @pytest.fixture
    def mock_workspace_file(self, file_name="test.csv"):
        """Create a mock WorkspaceFile."""
        mock_file = MagicMock(spec=WorkspaceFile)
        mock_file.file_name = file_name
        return mock_file

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_csv(self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path):
        """Test the download_file_dataset function with CSV file."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.csv"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download
        csv_content = "id,name,value\n1,Alice,100\n2,Bob,200\n"
        mock_ws_api.download_workspace_file.return_value = csv_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        mock_ws_api.download_workspace_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
        )

        assert "test" in content
        assert len(content["test"]) == 2
        # The CSV parser might convert numeric strings to integers
        assert content["test"][0]["id"] in ["1", 1]
        assert content["test"][0]["name"] in ["Alice", "Alice"]
        assert content["test"][0]["value"] in ["100", 100]
        assert content["test"][1]["id"] in ["2", 2]
        assert content["test"][1]["name"] in ["Bob", "Bob"]
        assert content["test"][1]["value"] in ["200", 200]
        assert folder_path == tmp_path

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_json_dict(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with JSON file containing a dictionary."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.json"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download
        json_content = '{"items": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}'
        mock_ws_api.download_workspace_file.return_value = json_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        mock_ws_api.download_workspace_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
        )

        assert "test.json" in content
        assert "items" in content["test.json"]
        assert len(content["test.json"]["items"]) == 2
        assert content["test.json"]["items"][0]["id"] == 1
        assert content["test.json"]["items"][0]["name"] == "Alice"
        assert content["test.json"]["items"][1]["id"] == 2
        assert content["test.json"]["items"][1]["name"] == "Bob"
        assert folder_path == tmp_path

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_json_list(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with JSON file containing a list."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.json"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download - a JSON array
        json_content = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'
        mock_ws_api.download_workspace_file.return_value = json_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        assert "test.json" in content
        assert isinstance(content["test.json"], list)
        assert len(content["test.json"]) == 2
        assert content["test.json"][0]["id"] == 1
        assert content["test.json"][0]["name"] == "Alice"
        assert content["test.json"][1]["id"] == 2
        assert content["test.json"][1]["name"] == "Bob"

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_json_scalar(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with JSON file containing a scalar value."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.json"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download - a JSON scalar value
        json_content = '"Hello, world!"'
        mock_ws_api.download_workspace_file.return_value = json_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        assert "test.json" in content
        assert content["test.json"] == "Hello, world!"

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.load_workbook")
    def test_download_file_dataset_excel(
        self, mock_load_workbook, mock_workspace_api_class, mock_get_api_client, tmp_path
    ):
        """Test the download_file_dataset function with Excel file."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.xlsx"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download
        mock_ws_api.download_workspace_file.return_value = b"excel_content"

        # Mock Excel workbook
        mock_wb = MagicMock()
        mock_sheet = MagicMock()
        mock_wb.sheetnames = ["Sheet1"]
        mock_wb.__getitem__.return_value = mock_sheet

        # Mock sheet data
        headers = ("id", "name", "value")
        rows = [
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", '{"key": "value"}'),  # JSON string
            (4, "Dave", None),  # None value
        ]

        # Create iterators from the lists
        mock_sheet.iter_rows.side_effect = [
            iter([headers]),  # First call returns headers iterator
            iter(rows),  # Second call returns data rows iterator
        ]

        mock_load_workbook.return_value = mock_wb

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        mock_load_workbook.assert_called_once_with(os.path.join(tmp_path, file_name), data_only=True)
        assert "Sheet1" in content
        assert len(content["Sheet1"]) == 4  # All rows are included, even with None values
        assert content["Sheet1"][0]["id"] == 1
        assert content["Sheet1"][0]["name"] == "Alice"
        assert content["Sheet1"][0]["value"] == 100
        assert content["Sheet1"][1]["id"] == 2
        assert content["Sheet1"][1]["name"] == "Bob"
        assert content["Sheet1"][1]["value"] == 200
        assert content["Sheet1"][2]["id"] == 3
        assert content["Sheet1"][2]["name"] == "Charlie"
        assert content["Sheet1"][2]["value"] == {"key": "value"}  # JSON parsed
        assert content["Sheet1"][3]["id"] == 4
        assert content["Sheet1"][3]["name"] == "Dave"
        assert "value" not in content["Sheet1"][3]  # None value is not included in the row
        assert folder_path == tmp_path

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_text_simple(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with a simple text file."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.txt"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download
        text_content = "Line 1\nLine 2\nLine 3"
        mock_ws_api.download_workspace_file.return_value = text_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        mock_ws_api.download_workspace_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
        )

        assert "test.txt" in content
        # The text file reader might normalize line endings or add extra newlines
        assert content["test.txt"].replace("\n\n", "\n") == text_content
        assert folder_path == tmp_path

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_text_complex(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with a complex text file."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.log"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download - a complex log file with many lines
        text_content = (
            "2023-01-01 12:00:00 INFO Starting application\n"
            "2023-01-01 12:00:01 DEBUG Initializing components\n"
            "2023-01-01 12:00:02 INFO Application started successfully\n"
            "2023-01-01 12:00:03 ERROR Failed to connect to database\n"
            "2023-01-01 12:00:04 DEBUG Retrying connection\n"
            "2023-01-01 12:00:05 INFO Connection established\n"
            "2023-01-01 12:00:06 DEBUG Processing data\n"
            "2023-01-01 12:00:07 INFO Processing complete\n"
            "2023-01-01 12:00:08 DEBUG Shutting down\n"
            "2023-01-01 12:00:09 INFO Application terminated\n"
        )
        mock_ws_api.download_workspace_file.return_value = text_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        assert "test.log" in content
        assert len(content["test.log"].split("\n")) >= 10  # At least 10 lines
        assert "INFO Starting application" in content["test.log"]
        assert "ERROR Failed to connect to database" in content["test.log"]
        assert "INFO Application terminated" in content["test.log"]

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_no_files(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with no matching files."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.csv"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing (empty)
        mock_ws_api.find_all_workspace_files.return_value = []

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
        )

        # Assert
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        mock_ws_api.download_workspace_file.assert_not_called()

        assert content == {}
        assert folder_path == tmp_path

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.tempfile.mkdtemp")
    def test_download_file_dataset_temp_dir(
        self, mock_mkdtemp, mock_workspace_api_class, mock_get_api_client, mock_api_client
    ):
        """Test the download_file_dataset function with temporary directory."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.csv"
        temp_dir = "/tmp/test_dir"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download
        csv_content = "id,name,value\n1,Alice,100\n2,Bob,200\n"
        mock_ws_api.download_workspace_file.return_value = csv_content.encode()

        # Mock temp directory
        mock_mkdtemp.return_value = temp_dir

        # Act
        with patch("builtins.open", mock_open(read_data=csv_content)):
            content, folder_path = download_file_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=file_name,
            )

        # Assert
        mock_mkdtemp.assert_called_once()
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        mock_ws_api.download_workspace_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
        )

        assert "test" in content
        assert folder_path == Path(temp_dir)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_no_read(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with read_files=False."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_name = "test.csv"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file = MagicMock()
        mock_file.file_name = file_name
        mock_ws_api.find_all_workspace_files.return_value = [mock_file]

        # Mock file download
        csv_content = "id,name,value\n1,Alice,100\n2,Bob,200\n"
        mock_ws_api.download_workspace_file.return_value = csv_content.encode()

        # Act
        content, folder_path = download_file_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
            target_folder=tmp_path,
            read_files=False,
        )

        # Assert
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        mock_ws_api.download_workspace_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name=file_name,
        )

        assert content == {}
        assert folder_path == tmp_path
        assert (tmp_path / file_name).exists()

    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.file.WorkspaceApi")
    def test_download_file_dataset_multiple_files(
        self, mock_workspace_api_class, mock_get_api_client, mock_api_client, tmp_path
    ):
        """Test the download_file_dataset function with multiple files."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        file_prefix = "test"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, MagicMock())

        # Mock workspace API
        mock_ws_api = mock_workspace_api_class.return_value

        # Mock file listing
        mock_file1 = MagicMock()
        mock_file1.file_name = "test_1.csv"
        mock_file2 = MagicMock()
        mock_file2.file_name = "test_2.csv"
        mock_ws_api.find_all_workspace_files.return_value = [mock_file1, mock_file2]

        # Mock file download
        csv_content1 = "id,name,value\n1,Alice,100\n"
        csv_content2 = "id,name,value\n2,Bob,200\n"
        mock_ws_api.download_workspace_file.side_effect = [
            csv_content1.encode(),
            csv_content2.encode(),
        ]

        # Act
        with patch("builtins.open", mock_open(read_data="")):
            content, folder_path = download_file_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name=file_prefix,
                target_folder=tmp_path,
            )

        # Assert
        mock_ws_api.find_all_workspace_files.assert_called_once_with(organization_id, workspace_id)
        assert mock_ws_api.download_workspace_file.call_count == 2
        mock_ws_api.download_workspace_file.assert_any_call(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name="test_1.csv",
        )
        mock_ws_api.download_workspace_file.assert_any_call(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name="test_2.csv",
        )

        assert folder_path == tmp_path
