# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import csv
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.cosmotech_api.dataset.converters import (
    convert_dataset_to_files,
    convert_graph_dataset_to_files,
    convert_file_dataset_to_files,
)


class TestConvertersFunctions:
    """Tests for top-level functions in the converters module."""

    @patch("cosmotech.coal.cosmotech_api.dataset.converters.convert_graph_dataset_to_files")
    @patch("cosmotech.coal.cosmotech_api.dataset.converters.convert_file_dataset_to_files")
    @patch("tempfile.mkdtemp")
    @patch("pathlib.Path.mkdir")
    def test_convert_dataset_to_files_graph(self, mock_mkdir, mock_mkdtemp, mock_convert_file, mock_convert_graph):
        """Test the convert_dataset_to_files function with graph dataset."""
        # Arrange
        dataset_info = {
            "type": "adt",
            "content": {"nodes": [], "edges": []},
            "name": "test-dataset",
        }
        target_folder = "/tmp/target"

        # Mock convert_graph_dataset_to_files
        mock_convert_graph.return_value = Path("/tmp/target/converted")

        # Act
        result = convert_dataset_to_files(dataset_info, target_folder)

        # Assert
        mock_convert_graph.assert_called_once_with(dataset_info["content"], Path(target_folder))
        mock_convert_file.assert_not_called()
        assert result == Path("/tmp/target/converted")

    @patch("cosmotech.coal.cosmotech_api.dataset.converters.convert_graph_dataset_to_files")
    @patch("cosmotech.coal.cosmotech_api.dataset.converters.convert_file_dataset_to_files")
    @patch("tempfile.mkdtemp")
    @patch("pathlib.Path.mkdir")
    def test_convert_dataset_to_files_file(self, mock_mkdir, mock_mkdtemp, mock_convert_file, mock_convert_graph):
        """Test the convert_dataset_to_files function with file dataset."""
        # Arrange
        dataset_info = {
            "type": "csv",
            "content": {"test.csv": []},
            "name": "test-dataset",
        }
        target_folder = "/tmp/target"

        # Mock convert_file_dataset_to_files
        mock_convert_file.return_value = Path("/tmp/target/converted")

        # Act
        result = convert_dataset_to_files(dataset_info, target_folder)

        # Assert
        mock_convert_file.assert_called_once_with(dataset_info["content"], Path(target_folder), "csv")
        mock_convert_graph.assert_not_called()
        assert result == Path("/tmp/target/converted")

    @patch("cosmotech.coal.cosmotech_api.dataset.converters.convert_graph_dataset_to_files")
    @patch("cosmotech.coal.cosmotech_api.dataset.converters.convert_file_dataset_to_files")
    @patch("tempfile.mkdtemp")
    def test_convert_dataset_to_files_no_target(self, mock_mkdtemp, mock_convert_file, mock_convert_graph):
        """Test the convert_dataset_to_files function with no target folder."""
        # Arrange
        dataset_info = {
            "type": "adt",
            "content": {"nodes": [], "edges": []},
            "name": "test-dataset",
        }

        # Mock tempfile.mkdtemp
        mock_mkdtemp.return_value = "/tmp/temp-dir"

        # Mock convert_graph_dataset_to_files
        mock_convert_graph.return_value = Path("/tmp/temp-dir/converted")

        # Act
        result = convert_dataset_to_files(dataset_info)

        # Assert
        mock_mkdtemp.assert_called_once()
        mock_convert_graph.assert_called_once_with(dataset_info["content"], Path("/tmp/temp-dir"))
        mock_convert_file.assert_not_called()
        assert result == Path("/tmp/temp-dir/converted")

    @patch("csv.DictWriter")
    @patch("tempfile.mkdtemp")
    @patch("pathlib.Path.mkdir")
    @patch("builtins.open")
    @patch("cosmotech.coal.cosmotech_api.dataset.converters.sheet_to_header")
    def test_convert_graph_dataset_to_files(
        self, mock_sheet_to_header, mock_open, mock_mkdir, mock_mkdtemp, mock_dict_writer
    ):
        """Test the convert_graph_dataset_to_files function."""
        # Arrange
        content = {
            "Person": [
                {"id": "1", "name": "Alice", "age": 30},
                {"id": "2", "name": "Bob", "age": 25},
            ],
            "KNOWS": [
                {"src": "1", "dest": "2", "since": "2020"},
            ],
            "Empty": [],  # Empty entity type should be skipped
        }
        target_folder = "/tmp/target"

        # Mock sheet_to_header
        mock_sheet_to_header.side_effect = [
            ["id", "name", "age"],  # Person headers
            ["src", "dest", "since"],  # KNOWS headers
        ]

        # Mock DictWriter
        mock_writer = MagicMock()
        mock_dict_writer.return_value = mock_writer

        # Act
        result = convert_graph_dataset_to_files(content, target_folder)

        # Assert
        assert mock_open.call_count == 2  # Two files: Person.csv and KNOWS.csv
        assert mock_dict_writer.call_count == 2
        assert mock_writer.writeheader.call_count == 2
        assert mock_writer.writerow.call_count == 3  # Two Person rows + one KNOWS row
        assert result == Path(target_folder)

    @patch("tempfile.mkdtemp")
    @patch("pathlib.Path.mkdir")
    @patch("builtins.open")
    @patch("json.dump")
    def test_convert_file_dataset_to_files(self, mock_json_dump, mock_open, mock_mkdir, mock_mkdtemp):
        """Test the convert_file_dataset_to_files function."""
        # Arrange
        content = {
            "test.txt": "This is a text file",
            "test.json": {"key": "value"},
            "test.csv": [{"id": "1", "name": "Alice"}],
            "nested/test.txt": "Nested file",
        }
        target_folder = "/tmp/target"
        file_type = "mixed"

        # Mock file handles
        mock_file_handles = [MagicMock(), MagicMock(), MagicMock(), MagicMock()]
        mock_open.side_effect = mock_file_handles

        # Act
        result = convert_file_dataset_to_files(content, target_folder, file_type)

        # Assert
        assert mock_open.call_count == 4  # Four files
        assert mock_json_dump.call_count == 2  # Two JSON files (test.json and test.csv)
        assert result == Path(target_folder)

    @patch("tempfile.mkdtemp")
    @patch("pathlib.Path.mkdir")
    @patch("builtins.open")
    def test_convert_file_dataset_to_files_no_target(self, mock_open, mock_mkdir, mock_mkdtemp):
        """Test the convert_file_dataset_to_files function with no target folder."""
        # Arrange
        content = {
            "test.txt": "This is a text file",
        }
        file_type = "text"

        # Mock tempfile.mkdtemp
        mock_mkdtemp.return_value = "/tmp/temp-dir"

        # Mock file handles
        mock_file_handle = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file_handle

        # Act
        result = convert_file_dataset_to_files(content)

        # Assert
        mock_mkdtemp.assert_called_once()
        assert mock_open.call_count == 1
        assert result == Path("/tmp/temp-dir")
