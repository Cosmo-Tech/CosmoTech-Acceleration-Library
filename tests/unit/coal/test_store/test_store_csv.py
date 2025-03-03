# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
import pytest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.csv as pc

from cosmotech.coal.store.csv import store_csv_file, convert_store_table_to_csv
from cosmotech.coal.store.store import Store


class TestCsvFunctions:
    """Tests for top-level functions in the csv module."""

    @patch("pyarrow.csv.read_csv")
    @patch("pathlib.Path.exists")
    def test_store_csv_file_success(self, mock_exists, mock_read_csv):
        """Test the store_csv_file function with a valid CSV file."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/test.csv")
        mock_exists.return_value = True

        # Mock CSV data
        mock_data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_read_csv.return_value = mock_data

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        store_csv_file(table_name, csv_path, False, mock_store)

        # Assert
        mock_exists.assert_called_once_with()
        mock_read_csv.assert_called_once_with(csv_path)
        mock_store.add_table.assert_called_once()
        # Check that the table name and replace flag are passed correctly
        args, kwargs = mock_store.add_table.call_args
        assert kwargs["table_name"] == table_name
        assert kwargs["replace"] is False

    @patch("pathlib.Path.exists")
    def test_store_csv_file_file_not_found(self, mock_exists):
        """Test the store_csv_file function with a non-existent CSV file."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/nonexistent.csv")
        mock_exists.return_value = False

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            store_csv_file(table_name, csv_path, False, mock_store)

        mock_exists.assert_called_once_with()

    @patch("pyarrow.csv.read_csv")
    @patch("pathlib.Path.exists")
    def test_store_csv_file_with_column_sanitization(self, mock_exists, mock_read_csv):
        """Test the store_csv_file function with column sanitization."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/test.csv")
        mock_exists.return_value = True

        # Mock CSV data with columns that need sanitization
        mock_data = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id with space", "name-with-dash"]
        )
        mock_read_csv.return_value = mock_data

        # Mock store and sanitize_column
        mock_store = MagicMock(spec=Store)
        Store.sanitize_column = MagicMock(side_effect=lambda x: x.replace(" ", "_").replace("-", "_"))

        # Act
        store_csv_file(table_name, csv_path, False, mock_store)

        # Assert
        mock_exists.assert_called_once_with()
        mock_read_csv.assert_called_once_with(csv_path)

        # Check that sanitize_column was called for each column
        assert Store.sanitize_column.call_count == 2
        Store.sanitize_column.assert_any_call("id with space")
        Store.sanitize_column.assert_any_call("name-with-dash")

        # Check that add_table was called with the sanitized data
        mock_store.add_table.assert_called_once()

    @patch("pyarrow.csv.write_csv")
    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_csv_success(self, mock_exists, mock_write_csv):
        """Test the convert_store_table_to_csv function with a valid table."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/output.csv")
        mock_exists.return_value = False

        # Mock store and table data
        mock_store = MagicMock(spec=Store)
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_store.get_table.return_value = mock_table

        # Mock mkdir
        with patch.object(pathlib.Path, "mkdir") as mock_mkdir:
            # Act
            convert_store_table_to_csv(table_name, csv_path, False, mock_store)

            # Assert
            mock_store.get_table.assert_called_once_with(table_name)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_write_csv.assert_called_once_with(mock_table, csv_path)

    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_csv_file_exists(self, mock_exists):
        """Test the convert_store_table_to_csv function when the output file already exists."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/output.csv")
        mock_exists.return_value = True

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act & Assert
        with pytest.raises(FileExistsError):
            convert_store_table_to_csv(table_name, csv_path, False, mock_store)

        mock_exists.assert_called_once_with()
        mock_store.get_table.assert_not_called()

    @patch("pyarrow.csv.write_csv")
    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_csv_replace_existing(self, mock_exists, mock_write_csv):
        """Test the convert_store_table_to_csv function with replace_existing_file=True."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/output.csv")
        mock_exists.return_value = True

        # Mock store and table data
        mock_store = MagicMock(spec=Store)
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_store.get_table.return_value = mock_table

        # Mock mkdir
        with patch.object(pathlib.Path, "mkdir") as mock_mkdir:
            # Act
            convert_store_table_to_csv(table_name, csv_path, True, mock_store)

            # Assert
            mock_store.get_table.assert_called_once_with(table_name)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_write_csv.assert_called_once_with(mock_table, csv_path)

    @patch("pyarrow.csv.write_csv")
    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_csv_directory_path(self, mock_exists, mock_write_csv):
        """Test the convert_store_table_to_csv function with a directory path."""
        # Arrange
        table_name = "test_table"
        csv_path = pathlib.Path("/path/to/directory")  # Not ending with .csv
        mock_exists.return_value = False

        # Mock store and table data
        mock_store = MagicMock(spec=Store)
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_store.get_table.return_value = mock_table

        # Mock mkdir
        with patch.object(pathlib.Path, "mkdir") as mock_mkdir:
            # Act
            convert_store_table_to_csv(table_name, csv_path, False, mock_store)

            # Assert
            mock_store.get_table.assert_called_once_with(table_name)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            # Check that the path was modified to include the table name
            expected_path = csv_path / f"{table_name}.csv"
            mock_write_csv.assert_called_once_with(mock_table, expected_path)
