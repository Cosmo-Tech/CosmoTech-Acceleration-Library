# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from cosmotech.coal.store.parquet import (
    convert_store_table_to_parquet,
    store_parquet_file,
)
from cosmotech.coal.store.store import Store


class TestParquetFunctions:
    """Tests for top-level functions in the parquet module."""

    @patch("pyarrow.parquet.ParquetFile")
    @patch("pathlib.Path.exists")
    def test_store_parquet_file_success(self, mock_exists, mock_parquet_file):
        """Test the store_parquet_file function with a valid Parquet file."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/test.parquet")
        mock_exists.return_value = True

        # Mock Parquet data
        mock_data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_parquet_file.return_value.read.return_value = mock_data

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        store_parquet_file(table_name, parquet_path, False, mock_store)

        # Assert
        mock_exists.assert_called_once_with()
        mock_parquet_file.assert_called_once_with(parquet_path)
        mock_parquet_file.return_value.read.assert_called_once_with()
        mock_store.add_table.assert_called_once()
        args, kwargs = mock_store.add_table.call_args
        assert kwargs["table_name"] == table_name
        assert kwargs["replace"] is False

    @patch("pathlib.Path.exists")
    def test_store_parquet_file_not_found(self, mock_exists):
        """Test the store_parquet_file function with a non-existent Parquet file."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/nonexistent.parquet")
        mock_exists.return_value = False

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            store_parquet_file(table_name, parquet_path, False, mock_store)

        mock_exists.assert_called_once_with()

    @patch("pyarrow.parquet.ParquetFile")
    @patch("pathlib.Path.exists")
    def test_store_parquet_file_with_column_sanitization(self, mock_exists, mock_parquet_file):
        """Test the store_parquet_file function with column sanitization."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/test.parquet")
        mock_exists.return_value = True

        # Mock Parquet data with columns that need sanitization
        mock_data = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id with space", "name-with-dash"]
        )
        mock_parquet_file.return_value.read.return_value = mock_data

        # Mock store and sanitize_column
        mock_store = MagicMock(spec=Store)
        Store.sanitize_column = MagicMock(side_effect=lambda x: x.replace(" ", "_"))

        # Act
        store_parquet_file(table_name, parquet_path, False, mock_store)

        # Assert
        assert Store.sanitize_column.call_count == 2
        Store.sanitize_column.assert_any_call("id with space")
        Store.sanitize_column.assert_any_call("name-with-dash")
        mock_store.add_table.assert_called_once()

    @patch("pyarrow.parquet.ParquetFile")
    @patch("pathlib.Path.exists")
    def test_store_parquet_file_with_replace(self, mock_exists, mock_parquet_file):
        """Test the store_parquet_file function with replace_existing_file=True."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/test.parquet")
        mock_exists.return_value = True

        mock_data = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["id"])
        mock_parquet_file.return_value.read.return_value = mock_data

        mock_store = MagicMock(spec=Store)

        # Act
        store_parquet_file(table_name, parquet_path, True, mock_store)

        # Assert
        args, kwargs = mock_store.add_table.call_args
        assert kwargs["replace"] is True

    @patch("pyarrow.parquet.write_table")
    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_parquet_success(self, mock_exists, mock_write_table):
        """Test the convert_store_table_to_parquet function with a valid table."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/output.parquet")
        mock_exists.return_value = False

        mock_store = MagicMock(spec=Store)
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_store.get_table.return_value = mock_table

        with patch.object(pathlib.Path, "mkdir") as mock_mkdir:
            # Act
            convert_store_table_to_parquet(table_name, parquet_path, False, mock_store)

            # Assert
            mock_store.get_table.assert_called_once_with(table_name)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_write_table.assert_called_once_with(mock_table, parquet_path)

    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_parquet_file_exists(self, mock_exists):
        """Test the convert_store_table_to_parquet function when the output file already exists."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/output.parquet")
        mock_exists.return_value = True

        mock_store = MagicMock(spec=Store)

        # Act & Assert
        with pytest.raises(FileExistsError):
            convert_store_table_to_parquet(table_name, parquet_path, False, mock_store)

        mock_exists.assert_called_once_with()
        mock_store.get_table.assert_not_called()

    @patch("pyarrow.parquet.write_table")
    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_parquet_replace_existing(self, mock_exists, mock_write_table):
        """Test the convert_store_table_to_parquet function with replace_existing_file=True."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/output.parquet")
        mock_exists.return_value = True

        mock_store = MagicMock(spec=Store)
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_store.get_table.return_value = mock_table

        with patch.object(pathlib.Path, "mkdir") as mock_mkdir:
            # Act
            convert_store_table_to_parquet(table_name, parquet_path, True, mock_store)

            # Assert
            mock_store.get_table.assert_called_once_with(table_name)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_write_table.assert_called_once_with(mock_table, parquet_path)

    @patch("pyarrow.parquet.write_table")
    @patch("pathlib.Path.exists")
    def test_convert_store_table_to_parquet_directory_path(self, mock_exists, mock_write_table):
        """Test the convert_store_table_to_parquet function with a directory path."""
        # Arrange
        table_name = "test_table"
        parquet_path = pathlib.Path("/path/to/directory")  # Not ending with .parquet
        mock_exists.return_value = False

        mock_store = MagicMock(spec=Store)
        mock_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_store.get_table.return_value = mock_table

        with patch.object(pathlib.Path, "mkdir") as mock_mkdir:
            # Act
            convert_store_table_to_parquet(table_name, parquet_path, False, mock_store)

            # Assert
            mock_store.get_table.assert_called_once_with(table_name)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
            expected_path = parquet_path / f"{table_name}.parquet"
            mock_write_table.assert_called_once_with(mock_table, expected_path)
