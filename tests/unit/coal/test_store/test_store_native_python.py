# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

import pyarrow as pa

from cosmotech.coal.store.native_python import store_pylist, convert_table_as_pylist
from cosmotech.coal.store.store import Store


class TestNativePythonFunctions:
    """Tests for top-level functions in the native_python module."""

    def test_store_pylist(self):
        """Test the store_pylist function."""
        # Arrange
        table_name = "test_table"
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        with patch("cosmotech.coal.store.native_python.pa") as mock_pa:
            mock_table = MagicMock()
            mock_pa.Table.from_pylist.return_value = mock_table

            store_pylist(table_name, data, False, mock_store)

            # Assert
            mock_pa.Table.from_pylist.assert_called_once_with(data)
            mock_store.add_table.assert_called_once()
            # Check that the table name and replace flag are passed correctly
            args, kwargs = mock_store.add_table.call_args
            assert kwargs["table_name"] == table_name
            assert kwargs["data"] == mock_table
            assert kwargs["replace"] is False

    def test_store_pylist_with_replace(self):
        """Test the store_pylist function with replace_existing_file=True."""
        # Arrange
        table_name = "test_table"
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        with patch("cosmotech.coal.store.native_python.pa") as mock_pa:
            mock_table = MagicMock()
            mock_pa.Table.from_pylist.return_value = mock_table

            store_pylist(table_name, data, True, mock_store)

            # Assert
            mock_pa.Table.from_pylist.assert_called_once_with(data)
            mock_store.add_table.assert_called_once()
            # Check that the table name and replace flag are passed correctly
            args, kwargs = mock_store.add_table.call_args
            assert kwargs["table_name"] == table_name
            assert kwargs["data"] == mock_table
            assert kwargs["replace"] is True

    def test_convert_table_as_pylist(self):
        """Test the convert_table_as_pylist function."""
        # Arrange
        table_name = "test_table"
        expected_result = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]

        # Create a mock table with a to_pylist method
        mock_table = MagicMock()
        mock_table.to_pylist.return_value = expected_result

        # Mock the store and its get_table method
        mock_store = MagicMock(spec=Store)
        mock_store.get_table.return_value = mock_table

        # Act
        result = convert_table_as_pylist(table_name, mock_store)

        # Assert
        mock_store.get_table.assert_called_once_with(table_name)
        mock_table.to_pylist.assert_called_once()
        assert result == expected_result

    def test_convert_table_as_pylist_empty_table(self):
        """Test the convert_table_as_pylist function with an empty table."""
        # Arrange
        table_name = "empty_table"
        expected_result = []

        # Create a mock empty table with a to_pylist method
        mock_table = MagicMock()
        mock_table.to_pylist.return_value = expected_result

        # Mock the store and its get_table method
        mock_store = MagicMock(spec=Store)
        mock_store.get_table.return_value = mock_table

        # Act
        result = convert_table_as_pylist(table_name, mock_store)

        # Assert
        mock_store.get_table.assert_called_once_with(table_name)
        mock_table.to_pylist.assert_called_once()
        assert result == expected_result
