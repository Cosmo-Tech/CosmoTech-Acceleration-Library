# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

import pyarrow as pa

from cosmotech.coal.store.pyarrow import store_table, convert_store_table_to_dataframe
from cosmotech.coal.store.store import Store


class TestPyarrowFunctions:
    """Tests for top-level functions in the pyarrow module."""

    def test_store_table(self):
        """Test the store_table function."""
        # Arrange
        table_name = "test_table"

        # Create a test PyArrow Table
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["Alice", "Bob", "Charlie"])], names=["id", "name"])

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        store_table(table_name, data, False, mock_store)

        # Assert
        mock_store.add_table.assert_called_once()
        # Check that the table name and replace flag are passed correctly
        args, kwargs = mock_store.add_table.call_args
        assert kwargs["table_name"] == table_name
        assert kwargs["data"] == data
        assert kwargs["replace"] is False

    def test_store_table_with_replace(self):
        """Test the store_table function with replace_existing_file=True."""
        # Arrange
        table_name = "test_table"

        # Create a test PyArrow Table
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["Alice", "Bob", "Charlie"])], names=["id", "name"])

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        store_table(table_name, data, True, mock_store)

        # Assert
        mock_store.add_table.assert_called_once()
        # Check that the table name and replace flag are passed correctly
        args, kwargs = mock_store.add_table.call_args
        assert kwargs["table_name"] == table_name
        assert kwargs["data"] == data
        assert kwargs["replace"] is True

    def test_convert_store_table_to_dataframe(self):
        """Test the convert_store_table_to_dataframe function."""
        # Arrange
        table_name = "test_table"
        expected_table = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["Alice", "Bob", "Charlie"])], names=["id", "name"]
        )

        # Mock the store and its get_table method
        mock_store = MagicMock(spec=Store)
        mock_store.get_table.return_value = expected_table

        # Act
        result = convert_store_table_to_dataframe(table_name, mock_store)

        # Assert
        mock_store.get_table.assert_called_once_with(table_name)
        assert result == expected_table

    def test_convert_store_table_to_dataframe_empty_table(self):
        """Test the convert_store_table_to_dataframe function with an empty table."""
        # Arrange
        table_name = "empty_table"
        expected_table = pa.Table.from_arrays([], names=[])

        # Mock the store and its get_table method
        mock_store = MagicMock(spec=Store)
        mock_store.get_table.return_value = expected_table

        # Act
        result = convert_store_table_to_dataframe(table_name, mock_store)

        # Assert
        mock_store.get_table.assert_called_once_with(table_name)
        assert result == expected_table

    def test_convert_store_table_to_dataframe_with_custom_store(self):
        """Test the convert_store_table_to_dataframe function with a custom store."""
        # Arrange
        table_name = "test_table"
        expected_table = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["Alice", "Bob", "Charlie"])], names=["id", "name"]
        )

        # Create a custom store with a specific location
        custom_store = MagicMock(spec=Store)

        # Mock the get_table method to return our expected table
        custom_store.get_table.return_value = expected_table

        # Act
        result = convert_store_table_to_dataframe(table_name, custom_store)

        # Assert
        custom_store.get_table.assert_called_once_with(table_name)
        assert result == expected_table
