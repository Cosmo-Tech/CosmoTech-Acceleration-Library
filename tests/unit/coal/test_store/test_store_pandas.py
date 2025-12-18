# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

from cosmotech.coal.store.pandas import (
    convert_store_table_to_dataframe,
    store_dataframe,
)
from cosmotech.coal.store.store import Store


class TestPandasFunctions:
    """Tests for top-level functions in the pandas module."""

    def test_store_dataframe(self):
        """Test the store_dataframe function."""
        # Arrange
        table_name = "test_table"

        # Create a test DataFrame
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        with patch("cosmotech.coal.store.pandas.pyarrow") as mock_pyarrow:
            mock_table = MagicMock()
            mock_pyarrow.Table.from_pandas.return_value = mock_table

            store_dataframe(table_name, df, False, mock_store)

            # Assert
            mock_pyarrow.Table.from_pandas.assert_called_once_with(df)
            mock_store.add_table.assert_called_once()
            # Check that the table name and replace flag are passed correctly
            args, kwargs = mock_store.add_table.call_args
            assert kwargs["table_name"] == table_name
            assert kwargs["data"] == mock_table
            assert kwargs["replace"] is False

    def test_store_dataframe_with_replace(self):
        """Test the store_dataframe function with replace_existing_file=True."""
        # Arrange
        table_name = "test_table"

        # Create a test DataFrame
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Mock store
        mock_store = MagicMock(spec=Store)

        # Act
        with patch("cosmotech.coal.store.pandas.pyarrow") as mock_pyarrow:
            mock_table = MagicMock()
            mock_pyarrow.Table.from_pandas.return_value = mock_table

            store_dataframe(table_name, df, True, mock_store)

            # Assert
            mock_pyarrow.Table.from_pandas.assert_called_once_with(df)
            mock_store.add_table.assert_called_once()
            # Check that the table name and replace flag are passed correctly
            args, kwargs = mock_store.add_table.call_args
            assert kwargs["table_name"] == table_name
            assert kwargs["data"] == mock_table
            assert kwargs["replace"] is True

    def test_convert_store_table_to_dataframe(self):
        """Test the convert_store_table_to_dataframe function."""
        # Arrange
        table_name = "test_table"
        expected_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create a mock table with a to_pandas method
        mock_table = MagicMock()
        mock_table.to_pandas.return_value = expected_df

        # Mock the store and its get_table method
        mock_store = MagicMock(spec=Store)
        mock_store.get_table.return_value = mock_table

        # Act
        result = convert_store_table_to_dataframe(table_name, mock_store)

        # Assert
        mock_store.get_table.assert_called_once_with(table_name)
        mock_table.to_pandas.assert_called_once()
        pd.testing.assert_frame_equal(result, expected_df)

    def test_convert_store_table_to_dataframe_empty_table(self):
        """Test the convert_store_table_to_dataframe function with an empty table."""
        # Arrange
        table_name = "empty_table"
        expected_df = pd.DataFrame()

        # Create a mock empty table with a to_pandas method
        mock_table = MagicMock()
        mock_table.to_pandas.return_value = expected_df

        # Mock the store and its get_table method
        mock_store = MagicMock(spec=Store)
        mock_store.get_table.return_value = mock_table

        # Act
        result = convert_store_table_to_dataframe(table_name, mock_store)

        # Assert
        mock_store.get_table.assert_called_once_with(table_name)
        mock_table.to_pandas.assert_called_once()
        pd.testing.assert_frame_equal(result, expected_df)

    def test_convert_store_table_to_dataframe_with_custom_store(self):
        """Test the convert_store_table_to_dataframe function with a custom store."""
        # Arrange
        table_name = "test_table"
        expected_df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Create a custom store with a specific location
        custom_store = MagicMock(spec=Store)

        # Mock the table returned by get_table
        mock_table = MagicMock()
        mock_table.to_pandas.return_value = expected_df
        custom_store.get_table.return_value = mock_table

        # Act
        result = convert_store_table_to_dataframe(table_name, custom_store)

        # Assert
        custom_store.get_table.assert_called_once_with(table_name)
        mock_table.to_pandas.assert_called_once()
        pd.testing.assert_frame_equal(result, expected_df)
