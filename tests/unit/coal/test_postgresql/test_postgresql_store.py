# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
import pyarrow as pa
from unittest.mock import MagicMock, patch, call

from cosmotech.coal.postgresql.store import dump_store_to_postgresql


class TestStoreFunctions:
    """Tests for top-level functions in the store module."""

    @patch("cosmotech.coal.postgresql.store.Store")
    @patch("cosmotech.coal.postgresql.store.send_pyarrow_table_to_postgresql")
    def test_dump_store_to_postgresql_with_tables(self, mock_send_to_postgresql, mock_store_class):
        """Test the dump_store_to_postgresql function with tables in the store."""
        # Arrange
        mock_store_instance = MagicMock()
        mock_store_class.return_value = mock_store_instance

        # Mock store tables
        table_names = ["table1", "table2"]
        mock_store_instance.list_tables.return_value = table_names

        # Create mock PyArrow tables
        table1_data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        table2_data = pa.Table.from_arrays([pa.array([4, 5]), pa.array(["d", "e"])], names=["id", "value"])

        # Configure mock store to return tables
        mock_store_instance.get_table.side_effect = lambda name: {"table1": table1_data, "table2": table2_data}[name]

        # Mock send_pyarrow_table_to_postgresql to return row counts
        mock_send_to_postgresql.side_effect = [3, 2]  # 3 rows for table1, 2 rows for table2

        # PostgreSQL connection parameters
        store_folder = "/path/to/store"
        postgres_host = "localhost"
        postgres_port = 5432
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"
        table_prefix = "Test_"
        replace = True

        # Act
        dump_store_to_postgresql(
            store_folder,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            table_prefix,
            replace,
        )

        # Assert
        # Check that Store was initialized with the correct parameters
        mock_store_class.assert_called_once_with(store_location=store_folder)

        # Check that list_tables was called
        mock_store_instance.list_tables.assert_called_once()

        # Check that get_table was called for each table
        assert mock_store_instance.get_table.call_count == 2
        mock_store_instance.get_table.assert_has_calls([call("table1"), call("table2")])

        # Check that send_pyarrow_table_to_postgresql was called for each table with correct parameters
        assert mock_send_to_postgresql.call_count == 2
        mock_send_to_postgresql.assert_has_calls(
            [
                call(
                    table1_data,
                    f"{table_prefix}table1",
                    postgres_host,
                    postgres_port,
                    postgres_db,
                    postgres_schema,
                    postgres_user,
                    postgres_password,
                    replace,
                    False,
                ),
                call(
                    table2_data,
                    f"{table_prefix}table2",
                    postgres_host,
                    postgres_port,
                    postgres_db,
                    postgres_schema,
                    postgres_user,
                    postgres_password,
                    replace,
                    False,
                ),
            ]
        )

    @patch("cosmotech.coal.postgresql.store.Store")
    @patch("cosmotech.coal.postgresql.store.send_pyarrow_table_to_postgresql")
    def test_dump_store_to_postgresql_empty_store(self, mock_send_to_postgresql, mock_store_class):
        """Test the dump_store_to_postgresql function with an empty store."""
        # Arrange
        mock_store_instance = MagicMock()
        mock_store_class.return_value = mock_store_instance

        # Mock empty store (no tables)
        mock_store_instance.list_tables.return_value = []

        # PostgreSQL connection parameters
        store_folder = "/path/to/store"
        postgres_host = "localhost"
        postgres_port = 5432
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"

        # Act
        dump_store_to_postgresql(
            store_folder, postgres_host, postgres_port, postgres_db, postgres_schema, postgres_user, postgres_password
        )

        # Assert
        # Check that Store was initialized with the correct parameters
        mock_store_class.assert_called_once_with(store_location=store_folder)

        # Check that list_tables was called
        mock_store_instance.list_tables.assert_called_once()

        # Check that get_table was not called (no tables)
        mock_store_instance.get_table.assert_not_called()

        # Check that send_pyarrow_table_to_postgresql was not called (no tables)
        mock_send_to_postgresql.assert_not_called()

    @patch("cosmotech.coal.postgresql.store.Store")
    @patch("cosmotech.coal.postgresql.store.send_pyarrow_table_to_postgresql")
    def test_dump_store_to_postgresql_empty_table(self, mock_send_to_postgresql, mock_store_class):
        """Test the dump_store_to_postgresql function with a table that has no rows."""
        # Arrange
        mock_store_instance = MagicMock()
        mock_store_class.return_value = mock_store_instance

        # Mock store with one empty table
        table_names = ["empty_table"]
        mock_store_instance.list_tables.return_value = table_names

        # Create empty PyArrow table
        empty_table = pa.Table.from_arrays([], names=[])
        mock_store_instance.get_table.return_value = empty_table

        # PostgreSQL connection parameters
        store_folder = "/path/to/store"
        postgres_host = "localhost"
        postgres_port = 5432
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"
        table_prefix = "Test_"

        # Act
        dump_store_to_postgresql(
            store_folder,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            table_prefix,
        )

        # Assert
        # Check that Store was initialized with the correct parameters
        mock_store_class.assert_called_once_with(store_location=store_folder)

        # Check that list_tables was called
        mock_store_instance.list_tables.assert_called_once()

        # Check that get_table was called
        mock_store_instance.get_table.assert_called_once_with("empty_table")

        # Check that send_pyarrow_table_to_postgresql was not called (empty table)
        mock_send_to_postgresql.assert_not_called()

    @patch("cosmotech.coal.postgresql.store.Store")
    @patch("cosmotech.coal.postgresql.store.send_pyarrow_table_to_postgresql")
    def test_dump_store_to_postgresql_default_parameters(self, mock_send_to_postgresql, mock_store_class):
        """Test the dump_store_to_postgresql function with default parameters."""
        # Arrange
        mock_store_instance = MagicMock()
        mock_store_class.return_value = mock_store_instance

        # Mock store tables
        table_names = ["table1"]
        mock_store_instance.list_tables.return_value = table_names

        # Create mock PyArrow table
        table_data = pa.Table.from_arrays([pa.array([1, 2, 3])], names=["id"])
        mock_store_instance.get_table.return_value = table_data

        # Mock send_pyarrow_table_to_postgresql to return row count
        mock_send_to_postgresql.return_value = 3

        # PostgreSQL connection parameters (minimal required)
        store_folder = "/path/to/store"
        postgres_host = "localhost"
        postgres_port = 5432
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"

        # Act
        dump_store_to_postgresql(
            store_folder, postgres_host, postgres_port, postgres_db, postgres_schema, postgres_user, postgres_password
        )

        # Assert
        # Check that send_pyarrow_table_to_postgresql was called with default parameters
        mock_send_to_postgresql.assert_called_once_with(
            table_data,
            "Cosmotech_table1",  # Default table_prefix is "Cosmotech_"
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            True,  # Default replace is True
            False,
        )
