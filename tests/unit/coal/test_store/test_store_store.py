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
from adbc_driver_sqlite import dbapi

from cosmotech.coal.store.store import Store
from cosmotech.coal.utils import configuration


class TestStore:
    """Tests for the Store class."""

    def test_sanitize_column(self):
        """Test the sanitize_column method."""
        # Arrange
        column_name = "column with spaces"
        expected_result = "column_with_spaces"

        # Act
        result = Store.sanitize_column(column_name)

        # Assert
        assert result == expected_result

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.unlink")
    def test_reset(self, mock_unlink, mock_exists):
        """Test the reset method."""
        # Arrange
        mock_exists.return_value = True
        store = Store()

        # Act
        store.reset()

        # Assert
        mock_exists.assert_called_once()
        mock_unlink.assert_called_once()

    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.unlink")
    def test_reset_file_not_exists(self, mock_unlink, mock_exists):
        """Test the reset method when the database file doesn't exist."""
        # Arrange
        mock_exists.return_value = False
        store = Store()

        # Act
        store.reset()

        # Assert
        mock_exists.assert_called_once()
        mock_unlink.assert_not_called()

    @patch.object(Store, "table_exists")
    @patch.object(Store, "execute_query")
    def test_get_table(self, mock_execute_query, mock_table_exists):
        """Test the get_table method."""
        # Arrange
        table_name = "test_table"
        mock_table_exists.return_value = True
        expected_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])
        mock_execute_query.return_value = expected_table
        store = Store()

        # Act
        result = store.get_table(table_name)

        # Assert
        mock_table_exists.assert_called_once_with(table_name)
        mock_execute_query.assert_called_once_with(f"select * from {table_name}")
        assert result == expected_table

    @patch.object(Store, "table_exists")
    def test_get_table_not_exists(self, mock_table_exists):
        """Test the get_table method when the table doesn't exist."""
        # Arrange
        table_name = "nonexistent_table"
        mock_table_exists.return_value = False
        store = Store()

        # Act & Assert
        with pytest.raises(ValueError):
            store.get_table(table_name)

        mock_table_exists.assert_called_once_with(table_name)

    @patch.object(Store, "list_tables")
    def test_table_exists_true(self, mock_list_tables):
        """Test the table_exists method when the table exists."""
        # Arrange
        table_name = "existing_table"
        mock_list_tables.return_value = ["existing_table", "another_table"]
        store = Store()

        # Act
        result = store.table_exists(table_name)

        # Assert
        assert result is True
        mock_list_tables.assert_called_once()

    @patch.object(Store, "list_tables")
    def test_table_exists_false(self, mock_list_tables):
        """Test the table_exists method when the table doesn't exist."""
        # Arrange
        table_name = "nonexistent_table"
        mock_list_tables.return_value = ["existing_table", "another_table"]
        store = Store()

        # Act
        result = store.table_exists(table_name)

        # Assert
        assert result is False
        mock_list_tables.assert_called_once()

    @patch.object(Store, "table_exists")
    @patch("adbc_driver_sqlite.dbapi.connect")
    def test_get_table_schema(self, mock_connect, mock_table_exists):
        """Test the get_table_schema method."""
        # Arrange
        table_name = "test_table"
        mock_table_exists.return_value = True

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Mock schema
        expected_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        mock_conn.adbc_get_table_schema.return_value = expected_schema

        store = Store()

        # Act
        result = store.get_table_schema(table_name)

        # Assert
        mock_table_exists.assert_called_once_with(table_name)
        mock_conn.adbc_get_table_schema.assert_called_once_with(table_name)
        assert result == expected_schema

    @patch.object(Store, "table_exists")
    def test_get_table_schema_not_exists(self, mock_table_exists):
        """Test the get_table_schema method when the table doesn't exist."""
        # Arrange
        table_name = "nonexistent_table"
        mock_table_exists.return_value = False
        store = Store()

        # Act & Assert
        with pytest.raises(ValueError):
            store.get_table_schema(table_name)

        mock_table_exists.assert_called_once_with(table_name)

    @patch("adbc_driver_sqlite.dbapi.connect")
    def test_add_table(self, mock_connect):
        """Test the add_table method."""
        # Arrange
        table_name = "test_table"
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.adbc_ingest.return_value = 3  # 3 rows inserted

        store = Store()

        # Act
        store.add_table(table_name, data, False)

        # Assert
        mock_connect.assert_called_once()
        mock_cursor.adbc_ingest.assert_called_once_with(table_name, data, "create_append")

    @patch("adbc_driver_sqlite.dbapi.connect")
    def test_add_table_with_replace(self, mock_connect):
        """Test the add_table method with replace=True."""
        # Arrange
        table_name = "test_table"
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.adbc_ingest.return_value = 3  # 3 rows inserted

        store = Store()

        # Act
        store.add_table(table_name, data, True)

        # Assert
        mock_connect.assert_called_once()
        mock_cursor.adbc_ingest.assert_called_once_with(table_name, data, "replace")

    @patch("adbc_driver_sqlite.dbapi.connect")
    def test_list_tables(self, mock_connect):
        """Test the list_tables method."""
        # Arrange
        # Mock connection
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Mock objects result
        mock_objects = MagicMock()
        mock_conn.adbc_get_objects.return_value = mock_objects

        # Create mock tables data
        # Instead of using actual StringScalar, we'll use MagicMock
        table1 = MagicMock()
        table2 = MagicMock()
        table1.as_py.return_value = "table1"
        table2.as_py.return_value = "table2"

        # Create a structure similar to what adbc_get_objects returns
        tables_data = [{"table_name": table1}, {"table_name": table2}]

        # Mock the read_all method to return a structure with tables
        mock_objects.read_all.return_value = {"catalog_db_schemas": [[{"db_schema_tables": tables_data}]]}

        store = Store()

        # Act
        result = list(store.list_tables())

        # Assert
        mock_connect.assert_called_once()
        mock_conn.adbc_get_objects.assert_called_once_with(depth="all")
        assert result == ["table1", "table2"]

    @patch("adbc_driver_sqlite.dbapi.connect")
    def test_execute_query(self, mock_connect):
        """Test the execute_query method."""
        # Arrange
        sql_query = "SELECT * FROM test_table"
        expected_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock cursor methods
        mock_cursor.execute.return_value = None
        mock_cursor.fetch_arrow_table.return_value = expected_table

        store = Store()

        # Act
        result = store.execute_query(sql_query)

        # Assert
        mock_connect.assert_called_once()
        mock_cursor.adbc_statement.set_options.assert_called_once_with(**{"adbc.sqlite.query.batch_rows": "1024"})
        mock_cursor.execute.assert_called_once_with(sql_query)
        mock_cursor.fetch_arrow_table.assert_called_once()
        assert result == expected_table

    @patch("adbc_driver_sqlite.dbapi.connect")
    def test_execute_query_with_oserror(self, mock_connect):
        """Test the execute_query method with OSError handling."""
        # Arrange
        sql_query = "SELECT * FROM test_table"
        expected_table = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], names=["id", "name"])

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Set up to raise OSError on first attempt, then succeed on second attempt
        mock_cursor.adbc_statement.set_options.side_effect = [
            OSError("Batch size too large"),  # First call raises OSError
            None,  # Second call succeeds
        ]
        mock_cursor.execute.return_value = None
        mock_cursor.fetch_arrow_table.return_value = expected_table

        store = Store()

        # Act
        result = store.execute_query(sql_query)

        # Assert
        assert mock_connect.call_count == 2
        assert mock_cursor.adbc_statement.set_options.call_count == 2
        # First call with batch_size = 1024, second with batch_size = 2048
        mock_cursor.adbc_statement.set_options.assert_any_call(**{"adbc.sqlite.query.batch_rows": "1024"})
        mock_cursor.adbc_statement.set_options.assert_any_call(**{"adbc.sqlite.query.batch_rows": "2048"})
        mock_cursor.execute.assert_called_once_with(sql_query)
        mock_cursor.fetch_arrow_table.assert_called_once()
        assert result == expected_table

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    def test_init_default_parameters(self, mock_exists, mock_mkdir):
        """Test the __init__ method with default parameters."""
        # Arrange
        mock_exists.return_value = False

        # Act
        store = Store()

        # Assert
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        assert store._database_path.name == "db.sqlite"
        assert store._database == str(store._database_path)
        assert not store._tables  # Should be an empty dict

    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.unlink")
    def test_init_with_reset(self, mock_unlink, mock_exists, mock_mkdir):
        """Test the __init__ method with reset=True."""
        # Arrange
        mock_exists.return_value = True

        # Act
        store = Store(reset=True)

        # Assert
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_exists.assert_called_once()
        mock_unlink.assert_called_once()
        assert store._database_path.name == "db.sqlite"
        assert store._database == str(store._database_path)

    @patch("pathlib.Path.mkdir")
    def test_init_with_custom_location(self, mock_mkdir):
        """Test the __init__ method with a custom store_location."""
        # Arrange
        _c = configuration.Configuration()
        custom_location = "/custom/path"
        _c.coal.store = custom_location

        # Act
        store = Store(configuration=_c)

        # Assert
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        assert store.store_location == pathlib.Path(custom_location) / ".coal/store"
        assert store._database_path == pathlib.Path(custom_location) / ".coal/store" / "db.sqlite"
        assert store._database == str(store._database_path)
