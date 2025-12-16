# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from unittest.mock import MagicMock, patch

import adbc_driver_manager
import pyarrow as pa
import pytest

from cosmotech.coal.postgresql.utils import (
    PostgresUtils,
    adapt_table_to_schema,
)
from cosmotech.coal.utils.configuration import Configuration


@pytest.fixture
def base_configuration():
    _c = Configuration(
        {
            "postgres": {
                "host": "localhost",
                "port": "5432",
                "db_name": "testdb",
                "db_schema": "dbschema",
                "user_name": "user",
                "user_password": "password",
                "password_encoding": False,
            }
        }
    )
    return _c


@pytest.fixture
def base_configuration_encode(base_configuration):
    _c = base_configuration
    _c.postgres.user_password = "pass@word!"
    _c.postgres.password_encoding = True
    return _c


class TestPostgresqlFunctions:
    """Tests for top-level functions in the postgresql module."""

    def test_generate_postgresql_full_uri(self, base_configuration):
        """Test the generate_postgresql_full_uri function."""
        # Arrange
        _psql = PostgresUtils(base_configuration)

        # Act
        result = _psql.full_uri

        # Assert
        assert result == "postgresql://user:password@localhost:5432/testdb"

    def test_generate_postgresql_full_uri_with_special_chars(self, base_configuration_encode):
        """Test the generate_postgresql_full_uri function with special characters in password."""
        # Arrange
        _psql = PostgresUtils(base_configuration_encode)

        # Act
        result = _psql.full_uri

        # Assert
        assert result == "postgresql://user:pass%40word%21@localhost:5432/testdb"

    def test_generate_postgresql_full_uri_with_special_chars_no_encode(self, base_configuration_encode):
        """Test the generate_postgresql_full_uri function with special characters in password."""
        # Arrange
        base_configuration_encode.postgres.password_encoding = False
        _psql = PostgresUtils(base_configuration_encode)

        # Act
        result = _psql.full_uri
        # Assert
        assert result == "postgresql://user:pass@word!@localhost:5432/testdb"

    @patch("adbc_driver_postgresql.dbapi.connect")
    def test_get_postgresql_table_schema_found(self, mock_connect, base_configuration):
        """Test the get_postgresql_table_schema function when table is found."""
        # Arrange
        target_table_name = "test_table"
        _psql = PostgresUtils(base_configuration)

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Mock get_table_schema result
        expected_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        mock_conn.adbc_get_table_schema.return_value = expected_schema

        # Act
        result = _psql.get_postgresql_table_schema(target_table_name)

        # Assert
        assert result == expected_schema
        mock_conn.adbc_get_table_schema.assert_called_once_with(
            target_table_name,
            db_schema_filter=_psql.db_schema,
        )

    @patch("adbc_driver_postgresql.dbapi.connect")
    def test_get_postgresql_table_schema_not_found(self, mock_connect, base_configuration):
        """Test the get_postgresql_table_schema function when table is not found."""
        # Arrange
        target_table_name = "test_table"
        _psql = PostgresUtils(base_configuration)
        print(base_configuration.postgres)

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        mock_conn.adbc_get_table_schema.side_effect = adbc_driver_manager.ProgrammingError(
            status_code=adbc_driver_manager.AdbcStatusCode.UNKNOWN, message="Table not found"
        )

        # Act
        result = _psql.get_postgresql_table_schema(
            target_table_name,
        )

        # Assert
        assert result is None
        mock_conn.adbc_get_table_schema.assert_called_once_with(
            target_table_name,
            db_schema_filter=_psql.db_schema,
        )

    def test_adapt_table_to_schema_same_schema(self):
        """Test the adapt_table_to_schema function with same schema."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        # Act
        result = adapt_table_to_schema(data, schema)

        # Assert
        assert result.schema == schema
        assert result.column_names == ["id", "name"]
        assert result.num_rows == 3
        assert result.column(0).equals(data.column(0))
        assert result.column(1).equals(data.column(1))

    def test_adapt_table_to_schema_missing_columns(self):
        """Test the adapt_table_to_schema function with missing columns."""
        # Arrange
        original_schema = pa.schema([pa.field("id", pa.int64())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3])], schema=original_schema)

        target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        # Act
        result = adapt_table_to_schema(data, target_schema)

        # Assert
        assert result.schema == target_schema
        assert result.column_names == ["id", "name"]
        assert result.num_rows == 3
        assert result.column(0).equals(data.column(0))
        # Check that name column is all nulls
        assert result.column(1).null_count == 3

    def test_adapt_table_to_schema_extra_columns(self):
        """Test the adapt_table_to_schema function with extra columns."""
        # Arrange
        original_schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("extra", pa.float64())]
        )
        data = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"]), pa.array([1.1, 2.2, 3.3])], schema=original_schema
        )

        target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

        # Act
        result = adapt_table_to_schema(data, target_schema)

        # Assert
        assert result.schema == target_schema
        assert result.column_names == ["id", "name"]
        assert result.num_rows == 3
        assert result.column(0).equals(data.column(0))
        assert result.column(1).equals(data.column(1))

    def test_adapt_table_to_schema_type_conversion(self):
        """Test the adapt_table_to_schema function with type conversion."""
        # Arrange
        original_schema = pa.schema([pa.field("id", pa.int32()), pa.field("value", pa.float32())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array([1.1, 2.2, 3.3])], schema=original_schema)

        target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.float64())])

        # Act
        result = adapt_table_to_schema(data, target_schema)

        # Assert
        assert result.schema == target_schema
        assert result.column_names == ["id", "value"]
        assert result.num_rows == 3
        assert result.column(0).type == pa.int64()
        assert result.column(1).type == pa.float64()

    @patch("pyarrow.compute.cast")
    def test_adapt_table_to_schema_failed_conversion(self, mock_cast):
        """Test the adapt_table_to_schema function with failed conversion."""
        # Arrange
        original_schema = pa.schema([pa.field("id", pa.int64()), pa.field("text", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=original_schema)

        target_schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("text", pa.float64())]  # String to float conversion will fail
        )

        # Mock cast to raise ArrowInvalid
        mock_cast.side_effect = pa.ArrowInvalid("Cannot cast string to float")

        # Act
        result = adapt_table_to_schema(data, target_schema)

        # Assert
        assert result.schema == target_schema
        assert result.column_names == ["id", "text"]
        assert result.num_rows == 3
        assert result.column(0).equals(data.column(0))
        # Check that text column is all nulls due to failed conversion
        assert result.column(1).null_count == 3

    @patch("adbc_driver_postgresql.dbapi.connect")
    def test_send_pyarrow_table_to_postgresql_new_table(self, mock_connect, base_configuration):
        """Test the send_pyarrow_table_to_postgresql function with a new table."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        _psql = PostgresUtils(base_configuration)

        target_table_name = "test_table"
        replace = False

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock get_table_schema result to return None (table doesn't exist)
        mock_conn.adbc_get_table_schema.return_value = None

        # Mock adbc_ingest to return row count
        mock_cursor.adbc_ingest.return_value = 3

        # Act
        result = _psql.send_pyarrow_table_to_postgresql(data, target_table_name, replace)

        # Assert
        assert result == 3
        mock_conn.adbc_get_table_schema.assert_called_once_with(
            target_table_name,
            db_schema_filter=_psql.db_schema,
        )
        mock_cursor.adbc_ingest.assert_called_once_with(
            target_table_name, data, "create_append", db_schema_name=_psql.db_schema
        )

    @patch("adbc_driver_postgresql.dbapi.connect")
    @patch("cosmotech.coal.postgresql.utils.adapt_table_to_schema")
    def test_send_pyarrow_table_to_postgresql_existing_table_append(
        self, mock_adapt_schema, mock_connect, base_configuration
    ):
        """Test the send_pyarrow_table_to_postgresql function with an existing table in append mode."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        _psql = PostgresUtils(base_configuration)

        target_table_name = "test_table"
        replace = False

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock get_postgresql_table_schema to return a schema (table exists)
        existing_schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("extra", pa.float64())]
        )
        mock_conn.adbc_get_table_schema.return_value = existing_schema

        # Mock adapt_table_to_schema to return adapted data
        adapted_data = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"]), pa.array([None, None, None])], schema=existing_schema
        )
        mock_adapt_schema.return_value = adapted_data

        # Mock adbc_ingest to return row count
        mock_cursor.adbc_ingest.return_value = 3

        # Act
        result = _psql.send_pyarrow_table_to_postgresql(
            data,
            target_table_name,
            replace,
        )

        # Assert
        assert result == 3
        mock_conn.adbc_get_table_schema.assert_called_once_with(
            target_table_name,
            db_schema_filter=_psql.db_schema,
        )
        mock_adapt_schema.assert_called_once_with(data, existing_schema)
        mock_cursor.adbc_ingest.assert_called_once_with(
            target_table_name, adapted_data, "create_append", db_schema_name=_psql.db_schema
        )

    @patch("adbc_driver_postgresql.dbapi.connect")
    def test_send_pyarrow_table_to_postgresql_existing_table_replace(self, mock_connect, base_configuration):
        """Test the send_pyarrow_table_to_postgresql function with an existing table in replace mode."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        _psql = PostgresUtils(base_configuration)

        target_table_name = "test_table"
        replace = True

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock get_postgresql_table_schema to return a schema (table exists)
        existing_schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("extra", pa.float64())]
        )
        mock_conn.adbc_get_table_schema.return_value = existing_schema

        # Mock adbc_ingest to return row count
        mock_cursor.adbc_ingest.return_value = 3

        # Act
        result = _psql.send_pyarrow_table_to_postgresql(
            data,
            target_table_name,
            replace,
        )

        # Assert
        assert result == 3
        mock_conn.adbc_get_table_schema.assert_called_once_with(target_table_name, db_schema_filter=_psql.db_schema)
        mock_cursor.adbc_ingest.assert_called_once_with(
            target_table_name, data, "replace", db_schema_name=_psql.db_schema
        )
