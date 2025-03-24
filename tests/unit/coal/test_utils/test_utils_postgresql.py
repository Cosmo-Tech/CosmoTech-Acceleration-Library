# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from unittest.mock import MagicMock
from unittest.mock import patch

import adbc_driver_manager
import pyarrow as pa

from cosmotech.coal.utils.postgresql import adapt_table_to_schema
from cosmotech.coal.utils.postgresql import generate_postgresql_full_uri
from cosmotech.coal.utils.postgresql import get_postgresql_table_schema
from cosmotech.coal.utils.postgresql import send_pyarrow_table_to_postgresql


class TestPostgresqlFunctions:
    """Tests for top-level functions in the postgresql module."""

    def test_generate_postgresql_full_uri(self):
        """Test the generate_postgresql_full_uri function."""
        # Arrange
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_user = "user"
        postgres_password = "password"

        # Act
        result = generate_postgresql_full_uri(
            postgres_host, postgres_port, postgres_db, postgres_user, postgres_password
        )

        # Assert
        assert result == "postgresql://user:password@localhost:5432/testdb"

    def test_generate_postgresql_full_uri_with_special_chars(self):
        """Test the generate_postgresql_full_uri function with special characters in password."""
        # Arrange
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_user = "user"
        postgres_password = "pass@word!"
        force_encode = True

        # Act
        result = generate_postgresql_full_uri(
            postgres_host, postgres_port, postgres_db, postgres_user, postgres_password, force_encode
        )

        # Assert
        assert result == "postgresql://user:pass%40word%21@localhost:5432/testdb"

    def test_generate_postgresql_full_uri_with_special_chars_no_encode(self):
        """Test the generate_postgresql_full_uri function with special characters in password."""
        # Arrange
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_user = "user"
        postgres_password = "pass@word!"
        force_encode = False

        # Act
        result = generate_postgresql_full_uri(
            postgres_host, postgres_port, postgres_db, postgres_user, postgres_password, force_encode
        )

        # Assert
        assert result == "postgresql://user:pass@word!@localhost:5432/testdb"

    @patch("adbc_driver_postgresql.dbapi.connect")
    def test_get_postgresql_table_schema_found(self, mock_connect):
        """Test the get_postgresql_table_schema function when table is found."""
        # Arrange
        target_table_name = "test_table"
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        # Mock get_table_schema result
        expected_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        mock_conn.adbc_get_table_schema.return_value = expected_schema

        # Act
        result = get_postgresql_table_schema(
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
        )

        # Assert
        assert result == expected_schema
        mock_conn.adbc_get_table_schema.assert_called_once_with(
            target_table_name,
            db_schema_filter=postgres_schema,
        )

    @patch("adbc_driver_postgresql.dbapi.connect")
    def test_get_postgresql_table_schema_not_found(self, mock_connect):
        """Test the get_postgresql_table_schema function when table is not found."""
        # Arrange
        target_table_name = "test_table"
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn

        mock_conn.adbc_get_table_schema.side_effect = adbc_driver_manager.ProgrammingError(
            status_code=adbc_driver_manager.AdbcStatusCode.UNKNOWN, message="Table not found"
        )

        # Act
        result = get_postgresql_table_schema(
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
        )

        # Assert
        assert result is None
        mock_conn.adbc_get_table_schema.assert_called_once_with(
            target_table_name,
            db_schema_filter=postgres_schema,
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
    @patch("cosmotech.coal.utils.postgresql.get_postgresql_table_schema")
    def test_send_pyarrow_table_to_postgresql_new_table(self, mock_get_schema, mock_connect):
        """Test the send_pyarrow_table_to_postgresql function with a new table."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        target_table_name = "test_table"
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"
        replace = False
        force_encode = True

        # Mock get_postgresql_table_schema to return None (table doesn't exist)
        mock_get_schema.return_value = None

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock adbc_ingest to return row count
        mock_cursor.adbc_ingest.return_value = 3

        # Act
        result = send_pyarrow_table_to_postgresql(
            data,
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            replace,
            force_encode,
        )

        # Assert
        assert result == 3
        mock_get_schema.assert_called_once_with(
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            force_encode,
        )
        mock_cursor.adbc_ingest.assert_called_once_with(
            target_table_name, data, "create_append", db_schema_name=postgres_schema
        )

    @patch("adbc_driver_postgresql.dbapi.connect")
    @patch("cosmotech.coal.utils.postgresql.get_postgresql_table_schema")
    @patch("cosmotech.coal.utils.postgresql.adapt_table_to_schema")
    def test_send_pyarrow_table_to_postgresql_existing_table_append(
        self, mock_adapt_schema, mock_get_schema, mock_connect
    ):
        """Test the send_pyarrow_table_to_postgresql function with an existing table in append mode."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        target_table_name = "test_table"
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"
        replace = False
        force_encode = True

        # Mock get_postgresql_table_schema to return a schema (table exists)
        existing_schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("extra", pa.float64())]
        )
        mock_get_schema.return_value = existing_schema

        # Mock adapt_table_to_schema to return adapted data
        adapted_data = pa.Table.from_arrays(
            [pa.array([1, 2, 3]), pa.array(["a", "b", "c"]), pa.array([None, None, None])], schema=existing_schema
        )
        mock_adapt_schema.return_value = adapted_data

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock adbc_ingest to return row count
        mock_cursor.adbc_ingest.return_value = 3

        # Act
        result = send_pyarrow_table_to_postgresql(
            data,
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            replace,
            force_encode,
        )

        # Assert
        assert result == 3
        mock_get_schema.assert_called_once_with(
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            force_encode,
        )
        mock_adapt_schema.assert_called_once_with(data, existing_schema)
        mock_cursor.adbc_ingest.assert_called_once_with(
            target_table_name, adapted_data, "create_append", db_schema_name=postgres_schema
        )

    @patch("adbc_driver_postgresql.dbapi.connect")
    @patch("cosmotech.coal.utils.postgresql.get_postgresql_table_schema")
    def test_send_pyarrow_table_to_postgresql_existing_table_replace(self, mock_get_schema, mock_connect):
        """Test the send_pyarrow_table_to_postgresql function with an existing table in replace mode."""
        # Arrange
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
        data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)

        target_table_name = "test_table"
        postgres_host = "localhost"
        postgres_port = "5432"
        postgres_db = "testdb"
        postgres_schema = "public"
        postgres_user = "user"
        postgres_password = "password"
        replace = True
        force_encode = True

        # Mock get_postgresql_table_schema to return a schema (table exists)
        existing_schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("name", pa.string()), pa.field("extra", pa.float64())]
        )
        mock_get_schema.return_value = existing_schema

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock adbc_ingest to return row count
        mock_cursor.adbc_ingest.return_value = 3

        # Act
        result = send_pyarrow_table_to_postgresql(
            data,
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            replace,
            force_encode,
        )

        # Assert
        assert result == 3
        mock_get_schema.assert_called_once_with(
            target_table_name,
            postgres_host,
            postgres_port,
            postgres_db,
            postgres_schema,
            postgres_user,
            postgres_password,
            force_encode,
        )
        mock_cursor.adbc_ingest.assert_called_once_with(
            target_table_name, data, "replace", db_schema_name=postgres_schema
        )
