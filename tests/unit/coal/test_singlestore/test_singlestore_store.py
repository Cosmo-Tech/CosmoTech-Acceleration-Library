# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, mock_open, patch

import pytest
import singlestoredb as s2

from cosmotech.coal.singlestore.store import _get_data, load_from_singlestore


class TestStoreFunctions:
    """Tests for top-level functions in the store module."""

    @patch("cosmotech.coal.singlestore.store.Store")
    @patch("cosmotech.coal.singlestore.store.store_csv_file")
    @patch("cosmotech.coal.singlestore.store.s2.connect")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    @patch("os.path.exists")
    def test_load_from_singlestore(
        self, mock_os_exists, mock_glob, mock_exists, mock_mkdir, mock_connect, mock_store_csv_file, mock_store
    ):
        """Test the load_from_singlestore function."""
        # Arrange
        single_store_host = "localhost"
        single_store_port = 3306
        single_store_db = "test_db"
        single_store_user = "user"
        single_store_password = "password"
        store_folder = "/tmp/store"
        single_store_tables = "table1,table2"

        # Mock Path.exists to return False so that mkdir is called
        mock_exists.return_value = False

        # Mock os.path.exists to return True for the CSV files
        mock_os_exists.return_value = True

        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Mock the cursor.fetchall to return table names
        mock_cursor.fetchall.return_value = [{"TABLE_NAME": "table1"}, {"TABLE_NAME": "table2"}]

        # Mock Path.glob to return paths to CSV files
        mock_csv_path1 = MagicMock()
        mock_csv_path1.name = "table1.csv"
        mock_csv_path2 = MagicMock()
        mock_csv_path2.name = "table2.csv"
        mock_glob.return_value = [mock_csv_path1, mock_csv_path2]

        # Mock Store instance
        mock_store_instance = MagicMock()
        mock_store.return_value = mock_store_instance

        # Act
        with patch("builtins.open", mock_open()) as mock_file:
            load_from_singlestore(
                single_store_host=single_store_host,
                single_store_port=single_store_port,
                single_store_db=single_store_db,
                single_store_user=single_store_user,
                single_store_password=single_store_password,
                store_folder=store_folder,
                single_store_tables=single_store_tables,
            )

        # Assert
        # Verify that the directory was created
        mock_exists.assert_called_once_with("/tmp/store/singlestore")
        mock_mkdir.assert_called_once()

        # Verify that the connection was established with the correct parameters
        mock_connect.assert_called_once_with(
            host=single_store_host,
            port=single_store_port,
            database=single_store_db,
            user=single_store_user,
            password=single_store_password,
            results_type="dicts",
        )

        # Verify that _get_data was called for each table
        assert mock_cursor.execute.call_count >= 2  # At least one call per table

        # Verify that store_csv_file was called for each CSV file
        assert mock_store_csv_file.call_count == 2
        mock_store_csv_file.assert_any_call("table1", mock_csv_path1, store=mock_store_instance)
        mock_store_csv_file.assert_any_call("table2", mock_csv_path2, store=mock_store_instance)

        # Verify that Store was initialized correctly
        mock_store.assert_called_with(False, store_folder)

    def test_get_data(self):
        """Test the _get_data function."""
        # Arrange
        table_name = "test_table"
        output_directory = "/tmp/output"
        cursor = MagicMock()

        # Mock data returned from the database
        mock_rows = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        cursor.fetchall.return_value = mock_rows

        # Act
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("csv.DictWriter") as mock_dict_writer:
                # Mock the DictWriter
                mock_writer = MagicMock()
                mock_dict_writer.return_value = mock_writer

                # Ensure the directory exists
                with patch("os.path.exists") as mock_exists:
                    mock_exists.return_value = True

                    _get_data(table_name, output_directory, cursor)

        # Assert
        # Verify that the SQL query was executed
        cursor.execute.assert_called_once_with("SELECT * FROM test_table")

        # Verify that fetchall was called
        cursor.fetchall.assert_called_once()

    @patch("cosmotech.coal.singlestore.store.Store")
    @patch("cosmotech.coal.singlestore.store.store_csv_file")
    @patch("cosmotech.coal.singlestore.store.s2.connect")
    @patch("pathlib.Path.mkdir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    @patch("os.path.exists")
    def test_load_from_singlestore_no_tables_specified(
        self, mock_os_exists, mock_glob, mock_exists, mock_mkdir, mock_connect, mock_store_csv_file, mock_store
    ):
        """Test the load_from_singlestore function when no tables are specified."""
        # Arrange
        single_store_host = "localhost"
        single_store_port = 3306
        single_store_db = "test_db"
        single_store_user = "user"
        single_store_password = "password"
        store_folder = "/tmp/store"

        # Mock Path.exists to return False so that mkdir is called
        mock_exists.return_value = False

        # Mock os.path.exists to return True for the CSV files
        mock_os_exists.return_value = True

        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Mock the cursor.fetchall to return table names for SHOW TABLES
        mock_cursor.fetchall.return_value = [
            {"TABLE_NAME": "table1"},
            {"TABLE_NAME": "table2"},
            {"TABLE_NAME": "table3"},
        ]

        # Mock Path.glob to return paths to CSV files
        mock_csv_paths = [MagicMock() for _ in range(3)]
        for i, path in enumerate(mock_csv_paths):
            path.name = f"table{i+1}.csv"
        mock_glob.return_value = mock_csv_paths

        # Mock Store instance
        mock_store_instance = MagicMock()
        mock_store.return_value = mock_store_instance

        # Act
        with patch("builtins.open", mock_open()) as mock_file:
            load_from_singlestore(
                single_store_host=single_store_host,
                single_store_port=single_store_port,
                single_store_db=single_store_db,
                single_store_user=single_store_user,
                single_store_password=single_store_password,
                store_folder=store_folder,
            )

        # Assert
        # Verify that the cursor was used to execute a query
        assert mock_cursor.execute.called

        # Verify that store_csv_file was called for each CSV file
        assert mock_store_csv_file.call_count == 3
