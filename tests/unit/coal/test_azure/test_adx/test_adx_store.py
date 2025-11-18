# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
from unittest.mock import ANY, MagicMock, patch

import pyarrow as pa
import pytest
from azure.kusto.data import KustoClient
from azure.kusto.ingest import IngestionResult, QueuedIngestClient

from cosmotech.coal.azure.adx.store import (
    process_tables,
    send_pyarrow_table_to_adx,
    send_store_to_adx,
    send_table_data,
)
from cosmotech.coal.store.store import Store


class TestAdxStoreFunctions:
    """Tests for ADX store module functions."""

    @pytest.fixture
    def mock_ingest_client(self):
        """Create a mock QueuedIngestClient."""
        return MagicMock(spec=QueuedIngestClient)

    @pytest.fixture
    def mock_kusto_client(self):
        """Create a mock KustoClient."""
        return MagicMock(spec=KustoClient)

    @pytest.fixture
    def sample_table(self):
        """Create a sample PyArrow table for testing."""
        return pa.table({"id": ["1", "2", "3"], "name": ["Alice", "Bob", "Charlie"], "value": [1.5, 2.5, 3.5]})

    def test_send_table_data(self, mock_ingest_client, sample_table):
        """Test send_table_data function."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        operation_tag = "test-tag"

        mock_result = MagicMock(spec=IngestionResult)
        mock_result.source_id = "test-source-id-123"

        with patch("cosmotech.coal.azure.adx.store.send_pyarrow_table_to_adx") as mock_send:
            mock_send.return_value = mock_result

            # Act
            source_id, returned_table_name = send_table_data(
                mock_ingest_client, database, table_name, sample_table, operation_tag
            )

            # Assert
            assert source_id == "test-source-id-123"
            assert returned_table_name == table_name
            mock_send.assert_called_once_with(mock_ingest_client, database, table_name, sample_table, operation_tag)

    def test_send_pyarrow_table_to_adx(self, mock_ingest_client, sample_table):
        """Test send_pyarrow_table_to_adx function."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        drop_by_tag = "test-tag"

        mock_result = MagicMock(spec=IngestionResult)
        mock_ingest_client.ingest_from_file.return_value = mock_result

        # Act
        result = send_pyarrow_table_to_adx(mock_ingest_client, database, table_name, sample_table, drop_by_tag)

        # Assert
        assert result == mock_result
        mock_ingest_client.ingest_from_file.assert_called_once()

        # Check that the ingestion properties were set correctly
        call_args = mock_ingest_client.ingest_from_file.call_args
        properties = call_args[0][1]
        assert properties.database == database
        assert properties.table == table_name
        assert drop_by_tag in properties.drop_by_tags

    def test_send_pyarrow_table_to_adx_no_tag(self, mock_ingest_client, sample_table):
        """Test send_pyarrow_table_to_adx without drop_by_tag."""
        # Arrange
        database = "test-database"
        table_name = "test-table"

        mock_result = MagicMock(spec=IngestionResult)
        mock_ingest_client.ingest_from_file.return_value = mock_result

        # Act
        result = send_pyarrow_table_to_adx(mock_ingest_client, database, table_name, sample_table, drop_by_tag=None)

        # Assert
        assert result == mock_result
        mock_ingest_client.ingest_from_file.assert_called_once()

        # Check that drop_by_tags is None
        call_args = mock_ingest_client.ingest_from_file.call_args
        properties = call_args[0][1]
        assert properties.drop_by_tags is None

    def test_send_pyarrow_table_to_adx_cleans_temp_file(self, mock_ingest_client, sample_table):
        """Test that send_pyarrow_table_to_adx cleans up temporary files."""
        # Arrange
        database = "test-database"
        table_name = "test-table"

        mock_result = MagicMock(spec=IngestionResult)
        mock_ingest_client.ingest_from_file.return_value = mock_result

        created_files = []

        def track_file(file_path, properties):
            created_files.append(file_path)
            return mock_result

        mock_ingest_client.ingest_from_file.side_effect = track_file

        # Act
        send_pyarrow_table_to_adx(mock_ingest_client, database, table_name, sample_table)

        # Assert
        assert len(created_files) == 1
        # File should be cleaned up
        assert not os.path.exists(created_files[0])

    @patch("cosmotech.coal.azure.adx.store.Store")
    def test_process_tables(self, mock_store_class, mock_kusto_client, mock_ingest_client, sample_table):
        """Test process_tables function."""
        # Arrange
        mock_store = MagicMock(spec=Store)
        mock_store_class.return_value = mock_store
        mock_store.list_tables.return_value = ["table1", "table2"]
        mock_store.get_table.return_value = sample_table

        database = "test-database"
        operation_tag = "test-tag"

        mock_result = MagicMock(spec=IngestionResult)
        mock_result.source_id = "source-id-1"

        with (
            patch("cosmotech.coal.azure.adx.store.check_and_create_table"),
            patch("cosmotech.coal.azure.adx.store.send_table_data") as mock_send,
        ):
            mock_send.return_value = ("source-id-1", "table1")

            # Act
            source_ids, mapping = process_tables(
                mock_store, mock_kusto_client, mock_ingest_client, database, operation_tag
            )

            # Assert
            assert len(source_ids) == 2
            assert "source-id-1" in source_ids
            assert "source-id-1" in mapping

    @patch("cosmotech.coal.azure.adx.store.Store")
    def test_process_tables_empty_table(self, mock_store_class, mock_kusto_client, mock_ingest_client):
        """Test process_tables skips empty tables."""
        # Arrange
        mock_store = MagicMock(spec=Store)
        mock_store_class.return_value = mock_store
        mock_store.list_tables.return_value = ["empty_table"]

        # Create an empty table
        empty_table = pa.table({"id": []})
        mock_store.get_table.return_value = empty_table

        database = "test-database"
        operation_tag = "test-tag"

        with patch("cosmotech.coal.azure.adx.store.send_table_data") as mock_send:
            # Act
            source_ids, mapping = process_tables(
                mock_store, mock_kusto_client, mock_ingest_client, database, operation_tag
            )

            # Assert
            assert len(source_ids) == 0
            assert len(mapping) == 0
            mock_send.assert_not_called()

    @patch("cosmotech.coal.azure.adx.store.initialize_clients")
    @patch("cosmotech.coal.azure.adx.store.Store")
    @patch("cosmotech.coal.azure.adx.store.process_tables")
    @patch("cosmotech.coal.azure.adx.store.monitor_ingestion")
    @patch("cosmotech.coal.azure.adx.store.handle_failures")
    def test_send_store_to_adx_success(
        self, mock_handle_failures, mock_monitor, mock_process, mock_store_class, mock_init_clients
    ):
        """Test send_store_to_adx successful execution."""
        # Arrange
        mock_kusto = MagicMock(spec=KustoClient)
        mock_ingest = MagicMock(spec=QueuedIngestClient)
        mock_init_clients.return_value = (mock_kusto, mock_ingest)

        mock_store = MagicMock(spec=Store)
        mock_store_class.return_value = mock_store

        mock_process.return_value = (["source-id-1"], {"source-id-1": "table1"})
        mock_monitor.return_value = False  # No failures
        mock_handle_failures.return_value = False  # Should not abort

        # Act
        result = send_store_to_adx(
            adx_uri="https://adx.example.com",
            adx_ingest_uri="https://ingest.adx.example.com",
            database_name="test-db",
            wait=True,
            tag="custom-tag",
        )

        # Assert
        assert result is True
        mock_init_clients.assert_called_once()
        mock_process.assert_called_once()
        mock_monitor.assert_called_once()
        mock_handle_failures.assert_called_once()

    @patch("cosmotech.coal.azure.adx.store.initialize_clients")
    @patch("cosmotech.coal.azure.adx.store.Store")
    @patch("cosmotech.coal.azure.adx.store.process_tables")
    @patch("cosmotech.coal.azure.adx.store.handle_failures")
    def test_send_store_to_adx_no_wait(self, mock_handle_failures, mock_process, mock_store_class, mock_init_clients):
        """Test send_store_to_adx without waiting for ingestion."""
        # Arrange
        mock_kusto = MagicMock(spec=KustoClient)
        mock_ingest = MagicMock(spec=QueuedIngestClient)
        mock_init_clients.return_value = (mock_kusto, mock_ingest)

        mock_store = MagicMock(spec=Store)
        mock_store_class.return_value = mock_store

        mock_process.return_value = (["source-id-1"], {"source-id-1": "table1"})
        mock_handle_failures.return_value = False

        # Act
        result = send_store_to_adx(
            adx_uri="https://adx.example.com",
            adx_ingest_uri="https://ingest.adx.example.com",
            database_name="test-db",
            wait=False,
        )

        # Assert
        assert result is True
        mock_handle_failures.assert_called_once_with(mock_kusto, "test-db", ANY, False)

    @patch("cosmotech.coal.azure.adx.store.initialize_clients")
    @patch("cosmotech.coal.azure.adx.store.Store")
    @patch("cosmotech.coal.azure.adx.store.process_tables")
    @patch("cosmotech.coal.azure.adx.store.handle_failures")
    def test_send_store_to_adx_with_failures(
        self, mock_handle_failures, mock_process, mock_store_class, mock_init_clients
    ):
        """Test send_store_to_adx when failures occur and should abort."""
        # Arrange
        mock_kusto = MagicMock(spec=KustoClient)
        mock_ingest = MagicMock(spec=QueuedIngestClient)
        mock_init_clients.return_value = (mock_kusto, mock_ingest)

        mock_store = MagicMock(spec=Store)
        mock_store_class.return_value = mock_store

        mock_process.return_value = (["source-id-1"], {"source-id-1": "table1"})
        mock_handle_failures.return_value = True  # Should abort

        # Act
        result = send_store_to_adx(
            adx_uri="https://adx.example.com", adx_ingest_uri="https://ingest.adx.example.com", database_name="test-db"
        )

        # Assert
        assert result is False

    @patch("cosmotech.coal.azure.adx.store.initialize_clients")
    @patch("cosmotech.coal.azure.adx.store.Store")
    @patch("cosmotech.coal.azure.adx.store.process_tables")
    @patch("cosmotech.coal.azure.adx.store._drop_by_tag")
    def test_send_store_to_adx_exception_rollback(self, mock_drop, mock_process, mock_store_class, mock_init_clients):
        """Test send_store_to_adx performs rollback on exception."""
        # Arrange
        mock_kusto = MagicMock(spec=KustoClient)
        mock_ingest = MagicMock(spec=QueuedIngestClient)
        mock_init_clients.return_value = (mock_kusto, mock_ingest)

        mock_store = MagicMock(spec=Store)
        mock_store_class.return_value = mock_store

        mock_process.side_effect = Exception("Test exception")

        # Act & Assert
        with pytest.raises(Exception, match="Test exception"):
            send_store_to_adx(
                adx_uri="https://adx.example.com",
                adx_ingest_uri="https://ingest.adx.example.com",
                database_name="test-db",
                tag="rollback-tag",
            )

        # Verify rollback was called
        mock_drop.assert_called_once_with(mock_kusto, "test-db", "rollback-tag")
