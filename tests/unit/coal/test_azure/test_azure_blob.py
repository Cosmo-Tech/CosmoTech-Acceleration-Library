# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import io
import pytest
from unittest.mock import MagicMock, patch, mock_open

import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.parquet as pq
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from cosmotech.coal.azure.blob import dump_store_to_azure, VALID_TYPES
from cosmotech.coal.store.store import Store


class TestBlobFunctions:
    """Tests for top-level functions in the blob module."""

    def test_dump_store_to_azure_invalid_output_type(self):
        """Test the dump_store_to_azure function with an invalid output type."""
        # Arrange
        store_folder = "/path/to/store"
        account_name = "teststorageaccount"
        container_name = "testcontainer"
        tenant_id = "test-tenant-id"
        client_id = "test-client-id"
        client_secret = "test-client-secret"
        output_type = "invalid_type"  # Not in VALID_TYPES

        # Mock Store
        mock_store = MagicMock(spec=Store)

        with patch("cosmotech.coal.azure.blob.Store", return_value=mock_store):
            # Act & Assert
            with pytest.raises(ValueError, match="Invalid output type"):
                dump_store_to_azure(
                    store_folder=store_folder,
                    account_name=account_name,
                    container_name=container_name,
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                    output_type=output_type,
                )

    def test_dump_store_to_azure_sqlite(self):
        """Test the dump_store_to_azure function with SQLite output type."""
        # Arrange
        store_folder = "/path/to/store"
        account_name = "teststorageaccount"
        container_name = "testcontainer"
        tenant_id = "test-tenant-id"
        client_id = "test-client-id"
        client_secret = "test-client-secret"
        output_type = "sqlite"
        file_prefix = "prefix_"

        # Mock Store
        mock_store = MagicMock(spec=Store)
        mock_store._database_path = "/path/to/store/db.sqlite"

        # Mock BlobServiceClient and ContainerClient
        mock_container_client = MagicMock(spec=ContainerClient)
        mock_blob_service_client = MagicMock(spec=BlobServiceClient)
        mock_blob_service_client.get_container_client.return_value = mock_container_client

        # Mock ClientSecretCredential
        mock_credential = MagicMock(spec=ClientSecretCredential)

        # Mock file open
        mock_file_data = b"sqlite file content"

        with (
            patch("cosmotech.coal.azure.blob.Store", return_value=mock_store),
            patch("cosmotech.coal.azure.blob.BlobServiceClient", return_value=mock_blob_service_client),
            patch("cosmotech.coal.azure.blob.ClientSecretCredential", return_value=mock_credential),
            patch("builtins.open", mock_open(read_data=mock_file_data)),
        ):
            # Act
            dump_store_to_azure(
                store_folder=store_folder,
                account_name=account_name,
                container_name=container_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                output_type=output_type,
                file_prefix=file_prefix,
            )

            # Assert
            mock_blob_service_client.get_container_client.assert_called_once_with(container_name)
            mock_container_client.upload_blob.assert_called_once()

            # Check the call arguments without comparing the exact mock object
            call_args = mock_container_client.upload_blob.call_args
            assert call_args.kwargs["name"] == "prefix_db.sqlite"
            assert call_args.kwargs["overwrite"] is True
            # We don't check the exact data object since it's a mock and the identity might differ

    def test_dump_store_to_azure_csv(self):
        """Test the dump_store_to_azure function with CSV output type."""
        # Arrange
        store_folder = "/path/to/store"
        account_name = "teststorageaccount"
        container_name = "testcontainer"
        tenant_id = "test-tenant-id"
        client_id = "test-client-id"
        client_secret = "test-client-secret"
        output_type = "csv"
        file_prefix = "prefix_"

        # Mock Store
        mock_store = MagicMock(spec=Store)
        mock_store.list_tables.return_value = ["table1", "table2", "empty_table"]

        # Create PyArrow tables for testing
        table1 = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        table2 = pa.table({"col3": [4, 5, 6], "col4": ["d", "e", "f"]})
        empty_table = pa.table({})

        def get_table_side_effect(table_name):
            if table_name == "table1":
                return table1
            elif table_name == "table2":
                return table2
            elif table_name == "empty_table":
                return empty_table

        mock_store.get_table.side_effect = get_table_side_effect

        # Mock BlobServiceClient and ContainerClient
        mock_container_client = MagicMock(spec=ContainerClient)
        mock_blob_service_client = MagicMock(spec=BlobServiceClient)
        mock_blob_service_client.get_container_client.return_value = mock_container_client

        # Mock ClientSecretCredential
        mock_credential = MagicMock(spec=ClientSecretCredential)

        # Mock BytesIO
        mock_bytesio = MagicMock(spec=io.BytesIO)
        mock_bytesio.read.return_value = b"csv data"

        with (
            patch("cosmotech.coal.azure.blob.Store", return_value=mock_store),
            patch("cosmotech.coal.azure.blob.BlobServiceClient", return_value=mock_blob_service_client),
            patch("cosmotech.coal.azure.blob.ClientSecretCredential", return_value=mock_credential),
            patch("cosmotech.coal.azure.blob.BytesIO", return_value=mock_bytesio),
            patch("pyarrow.csv.write_csv") as mock_write_csv,
        ):
            # Act
            dump_store_to_azure(
                store_folder=store_folder,
                account_name=account_name,
                container_name=container_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                output_type=output_type,
                file_prefix=file_prefix,
            )

            # Assert
            mock_blob_service_client.get_container_client.assert_called_once_with(container_name)
            assert mock_container_client.upload_blob.call_count == 2  # Only for non-empty tables
            mock_container_client.upload_blob.assert_any_call(
                name="prefix_table1.csv", data=mock_bytesio, length=len(b"csv data"), overwrite=True
            )
            mock_container_client.upload_blob.assert_any_call(
                name="prefix_table2.csv", data=mock_bytesio, length=len(b"csv data"), overwrite=True
            )
            assert mock_write_csv.call_count == 2
            mock_write_csv.assert_any_call(table1, mock_bytesio)
            mock_write_csv.assert_any_call(table2, mock_bytesio)

    def test_dump_store_to_azure_parquet(self):
        """Test the dump_store_to_azure function with Parquet output type."""
        # Arrange
        store_folder = "/path/to/store"
        account_name = "teststorageaccount"
        container_name = "testcontainer"
        tenant_id = "test-tenant-id"
        client_id = "test-client-id"
        client_secret = "test-client-secret"
        output_type = "parquet"
        file_prefix = "prefix_"

        # Mock Store
        mock_store = MagicMock(spec=Store)
        mock_store.list_tables.return_value = ["table1", "table2", "empty_table"]

        # Create PyArrow tables for testing
        table1 = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        table2 = pa.table({"col3": [4, 5, 6], "col4": ["d", "e", "f"]})
        empty_table = pa.table({})

        def get_table_side_effect(table_name):
            if table_name == "table1":
                return table1
            elif table_name == "table2":
                return table2
            elif table_name == "empty_table":
                return empty_table

        mock_store.get_table.side_effect = get_table_side_effect

        # Mock BlobServiceClient and ContainerClient
        mock_container_client = MagicMock(spec=ContainerClient)
        mock_blob_service_client = MagicMock(spec=BlobServiceClient)
        mock_blob_service_client.get_container_client.return_value = mock_container_client

        # Mock ClientSecretCredential
        mock_credential = MagicMock(spec=ClientSecretCredential)

        # Mock BytesIO
        mock_bytesio = MagicMock(spec=io.BytesIO)
        mock_bytesio.read.return_value = b"parquet data"

        with (
            patch("cosmotech.coal.azure.blob.Store", return_value=mock_store),
            patch("cosmotech.coal.azure.blob.BlobServiceClient", return_value=mock_blob_service_client),
            patch("cosmotech.coal.azure.blob.ClientSecretCredential", return_value=mock_credential),
            patch("cosmotech.coal.azure.blob.BytesIO", return_value=mock_bytesio),
            patch("pyarrow.parquet.write_table") as mock_write_table,
        ):
            # Act
            dump_store_to_azure(
                store_folder=store_folder,
                account_name=account_name,
                container_name=container_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                output_type=output_type,
                file_prefix=file_prefix,
            )

            # Assert
            mock_blob_service_client.get_container_client.assert_called_once_with(container_name)
            assert mock_container_client.upload_blob.call_count == 2  # Only for non-empty tables
            mock_container_client.upload_blob.assert_any_call(
                name="prefix_table1.parquet", data=mock_bytesio, length=len(b"parquet data"), overwrite=True
            )
            mock_container_client.upload_blob.assert_any_call(
                name="prefix_table2.parquet", data=mock_bytesio, length=len(b"parquet data"), overwrite=True
            )
            assert mock_write_table.call_count == 2
            mock_write_table.assert_any_call(table1, mock_bytesio)
            mock_write_table.assert_any_call(table2, mock_bytesio)

    def test_dump_store_to_azure_empty_tables(self):
        """Test the dump_store_to_azure function with empty tables."""
        # Arrange
        store_folder = "/path/to/store"
        account_name = "teststorageaccount"
        container_name = "testcontainer"
        tenant_id = "test-tenant-id"
        client_id = "test-client-id"
        client_secret = "test-client-secret"
        output_type = "csv"

        # Mock Store with only empty tables
        mock_store = MagicMock(spec=Store)
        mock_store.list_tables.return_value = ["empty_table1", "empty_table2"]

        # Create empty PyArrow tables
        empty_table = pa.table({})
        mock_store.get_table.return_value = empty_table

        # Mock BlobServiceClient and ContainerClient
        mock_container_client = MagicMock(spec=ContainerClient)
        mock_blob_service_client = MagicMock(spec=BlobServiceClient)
        mock_blob_service_client.get_container_client.return_value = mock_container_client

        # Mock ClientSecretCredential
        mock_credential = MagicMock(spec=ClientSecretCredential)

        with (
            patch("cosmotech.coal.azure.blob.Store", return_value=mock_store),
            patch("cosmotech.coal.azure.blob.BlobServiceClient", return_value=mock_blob_service_client),
            patch("cosmotech.coal.azure.blob.ClientSecretCredential", return_value=mock_credential),
            patch("cosmotech.coal.azure.blob.BytesIO") as mock_bytesio,
            patch("pyarrow.csv.write_csv") as mock_write_csv,
        ):
            # Act
            dump_store_to_azure(
                store_folder=store_folder,
                account_name=account_name,
                container_name=container_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                output_type=output_type,
            )

            # Assert
            mock_blob_service_client.get_container_client.assert_called_once_with(container_name)
            mock_container_client.upload_blob.assert_not_called()  # No uploads for empty tables
            mock_write_csv.assert_not_called()  # No writes for empty tables
