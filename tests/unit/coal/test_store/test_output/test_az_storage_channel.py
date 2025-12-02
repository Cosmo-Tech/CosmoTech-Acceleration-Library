# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import patch

import pytest

from cosmotech.coal.store.output.az_storage_channel import AzureStorageChannel


class TestAzureStorageChannel:
    """Tests for the AzureStorageChannel class."""

    def test_init_with_configuration(self):
        """Test AzureStorageChannel initialization with configuration."""
        # Arrange
        config = {
            "cosmotech": {"dataset_absolute_path": "/path/to/dataset"},
            "azure": {
                "account_name": "test_account",
                "container_name": "test_container",
                "tenant_id": "test_tenant",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "output_type": "csv",
                "file_prefix": "prefix_",
            },
        }

        # Act
        channel = AzureStorageChannel(config)

        # Assert
        assert channel.configuration is not None

    def test_required_keys(self):
        """Test that required_keys are properly defined."""
        # Assert
        assert "cosmotech" in AzureStorageChannel.required_keys
        assert "azure" in AzureStorageChannel.required_keys
        assert "dataset_absolute_path" in AzureStorageChannel.required_keys["cosmotech"]
        assert "account_name" in AzureStorageChannel.required_keys["azure"]
        assert "container_name" in AzureStorageChannel.required_keys["azure"]
        assert "tenant_id" in AzureStorageChannel.required_keys["azure"]
        assert "client_id" in AzureStorageChannel.required_keys["azure"]
        assert "client_secret" in AzureStorageChannel.required_keys["azure"]
        assert "output_type" in AzureStorageChannel.required_keys["azure"]
        assert "file_prefix" in AzureStorageChannel.required_keys["azure"]

    @patch("cosmotech.coal.store.output.az_storage_channel.dump_store_to_azure")
    def test_send_without_filter(self, mock_dump):
        """Test sending data without table filter."""
        # Arrange
        config = {
            "cosmotech": {"dataset_absolute_path": "/path/to/dataset"},
            "azure": {
                "account_name": "test_account",
                "container_name": "test_container",
                "tenant_id": "test_tenant",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "output_type": "csv",
                "file_prefix": "prefix_",
            },
        }

        channel = AzureStorageChannel(config)

        # Act
        channel.send()

        # Assert
        mock_dump.assert_called_once_with(
            store_folder="/path/to/dataset",
            account_name="test_account",
            container_name="test_container",
            tenant_id="test_tenant",
            client_id="test_client",
            client_secret="test_secret",
            output_type="csv",
            file_prefix="prefix_",
            selected_tables=None,
        )

    @patch("cosmotech.coal.store.output.az_storage_channel.dump_store_to_azure")
    def test_send_with_filter(self, mock_dump):
        """Test sending data with table filter."""
        # Arrange
        config = {
            "cosmotech": {"dataset_absolute_path": "/path/to/dataset"},
            "azure": {
                "account_name": "test_account",
                "container_name": "test_container",
                "tenant_id": "test_tenant",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "output_type": "parquet",
                "file_prefix": "data_",
            },
        }

        channel = AzureStorageChannel(config)
        tables_filter = ["table1", "table2"]

        # Act
        channel.send(filter=tables_filter)

        # Assert
        mock_dump.assert_called_once_with(
            store_folder="/path/to/dataset",
            account_name="test_account",
            container_name="test_container",
            tenant_id="test_tenant",
            client_id="test_client",
            client_secret="test_secret",
            output_type="parquet",
            file_prefix="data_",
            selected_tables=["table1", "table2"],
        )

    def test_delete(self):
        """Test delete method (should do nothing)."""
        # Arrange
        config = {
            "cosmotech": {"dataset_absolute_path": "/path/to/dataset"},
            "azure": {
                "account_name": "test_account",
                "container_name": "test_container",
                "tenant_id": "test_tenant",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "output_type": "csv",
                "file_prefix": "prefix_",
            },
        }

        channel = AzureStorageChannel(config)

        # Act
        result = channel.delete()

        # Assert
        # Should not raise any exception and return None
        assert result is None
