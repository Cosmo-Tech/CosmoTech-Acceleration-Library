# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import patch

import pytest

from cosmotech.coal.store.output.az_storage_channel import AzureStorageChannel
from cosmotech.coal.utils.configuration import Configuration


@pytest.fixture
def base_azure_storage_config():
    return Configuration(
        {
            "coal": {"store": "$cosmotech.parameters_absolute_path"},
            "cosmotech": {"dataset_absolute_path": "/path/to/dataset", "parameters_absolute_path": "/path/to/params"},
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
    )


class TestAzureStorageChannel:
    """Tests for the AzureStorageChannel class."""

    def test_init_with_configuration(self, base_azure_storage_config):
        """Test AzureStorageChannel initialization with configuration."""
        # Act
        channel = AzureStorageChannel(base_azure_storage_config)

        # Assert
        assert channel.configuration is not None

    def test_required_keys(self):
        """Test that required_keys are properly defined."""
        # Assert
        assert "coal" in AzureStorageChannel.required_keys
        assert "store" in AzureStorageChannel.required_keys["coal"]
        assert "azure" in AzureStorageChannel.required_keys
        assert "account_name" in AzureStorageChannel.required_keys["azure"]
        assert "container_name" in AzureStorageChannel.required_keys["azure"]
        assert "tenant_id" in AzureStorageChannel.required_keys["azure"]
        assert "client_id" in AzureStorageChannel.required_keys["azure"]
        assert "client_secret" in AzureStorageChannel.required_keys["azure"]
        assert "output_type" in AzureStorageChannel.required_keys["azure"]
        assert "file_prefix" in AzureStorageChannel.required_keys["azure"]

    @patch("cosmotech.coal.store.output.az_storage_channel.dump_store_to_azure")
    @patch("cosmotech.coal.azure.blob.Store")
    @patch("cosmotech.coal.azure.blob.ClientSecretCredential")
    def test_send_without_filter(self, mock_client_secret, mock_store, mock_dump, base_azure_storage_config):
        """Test sending data without table filter."""
        channel = AzureStorageChannel(base_azure_storage_config)

        # Act
        channel.send()

        # Assert
        mock_dump.assert_called_once_with(base_azure_storage_config, selected_tables=None)

    @patch("cosmotech.coal.store.output.az_storage_channel.dump_store_to_azure")
    @patch("cosmotech.coal.azure.blob.Store")
    @patch("cosmotech.coal.azure.blob.ClientSecretCredential")
    def test_send_with_filter(self, mock_client_secret, mock_store, mock_dump, base_azure_storage_config):
        """Test sending data with table filter."""

        channel = AzureStorageChannel(base_azure_storage_config)
        tables_filter = ["table1", "table2"]

        # Act
        channel.send(filter=tables_filter)

        # Assert
        mock_dump.assert_called_once_with(
            base_azure_storage_config,
            selected_tables=["table1", "table2"],
        )

    def test_delete(self, base_azure_storage_config):
        """Test delete method (should do nothing)."""
        channel = AzureStorageChannel(base_azure_storage_config)

        # Act
        result = channel.delete()

        # Assert
        # Should not raise any exception and return None
        assert result is None
