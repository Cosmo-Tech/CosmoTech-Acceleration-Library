# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

import pytest
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient

from cosmotech.coal.azure.adx.auth import (
    create_ingest_client,
    create_kusto_client,
    get_cluster_urls,
)


class TestAuthFunctions:
    """Tests for top-level functions in the auth module."""

    @pytest.fixture
    def mock_env_vars(self, monkeypatch):
        """Set up environment variables for testing."""
        monkeypatch.setenv("AZURE_CLIENT_ID", "test-client-id")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "test-client-secret")
        monkeypatch.setenv("AZURE_TENANT_ID", "test-tenant-id")

    @pytest.fixture
    def mock_kcsb(self):
        """Create a mock KustoConnectionStringBuilder."""
        return MagicMock(spec=KustoConnectionStringBuilder)

    @patch("cosmotech.coal.azure.adx.auth.KustoConnectionStringBuilder")
    @patch("cosmotech.coal.azure.adx.auth.KustoClient")
    def test_create_kusto_client_with_env_vars(self, mock_kusto_client_class, mock_kcsb_class, mock_env_vars):
        """Test create_kusto_client with environment variables."""
        # Arrange
        cluster_url = "https://test-cluster.kusto.windows.net"
        mock_kcsb = MagicMock()
        mock_kcsb_class.with_aad_application_key_authentication.return_value = mock_kcsb
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_kusto_client_class.return_value = mock_kusto_client

        # Act
        result = create_kusto_client(cluster_url)

        # Assert
        mock_kcsb_class.with_aad_application_key_authentication.assert_called_once_with(
            cluster_url, "test-client-id", "test-client-secret", "test-tenant-id"
        )
        mock_kusto_client_class.assert_called_once_with(mock_kcsb)
        assert result == mock_kusto_client

    @patch("cosmotech.coal.azure.adx.auth.KustoConnectionStringBuilder")
    @patch("cosmotech.coal.azure.adx.auth.KustoClient")
    def test_create_kusto_client_with_provided_credentials(self, mock_kusto_client_class, mock_kcsb_class):
        """Test create_kusto_client with provided credentials."""
        # Arrange
        cluster_url = "https://test-cluster.kusto.windows.net"
        client_id = "provided-client-id"
        client_secret = "provided-client-secret"
        tenant_id = "provided-tenant-id"

        mock_kcsb = MagicMock()
        mock_kcsb_class.with_aad_application_key_authentication.return_value = mock_kcsb
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_kusto_client_class.return_value = mock_kusto_client

        # Act
        result = create_kusto_client(cluster_url, client_id, client_secret, tenant_id)

        # Assert
        mock_kcsb_class.with_aad_application_key_authentication.assert_called_once_with(
            cluster_url, client_id, client_secret, tenant_id
        )
        mock_kusto_client_class.assert_called_once_with(mock_kcsb)
        assert result == mock_kusto_client

    @patch("cosmotech.coal.azure.adx.auth.KustoConnectionStringBuilder")
    @patch("cosmotech.coal.azure.adx.auth.KustoClient")
    def test_create_kusto_client_with_cli_auth(self, mock_kusto_client_class, mock_kcsb_class, monkeypatch):
        """Test create_kusto_client with CLI authentication when env vars are not available."""
        # Arrange
        cluster_url = "https://test-cluster.kusto.windows.net"
        # Remove environment variables
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        mock_kcsb = MagicMock()
        mock_kcsb_class.with_az_cli_authentication.return_value = mock_kcsb
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_kusto_client_class.return_value = mock_kusto_client

        # Act
        result = create_kusto_client(cluster_url)

        # Assert
        mock_kcsb_class.with_az_cli_authentication.assert_called_once_with(cluster_url)
        mock_kusto_client_class.assert_called_once_with(mock_kcsb)
        assert result == mock_kusto_client

    @patch("cosmotech.coal.azure.adx.auth.KustoConnectionStringBuilder")
    @patch("cosmotech.coal.azure.adx.auth.QueuedIngestClient")
    def test_create_ingest_client_with_env_vars(self, mock_ingest_client_class, mock_kcsb_class, mock_env_vars):
        """Test create_ingest_client with environment variables."""
        # Arrange
        ingest_url = "https://ingest-test-cluster.kusto.windows.net"
        mock_kcsb = MagicMock()
        mock_kcsb_class.with_aad_application_key_authentication.return_value = mock_kcsb
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        mock_ingest_client_class.return_value = mock_ingest_client

        # Act
        result = create_ingest_client(ingest_url)

        # Assert
        mock_kcsb_class.with_aad_application_key_authentication.assert_called_once_with(
            ingest_url, "test-client-id", "test-client-secret", "test-tenant-id"
        )
        mock_ingest_client_class.assert_called_once_with(mock_kcsb)
        assert result == mock_ingest_client

    @patch("cosmotech.coal.azure.adx.auth.KustoConnectionStringBuilder")
    @patch("cosmotech.coal.azure.adx.auth.QueuedIngestClient")
    def test_create_ingest_client_with_provided_credentials(self, mock_ingest_client_class, mock_kcsb_class):
        """Test create_ingest_client with provided credentials."""
        # Arrange
        ingest_url = "https://ingest-test-cluster.kusto.windows.net"
        client_id = "provided-client-id"
        client_secret = "provided-client-secret"
        tenant_id = "provided-tenant-id"

        mock_kcsb = MagicMock()
        mock_kcsb_class.with_aad_application_key_authentication.return_value = mock_kcsb
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        mock_ingest_client_class.return_value = mock_ingest_client

        # Act
        result = create_ingest_client(ingest_url, client_id, client_secret, tenant_id)

        # Assert
        mock_kcsb_class.with_aad_application_key_authentication.assert_called_once_with(
            ingest_url, client_id, client_secret, tenant_id
        )
        mock_ingest_client_class.assert_called_once_with(mock_kcsb)
        assert result == mock_ingest_client

    @patch("cosmotech.coal.azure.adx.auth.KustoConnectionStringBuilder")
    @patch("cosmotech.coal.azure.adx.auth.QueuedIngestClient")
    def test_create_ingest_client_with_cli_auth(self, mock_ingest_client_class, mock_kcsb_class, monkeypatch):
        """Test create_ingest_client with CLI authentication when env vars are not available."""
        # Arrange
        ingest_url = "https://ingest-test-cluster.kusto.windows.net"
        # Remove environment variables
        monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
        monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("AZURE_TENANT_ID", raising=False)

        mock_kcsb = MagicMock()
        mock_kcsb_class.with_az_cli_authentication.return_value = mock_kcsb
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        mock_ingest_client_class.return_value = mock_ingest_client

        # Act
        result = create_ingest_client(ingest_url)

        # Assert
        mock_kcsb_class.with_az_cli_authentication.assert_called_once_with(ingest_url)
        mock_ingest_client_class.assert_called_once_with(mock_kcsb)
        assert result == mock_ingest_client

    def test_get_cluster_urls(self):
        """Test the get_cluster_urls function."""
        # Arrange
        cluster_name = "test-cluster"
        cluster_region = "westeurope"
        expected_cluster_url = "https://test-cluster.westeurope.kusto.windows.net"
        expected_ingest_url = "https://ingest-test-cluster.westeurope.kusto.windows.net"

        # Act
        cluster_url, ingest_url = get_cluster_urls(cluster_name, cluster_region)

        # Assert
        assert cluster_url == expected_cluster_url
        assert ingest_url == expected_ingest_url
