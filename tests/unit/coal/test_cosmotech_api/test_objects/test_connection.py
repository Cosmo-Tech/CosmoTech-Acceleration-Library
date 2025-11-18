# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.configuration import Configuration


class TestConnection:
    """Tests for the Connection class."""

    @patch.dict(os.environ, {}, clear=True)
    def test_connection_no_env_vars_raises_error(self):
        """Test that Connection raises EnvironmentError when no valid environment variables are set."""
        mock_config = MagicMock(spec=Configuration)

        with pytest.raises(EnvironmentError):
            Connection(configuration=mock_config)

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_connection_with_api_key(self, mock_cosmotech_config, mock_api_client):
        """Test Connection initialization with API key."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        connection = Connection(configuration=mock_config)

        assert connection.api_client == mock_client_instance
        assert connection.api_type == "Cosmo Tech API Key"
        mock_cosmotech_config.assert_called_once_with(host="https://api.example.com")
        mock_api_client.assert_called_once_with(mock_configuration_instance, "X-CSM-API-KEY", "test-api-key")

    @patch.dict(
        os.environ,
        {
            "CSM_API_KEY": "test-api-key",
            "CSM_API_URL": "https://api.example.com",
            "CSM_API_KEY_HEADER": "X-Custom-Header",
        },
        clear=True,
    )
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_connection_with_custom_api_key_header(self, mock_cosmotech_config, mock_api_client):
        """Test Connection initialization with custom API key header."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        connection = Connection(configuration=mock_config)

        assert connection.api_client == mock_client_instance
        assert connection.api_type == "Cosmo Tech API Key"
        mock_api_client.assert_called_once_with(mock_configuration_instance, "X-Custom-Header", "test-api-key")

    @patch.dict(
        os.environ,
        {
            "AZURE_CLIENT_ID": "test-client-id",
            "AZURE_CLIENT_SECRET": "test-client-secret",
            "AZURE_TENANT_ID": "test-tenant-id",
            "CSM_API_URL": "https://api.example.com",
            "CSM_API_SCOPE": "https://scope.example.com/.default",
        },
        clear=True,
    )
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    @patch("azure.identity.EnvironmentCredential")
    def test_connection_with_azure(self, mock_env_credential, mock_cosmotech_config, mock_api_client):
        """Test Connection initialization with Azure credentials."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock Azure token
        mock_token = MagicMock()
        mock_token.token = "azure-access-token"
        mock_credentials = MagicMock()
        mock_credentials.get_token.return_value = mock_token
        mock_env_credential.return_value = mock_credentials

        connection = Connection(configuration=mock_config)

        assert connection.api_client == mock_client_instance
        assert connection.api_type == "Azure Entra Connection"
        mock_env_credential.assert_called_once()
        mock_credentials.get_token.assert_called_once_with("https://scope.example.com/.default")
        mock_cosmotech_config.assert_called_once_with(host="https://api.example.com", access_token="azure-access-token")

    @patch.dict(
        os.environ,
        {
            "IDP_TENANT_ID": "test-tenant",
            "IDP_CLIENT_ID": "test-client-id",
            "IDP_CLIENT_SECRET": "test-client-secret",
            "IDP_BASE_URL": "https://idp.example.com",
            "CSM_API_URL": "https://api.example.com",
        },
        clear=True,
    )
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    @patch("keycloak.KeycloakOpenID")
    def test_connection_with_keycloak(self, mock_keycloak, mock_cosmotech_config, mock_api_client):
        """Test Connection initialization with Keycloak."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock Keycloak token
        mock_keycloak_instance = MagicMock()
        mock_keycloak_instance.token.return_value = {"access_token": "keycloak-access-token"}
        mock_keycloak.return_value = mock_keycloak_instance

        connection = Connection(configuration=mock_config)

        assert connection.api_client == mock_client_instance
        assert connection.api_type == "Keycloak Connection"
        mock_keycloak.assert_called_once_with(
            server_url="https://idp.example.com/",
            client_id="test-client-id",
            realm_name="test-tenant",
            client_secret_key="test-client-secret",
        )
        mock_keycloak_instance.token.assert_called_once_with(grant_type="client_credentials")
        mock_cosmotech_config.assert_called_once_with(
            host="https://api.example.com", access_token="keycloak-access-token"
        )

    @patch.dict(
        os.environ,
        {
            "IDP_TENANT_ID": "test-tenant",
            "IDP_CLIENT_ID": "test-client-id",
            "IDP_CLIENT_SECRET": "test-client-secret",
            "IDP_BASE_URL": "https://idp.example.com/",
            "CSM_API_URL": "https://api.example.com",
            "IDP_CA_CERT": "/path/to/cert.pem",
        },
        clear=True,
    )
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    @patch("keycloak.KeycloakOpenID")
    @patch("pathlib.Path.exists")
    def test_connection_with_keycloak_and_cert(
        self, mock_exists, mock_keycloak, mock_cosmotech_config, mock_api_client
    ):
        """Test Connection initialization with Keycloak and CA certificate."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance
        mock_exists.return_value = True

        # Mock Keycloak token
        mock_keycloak_instance = MagicMock()
        mock_keycloak_instance.token.return_value = {"access_token": "keycloak-access-token"}
        mock_keycloak.return_value = mock_keycloak_instance

        connection = Connection(configuration=mock_config)

        assert connection.api_client == mock_client_instance
        assert connection.api_type == "Keycloak Connection"
        mock_keycloak.assert_called_once_with(
            server_url="https://idp.example.com/",
            client_id="test-client-id",
            realm_name="test-tenant",
            client_secret_key="test-client-secret",
            verify="/path/to/cert.pem",
        )

    @patch.dict(
        os.environ,
        {
            "IDP_TENANT_ID": "test-tenant",
            "IDP_CLIENT_ID": "test-client-id",
            "IDP_CLIENT_SECRET": "test-client-secret",
            "IDP_BASE_URL": "https://idp.example.com",
            "CSM_API_URL": "https://api.example.com",
        },
        clear=True,
    )
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    @patch("keycloak.KeycloakOpenID")
    def test_connection_keycloak_url_without_trailing_slash(
        self, mock_keycloak, mock_cosmotech_config, mock_api_client
    ):
        """Test that Connection adds trailing slash to Keycloak URL if missing."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock Keycloak token
        mock_keycloak_instance = MagicMock()
        mock_keycloak_instance.token.return_value = {"access_token": "keycloak-access-token"}
        mock_keycloak.return_value = mock_keycloak_instance

        connection = Connection(configuration=mock_config)

        # Verify that the URL has a trailing slash added
        call_args = mock_keycloak.call_args
        assert call_args[1]["server_url"] == "https://idp.example.com/"

    @patch.dict(
        os.environ,
        {
            "CSM_API_KEY": "test-api-key"
            # Missing CSM_API_URL
        },
        clear=True,
    )
    def test_connection_with_incomplete_api_key_vars(self):
        """Test that Connection raises EnvironmentError with incomplete API key variables."""
        mock_config = MagicMock(spec=Configuration)

        with pytest.raises(EnvironmentError):
            Connection(configuration=mock_config)

    @patch.dict(
        os.environ,
        {
            "AZURE_CLIENT_ID": "test-client-id",
            "AZURE_CLIENT_SECRET": "test-client-secret",
            # Missing other Azure variables
        },
        clear=True,
    )
    def test_connection_with_incomplete_azure_vars(self):
        """Test that Connection raises EnvironmentError with incomplete Azure variables."""
        mock_config = MagicMock(spec=Configuration)

        with pytest.raises(EnvironmentError):
            Connection(configuration=mock_config)

    @patch.dict(
        os.environ,
        {
            "IDP_TENANT_ID": "test-tenant",
            "IDP_CLIENT_ID": "test-client-id",
            # Missing other Keycloak variables
        },
        clear=True,
    )
    def test_connection_with_incomplete_keycloak_vars(self):
        """Test that Connection raises EnvironmentError with incomplete Keycloak variables."""
        mock_config = MagicMock(spec=Configuration)

        with pytest.raises(EnvironmentError):
            Connection(configuration=mock_config)

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_api_client_returns_tuple(self, mock_cosmotech_config, mock_api_client):
        """Test that get_api_client returns a tuple of (ApiClient, str)."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        connection = Connection(configuration=mock_config)
        api_client, api_type = connection.get_api_client()

        assert api_client == mock_client_instance
        assert isinstance(api_type, str)
        assert api_type == "Cosmo Tech API Key"
