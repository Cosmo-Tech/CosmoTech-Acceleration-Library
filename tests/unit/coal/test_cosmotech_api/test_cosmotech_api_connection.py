# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import pathlib
import pytest
from unittest.mock import MagicMock, patch, mock_open

import cosmotech_api
from azure.identity import EnvironmentCredential
from azure.core.credentials import AccessToken

from cosmotech.coal.cosmotech_api.connection import (
    get_api_client,
    api_env_keys,
    azure_env_keys,
    keycloak_env_keys,
)


class TestConnectionFunctions:
    """Tests for top-level functions in the connection module."""

    def setup_method(self):
        """Set up test environment."""
        # Save original environment
        self.original_environ = os.environ.copy()

    def teardown_method(self):
        """Tear down test environment."""
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_environ)

    def test_get_api_client_no_env_vars(self):
        """Test get_api_client with no environment variables set."""
        # Arrange
        with patch.dict(os.environ, {}, clear=True):
            # Act & Assert
            with pytest.raises(EnvironmentError, match="No environment variables found for connection"):
                get_api_client()

    def test_get_api_client_with_api_key(self):
        """Test get_api_client with API key environment variables."""
        # Arrange
        api_url = "https://api.example.com"
        api_key = "test-api-key"
        api_key_header = "X-CSM-API-KEY"

        env_vars = {
            "CSM_API_URL": api_url,
            "CSM_API_KEY": api_key,
            "CSM_API_KEY_HEADER": api_key_header,
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with patch("cosmotech_api.Configuration") as mock_configuration:
                with patch("cosmotech_api.ApiClient") as mock_api_client:
                    mock_config = MagicMock()
                    mock_configuration.return_value = mock_config

                    mock_client = MagicMock()
                    mock_api_client.return_value = mock_client

                    # Act
                    client, connection_type = get_api_client()

                    # Assert
                    mock_configuration.assert_called_once_with(host=api_url)
                    mock_api_client.assert_called_once_with(mock_config, api_key_header, api_key)
                    assert client == mock_client
                    assert connection_type == "Cosmo Tech API Key"

    @patch("azure.identity.EnvironmentCredential")
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_api_client_with_azure(self, mock_configuration, mock_api_client, mock_env_credential):
        """Test get_api_client with Azure environment variables."""
        # Arrange
        mock_config = MagicMock()
        mock_configuration.return_value = mock_config

        mock_client = MagicMock()
        mock_api_client.return_value = mock_client

        mock_credentials = MagicMock(spec=EnvironmentCredential)
        mock_env_credential.return_value = mock_credentials

        mock_token = AccessToken("test-token", 0)
        mock_credentials.get_token.return_value = mock_token

        api_url = "https://api.example.com"
        api_scope = "api://example/.default"

        env_vars = {
            "CSM_API_URL": api_url,
            "CSM_API_SCOPE": api_scope,
            "AZURE_CLIENT_ID": "test-client-id",
            "AZURE_CLIENT_SECRET": "test-client-secret",
            "AZURE_TENANT_ID": "test-tenant-id",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Act
            client, connection_type = get_api_client()

            # Assert
            mock_env_credential.assert_called_once()
            mock_credentials.get_token.assert_called_once_with(api_scope)
            mock_configuration.assert_called_once_with(host=api_url, access_token=mock_token.token)
            mock_api_client.assert_called_once_with(mock_config)
            assert client == mock_client
            assert connection_type == "Azure Entra Connection"

    @patch("keycloak.KeycloakOpenID")
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_api_client_with_keycloak(self, mock_configuration, mock_api_client, mock_keycloak):
        """Test get_api_client with Keycloak environment variables."""
        # Arrange
        mock_config = MagicMock()
        mock_configuration.return_value = mock_config

        mock_client = MagicMock()
        mock_api_client.return_value = mock_client

        mock_keycloak_instance = MagicMock()
        mock_keycloak.return_value = mock_keycloak_instance

        mock_token = {"access_token": "test-token"}
        mock_keycloak_instance.token.return_value = mock_token

        api_url = "https://api.example.com"
        idp_base_url = "https://idp.example.com"
        idp_tenant_id = "test-tenant"
        idp_client_id = "test-client-id"
        idp_client_secret = "test-client-secret"

        env_vars = {
            "CSM_API_URL": api_url,
            "IDP_BASE_URL": idp_base_url,
            "IDP_TENANT_ID": idp_tenant_id,
            "IDP_CLIENT_ID": idp_client_id,
            "IDP_CLIENT_SECRET": idp_client_secret,
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Act
            client, connection_type = get_api_client()

            # Assert
            mock_keycloak.assert_called_once_with(
                server_url=idp_base_url + "/",  # The code adds a trailing slash
                client_id=idp_client_id,
                realm_name=idp_tenant_id,
                client_secret_key=idp_client_secret,
            )
            mock_keycloak_instance.token.assert_called_once_with(grant_type="client_credentials")
            mock_configuration.assert_called_once_with(host=api_url, access_token=mock_token["access_token"])
            mock_api_client.assert_called_once_with(mock_config)
            assert client == mock_client
            assert connection_type == "Keycloak Connection"

    @patch("keycloak.KeycloakOpenID")
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_api_client_with_keycloak_and_ca_cert(self, mock_configuration, mock_api_client, mock_keycloak):
        """Test get_api_client with Keycloak environment variables and CA certificate."""
        # Arrange
        mock_config = MagicMock()
        mock_configuration.return_value = mock_config

        mock_client = MagicMock()
        mock_api_client.return_value = mock_client

        mock_keycloak_instance = MagicMock()
        mock_keycloak.return_value = mock_keycloak_instance

        mock_token = {"access_token": "test-token"}
        mock_keycloak_instance.token.return_value = mock_token

        api_url = "https://api.example.com"
        idp_base_url = "https://idp.example.com"
        idp_tenant_id = "test-tenant"
        idp_client_id = "test-client-id"
        idp_client_secret = "test-client-secret"
        ca_cert_path = "/path/to/ca.crt"

        env_vars = {
            "CSM_API_URL": api_url,
            "IDP_BASE_URL": idp_base_url,
            "IDP_TENANT_ID": idp_tenant_id,
            "IDP_CLIENT_ID": idp_client_id,
            "IDP_CLIENT_SECRET": idp_client_secret,
            "IDP_CA_CERT": ca_cert_path,
        }

        with patch.dict(os.environ, env_vars, clear=True):
            with patch("pathlib.Path.exists", return_value=True):
                # Act
                client, connection_type = get_api_client()

                # Assert
                mock_keycloak.assert_called_once_with(
                    server_url=idp_base_url + "/",  # The code adds a trailing slash
                    client_id=idp_client_id,
                    realm_name=idp_tenant_id,
                    client_secret_key=idp_client_secret,
                    verify=ca_cert_path,
                )
                mock_keycloak_instance.token.assert_called_once_with(grant_type="client_credentials")
                mock_configuration.assert_called_once_with(host=api_url, access_token=mock_token["access_token"])
                mock_api_client.assert_called_once_with(mock_config)
                assert client == mock_client
                assert connection_type == "Keycloak Connection"

    @patch("keycloak.KeycloakOpenID")
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_api_client_with_keycloak_trailing_slash(self, mock_configuration, mock_api_client, mock_keycloak):
        """Test get_api_client with Keycloak environment variables and trailing slash in URL."""
        # Arrange
        mock_config = MagicMock()
        mock_configuration.return_value = mock_config

        mock_client = MagicMock()
        mock_api_client.return_value = mock_client

        mock_keycloak_instance = MagicMock()
        mock_keycloak.return_value = mock_keycloak_instance

        mock_token = {"access_token": "test-token"}
        mock_keycloak_instance.token.return_value = mock_token

        api_url = "https://api.example.com"
        idp_base_url = "https://idp.example.com"  # No trailing slash
        idp_tenant_id = "test-tenant"
        idp_client_id = "test-client-id"
        idp_client_secret = "test-client-secret"

        env_vars = {
            "CSM_API_URL": api_url,
            "IDP_BASE_URL": idp_base_url,
            "IDP_TENANT_ID": idp_tenant_id,
            "IDP_CLIENT_ID": idp_client_id,
            "IDP_CLIENT_SECRET": idp_client_secret,
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Act
            client, connection_type = get_api_client()

            # Assert
            # Should add trailing slash to server_url
            mock_keycloak.assert_called_once_with(
                server_url=idp_base_url + "/",
                client_id=idp_client_id,
                realm_name=idp_tenant_id,
                client_secret_key=idp_client_secret,
            )
            mock_keycloak_instance.token.assert_called_once_with(grant_type="client_credentials")
            mock_configuration.assert_called_once_with(host=api_url, access_token=mock_token["access_token"])
            mock_api_client.assert_called_once_with(mock_config)
            assert client == mock_client
            assert connection_type == "Keycloak Connection"

    def test_get_api_client_no_valid_connection(self):
        """Test get_api_client with incomplete environment variables."""
        # Arrange
        # Set only some of the required environment variables for each connection type
        env_vars = {
            "CSM_API_URL": "https://api.example.com",  # Common to all connection types
            "AZURE_CLIENT_ID": "test-client-id",  # Missing other Azure variables
            "IDP_TENANT_ID": "test-tenant",  # Missing other Keycloak variables
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Act & Assert
            with pytest.raises(EnvironmentError, match="No environment variables found for connection"):
                get_api_client()

    def test_env_keys_constants(self):
        """Test that the environment key sets are correctly defined."""
        # Assert
        assert api_env_keys == {"CSM_API_KEY", "CSM_API_URL"}
        assert azure_env_keys == {
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
            "AZURE_TENANT_ID",
            "CSM_API_URL",
            "CSM_API_SCOPE",
        }
        assert keycloak_env_keys == {
            "IDP_TENANT_ID",
            "IDP_CLIENT_ID",
            "IDP_CLIENT_SECRET",
            "IDP_BASE_URL",
            "CSM_API_URL",
        }
