# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.cosmotech_api.apis.meta import MetaApi
from cosmotech.coal.cosmotech_api.apis.organization import OrganizationApi
from cosmotech.coal.cosmotech_api.apis.run import RunApi
from cosmotech.coal.cosmotech_api.apis.solution import SolutionApi
from cosmotech.coal.utils.configuration import Configuration


class TestRunApi:
    """Tests for the RunApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_run_api_initialization(self, mock_cosmotech_config, mock_api_client):
        """Test RunApi initialization."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = RunApi(configuration=mock_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == mock_config

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_run_metadata(self, mock_cosmotech_config, mock_api_client):
        """Test getting run metadata."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock run object
        mock_run = MagicMock()
        mock_run.model_dump.return_value = {"id": "run-123", "state": "Running"}

        api = RunApi(configuration=mock_config)
        api.get_run = MagicMock(return_value=mock_run)

        result = api.get_run_metadata("org-123", "ws-456", "runner-789", "run-001")

        assert result == {"id": "run-123", "state": "Running"}
        api.get_run.assert_called_once_with("org-123", "ws-456", "runner-789", "run-001")

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_get_run_metadata_with_filters(self, mock_cosmotech_config, mock_api_client):
        """Test getting run metadata with include/exclude filters."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        # Mock run object
        mock_run = MagicMock()
        mock_run.model_dump.return_value = {"id": "run-123"}

        api = RunApi(configuration=mock_config)
        api.get_run = MagicMock(return_value=mock_run)

        result = api.get_run_metadata("org-123", "ws-456", "runner-789", "run-001", include=["id"], exclude=["details"])

        assert result == {"id": "run-123"}
        mock_run.model_dump.assert_called_once_with(
            by_alias=True, exclude_none=True, include=["id"], exclude=["details"], mode="json"
        )


class TestMetaApi:
    """Tests for the MetaApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_meta_api_initialization(self, mock_cosmotech_config, mock_api_client):
        """Test MetaApi initialization."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = MetaApi(configuration=mock_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == mock_config


class TestOrganizationApi:
    """Tests for the OrganizationApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_organization_api_initialization(self, mock_cosmotech_config, mock_api_client):
        """Test OrganizationApi initialization."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = OrganizationApi(configuration=mock_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == mock_config


class TestSolutionApi:
    """Tests for the SolutionApi class."""

    @patch.dict(os.environ, {"CSM_API_KEY": "test-api-key", "CSM_API_URL": "https://api.example.com"}, clear=True)
    @patch("cosmotech_api.ApiClient")
    @patch("cosmotech_api.Configuration")
    def test_solution_api_initialization(self, mock_cosmotech_config, mock_api_client):
        """Test SolutionApi initialization."""
        mock_config = MagicMock(spec=Configuration)
        mock_client_instance = MagicMock()
        mock_api_client.return_value = mock_client_instance
        mock_configuration_instance = MagicMock()
        mock_cosmotech_config.return_value = mock_configuration_instance

        api = SolutionApi(configuration=mock_config)

        assert api.api_client == mock_client_instance
        assert api.configuration == mock_config
