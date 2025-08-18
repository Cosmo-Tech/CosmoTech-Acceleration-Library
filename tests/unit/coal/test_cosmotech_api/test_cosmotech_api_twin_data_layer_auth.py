# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import pathlib
import tempfile
from io import StringIO
from unittest.mock import MagicMock, patch, mock_open, call

import pytest
import requests

from cosmotech.coal.utils.semver import semver_of
from cosmotech_api import DatasetApi, RunnerApi
if semver_of('cosmotech_api').major < 5:
    from cosmotech_api import DatasetTwinGraphQuery
    from cosmotech.coal.cosmotech_api.twin_data_layer import (
        send_files_to_tdl,
        load_files_from_tdl,
        _process_csv_file,
        _get_node_properties,
        _get_relationship_properties,
    )

from cosmotech.orchestrator.utils.translate import T

skip_under_v5 = pytest.mark.skipif(
    semver_of('cosmotech_api').major >= 5, reason='not supported under version 5'
)


@skip_under_v5
class TestTwinDataLayerAuth:
    """Tests for authentication in the twin_data_layer module."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        mock_client = MagicMock()
        mock_client.default_headers = {"Default-Header": "value"}
        mock_client.configuration.auth_settings.return_value = {
            "auth1": {"type": "apiKey", "in": "header", "key": "Authorization", "value": "Bearer token"},
            "auth2": {"type": "basic", "in": "header", "key": "Authorization", "value": "Basic credentials"},
        }
        return mock_client

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.CSVSourceFile")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._process_csv_file")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.is_dir")
    def test_send_files_to_tdl_auth_params(
        self,
        mock_is_dir,
        mock_glob,
        mock_process_csv_file,
        mock_csv_source_file,
        mock_get_dataset_id,
        mock_get_api_client,
    ):
        """Test the send_files_to_tdl function applies authentication parameters."""
        # Arrange
        mock_api_client = MagicMock()
        mock_api_client.default_headers = {"Default-Header": "value"}

        # Set up auth settings with multiple auth types
        auth_settings = {
            "auth1": {"type": "apiKey", "in": "header", "key": "Authorization", "value": "Bearer token"},
            "auth2": {"type": "basic", "in": "header", "key": "Basic-Auth", "value": "Basic credentials"},
        }
        mock_api_client.configuration.auth_settings.return_value = auth_settings

        mock_get_api_client.return_value = (mock_api_client, MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"
        mock_is_dir.return_value = True

        # Mock CSV files
        mock_node_file = MagicMock()
        mock_node_file.name = "node.csv"
        mock_glob.return_value = [mock_node_file]

        # Mock CSVSourceFile instances
        mock_node_csv = MagicMock()
        mock_node_csv.is_node = True
        mock_node_csv.generate_query_insert.return_value = "CREATE (:node {id: $id})"

        mock_csv_source_file.return_value = mock_node_csv

        # Mock dataset API
        mock_dataset = MagicMock()
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset
        mock_dataset_api.api_client = mock_api_client

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi", return_value=mock_dataset_api):
            # Act
            send_files_to_tdl("http://api.example.com", "org-123", "ws-123", "runner-123", "/data/dir")

            # Assert
            # Verify that _apply_auth_params was called for each auth type
            assert mock_api_client._apply_auth_params.call_count == len(auth_settings)

            # Check that the header was passed to _process_csv_file with auth params applied
            expected_header = {
                "Accept": "application/json",
                "Content-Type": "text/csv",
                "User-Agent": "OpenAPI-Generator/1.0.0/python",
                "Default-Header": "value",
            }

            # Verify that _process_csv_file was called with the expected header
            mock_process_csv_file.assert_called_once()
            actual_header = mock_process_csv_file.call_args[1]["header"]

            # Check that the header contains the expected keys
            for key in expected_header:
                assert key in actual_header
