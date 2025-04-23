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
from cosmotech_api import DatasetApi, RunnerApi, DatasetTwinGraphQuery

from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.cosmotech_api.twin_data_layer import (
    send_files_to_tdl,
    load_files_from_tdl,
    _process_csv_file,
    _get_node_properties,
    _get_relationship_properties,
)


class TestTwinDataLayerEdgeCases:
    """Tests for edge cases in the twin_data_layer module."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        mock_client = MagicMock()
        mock_client.default_headers = {}
        mock_client.configuration.auth_settings.return_value = {}
        return mock_client

    @pytest.fixture
    def mock_dataset_api(self):
        """Create a mock DatasetApi."""
        mock_api = MagicMock(spec=DatasetApi)
        return mock_api

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.CSVSourceFile")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._process_csv_file")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.is_dir")
    def test_send_files_to_tdl_update_status(
        self,
        mock_is_dir,
        mock_glob,
        mock_process_csv_file,
        mock_csv_source_file,
        mock_get_dataset_id,
        mock_get_api_client,
    ):
        """Test the send_files_to_tdl function updates dataset status."""
        # Arrange
        mock_api_client = MagicMock()
        mock_api_client.default_headers = {}
        mock_api_client.configuration.auth_settings.return_value = {}

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
            # Check that dataset status is updated at the beginning
            assert mock_dataset_api.update_dataset.call_count == 2

            # First call should set ingestion_status to SUCCESS
            first_call = mock_dataset_api.update_dataset.call_args_list[0]
            assert first_call[0][0] == "org-123"
            assert first_call[0][1] == "dataset-123"
            assert first_call[0][2].ingestion_status == "SUCCESS"

            # Last call should set both ingestion_status and twincache_status
            last_call = mock_dataset_api.update_dataset.call_args_list[1]
            assert last_call[0][0] == "org-123"
            assert last_call[0][1] == "dataset-123"
            assert last_call[0][2].ingestion_status == "SUCCESS"
            assert last_call[0][2].twincache_status == "FULL"

    @patch("requests.post")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.LOGGER")
    def test_process_csv_file_with_errors(self, mock_logger, mock_post):
        """Test the _process_csv_file function with errors."""
        # Arrange
        file_path = pathlib.Path("test.csv")
        query = "CREATE (:test {id: $id})"
        api_url = "http://api.example.com"
        organization_id = "org-123"
        dataset_id = "dataset-123"
        header = {"Content-Type": "text/csv"}

        # Mock CSV file content
        csv_content = "id,name\n1,Alice\n2,Bob\n"

        # Mock response with errors
        mock_response = MagicMock()
        mock_response.content = json.dumps({"errors": ["Error 1", "Error 2"]}).encode()
        mock_post.return_value = mock_response

        with patch("builtins.open", mock_open(read_data=csv_content)):
            # Act & Assert
            with pytest.raises(ValueError) as excinfo:
                _process_csv_file(file_path, query, api_url, organization_id, dataset_id, header)

            assert f"Error importing data from {file_path}" in str(excinfo.value)

            # Verify that errors were logged
            mock_logger.error.assert_any_call(T("coal.logs.storage.import_errors").format(count=2))
            mock_logger.error.assert_any_call(T("coal.logs.storage.error_detail").format(error="Error 1"))
            mock_logger.error.assert_any_call(T("coal.logs.storage.error_detail").format(error="Error 2"))

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._get_node_properties")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._get_relationship_properties")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._execute_queries")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._write_files")
    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.mkdir")
    def test_load_files_from_tdl_empty_properties(
        self,
        mock_mkdir,
        mock_is_file,
        mock_write_files,
        mock_execute_queries,
        mock_get_relationship_properties,
        mock_get_node_properties,
        mock_get_dataset_id,
        mock_get_api_client,
        mock_dataset_api,
    ):
        """Test the load_files_from_tdl function with empty properties."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"
        mock_is_file.return_value = False

        # Mock dataset API
        mock_dataset = MagicMock()
        mock_dataset.ingestion_status = "SUCCESS"
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock empty node and relationship properties
        mock_get_node_properties.return_value = {}
        mock_get_relationship_properties.return_value = {}

        # Mock execute queries
        mock_execute_queries.return_value = ({}, {})

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi", return_value=mock_dataset_api):
            # Act
            load_files_from_tdl("org-123", "ws-123", "/data/dir", "runner-123")

            # Assert
            mock_get_dataset_id.assert_called_once_with("org-123", "ws-123", "runner-123")
            mock_dataset_api.find_dataset_by_id.assert_called_once_with("org-123", "dataset-123")
            mock_get_node_properties.assert_called_once()
            mock_get_relationship_properties.assert_called_once()
            mock_execute_queries.assert_called_once_with(mock_dataset_api, "org-123", "dataset-123", {})
            mock_write_files.assert_called_once_with(pathlib.Path("/data/dir"), {}, {})

    def test_get_relationship_properties_multiple_keys(self, mock_dataset_api):
        """Test the _get_relationship_properties function with multiple keys for the same label."""
        # Arrange
        organization_id = "org-123"
        dataset_id = "dataset-123"

        # Mock query result with multiple entries for the same label
        mock_dataset_api.twingraph_query.return_value = [
            {"label": "KNOWS", "keys": ["since"]},
            {"label": "KNOWS", "keys": ["met_at"]},  # Same label, different keys
            {"label": "WORKS_AT", "keys": ["role"]},
        ]

        # Act
        result = _get_relationship_properties(mock_dataset_api, organization_id, dataset_id)

        # Assert
        mock_dataset_api.twingraph_query.assert_called_once()
        assert "KNOWS" in result
        assert "WORKS_AT" in result
        assert result["KNOWS"] == {"since", "met_at"}  # Combined keys
        assert result["WORKS_AT"] == {"role"}
