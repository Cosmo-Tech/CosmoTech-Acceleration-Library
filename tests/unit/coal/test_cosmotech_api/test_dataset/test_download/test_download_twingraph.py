# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
import cosmotech_api
from cosmotech_api import DatasetApi, TwingraphApi

from cosmotech.coal.cosmotech_api.dataset.download.twingraph import (
    download_twingraph_dataset,
    download_legacy_twingraph_dataset,
)


class TestTwingraphFunctions:
    """Tests for top-level functions in the twingraph module."""

    @patch("cosmotech.coal.cosmotech_api.dataset.download.twingraph.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.twingraph.get_content_from_twin_graph_data")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.twingraph.convert_dataset_to_files")
    @patch("tempfile.mkdtemp")
    def test_download_twingraph_dataset(self, mock_mkdtemp, mock_convert, mock_get_content, mock_get_api_client):
        """Test the download_twingraph_dataset function."""
        # Arrange
        organization_id = "org-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_api_client = MagicMock()
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")
        mock_api_client.__enter__.return_value = mock_api_client

        # Mock dataset API
        mock_dataset_api_instance = MagicMock(spec=DatasetApi)

        # Mock query results
        mock_nodes = [{"id": "node1"}, {"id": "node2"}]
        mock_edges = [{"src": "node1", "dest": "node2"}]
        mock_dataset_api_instance.twingraph_query.side_effect = [mock_nodes, mock_edges]

        # Mock content processing
        mock_content = {"nodes": mock_nodes, "edges": mock_edges}
        mock_get_content.return_value = mock_content

        # Mock file conversion
        mock_convert.return_value = Path(target_folder)

        # Act
        with patch.object(DatasetApi, "__new__", return_value=mock_dataset_api_instance):
            result_content, result_path = download_twingraph_dataset(
                organization_id=organization_id, dataset_id=dataset_id, target_folder=target_folder
            )

        # Assert
        # Verify API client was obtained
        mock_get_api_client.assert_called_once()

        # Verify queries were executed
        assert mock_dataset_api_instance.twingraph_query.call_count == 2

        # Verify content was processed
        mock_get_content.assert_called_once_with(mock_nodes, mock_edges, True)

        # Verify files were converted
        mock_convert.assert_called_once()
        convert_args = mock_convert.call_args[0]
        assert convert_args[0]["type"] == "twincache"
        assert convert_args[0]["content"] == mock_content
        assert convert_args[1] == target_folder

        # Verify results
        assert result_content == mock_content
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.twingraph.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.twingraph.get_content_from_twin_graph_data")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.twingraph.convert_dataset_to_files")
    @patch("tempfile.mkdtemp")
    def test_download_legacy_twingraph_dataset(self, mock_mkdtemp, mock_convert, mock_get_content, mock_get_api_client):
        """Test the download_legacy_twingraph_dataset function."""
        # Arrange
        organization_id = "org-123"
        cache_name = "cache-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_api_client = MagicMock()
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")
        mock_api_client.__enter__.return_value = mock_api_client

        # Mock twingraph API
        mock_twingraph_api_instance = MagicMock(spec=TwingraphApi)

        # Mock query results
        mock_nodes = [{"id": "node1"}, {"id": "node2"}]
        mock_edges = [{"src": "node1", "dest": "node2"}]
        mock_twingraph_api_instance.query.side_effect = [mock_nodes, mock_edges]

        # Mock content processing
        mock_content = {"nodes": mock_nodes, "edges": mock_edges}
        mock_get_content.return_value = mock_content

        # Mock file conversion
        mock_convert.return_value = Path(target_folder)

        # Act
        with patch.object(TwingraphApi, "__new__", return_value=mock_twingraph_api_instance):
            result_content, result_path = download_legacy_twingraph_dataset(
                organization_id=organization_id, cache_name=cache_name, target_folder=target_folder
            )

        # Assert
        # Verify API client was obtained
        mock_get_api_client.assert_called_once()

        # Verify queries were executed
        assert mock_twingraph_api_instance.query.call_count == 2

        # Verify content was processed
        mock_get_content.assert_called_once_with(mock_nodes, mock_edges, False)

        # Verify files were converted
        mock_convert.assert_called_once()
        convert_args = mock_convert.call_args[0]
        assert convert_args[0]["type"] == "twincache"
        assert convert_args[0]["content"] == mock_content
        assert convert_args[1] == target_folder

        # Verify results
        assert result_content == mock_content
        assert result_path == Path(target_folder)
