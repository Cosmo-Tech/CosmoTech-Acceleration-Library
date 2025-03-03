# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, call

from azure.kusto.data import KustoClient
from azure.kusto.data.response import KustoResponseDataSet
from azure.kusto.ingest import QueuedIngestClient

from cosmotech.coal.azure.adx.wrapper import (
    ADXQueriesWrapper,
    IngestionStatus,
)


class TestADXQueriesWrapper:
    """Tests for the ADXQueriesWrapper class."""

    @pytest.fixture
    def mock_kusto_client(self):
        """Create a mock KustoClient."""
        return MagicMock(spec=KustoClient)

    @pytest.fixture
    def mock_ingest_client(self):
        """Create a mock QueuedIngestClient."""
        return MagicMock(spec=QueuedIngestClient)

    @pytest.fixture
    def mock_dataframe(self):
        """Create a mock pandas DataFrame."""
        return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.3, 30.1]})

    @patch("cosmotech.coal.azure.adx.wrapper.create_kusto_client")
    @patch("cosmotech.coal.azure.adx.wrapper.create_ingest_client")
    @patch("cosmotech.coal.azure.adx.wrapper.get_cluster_urls")
    def test_init_with_urls(self, mock_get_cluster_urls, mock_create_ingest_client, mock_create_kusto_client):
        """Test initializing ADXQueriesWrapper with URLs."""
        # Arrange
        database = "test-database"
        cluster_url = "https://test-cluster.kusto.windows.net"
        ingest_url = "https://ingest-test-cluster.kusto.windows.net"

        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)

        mock_create_kusto_client.return_value = mock_kusto_client
        mock_create_ingest_client.return_value = mock_ingest_client

        # Act
        wrapper = ADXQueriesWrapper(database, cluster_url, ingest_url)

        # Assert
        mock_create_kusto_client.assert_called_once_with(cluster_url)
        mock_create_ingest_client.assert_called_once_with(ingest_url)
        mock_get_cluster_urls.assert_not_called()

        assert wrapper.kusto_client == mock_kusto_client
        assert wrapper.ingest_client == mock_ingest_client
        assert wrapper.database == database
        assert wrapper.timeout == 900

    @patch("cosmotech.coal.azure.adx.wrapper.create_kusto_client")
    @patch("cosmotech.coal.azure.adx.wrapper.create_ingest_client")
    @patch("cosmotech.coal.azure.adx.wrapper.get_cluster_urls")
    def test_init_with_cluster_name(self, mock_get_cluster_urls, mock_create_ingest_client, mock_create_kusto_client):
        """Test initializing ADXQueriesWrapper with cluster name and region."""
        # Arrange
        database = "test-database"
        cluster_name = "test-cluster"
        cluster_region = "westeurope"

        cluster_url = "https://test-cluster.westeurope.kusto.windows.net"
        ingest_url = "https://ingest-test-cluster.westeurope.kusto.windows.net"

        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)

        mock_get_cluster_urls.return_value = (cluster_url, ingest_url)
        mock_create_kusto_client.return_value = mock_kusto_client
        mock_create_ingest_client.return_value = mock_ingest_client

        # Act
        wrapper = ADXQueriesWrapper(database, cluster_name=cluster_name, cluster_region=cluster_region)

        # Assert
        mock_get_cluster_urls.assert_called_once_with(cluster_name, cluster_region)
        mock_create_kusto_client.assert_called_once_with(cluster_url)
        mock_create_ingest_client.assert_called_once_with(ingest_url)

        assert wrapper.kusto_client == mock_kusto_client
        assert wrapper.ingest_client == mock_ingest_client
        assert wrapper.database == database

    @patch("cosmotech.coal.azure.adx.wrapper.type_mapping")
    def test_type_mapping(self, mock_type_mapping):
        """Test the type_mapping method."""
        # Arrange
        database = "test-database"
        cluster_url = "https://test-cluster.kusto.windows.net"
        ingest_url = "https://ingest-test-cluster.kusto.windows.net"

        wrapper = MagicMock(spec=ADXQueriesWrapper)
        wrapper.type_mapping.side_effect = ADXQueriesWrapper.type_mapping.__get__(wrapper)

        key = "test-key"
        value = "test-value"
        expected_result = "string"

        mock_type_mapping.return_value = expected_result

        # Act
        result = ADXQueriesWrapper.type_mapping(wrapper, key, value)

        # Assert
        mock_type_mapping.assert_called_once_with(key, value)
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.send_to_adx")
    def test_send_to_adx(self, mock_send_to_adx, mock_kusto_client, mock_ingest_client):
        """Test the send_to_adx method."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        dict_list = [{"id": 1, "name": "Alice"}]
        ignore_table_creation = True
        drop_by_tag = "test-tag"

        expected_result = MagicMock()
        mock_send_to_adx.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.kusto_client = mock_kusto_client
        wrapper.ingest_client = mock_ingest_client
        wrapper.database = database

        # Act
        result = wrapper.send_to_adx(dict_list, table_name, ignore_table_creation, drop_by_tag)

        # Assert
        mock_send_to_adx.assert_called_once_with(
            mock_kusto_client, mock_ingest_client, database, dict_list, table_name, ignore_table_creation, drop_by_tag
        )
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.ingest_dataframe")
    def test_ingest_dataframe(self, mock_ingest_dataframe, mock_ingest_client, mock_dataframe):
        """Test the ingest_dataframe method."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        drop_by_tag = "test-tag"

        expected_result = MagicMock()
        mock_ingest_dataframe.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.ingest_client = mock_ingest_client
        wrapper.database = database

        # Act
        result = wrapper.ingest_dataframe(table_name, mock_dataframe, drop_by_tag)

        # Assert
        mock_ingest_dataframe.assert_called_once_with(
            mock_ingest_client, database, table_name, mock_dataframe, drop_by_tag
        )
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.check_ingestion_status")
    def test_check_ingestion_status(self, mock_check_ingestion_status, mock_ingest_client):
        """Test the check_ingestion_status method."""
        # Arrange
        source_ids = ["source-id-1", "source-id-2"]
        timeout = 600
        logs = True

        expected_result = [("source-id-1", IngestionStatus.SUCCESS), ("source-id-2", IngestionStatus.FAILURE)]
        mock_check_ingestion_status.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.ingest_client = mock_ingest_client
        wrapper.timeout = 900

        # Act
        result = list(wrapper.check_ingestion_status(source_ids, timeout, logs))

        # Assert
        mock_check_ingestion_status.assert_called_once_with(mock_ingest_client, source_ids, timeout, logs)
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.run_command_query")
    def test_run_command_query(self, mock_run_command_query, mock_kusto_client):
        """Test the run_command_query method."""
        # Arrange
        database = "test-database"
        query = "test-command-query"

        expected_result = MagicMock(spec=KustoResponseDataSet)
        mock_run_command_query.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.kusto_client = mock_kusto_client
        wrapper.database = database

        # Act
        result = wrapper.run_command_query(query)

        # Assert
        mock_run_command_query.assert_called_once_with(mock_kusto_client, database, query)
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.run_query")
    def test_run_query(self, mock_run_query, mock_kusto_client):
        """Test the run_query method."""
        # Arrange
        database = "test-database"
        query = "test-query"

        expected_result = MagicMock(spec=KustoResponseDataSet)
        mock_run_query.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.kusto_client = mock_kusto_client
        wrapper.database = database

        # Act
        result = wrapper.run_query(query)

        # Assert
        mock_run_query.assert_called_once_with(mock_kusto_client, database, query)
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.table_exists")
    def test_table_exists(self, mock_table_exists, mock_kusto_client):
        """Test the table_exists method."""
        # Arrange
        database = "test-database"
        table_name = "test-table"

        expected_result = True
        mock_table_exists.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.kusto_client = mock_kusto_client
        wrapper.database = database

        # Act
        result = wrapper.table_exists(table_name)

        # Assert
        mock_table_exists.assert_called_once_with(mock_kusto_client, database, table_name)
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.wrapper.create_table")
    def test_create_table(self, mock_create_table, mock_kusto_client):
        """Test the create_table method."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        schema = {"id": "string", "name": "string"}

        expected_result = True
        mock_create_table.return_value = expected_result

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.kusto_client = mock_kusto_client
        wrapper.database = database

        # Act
        result = wrapper.create_table(table_name, schema)

        # Assert
        mock_create_table.assert_called_once_with(mock_kusto_client, database, table_name, schema)
        assert result == expected_result

    @patch("cosmotech.coal.azure.adx.ingestion.clear_ingestion_status_queues")
    def test_clear_ingestion_status_queues(self, mock_clear_ingestion_status_queues, mock_ingest_client):
        """Test the _clear_ingestion_status_queues method."""
        # Arrange
        confirmation = True

        wrapper = ADXQueriesWrapper.__new__(ADXQueriesWrapper)
        wrapper.ingest_client = mock_ingest_client

        # Act
        wrapper._clear_ingestion_status_queues(confirmation)

        # Assert
        mock_clear_ingestion_status_queues.assert_called_once_with(mock_ingest_client, confirmation)
