# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock

import pytest
from azure.kusto.data import KustoClient
from azure.kusto.data.response import KustoResponseDataSet

from cosmotech.coal.azure.adx.query import run_command_query, run_query


class TestQueryFunctions:
    """Tests for top-level functions in the query module."""

    @pytest.fixture
    def mock_kusto_client(self):
        """Create a mock KustoClient."""
        return MagicMock(spec=KustoClient)

    @pytest.fixture
    def mock_response(self):
        """Create a mock KustoResponseDataSet."""
        mock_resp = MagicMock(spec=KustoResponseDataSet)
        mock_resp.primary_results = [MagicMock()]
        mock_resp.primary_results[0].__len__.return_value = 5
        return mock_resp

    def test_run_query(self, mock_kusto_client, mock_response):
        """Test the run_query function."""
        # Arrange
        database = "test-database"
        query = "test-query"
        mock_kusto_client.execute.return_value = mock_response

        # Act
        result = run_query(mock_kusto_client, database, query)

        # Assert
        mock_kusto_client.execute.assert_called_once_with(database, query)
        assert result == mock_response

    def test_run_query_empty_results(self, mock_kusto_client):
        """Test the run_query function with empty results."""
        # Arrange
        database = "test-database"
        query = "test-query"
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.primary_results = []
        mock_kusto_client.execute.return_value = mock_response

        # Act
        result = run_query(mock_kusto_client, database, query)

        # Assert
        mock_kusto_client.execute.assert_called_once_with(database, query)
        assert result == mock_response

    def test_run_command_query(self, mock_kusto_client, mock_response):
        """Test the run_command_query function."""
        # Arrange
        database = "test-database"
        query = "test-command-query"
        mock_kusto_client.execute_mgmt.return_value = mock_response

        # Act
        result = run_command_query(mock_kusto_client, database, query)

        # Assert
        mock_kusto_client.execute_mgmt.assert_called_once_with(database, query)
        assert result == mock_response
