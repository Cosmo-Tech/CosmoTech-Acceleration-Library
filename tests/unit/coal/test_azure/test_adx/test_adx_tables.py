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

from cosmotech.coal.azure.adx.tables import create_table, table_exists


class TestTablesFunctions:
    """Tests for top-level functions in the tables module."""

    @pytest.fixture
    def mock_kusto_client(self):
        """Create a mock KustoClient."""
        return MagicMock(spec=KustoClient)

    def test_table_exists_true(self, mock_kusto_client):
        """Test the table_exists function when the table exists."""
        # Arrange
        database = "test-database"
        table_name = "test-table"

        # Mock the response with a table that matches
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.primary_results = [MagicMock()]
        mock_response.primary_results[0].__iter__.return_value = [("test-table",), ("other-table",)]
        mock_kusto_client.execute.return_value = mock_response

        # Act
        result = table_exists(mock_kusto_client, database, table_name)

        # Assert
        mock_kusto_client.execute.assert_called_once()
        assert result is True

    def test_table_exists_false(self, mock_kusto_client):
        """Test the table_exists function when the table does not exist."""
        # Arrange
        database = "test-database"
        table_name = "nonexistent-table"

        # Mock the response with tables that don't match
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.primary_results = [MagicMock()]
        mock_response.primary_results[0].__iter__.return_value = [("other-table-1",), ("other-table-2",)]
        mock_kusto_client.execute.return_value = mock_response

        # Act
        result = table_exists(mock_kusto_client, database, table_name)

        # Assert
        mock_kusto_client.execute.assert_called_once()
        assert result is False

    def test_table_exists_empty_results(self, mock_kusto_client):
        """Test the table_exists function with empty results."""
        # Arrange
        database = "test-database"
        table_name = "test-table"

        # Mock the response with no tables
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.primary_results = [MagicMock()]
        mock_response.primary_results[0].__iter__.return_value = []
        mock_kusto_client.execute.return_value = mock_response

        # Act
        result = table_exists(mock_kusto_client, database, table_name)

        # Assert
        mock_kusto_client.execute.assert_called_once()
        assert result is False

    def test_create_table_success(self, mock_kusto_client):
        """Test the create_table function with successful creation."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        schema = {"id": "string", "name": "string", "value": "real"}

        # Act
        result = create_table(mock_kusto_client, database, table_name, schema)

        # Assert
        mock_kusto_client.execute.assert_called_once()
        # Check that the query contains all column definitions
        query = mock_kusto_client.execute.call_args[0][1]
        assert f".create-merge table {table_name}" in query
        assert "id:string" in query
        assert "name:string" in query
        assert "value:real" in query
        assert result is True

    def test_create_table_failure(self, mock_kusto_client):
        """Test the create_table function with a failure."""
        # Arrange
        database = "test-database"
        table_name = "test-table"
        schema = {"id": "string", "name": "string", "value": "real"}

        # Mock the client to raise an exception
        mock_kusto_client.execute.side_effect = Exception("Test exception")

        # Act
        result = create_table(mock_kusto_client, database, table_name, schema)

        # Assert
        mock_kusto_client.execute.assert_called_once()
        assert result is False

    def test_check_and_create_table_creates_new_table(self, mock_kusto_client):
        """Test check_and_create_table when table doesn't exist."""
        # Arrange
        database = "test-database"
        table_name = "new-table"

        # Create a mock PyArrow table
        import pyarrow as pa

        data = pa.table({"id": ["1", "2", "3"], "name": ["Alice", "Bob", "Charlie"], "value": [1.5, 2.5, 3.5]})

        # Mock the response indicating table doesn't exist
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.primary_results = [MagicMock()]
        mock_response.primary_results[0].__iter__.return_value = []
        mock_kusto_client.execute.return_value = mock_response

        # Act
        from cosmotech.coal.azure.adx.tables import check_and_create_table

        result = check_and_create_table(mock_kusto_client, database, table_name, data)

        # Assert
        assert result is True
        # Should be called twice: once for checking existence, once for creating
        assert mock_kusto_client.execute.call_count == 2

    def test_check_and_create_table_existing_table(self, mock_kusto_client):
        """Test check_and_create_table when table already exists."""
        # Arrange
        database = "test-database"
        table_name = "existing-table"

        # Create a mock PyArrow table
        import pyarrow as pa

        data = pa.table({"id": ["1", "2", "3"], "name": ["Alice", "Bob", "Charlie"]})

        # Mock the response indicating table exists
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.primary_results = [MagicMock()]
        mock_response.primary_results[0].__iter__.return_value = [("existing-table",)]
        mock_kusto_client.execute.return_value = mock_response

        # Act
        from cosmotech.coal.azure.adx.tables import check_and_create_table

        result = check_and_create_table(mock_kusto_client, database, table_name, data)

        # Assert
        assert result is False
        # Should only be called once for checking existence
        assert mock_kusto_client.execute.call_count == 1

    def test_drop_by_tag_success(self, mock_kusto_client):
        """Test _drop_by_tag with successful execution."""
        # Arrange
        database = "test-database"
        tag = "test-tag-123"

        # Act
        from cosmotech.coal.azure.adx.tables import _drop_by_tag

        _drop_by_tag(mock_kusto_client, database, tag)

        # Assert
        mock_kusto_client.execute_mgmt.assert_called_once()
        call_args = mock_kusto_client.execute_mgmt.call_args
        assert call_args[0][0] == database
        assert f"drop-by:{tag}" in call_args[0][1]

    def test_drop_by_tag_failure(self, mock_kusto_client):
        """Test _drop_by_tag when execution fails."""
        # Arrange
        database = "test-database"
        tag = "test-tag-456"

        # Mock the client to raise an exception
        mock_kusto_client.execute_mgmt.side_effect = Exception("Drop failed")

        # Act - should not raise, just log the error
        from cosmotech.coal.azure.adx.tables import _drop_by_tag

        _drop_by_tag(mock_kusto_client, database, tag)

        # Assert
        mock_kusto_client.execute_mgmt.assert_called_once()
