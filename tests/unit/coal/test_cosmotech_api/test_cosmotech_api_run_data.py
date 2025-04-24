# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import tempfile
from csv import DictReader
from unittest.mock import MagicMock, patch, mock_open

import pytest
from cosmotech_api import SendRunDataRequest, RunDataQuery

from cosmotech.coal.cosmotech_api.run_data import send_csv_to_run_data, send_store_to_run_data, load_csv_from_run_data


class TestRunDataFunctions:
    """Tests for top-level functions in the run_data module."""

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    def test_send_csv_to_run_data(self, mock_get_api_client):
        """Test the send_csv_to_run_data function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"

        # Create a temporary directory with a CSV file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a CSV file
            csv_content = "id,name,value\n1,test1,100\n2,test2,200\n"
            csv_path = os.path.join(temp_dir, "test_table.csv")
            with open(csv_path, "w") as f:
                f.write(csv_content)

            # Mock API client
            mock_api_client = MagicMock()
            mock_api_client.__enter__.return_value = mock_api_client
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # Mock RunApi
            mock_run_api = MagicMock()

            with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                # Act
                send_csv_to_run_data(
                    source_folder=temp_dir,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    run_id=run_id,
                )

                # Assert
                mock_run_api.send_run_data.assert_called_once()
                call_args = mock_run_api.send_run_data.call_args[0]
                assert call_args[0] == organization_id
                assert call_args[1] == workspace_id
                assert call_args[2] == runner_id
                assert call_args[3] == run_id

                # Check the request data
                request = call_args[4]
                assert isinstance(request, SendRunDataRequest)
                assert request.id == "test_table"
                assert len(request.data) == 2
                assert request.data[0]["id"] == 1  # Integer, not string
                assert request.data[0]["name"] == "test1"
                assert request.data[0]["value"] == 100  # Integer, not string
                assert request.data[1]["id"] == 2  # Integer, not string
                assert request.data[1]["name"] == "test2"
                assert request.data[1]["value"] == 200  # Integer, not string

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    def test_send_csv_to_run_data_with_json_values(self, mock_get_api_client):
        """Test the send_csv_to_run_data function with JSON values."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"

        # Create a temporary directory with a CSV file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a CSV file with JSON values
            csv_content = 'id,name,metadata\n1,test1,"{""key"": ""value""}"\n2,test2,"{""numbers"": [1, 2, 3]}"\n'
            csv_path = os.path.join(temp_dir, "test_table.csv")
            with open(csv_path, "w") as f:
                f.write(csv_content)

            # Mock API client
            mock_api_client = MagicMock()
            mock_api_client.__enter__.return_value = mock_api_client
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # Mock RunApi
            mock_run_api = MagicMock()

            with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                # Act
                send_csv_to_run_data(
                    source_folder=temp_dir,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    run_id=run_id,
                )

                # Assert
                mock_run_api.send_run_data.assert_called_once()
                call_args = mock_run_api.send_run_data.call_args[0]

                # Check the request data
                request = call_args[4]
                assert isinstance(request, SendRunDataRequest)
                assert request.id == "test_table"
                assert len(request.data) == 2
                assert request.data[0]["id"] == 1  # Integer, not string
                assert request.data[0]["name"] == "test1"
                assert request.data[0]["metadata"] == {"key": "value"}
                assert request.data[1]["id"] == 2  # Integer, not string
                assert request.data[1]["name"] == "test2"
                assert request.data[1]["metadata"] == {"numbers": [1, 2, 3]}

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    def test_send_csv_to_run_data_folder_not_found(self, mock_get_api_client):
        """Test the send_csv_to_run_data function with a non-existent folder."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        non_existent_folder = "/path/to/non/existent/folder"

        # Act & Assert
        with pytest.raises(FileNotFoundError) as excinfo:
            send_csv_to_run_data(
                source_folder=non_existent_folder,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                run_id=run_id,
            )

        assert str(excinfo.value) == f"{non_existent_folder} does not exist"
        mock_get_api_client.assert_not_called()

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.run_data.Store")
    @patch("cosmotech.coal.cosmotech_api.run_data.convert_table_as_pylist")
    def test_send_store_to_run_data(self, mock_convert_table, mock_store_class, mock_get_api_client):
        """Test the send_store_to_run_data function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock Store
            mock_store = MagicMock()
            mock_store.list_tables.return_value = ["table1", "table2"]
            mock_store.get_table_schema.return_value.names = ["id", "name", "value"]
            mock_store_class.return_value = mock_store

            # Mock convert_table_as_pylist
            table1_data = [
                {"id": 1, "name": "test1", "value": 100},
                {"id": 2, "name": "test2", "value": 200},
            ]
            table2_data = [
                {"id": 3, "name": "test3", "value": 300, "extra": None},
                {"id": 4, "name": "test4", "value": 400, "extra": None},
            ]
            mock_convert_table.side_effect = [table1_data, table2_data]

            # Mock API client
            mock_api_client = MagicMock()
            mock_api_client.__enter__.return_value = mock_api_client
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # Mock RunApi
            mock_run_api = MagicMock()

            with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                # Act
                send_store_to_run_data(
                    store_folder=temp_dir,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    run_id=run_id,
                )

                # Assert
                assert mock_run_api.send_run_data.call_count == 2

                # Check first call (table1)
                call_args1 = mock_run_api.send_run_data.call_args_list[0][0]
                assert call_args1[0] == organization_id
                assert call_args1[1] == workspace_id
                assert call_args1[2] == runner_id
                assert call_args1[3] == run_id
                request1 = call_args1[4]
                assert isinstance(request1, SendRunDataRequest)
                assert request1.id == "table1"
                assert len(request1.data) == 2

                # Check second call (table2)
                call_args2 = mock_run_api.send_run_data.call_args_list[1][0]
                request2 = call_args2[4]
                assert isinstance(request2, SendRunDataRequest)
                assert request2.id == "table2"
                assert len(request2.data) == 2
                # The None values should be present in the data
                assert "extra" in request2.data[0]
                assert request2.data[0]["extra"] is None
                assert "extra" in request2.data[1]
                assert request2.data[1]["extra"] is None

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.run_data.Store")
    def test_send_store_to_run_data_empty_table(self, mock_store_class, mock_get_api_client):
        """Test the send_store_to_run_data function with an empty table."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock Store
            mock_store = MagicMock()
            mock_store.list_tables.return_value = ["empty_table"]
            mock_store_class.return_value = mock_store

            # Mock convert_table_as_pylist to return empty list
            with patch("cosmotech.coal.cosmotech_api.run_data.convert_table_as_pylist", return_value=[]):
                # Mock API client
                mock_api_client = MagicMock()
                mock_api_client.__enter__.return_value = mock_api_client
                mock_get_api_client.return_value = (mock_api_client, "API Key")

                # Mock RunApi
                mock_run_api = MagicMock()

                with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                    # Act
                    send_store_to_run_data(
                        store_folder=temp_dir,
                        organization_id=organization_id,
                        workspace_id=workspace_id,
                        runner_id=runner_id,
                        run_id=run_id,
                    )

                    # Assert
                    # No data should be sent for empty tables
                    mock_run_api.send_run_data.assert_not_called()

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.run_data.Store")
    def test_send_store_to_run_data_folder_not_found(self, mock_store_class, mock_get_api_client):
        """Test the send_store_to_run_data function with a non-existent folder."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        non_existent_folder = "/path/to/non/existent/folder"

        # Act & Assert
        with pytest.raises(FileNotFoundError) as excinfo:
            send_store_to_run_data(
                store_folder=non_existent_folder,
                organization_id=organization_id,
                workspace_id=workspace_id,
                runner_id=runner_id,
                run_id=run_id,
            )

        assert str(excinfo.value) == f"{non_existent_folder} does not exist"
        mock_get_api_client.assert_not_called()
        mock_store_class.assert_not_called()

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    def test_load_csv_from_run_data(self, mock_get_api_client):
        """Test the load_csv_from_run_data function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        query = "SELECT * FROM test_table"

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock API client
            mock_api_client = MagicMock()
            mock_api_client.__enter__.return_value = mock_api_client
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # Mock RunApi
            mock_run_api = MagicMock()
            mock_query_result = MagicMock()
            mock_query_result.result = [
                {"id": 1, "name": "test1", "value": 100},
                {"id": 2, "name": "test2", "value": 200},
            ]
            mock_run_api.query_run_data.return_value = mock_query_result

            with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                # Act
                load_csv_from_run_data(
                    target_folder=temp_dir,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    run_id=run_id,
                    file_name="test_results",
                    query=query,
                )

                # Assert
                mock_run_api.query_run_data.assert_called_once()
                call_args = mock_run_api.query_run_data.call_args[0]
                assert call_args[0] == organization_id
                assert call_args[1] == workspace_id
                assert call_args[2] == runner_id
                assert call_args[3] == run_id
                assert call_args[4].query == query

                # Check that the CSV file was created
                csv_path = os.path.join(temp_dir, "test_results.csv")
                assert os.path.exists(csv_path)

                # Check the CSV content
                with open(csv_path, "r") as f:
                    reader = DictReader(f)
                    rows = list(reader)
                    assert len(rows) == 2
                    assert rows[0]["id"] == "1"
                    assert rows[0]["name"] == "test1"
                    assert rows[0]["value"] == "100"
                    assert rows[1]["id"] == "2"
                    assert rows[1]["name"] == "test2"
                    assert rows[1]["value"] == "200"

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    def test_load_csv_from_run_data_no_results(self, mock_get_api_client):
        """Test the load_csv_from_run_data function with no results."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        query = "SELECT * FROM empty_table"

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock API client
            mock_api_client = MagicMock()
            mock_api_client.__enter__.return_value = mock_api_client
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # Mock RunApi
            mock_run_api = MagicMock()
            mock_query_result = MagicMock()
            mock_query_result.result = None  # No results
            mock_run_api.query_run_data.return_value = mock_query_result

            with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                # Act
                load_csv_from_run_data(
                    target_folder=temp_dir,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    run_id=run_id,
                    file_name="empty_results",
                    query=query,
                )

                # Assert
                mock_run_api.query_run_data.assert_called_once()

                # Check that no CSV file was created
                csv_path = os.path.join(temp_dir, "empty_results.csv")
                assert not os.path.exists(csv_path)

    @patch("cosmotech.coal.cosmotech_api.run_data.get_api_client")
    def test_load_csv_from_run_data_empty_results(self, mock_get_api_client):
        """Test the load_csv_from_run_data function with empty results."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        runner_id = "runner-123"
        run_id = "run-123"
        query = "SELECT * FROM empty_table"

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock API client
            mock_api_client = MagicMock()
            mock_api_client.__enter__.return_value = mock_api_client
            mock_get_api_client.return_value = (mock_api_client, "API Key")

            # Mock RunApi
            mock_run_api = MagicMock()
            mock_query_result = MagicMock()
            mock_query_result.result = []  # Empty results
            mock_run_api.query_run_data.return_value = mock_query_result

            with patch("cosmotech.coal.cosmotech_api.run_data.RunApi", return_value=mock_run_api):
                # Act
                load_csv_from_run_data(
                    target_folder=temp_dir,
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    runner_id=runner_id,
                    run_id=run_id,
                    file_name="empty_results",
                    query=query,
                )

                # Assert
                mock_run_api.query_run_data.assert_called_once()

                # Check that no CSV file was created (empty list is falsy in Python)
                csv_path = os.path.join(temp_dir, "empty_results.csv")
                assert not os.path.exists(csv_path)
