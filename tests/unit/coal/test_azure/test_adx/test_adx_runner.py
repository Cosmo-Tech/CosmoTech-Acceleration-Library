# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import tempfile
import pytest
from unittest.mock import MagicMock, patch, call, mock_open

from azure.kusto.data.response import KustoResponseDataSet

from cosmotech.coal.azure.adx.runner import (
    prepare_csv_content,
    construct_create_query,
    insert_csv_files,
    send_runner_data,
)
from azure.kusto.data import KustoClient
from azure.kusto.ingest import QueuedIngestClient
from cosmotech.coal.azure.adx.ingestion import IngestionStatus


class TestRunnerFunctions:
    """Tests for top-level functions in the runner module."""

    @pytest.fixture
    def mock_csv_files(self, tmp_path):
        """Create mock CSV files for testing."""
        # Create a test folder with CSV files
        folder = tmp_path / "test_csv"
        folder.mkdir()

        # Create a CSV file with headers
        csv1 = folder / "entities.csv"
        csv1.write_text("id,name,value\n1,Entity 1,100\n2,Entity 2,200\n")

        # Create another CSV file with different headers
        csv2 = folder / "relationships.csv"
        csv2.write_text("source,target,type\n1,2,CONTAINS\n")

        return str(folder)

    def test_prepare_csv_content(self, mock_csv_files):
        """Test the prepare_csv_content function."""
        # Act
        result = prepare_csv_content(mock_csv_files)

        # Assert
        assert len(result) == 2

        # Check that both CSV files were found
        csv_paths = list(result.keys())
        assert any("entities.csv" in path for path in csv_paths)
        assert any("relationships.csv" in path for path in csv_paths)

        # Check the content of the first CSV file
        entities_path = next(path for path in csv_paths if "entities.csv" in path)
        entities_info = result[entities_path]
        assert entities_info["filename"] == "entities"
        assert "id" in entities_info["headers"]
        assert "name" in entities_info["headers"]
        assert "value" in entities_info["headers"]
        assert entities_info["headers"]["id"] == "string"

        # Check the content of the second CSV file
        relationships_path = next(path for path in csv_paths if "relationships.csv" in path)
        relationships_info = result[relationships_path]
        assert relationships_info["filename"] == "relationships"
        assert "source" in relationships_info["headers"]
        assert "target" in relationships_info["headers"]
        assert "type" in relationships_info["headers"]

    def test_prepare_csv_content_empty_folder(self, tmp_path):
        """Test the prepare_csv_content function with an empty folder."""
        # Create an empty folder
        empty_folder = tmp_path / "empty"
        empty_folder.mkdir()

        # Act
        result = prepare_csv_content(str(empty_folder))

        # Assert
        assert result == {}

    def test_construct_create_query(self):
        """Test the construct_create_query function."""
        # Arrange
        files_data = {
            "/path/to/entities.csv": {
                "filename": "entities",
                "headers": {"id": "string", "name": "string", "value": "real"},
            },
            "/path/to/relationships.csv": {
                "filename": "relationships",
                "headers": {"source": "string", "target": "string", "type": "string"},
            },
        }

        # Act
        result = construct_create_query(files_data)

        # Assert
        assert len(result) == 2
        assert "entities" in result
        assert "relationships" in result

        # Check the queries
        assert result["entities"].startswith(".create-merge table entities")
        assert "id:string" in result["entities"]
        assert "name:string" in result["entities"]
        assert "value:real" in result["entities"]

        assert result["relationships"].startswith(".create-merge table relationships")
        assert "source:string" in result["relationships"]
        assert "target:string" in result["relationships"]
        assert "type:string" in result["relationships"]

    def test_construct_create_query_empty_data(self):
        """Test the construct_create_query function with empty data."""
        # Act
        result = construct_create_query({})

        # Assert
        assert result == {}

    @patch("cosmotech.coal.azure.adx.runner.FileDescriptor")
    def test_insert_csv_files(self, mock_file_descriptor_class, mock_csv_files):
        """Test the insert_csv_files function."""
        # Arrange
        files_data = prepare_csv_content(mock_csv_files)
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        runner_id = "r-123"
        database = "test-db"

        # Mock ingestion results
        mock_ingestion_result1 = MagicMock()
        mock_ingestion_result1.source_id = "source-id-1"
        mock_ingestion_result2 = MagicMock()
        mock_ingestion_result2.source_id = "source-id-2"
        mock_ingest_client.ingest_from_file.side_effect = [mock_ingestion_result1, mock_ingestion_result2]

        # Act
        insert_csv_files(files_data, mock_kusto_client, mock_ingest_client, runner_id, database, wait=False)

        # Assert
        # Verify that ingest_from_file was called for each CSV file
        assert mock_ingest_client.ingest_from_file.call_count == len(files_data)

        # Verify the ingestion properties
        for call_args in mock_ingest_client.ingest_from_file.call_args_list:
            ingestion_props = call_args[0][1]
            assert ingestion_props.database == database
            assert ingestion_props.drop_by_tags == [runner_id]
            assert ingestion_props.additional_properties == {"ignoreFirstRecord": "true"}

    @patch("cosmotech.coal.azure.adx.runner.check_ingestion_status")
    @patch("cosmotech.coal.azure.adx.runner.FileDescriptor")
    def test_insert_csv_files_with_wait(self, mock_file_descriptor_class, mock_check_ingestion_status, mock_csv_files):
        """Test the insert_csv_files function with wait=True."""
        # Arrange
        files_data = prepare_csv_content(mock_csv_files)
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        runner_id = "r-123"
        database = "test-db"

        # Mock ingestion results
        mock_ingestion_result1 = MagicMock()
        mock_ingestion_result1.source_id = "source-id-1"
        mock_ingestion_result2 = MagicMock()
        mock_ingestion_result2.source_id = "source-id-2"
        mock_ingest_client.ingest_from_file.side_effect = [mock_ingestion_result1, mock_ingestion_result2]

        # Mock check_ingestion_status
        mock_check_ingestion_status.return_value = [
            ("source-id-1", IngestionStatus.SUCCESS),
            ("source-id-2", IngestionStatus.SUCCESS),
        ]

        # Act
        insert_csv_files(files_data, mock_kusto_client, mock_ingest_client, runner_id, database, wait=True)

        # Assert
        # Verify that check_ingestion_status was called
        mock_check_ingestion_status.assert_called()  # Use assert_called instead of assert_called_once
        # Check that the first argument is the ingest client
        assert mock_check_ingestion_status.call_args[0][0] == mock_ingest_client
        # Check that the source_ids parameter contains the expected IDs
        source_ids = mock_check_ingestion_status.call_args[1]["source_ids"]
        assert isinstance(source_ids, list)
        assert len(source_ids) == 2
        assert "source-id-1" in source_ids
        assert "source-id-2" in source_ids

    @patch("cosmotech.coal.azure.adx.runner.check_ingestion_status")
    @patch("cosmotech.coal.azure.adx.runner.FileDescriptor")
    def test_insert_csv_files_with_wait_max_retries(
        self, mock_file_descriptor_class, mock_check_ingestion_status, mock_csv_files
    ):
        """Test the insert_csv_files function with wait=True and retry are maxed out"""
        # Arrange
        files_data = prepare_csv_content(mock_csv_files)
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        runner_id = "r-123"
        database = "test-db"

        # Mock ingestion results
        mock_ingestion_result1 = MagicMock()
        mock_ingestion_result1.source_id = "source-id-1"
        mock_ingestion_result2 = MagicMock()
        mock_ingestion_result2.source_id = "source-id-2"
        mock_ingest_client.ingest_from_file.side_effect = [mock_ingestion_result1, mock_ingestion_result2]

        # Mock check_ingestion_status
        mock_check_ingestion_status.return_value = [
            ("source-id-1", IngestionStatus.QUEUED),
            ("source-id-2", IngestionStatus.QUEUED),
        ]

        # Act
        insert_csv_files(
            files_data,
            mock_kusto_client,
            mock_ingest_client,
            runner_id,
            database,
            wait=True,
            wait_limit=2,
            wait_duration=0,
        )

        # Assert
        # Verify that check_ingestion_status was called
        mock_check_ingestion_status.assert_called()  # Use assert_called instead of assert_called_once
        # Check that the first argument is the ingest client
        assert mock_check_ingestion_status.call_args[0][0] == mock_ingest_client
        # Check that the source_ids parameter contains the expected IDs
        source_ids = mock_check_ingestion_status.call_args[1]["source_ids"]
        assert isinstance(source_ids, list)
        assert len(source_ids) == 2
        assert "source-id-1" in source_ids
        assert "source-id-2" in source_ids

    @patch("cosmotech.coal.azure.adx.runner.prepare_csv_content")
    @patch("cosmotech.coal.azure.adx.runner.construct_create_query")
    @patch("cosmotech.coal.azure.adx.runner.insert_csv_files")
    @patch("cosmotech.coal.azure.adx.runner.initialize_clients")
    @patch("cosmotech.coal.azure.adx.runner.run_query")
    def test_send_runner_data(
        self,
        mock_run_query,
        mock_initialize_clients,
        mock_insert_csv_files,
        mock_construct_create_query,
        mock_prepare_csv_content,
    ):
        """Test the send_runner_data function."""
        # Arrange
        dataset_path = "/path/to/dataset"
        parameters_path = "/path/to/parameters"
        runner_id = "r-123"
        adx_uri = "https://adx.example.com"
        adx_ingest_uri = "https://ingest-adx.example.com"
        database_name = "test-db"

        # Mock prepare_csv_content
        mock_csv_content = {
            "/path/to/dataset/entities.csv": {"filename": "entities", "headers": {"id": "string", "name": "string"}}
        }
        mock_prepare_csv_content.return_value = mock_csv_content

        # Mock construct_create_query
        mock_queries = {"entities": ".create-merge table entities (id:string,name:string)"}
        mock_construct_create_query.return_value = mock_queries

        # Mock initialize_clients
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        mock_initialize_clients.return_value = (mock_kusto_client, mock_ingest_client)

        # Mock run_query response
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.errors_count = 0
        mock_run_query.return_value = mock_response

        # Act
        send_runner_data(
            dataset_path,
            parameters_path,
            runner_id,
            adx_uri,
            adx_ingest_uri,
            database_name,
            send_parameters=True,
            send_datasets=True,
            wait=True,
        )

        # Assert
        # Verify that initialize_clients was called with the correct parameters
        mock_initialize_clients.assert_called_once_with(adx_uri, adx_ingest_uri)

        # Verify that prepare_csv_content was called for both paths
        mock_prepare_csv_content.assert_has_calls([call(parameters_path), call(dataset_path)])

        # Verify that construct_create_query was called
        mock_construct_create_query.assert_called_once()

        # Verify that run_query was called for each query
        assert mock_run_query.call_count == len(mock_queries)
        for k, v in mock_queries.items():
            mock_run_query.assert_any_call(mock_kusto_client, database_name, v)

        # Verify that insert_csv_files was called
        mock_insert_csv_files.assert_called_once_with(
            files_data=mock_csv_content,
            kusto_client=mock_kusto_client,
            ingest_client=mock_ingest_client,
            runner_id=runner_id,
            database=database_name,
            wait=True,
        )

    @patch("cosmotech.coal.azure.adx.runner.prepare_csv_content")
    @patch("cosmotech.coal.azure.adx.runner.construct_create_query")
    @patch("cosmotech.coal.azure.adx.runner.insert_csv_files")
    @patch("cosmotech.coal.azure.adx.runner.initialize_clients")
    @patch("cosmotech.coal.azure.adx.runner.run_query")
    def test_send_runner_data_table_creation_error(
        self,
        mock_run_query,
        mock_initialize_clients,
        mock_insert_csv_files,
        mock_construct_create_query,
        mock_prepare_csv_content,
    ):
        """Test the send_runner_data function with a table creation error."""
        # Arrange
        dataset_path = "/path/to/dataset"
        parameters_path = "/path/to/parameters"
        runner_id = "r-123"
        adx_uri = "https://adx.example.com"
        adx_ingest_uri = "https://ingest-adx.example.com"
        database_name = "test-db"

        # Mock prepare_csv_content
        mock_csv_content = {
            "/path/to/dataset/entities.csv": {"filename": "entities", "headers": {"id": "string", "name": "string"}}
        }
        mock_prepare_csv_content.return_value = mock_csv_content

        # Mock construct_create_query
        mock_queries = {"entities": ".create-merge table entities (id:string,name:string)"}
        mock_construct_create_query.return_value = mock_queries

        # Mock initialize_clients
        mock_kusto_client = MagicMock(spec=KustoClient)
        mock_ingest_client = MagicMock(spec=QueuedIngestClient)
        mock_initialize_clients.return_value = (mock_kusto_client, mock_ingest_client)

        # Mock run_query response with errors
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.errors_count = 1
        mock_response.get_exceptions.return_value = ["Test error"]
        mock_run_query.return_value = mock_response

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to create table entities"):
            send_runner_data(
                dataset_path,
                parameters_path,
                runner_id,
                adx_uri,
                adx_ingest_uri,
                database_name,
                send_parameters=True,
                send_datasets=True,
            )
