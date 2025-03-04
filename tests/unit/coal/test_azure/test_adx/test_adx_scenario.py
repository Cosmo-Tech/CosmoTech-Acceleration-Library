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

from cosmotech.coal.azure.adx.scenario import (
    prepare_csv_content,
    construct_create_query,
    insert_csv_files,
    send_scenario_data,
)
from cosmotech.coal.azure.adx.wrapper import ADXQueriesWrapper, IngestionStatus


class TestScenarioFunctions:
    """Tests for top-level functions in the scenario module."""

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

    @patch("cosmotech.coal.azure.adx.scenario.FileDescriptor")
    def test_insert_csv_files(self, mock_file_descriptor_class, mock_csv_files):
        """Test the insert_csv_files function."""
        # Arrange
        files_data = prepare_csv_content(mock_csv_files)
        mock_adx_client = MagicMock()  # Don't use spec here to allow adding ingest_client
        mock_adx_client.ingest_client = MagicMock()
        simulation_id = "sim-123"
        database = "test-db"

        # Mock ingestion results
        mock_ingestion_result1 = MagicMock()
        mock_ingestion_result1.source_id = "source-id-1"
        mock_ingestion_result2 = MagicMock()
        mock_ingestion_result2.source_id = "source-id-2"
        mock_adx_client.ingest_client.ingest_from_file.side_effect = [mock_ingestion_result1, mock_ingestion_result2]

        # Act
        insert_csv_files(files_data, mock_adx_client, simulation_id, database, wait=False)

        # Assert
        # Verify that ingest_from_file was called for each CSV file
        assert mock_adx_client.ingest_client.ingest_from_file.call_count == len(files_data)

        # Verify the ingestion properties
        for call_args in mock_adx_client.ingest_client.ingest_from_file.call_args_list:
            ingestion_props = call_args[0][1]
            assert ingestion_props.database == database
            assert ingestion_props.drop_by_tags == [simulation_id]
            assert ingestion_props.additional_properties == {"ignoreFirstRecord": "true"}

    @patch("cosmotech.coal.azure.adx.scenario.FileDescriptor")
    def test_insert_csv_files_with_wait(self, mock_file_descriptor_class, mock_csv_files):
        """Test the insert_csv_files function with wait=True."""
        # Arrange
        files_data = prepare_csv_content(mock_csv_files)
        mock_adx_client = MagicMock()  # Don't use spec here to allow adding ingest_client
        mock_adx_client.ingest_client = MagicMock()
        simulation_id = "sim-123"
        database = "test-db"

        # Mock ingestion results
        mock_ingestion_result1 = MagicMock()
        mock_ingestion_result1.source_id = "source-id-1"
        mock_ingestion_result2 = MagicMock()
        mock_ingestion_result2.source_id = "source-id-2"
        mock_adx_client.ingest_client.ingest_from_file.side_effect = [mock_ingestion_result1, mock_ingestion_result2]

        # Mock check_ingestion_status
        mock_adx_client.check_ingestion_status.return_value = [
            ("source-id-1", IngestionStatus.SUCCESS),
            ("source-id-2", IngestionStatus.SUCCESS),
        ]

        # Act
        insert_csv_files(files_data, mock_adx_client, simulation_id, database, wait=True)

        # Assert
        # Verify that check_ingestion_status was called
        mock_adx_client.check_ingestion_status.assert_called()  # Use assert_called instead of assert_called_once
        source_ids = mock_adx_client.check_ingestion_status.call_args[1]["source_ids"]
        assert "source-id-1" in source_ids
        assert "source-id-2" in source_ids

    @patch("cosmotech.coal.azure.adx.scenario.FileDescriptor")
    def test_insert_csv_files_with_wait_max_retries(self, mock_file_descriptor_class, mock_csv_files):
        """Test the insert_csv_files function with wait=True and retry are maxed out"""
        # Arrange
        files_data = prepare_csv_content(mock_csv_files)
        mock_adx_client = MagicMock()  # Don't use spec here to allow adding ingest_client
        mock_adx_client.ingest_client = MagicMock()
        simulation_id = "sim-123"
        database = "test-db"

        # Mock ingestion results
        mock_ingestion_result1 = MagicMock()
        mock_ingestion_result1.source_id = "source-id-1"
        mock_ingestion_result2 = MagicMock()
        mock_ingestion_result2.source_id = "source-id-2"
        mock_adx_client.ingest_client.ingest_from_file.side_effect = [mock_ingestion_result1, mock_ingestion_result2]

        # Mock check_ingestion_status
        mock_adx_client.check_ingestion_status.return_value = [
            ("source-id-1", IngestionStatus.QUEUED),
            ("source-id-2", IngestionStatus.QUEUED),
        ]

        # Act
        insert_csv_files(files_data, mock_adx_client, simulation_id, database, wait=True, wait_limit=2, wait_duration=0)

        # Assert
        # Verify that check_ingestion_status was called
        mock_adx_client.check_ingestion_status.assert_called()  # Use assert_called instead of assert_called_once
        source_ids = mock_adx_client.check_ingestion_status.call_args[1]["source_ids"]
        assert "source-id-1" in source_ids
        assert "source-id-2" in source_ids

    @patch("cosmotech.coal.azure.adx.scenario.prepare_csv_content")
    @patch("cosmotech.coal.azure.adx.scenario.construct_create_query")
    @patch("cosmotech.coal.azure.adx.scenario.insert_csv_files")
    @patch("cosmotech.coal.azure.adx.scenario.ADXQueriesWrapper")
    def test_send_scenario_data(
        self, mock_adx_wrapper_class, mock_insert_csv_files, mock_construct_create_query, mock_prepare_csv_content
    ):
        """Test the send_scenario_data function."""
        # Arrange
        dataset_path = "/path/to/dataset"
        parameters_path = "/path/to/parameters"
        simulation_id = "sim-123"
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

        # Mock ADXQueriesWrapper
        mock_adx_client = MagicMock(spec=ADXQueriesWrapper)
        mock_adx_wrapper_class.return_value = mock_adx_client

        # Mock run_query response
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.errors_count = 0
        mock_adx_client.run_query.return_value = mock_response

        # Act
        send_scenario_data(
            dataset_path,
            parameters_path,
            simulation_id,
            adx_uri,
            adx_ingest_uri,
            database_name,
            send_parameters=True,
            send_datasets=True,
            wait=True,
        )

        # Assert
        # Verify that ADXQueriesWrapper was created with the correct parameters
        mock_adx_wrapper_class.assert_called_once_with(
            database=database_name, cluster_url=adx_uri, ingest_url=adx_ingest_uri
        )

        # Verify that prepare_csv_content was called for both paths
        mock_prepare_csv_content.assert_has_calls([call(parameters_path), call(dataset_path)])

        # Verify that construct_create_query was called
        mock_construct_create_query.assert_called_once()

        # Verify that run_query was called for each query
        assert mock_adx_client.run_query.call_count == len(mock_queries)

        # Verify that insert_csv_files was called
        mock_insert_csv_files.assert_called_once_with(
            files_data=mock_csv_content,
            adx_client=mock_adx_client,
            simulation_id=simulation_id,
            database=database_name,
            wait=True,
        )

    @patch("cosmotech.coal.azure.adx.scenario.prepare_csv_content")
    @patch("cosmotech.coal.azure.adx.scenario.construct_create_query")
    @patch("cosmotech.coal.azure.adx.scenario.insert_csv_files")
    @patch("cosmotech.coal.azure.adx.scenario.ADXQueriesWrapper")
    def test_send_scenario_data_table_creation_error(
        self, mock_adx_wrapper_class, mock_insert_csv_files, mock_construct_create_query, mock_prepare_csv_content
    ):
        """Test the send_scenario_data function with a table creation error."""
        # Arrange
        dataset_path = "/path/to/dataset"
        parameters_path = "/path/to/parameters"
        simulation_id = "sim-123"
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

        # Mock ADXQueriesWrapper
        mock_adx_client = MagicMock(spec=ADXQueriesWrapper)
        mock_adx_wrapper_class.return_value = mock_adx_client

        # Mock run_query response with errors
        mock_response = MagicMock(spec=KustoResponseDataSet)
        mock_response.errors_count = 1
        mock_response.get_exceptions.return_value = ["Test error"]
        mock_adx_client.run_query.return_value = mock_response

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to create table entities"):
            send_scenario_data(
                dataset_path,
                parameters_path,
                simulation_id,
                adx_uri,
                adx_ingest_uri,
                database_name,
                send_parameters=True,
                send_datasets=True,
            )
