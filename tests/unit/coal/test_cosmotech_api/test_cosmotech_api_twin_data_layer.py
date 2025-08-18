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
from unittest.mock import MagicMock, patch, mock_open

import pytest
import requests

from cosmotech.coal.utils.semver import semver_of
from cosmotech_api import DatasetApi, RunnerApi
if semver_of('cosmotech_api').major < 5:
    from cosmotech_api import DatasetTwinGraphQuery

    from cosmotech.coal.cosmotech_api.twin_data_layer import (
        get_dataset_id_from_runner,
        send_files_to_tdl,
        load_files_from_tdl,
        CSVSourceFile,
        _process_csv_file,
        _get_node_properties,
        _get_relationship_properties,
        _execute_queries,
        _write_files,
        ID_COLUMN,
        SOURCE_COLUMN,
        TARGET_COLUMN,
    )

pytestmark = pytest.mark.skipif(
    semver_of('cosmotech_api').major >= 5, reason='not supported under version 5'
)


class TestCSVSourceFile:
    """Tests for the CSVSourceFile class."""

    @pytest.fixture
    def mock_csv_file(self):
        """Create a mock CSV file."""
        csv_content = "id,name,value\n1,test1,100\n2,test2,200\n"
        with patch("builtins.open", mock_open(read_data=csv_content)):
            yield pathlib.Path("test.csv")

    @pytest.fixture
    def mock_relation_csv_file(self):
        """Create a mock relation CSV file."""
        csv_content = "src,dest,weight\n1,2,10\n2,3,20\n"
        with patch("builtins.open", mock_open(read_data=csv_content)):
            yield pathlib.Path("relation.csv")

    def test_init_with_node_file(self, mock_csv_file):
        """Test initializing with a node file."""
        # Act
        csv_source = CSVSourceFile(mock_csv_file)

        # Assert
        assert csv_source.file_path == mock_csv_file
        assert csv_source.object_type == "test"
        assert csv_source.fields == ["id", "name", "value"]
        assert csv_source.id_column == "id"
        assert csv_source.is_node is True
        assert csv_source.content_fields == {"id": "id", "name": "name", "value": "value"}

    def test_init_with_relation_file(self, mock_relation_csv_file):
        """Test initializing with a relation file."""
        # Act
        csv_source = CSVSourceFile(mock_relation_csv_file)

        # Assert
        assert csv_source.file_path == mock_relation_csv_file
        assert csv_source.object_type == "relation"
        assert csv_source.fields == ["src", "dest", "weight"]
        assert csv_source.source_column == "src"
        assert csv_source.target_column == "dest"
        assert csv_source.is_node is False
        assert csv_source.content_fields == {"src": "src", "dest": "dest", "weight": "weight"}

    def test_init_with_invalid_file(self):
        """Test initializing with an invalid file."""
        # Arrange
        invalid_file = pathlib.Path("test.txt")

        # Act & Assert
        with pytest.raises(ValueError):
            CSVSourceFile(invalid_file)

    def test_init_with_invalid_node_file(self):
        """Test initializing with an invalid node file."""
        # Arrange
        csv_content = "name,value\ntest1,100\ntest2,200\n"
        mock_file = mock_open(read_data=csv_content)

        # We need to patch both the file existence check and the open call
        with patch("pathlib.Path.exists", return_value=True), patch("builtins.open", mock_file):
            invalid_file = pathlib.Path("invalid.csv")

            # Act & Assert
            with pytest.raises(ValueError):
                CSVSourceFile(invalid_file)

    def test_reload(self, mock_csv_file):
        """Test the reload method."""
        # Arrange
        csv_source = CSVSourceFile(mock_csv_file)

        # Act
        reloaded = csv_source.reload()

        # Assert
        assert reloaded is not csv_source
        assert reloaded.file_path == csv_source.file_path
        assert reloaded.fields == csv_source.fields

    def test_reload_inplace(self, mock_csv_file):
        """Test the reload method with inplace=True."""
        # Arrange
        csv_source = CSVSourceFile(mock_csv_file)

        # Act
        reloaded = csv_source.reload(inplace=True)

        # Assert
        assert reloaded is csv_source

    def test_generate_query_insert_node(self, mock_csv_file):
        """Test the generate_query_insert method for nodes."""
        # Arrange
        csv_source = CSVSourceFile(mock_csv_file)

        # Act
        query = csv_source.generate_query_insert()

        # Assert
        assert "CREATE (:test" in query
        assert "id: $id" in query
        assert "name: $name" in query
        assert "value: $value" in query

    def test_generate_query_insert_relation(self, mock_relation_csv_file):
        """Test the generate_query_insert method for relations."""
        # Arrange
        csv_source = CSVSourceFile(mock_relation_csv_file)

        # Act
        query = csv_source.generate_query_insert()

        # Assert
        assert "MATCH" in query
        assert "(source {id:$src})" in query
        assert "(target {id:$dest})" in query
        assert "CREATE (source)-[rel:relation" in query
        assert "weight: $weight" in query


class TestTwinDataLayerFunctions:
    """Tests for top-level functions in the twin_data_layer module."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        mock_client = MagicMock()
        mock_client.default_headers = {}
        mock_client.configuration.auth_settings.return_value = {}
        return mock_client

    @pytest.fixture
    def mock_runner_api(self):
        """Create a mock RunnerApi."""
        mock_api = MagicMock(spec=RunnerApi)
        return mock_api

    @pytest.fixture
    def mock_dataset_api(self):
        """Create a mock DatasetApi."""
        mock_api = MagicMock(spec=DatasetApi)
        return mock_api

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    def test_get_dataset_id_from_runner(self, mock_get_api_client, mock_runner_api):
        """Test the get_dataset_id_from_runner function with runner_id."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_get_api_client.return_value[0].default_headers = {}
        mock_runner = MagicMock()
        mock_runner.dataset_list = ["dataset-123"]
        mock_runner.id = "runner-123"
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.RunnerApi", return_value=mock_runner_api):
            # Act
            result = get_dataset_id_from_runner("org-123", "ws-123", "runner-123")

            # Assert
            assert result == "dataset-123"
            mock_runner_api.get_runner.assert_called_once_with("org-123", "ws-123", "runner-123")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    def test_get_dataset_id_from_runner_no_ids(self, mock_get_api_client, mock_runner_api):
        """Test the get_dataset_id_from_runner function with no IDs."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_runner = MagicMock()
        mock_runner.dataset_list = []
        mock_runner.id = "runner-123"
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.RunnerApi", return_value=mock_runner_api):
            # Act & Assert
            with pytest.raises(ValueError):
                get_dataset_id_from_runner("org-123", "ws-123", "runner-123")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    def test_get_dataset_id_from_runner_no_datasets(self, mock_get_api_client, mock_runner_api):
        """Test the get_dataset_id_from_runner function with no datasets."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_runner = MagicMock()
        mock_runner.dataset_list = []
        mock_runner.id = "runner-123"
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.RunnerApi", return_value=mock_runner_api):
            # Act & Assert
            with pytest.raises(ValueError):
                get_dataset_id_from_runner("org-123", "ws-123", "runner-123")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    def test_get_dataset_id_from_runner_multiple_datasets(self, mock_get_api_client, mock_runner_api):
        """Test the get_dataset_id_from_runner function with multiple datasets."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_runner = MagicMock()
        mock_runner.dataset_list = ["dataset-1", "dataset-2"]
        mock_runner.id = "runner-123"
        mock_runner_api.get_runner.return_value = mock_runner

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.RunnerApi", return_value=mock_runner_api):
            # Act & Assert
            with pytest.raises(ValueError):
                get_dataset_id_from_runner("org-123", "ws-123", "runner-123")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.CSVSourceFile")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._process_csv_file")
    @patch("pathlib.Path.glob")
    @patch("pathlib.Path.is_dir")
    def test_send_files_to_tdl(
        self,
        mock_is_dir,
        mock_glob,
        mock_process_csv_file,
        mock_csv_source_file,
        mock_get_dataset_id,
        mock_get_api_client,
    ):
        """Test the send_files_to_tdl function."""
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
        mock_relation_file = MagicMock()
        mock_relation_file.name = "relation.csv"
        mock_glob.return_value = [mock_node_file, mock_relation_file]

        # Mock CSVSourceFile instances
        mock_node_csv = MagicMock()
        mock_node_csv.is_node = True
        mock_node_csv.generate_query_insert.return_value = "CREATE (:node {id: $id})"

        mock_relation_csv = MagicMock()
        mock_relation_csv.is_node = False
        mock_relation_csv.generate_query_insert.return_value = (
            "MATCH (source), (target) CREATE (source)-[rel:relation]->(target)"
        )

        mock_csv_source_file.side_effect = [mock_node_csv, mock_relation_csv]

        # Mock dataset API
        mock_dataset = MagicMock()
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset
        mock_dataset_api.api_client = mock_api_client

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi", return_value=mock_dataset_api):
            # Act
            send_files_to_tdl("http://api.example.com", "org-123", "ws-123", "runner-123", "/data/dir")

            # Assert
            mock_get_dataset_id.assert_called_once_with("org-123", "ws-123", "runner-123")
            mock_dataset_api.find_dataset_by_id.assert_called_with("org-123", "dataset-123")
            mock_dataset_api.update_dataset.assert_called_with("org-123", "dataset-123", mock_dataset)
            mock_dataset_api.twingraph_query.assert_called_once()
            assert mock_process_csv_file.call_count == 2

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._get_node_properties")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._get_relationship_properties")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._execute_queries")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer._write_files")
    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.mkdir")
    def test_load_files_from_tdl(
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
        """Test the load_files_from_tdl function."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"
        mock_is_file.return_value = False

        # Mock dataset API
        mock_dataset = MagicMock()
        mock_dataset.ingestion_status = "SUCCESS"
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock node and relationship properties
        mock_get_node_properties.return_value = {"Person": {"id", "name"}}
        mock_get_relationship_properties.return_value = {"KNOWS": {"since"}}

        # Mock execute queries
        mock_execute_queries.return_value = (
            {"Person": [{"id": "1", "name": "Alice"}], "KNOWS": [{"src": "1", "dest": "2", "since": "2020"}]},
            {"Person": {"id", "name"}, "KNOWS": {"src", "dest", "since"}},
        )

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi", return_value=mock_dataset_api):
            # Act
            load_files_from_tdl("org-123", "ws-123", "/data/dir", "runner-123")

            # Assert
            mock_get_dataset_id.assert_called_once_with("org-123", "ws-123", "runner-123")
            mock_dataset_api.find_dataset_by_id.assert_called_once_with("org-123", "dataset-123")
            mock_get_node_properties.assert_called_once()
            mock_get_relationship_properties.assert_called_once()
            mock_execute_queries.assert_called_once()
            mock_write_files.assert_called_once()

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    def test_load_files_from_tdl_invalid_status(self, mock_get_dataset_id, mock_get_api_client, mock_dataset_api):
        """Test the load_files_from_tdl function with invalid dataset status."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"

        # Mock dataset API
        mock_dataset = MagicMock()
        mock_dataset.ingestion_status = "FAILED"
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        with patch("cosmotech.coal.cosmotech_api.twin_data_layer.DatasetApi", return_value=mock_dataset_api):
            # Act & Assert
            with pytest.raises(ValueError):
                load_files_from_tdl("org-123", "ws-123", "/data/dir", "runner-123")

    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.twin_data_layer.get_dataset_id_from_runner")
    @patch("pathlib.Path.is_file")
    def test_load_files_from_tdl_invalid_directory(self, mock_is_file, mock_get_dataset_id, mock_get_api_client):
        """Test the load_files_from_tdl function with invalid directory."""
        # Arrange
        mock_get_api_client.return_value = (MagicMock(), MagicMock())
        mock_get_dataset_id.return_value = "dataset-123"
        mock_is_file.return_value = True

        # Act & Assert
        with pytest.raises(ValueError):
            load_files_from_tdl("org-123", "ws-123", "/data/file.txt", "runner-123")

    @patch("requests.post")
    def test_process_csv_file(self, mock_post):
        """Test the _process_csv_file function."""
        # Arrange
        file_path = pathlib.Path("test.csv")
        query = "CREATE (:test {id: $id})"
        api_url = "http://api.example.com"
        organization_id = "org-123"
        dataset_id = "dataset-123"
        header = {"Content-Type": "text/csv"}

        # Mock CSV file content
        csv_content = "id,name\n1,Alice\n2,Bob\n"

        # Mock response
        mock_response = MagicMock()
        mock_response.content = json.dumps({"errors": []}).encode()
        mock_post.return_value = mock_response

        with patch("builtins.open", mock_open(read_data=csv_content)):
            # Act
            _process_csv_file(file_path, query, api_url, organization_id, dataset_id, header)

            # Assert
            mock_post.assert_called_once()
            assert (
                mock_post.call_args[0][0]
                == f"{api_url}/organizations/{organization_id}/datasets/{dataset_id}/batch?query={query}"
            )

    def test_get_node_properties(self, mock_dataset_api):
        """Test the _get_node_properties function."""
        # Arrange
        organization_id = "org-123"
        dataset_id = "dataset-123"

        # Mock query result
        mock_dataset_api.twingraph_query.return_value = [
            {"label": "Person", "keys": ["id", "name"]},
            {"label": "Company", "keys": ["id", "name", "founded"]},
        ]

        # Act
        result = _get_node_properties(mock_dataset_api, organization_id, dataset_id)

        # Assert
        mock_dataset_api.twingraph_query.assert_called_once()
        assert "Person" in result
        assert "Company" in result
        assert result["Person"] == {"id", "name"}
        assert result["Company"] == {"id", "name", "founded"}

    def test_get_relationship_properties(self, mock_dataset_api):
        """Test the _get_relationship_properties function."""
        # Arrange
        organization_id = "org-123"
        dataset_id = "dataset-123"

        # Mock query result
        mock_dataset_api.twingraph_query.return_value = [
            {"label": "KNOWS", "keys": ["since"]},
            {"label": "WORKS_AT", "keys": ["role", "since"]},
        ]

        # Act
        result = _get_relationship_properties(mock_dataset_api, organization_id, dataset_id)

        # Assert
        mock_dataset_api.twingraph_query.assert_called_once()
        assert "KNOWS" in result
        assert "WORKS_AT" in result
        assert result["KNOWS"] == {"since"}
        assert result["WORKS_AT"] == {"role", "since"}

    def test_execute_queries(self, mock_dataset_api):
        """Test the _execute_queries function."""
        # Arrange
        organization_id = "org-123"
        dataset_id = "dataset-123"
        item_queries = {
            "Person": "MATCH (n:Person) RETURN n.id as id, n.name as name",
            "KNOWS": "MATCH ()-[n:KNOWS]->() RETURN n.src as src, n.dest as dest, n.since as since",
        }

        # Mock query results
        mock_dataset_api.twingraph_query.side_effect = [
            [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
            [{"src": "1", "dest": "2", "since": "2020"}],
        ]

        # Act
        files_content, files_headers = _execute_queries(mock_dataset_api, organization_id, dataset_id, item_queries)

        # Assert
        assert mock_dataset_api.twingraph_query.call_count == 2
        assert "Person" in files_content
        assert "KNOWS" in files_content
        assert files_content["Person"] == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        assert files_content["KNOWS"] == [{"src": "1", "dest": "2", "since": "2020"}]
        assert files_headers["Person"] == {"id", "name"}
        assert files_headers["KNOWS"] == {"src", "dest", "since"}

    def test_write_files(self):
        """Test the _write_files function."""
        # Arrange
        with tempfile.TemporaryDirectory() as temp_dir:
            directory_path = pathlib.Path(temp_dir)
            files_content = {
                "Person": [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}],
                "KNOWS": [{"src": "1", "dest": "2", "since": "2020"}],
            }
            files_headers = {"Person": {"id", "name"}, "KNOWS": {"src", "dest", "since"}}

            # Act
            _write_files(directory_path, files_content, files_headers)

            # Assert
            assert (directory_path / "Person.csv").exists()
            assert (directory_path / "KNOWS.csv").exists()

            # Check Person.csv content
            with open(directory_path / "Person.csv", "r") as f:
                person_content = f.read()
                assert "id,name" in person_content
                assert "1,Alice" in person_content
                assert "2,Bob" in person_content

            # Check KNOWS.csv content
            with open(directory_path / "KNOWS.csv", "r") as f:
                knows_content = f.read()
                assert "src,dest,since" in knows_content
                assert "1,2,2020" in knows_content
