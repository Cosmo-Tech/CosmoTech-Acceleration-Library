# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

from cosmotech_api import DatasetApi

from cosmotech.coal.cosmotech_api.runner.datasets import (
    get_dataset_ids_from_runner,
    download_dataset,
    download_datasets_parallel,
    download_datasets_sequential,
    download_datasets,
    dataset_to_file,
)
from cosmotech.coal.utils.semver import semver_of


class TestDatasetsFunctions:
    """Tests for top-level functions in the datasets module."""

    def test_get_dataset_ids_from_runner(self):
        """Test the get_dataset_ids_from_runner function."""
        # Arrange
        runner_data = MagicMock()
        runner_data.dataset_list = ["dataset-1", "dataset-2"]

        # Create parameter values with a dataset ID
        param1 = MagicMock()
        param1.var_type = "%DATASETID%"
        param1.value = "dataset-3"

        param2 = MagicMock()
        param2.var_type = "string"
        param2.value = "not-a-dataset"

        param3 = MagicMock()
        param3.var_type = "%DATASETID%"
        param3.value = None

        runner_data.parameters_values = [param1, param2, param3]

        # Act
        result = get_dataset_ids_from_runner(runner_data)

        # Assert
        assert len(result) == 3
        assert "dataset-1" in result
        assert "dataset-2" in result
        assert "dataset-3" in result
        assert "not-a-dataset" not in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_adt_dataset")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major >= 5, reason='not supported in version 5'
    )
    def test_download_dataset_adt(self, mock_download_adt, mock_get_api_client):
        """Test the download_dataset function with ADT dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")  # Changed to not trigger credential creation

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset = MagicMock()
        mock_dataset.name = "test-dataset"
        mock_dataset.connector = MagicMock()
        mock_dataset.connector.parameters_values = {"AZURE_DIGITAL_TWINS_URL": "https://adt.example.com"}
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock ADT download
        mock_content = {"nodes": [], "edges": []}
        mock_folder_path = Path("/tmp/adt")
        mock_download_adt.return_value = (mock_content, mock_folder_path)

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_dataset_api.find_dataset_by_id.assert_called_once_with(
                organization_id=organization_id, dataset_id=dataset_id
            )
            mock_download_adt.assert_called_once()
            assert result["type"] == "adt"
            assert result["content"] == mock_content
            assert result["name"] == "test-dataset"
            assert result["folder_path"] == str(mock_folder_path)
            assert result["dataset_id"] == dataset_id

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_legacy_twingraph_dataset")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major >= 5, reason='not supported in version 5'
    )
    def test_download_dataset_legacy_twingraph(self, mock_download_legacy, mock_get_api_client):
        """Test the download_dataset function with legacy twin graph dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset = MagicMock()
        mock_dataset.name = "test-dataset"
        mock_dataset.connector = MagicMock()
        mock_dataset.connector.parameters_values = {"TWIN_CACHE_NAME": "test-cache"}
        mock_dataset.twingraph_id = None
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock legacy twin graph download
        mock_content = {"nodes": [], "edges": []}
        mock_folder_path = Path("/tmp/twingraph")
        mock_download_legacy.return_value = (mock_content, mock_folder_path)

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_dataset_api.find_dataset_by_id.assert_called_once_with(
                organization_id=organization_id, dataset_id=dataset_id
            )
            mock_download_legacy.assert_called_once_with(organization_id=organization_id, cache_name="test-cache")
            assert result["type"] == "twincache"
            assert result["content"] == mock_content
            assert result["name"] == "test-dataset"
            assert result["folder_path"] == str(mock_folder_path)
            assert result["dataset_id"] == dataset_id

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_file_dataset")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major >= 5, reason='not supported in version 5'
    )
    def test_download_dataset_storage(self, mock_download_file, mock_get_api_client):
        """Test the download_dataset function with storage dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset = MagicMock()
        mock_dataset.name = "test-dataset"
        mock_dataset.connector = MagicMock()
        mock_dataset.connector.parameters_values = {"AZURE_STORAGE_CONTAINER_BLOB_PREFIX": "test.csv"}
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock file download
        mock_content = {"test": [{"id": 1, "name": "test"}]}
        mock_folder_path = Path("/tmp/file")
        mock_download_file.return_value = (mock_content, mock_folder_path)

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_dataset_api.find_dataset_by_id.assert_called_once_with(
                organization_id=organization_id, dataset_id=dataset_id
            )
            mock_download_file.assert_called_once_with(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name="test.csv",
                read_files=True,
            )
            assert result["type"] == "csv"
            assert result["content"] == mock_content
            assert result["name"] == "test-dataset"
            assert result["folder_path"] == str(mock_folder_path)
            assert result["dataset_id"] == dataset_id
            assert result["file_name"] == "test.csv"

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_file_dataset")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major >= 5, reason='not supported in version 5'
    )
    def test_download_dataset_workspace_file(self, mock_download_file, mock_get_api_client):
        """Test the download_dataset function with workspace file dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset = MagicMock()
        mock_dataset.name = "test-dataset"
        mock_dataset.connector = MagicMock()
        mock_dataset.connector.parameters_values = {}
        mock_dataset.tags = ["workspaceFile"]
        mock_dataset.source = MagicMock()
        mock_dataset.source.location = "test.json"
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock file download
        mock_content = {"items": [{"id": 1, "name": "test"}]}
        mock_folder_path = Path("/tmp/file")
        mock_download_file.return_value = (mock_content, mock_folder_path)

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_dataset_api.find_dataset_by_id.assert_called_once_with(
                organization_id=organization_id, dataset_id=dataset_id
            )
            mock_download_file.assert_called_once_with(
                organization_id=organization_id,
                workspace_id=workspace_id,
                file_name="test.json",
                read_files=True,
            )
            assert result["type"] == "json"
            assert result["content"] == mock_content
            assert result["name"] == "test-dataset"
            assert result["folder_path"] == str(mock_folder_path)
            assert result["dataset_id"] == dataset_id
            assert result["file_name"] == "test.json"

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_twingraph_dataset")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major >= 5, reason='not supported in version 5'
    )
    def test_download_dataset_twingraph(self, mock_download_twingraph, mock_get_api_client):
        """Test the download_dataset function with twin graph dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset = MagicMock()
        mock_dataset.name = "test-dataset"
        mock_dataset.connector = MagicMock()
        mock_dataset.connector.parameters_values = {}
        mock_dataset.tags = None
        mock_dataset_api.find_dataset_by_id.return_value = mock_dataset

        # Mock twin graph download
        mock_content = {"nodes": [], "edges": []}
        mock_folder_path = Path("/tmp/twingraph")
        mock_download_twingraph.return_value = (mock_content, mock_folder_path)

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_dataset_api.find_dataset_by_id.assert_called_once_with(
                organization_id=organization_id, dataset_id=dataset_id
            )
            mock_download_twingraph.assert_called_once_with(organization_id=organization_id, dataset_id=dataset_id)
            assert result["type"] == "twincache"
            assert result["content"] == mock_content
            assert result["name"] == "test-dataset"
            assert result["folder_path"] == str(mock_folder_path)
            assert result["dataset_id"] == dataset_id

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    @pytest.mark.skipif(
        semver_of('cosmotech_api').major < 5, reason='supported only in version 5'
    )
    def test_download_dataset_v5(self, mock_get_api_client):
        """Test the download_dataset function with twin graph dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        dataset_part_id = "part-123"

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset API
        mock_dataset_api = MagicMock(spec=DatasetApi)
        mock_dataset_part = MagicMock()
        mock_dataset_part.id = dataset_part_id
        mock_dataset_part.source_name = "test-dataset-part.txt"
        mock_dataset = MagicMock()
        mock_dataset.id = dataset_id
        mock_dataset.name = "test-dataset"
        mock_dataset.parts = [mock_dataset_part]
        mock_dataset_api.get_dataset.return_value = mock_dataset

        # Mock file part download
        mock_content = b'test file part content in byte format'
        mock_dataset_api.download_dataset_part.return_value = mock_content

        with patch("cosmotech.coal.cosmotech_api.runner.datasets.DatasetApi", return_value=mock_dataset_api):
            # Act
            result = download_dataset(
                organization_id=organization_id,
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )

            # Assert
            mock_dataset_api.get_dataset.assert_called_once_with(
                organization_id=organization_id, workspace_id=workspace_id, dataset_id=dataset_id
            )
            mock_dataset_api.download_dataset_part.assert_called_once_with(
                    organization_id,
                    workspace_id,
                    dataset_id,
                    dataset_part_id)
            assert result["type"] == "csm_dataset"
            assert result["content"] == {'test-dataset-part.txt': 'test file part content in byte format'}
            assert result["name"] == "test-dataset"
            assert result["dataset_id"] == "dataset-123"

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("multiprocessing.Process")
    @patch("multiprocessing.Manager")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_parallel(self, mock_get_api_client, mock_manager, mock_process, mock_download_dataset):
        """Test the download_datasets_parallel function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock multiprocessing
        mock_return_dict = {}
        mock_error_dict = {}
        mock_manager_instance = MagicMock()
        mock_manager_instance.dict.side_effect = [mock_return_dict, mock_error_dict]
        mock_manager.return_value = mock_manager_instance

        # Mock processes
        mock_process_instance1 = MagicMock()
        mock_process_instance1.exitcode = 0
        mock_process_instance2 = MagicMock()
        mock_process_instance2.exitcode = 0
        mock_process.side_effect = [mock_process_instance1, mock_process_instance2]

        # Mock dataset download results
        mock_return_dict["dataset-1"] = {"type": "csv", "content": {}, "name": "dataset-1"}
        mock_return_dict["dataset-2"] = {"type": "json", "content": {}, "name": "dataset-2"}

        # Act
        result = download_datasets_parallel(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )

        # Assert
        assert mock_process.call_count == 2
        assert mock_process_instance1.start.called
        assert mock_process_instance2.start.called
        assert mock_process_instance1.join.called
        assert mock_process_instance2.join.called
        assert len(result) == 2
        assert "dataset-1" in result
        assert "dataset-2" in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_dataset")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.get_api_client")
    def test_download_datasets_sequential(self, mock_get_api_client, mock_download_dataset):
        """Test the download_datasets_sequential function."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock API client
        mock_api_client = MagicMock()
        mock_api_client.__enter__.return_value = mock_api_client
        mock_get_api_client.return_value = (mock_api_client, "API Key")

        # Mock dataset download results
        mock_download_dataset.side_effect = [
            {"type": "csv", "content": {}, "name": "dataset-1"},
            {"type": "json", "content": {}, "name": "dataset-2"},
        ]

        # Act
        result = download_datasets_sequential(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )

        # Assert
        assert mock_download_dataset.call_count == 2
        mock_download_dataset.assert_has_calls(
            [
                call(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_id="dataset-1",
                    read_files=True,
                ),
                call(
                    organization_id=organization_id,
                    workspace_id=workspace_id,
                    dataset_id="dataset-2",
                    read_files=True,
                ),
            ]
        )
        assert len(result) == 2
        assert "dataset-1" in result
        assert "dataset-2" in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_parallel_mode(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with parallel mode."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock download results
        mock_parallel.return_value = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
            "dataset-2": {"type": "json", "content": {}, "name": "dataset-2"},
        }

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            parallel=True,
        )

        # Assert
        mock_parallel.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=True,
        )
        mock_sequential.assert_not_called()
        assert len(result) == 2
        assert "dataset-1" in result
        assert "dataset-2" in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_sequential_mode(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with sequential mode."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1", "dataset-2"]

        # Mock download results
        mock_sequential.return_value = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
            "dataset-2": {"type": "json", "content": {}, "name": "dataset-2"},
        }

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            parallel=False,
        )

        # Assert
        mock_sequential.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=True,
        )
        mock_parallel.assert_not_called()
        assert len(result) == 2
        assert "dataset-1" in result
        assert "dataset-2" in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_parallel")
    @patch("cosmotech.coal.cosmotech_api.runner.datasets.download_datasets_sequential")
    def test_download_datasets_single_dataset(self, mock_sequential, mock_parallel):
        """Test the download_datasets function with a single dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_ids = ["dataset-1"]

        # Mock download results
        mock_sequential.return_value = {
            "dataset-1": {"type": "csv", "content": {}, "name": "dataset-1"},
        }

        # Act
        result = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            parallel=True,  # Even though parallel is True, it should use sequential for a single dataset
        )

        # Assert
        mock_sequential.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=True,
        )
        mock_parallel.assert_not_called()
        assert len(result) == 1
        assert "dataset-1" in result

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.convert_graph_dataset_to_files")
    def test_dataset_to_file_graph(self, mock_convert):
        """Test the dataset_to_file function with graph dataset."""
        # Arrange
        dataset_info = {
            "type": "twincache",
            "content": {"nodes": [], "edges": []},
            "name": "test-dataset",
            "folder_path": "/tmp/dataset",
        }
        target_folder = "/tmp/target"

        # Mock conversion
        mock_convert.return_value = Path("/tmp/target/converted")

        # Act
        result = dataset_to_file(dataset_info, target_folder)

        # Assert
        mock_convert.assert_called_once_with(dataset_info["content"], target_folder)
        assert result == "/tmp/target/converted"

    @patch("cosmotech.coal.cosmotech_api.runner.datasets.convert_graph_dataset_to_files")
    def test_dataset_to_file_graph_no_target(self, mock_convert):
        """Test the dataset_to_file function with graph dataset and no target folder."""
        # Arrange
        dataset_info = {
            "type": "adt",
            "content": {"nodes": [], "edges": []},
            "name": "test-dataset",
            "folder_path": "/tmp/dataset",
        }

        # Mock conversion
        mock_convert.return_value = Path("/tmp/converted")

        # Act
        result = dataset_to_file(dataset_info)

        # Assert
        mock_convert.assert_called_once_with(dataset_info["content"])
        assert result == "/tmp/converted"

    def test_dataset_to_file_with_folder_path(self):
        """Test the dataset_to_file function with folder path."""
        # Arrange
        dataset_info = {
            "type": "csv",
            "content": {},
            "name": "test-dataset",
            "folder_path": "/tmp/dataset",
        }

        # Act
        result = dataset_to_file(dataset_info)

        # Assert
        assert result == "/tmp/dataset"

    @patch("tempfile.mkdtemp")
    def test_dataset_to_file_fallback(self, mock_mkdtemp):
        """Test the dataset_to_file function with fallback to temp dir."""
        # Arrange
        dataset_info = {
            "type": "unknown",
            "content": {},
            "name": "test-dataset",
        }

        # Mock temp dir
        mock_mkdtemp.return_value = "/tmp/temp-dir"

        # Act
        result = dataset_to_file(dataset_info)

        # Assert
        mock_mkdtemp.assert_called_once()
        assert result == "/tmp/temp-dir"

    def test_dataset_to_file_with_target(self):
        """Test the dataset_to_file function with target folder."""
        # Arrange
        dataset_info = {
            "type": "unknown",
            "content": {},
            "name": "test-dataset",
        }
        target_folder = "/tmp/target"

        # Act
        result = dataset_to_file(dataset_info, target_folder)

        # Assert
        assert result == "/tmp/target"
