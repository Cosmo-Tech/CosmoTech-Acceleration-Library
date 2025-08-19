# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from cosmotech_api import DatasetApi

from cosmotech.coal.cosmotech_api.dataset.download.common import download_dataset_by_id
from cosmotech.coal.utils.semver import semver_of


@pytest.mark.skipif(semver_of("cosmotech_api").major >= 5, reason="not supported in version 5")
class TestCommonFunctions:
    """Tests for top-level functions in the common module."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = None
        return mock_client

    @pytest.fixture
    def mock_dataset_api(self):
        """Create a mock DatasetApi."""
        return MagicMock(spec=DatasetApi)

    def create_mock_dataset(self, dataset_type="default"):
        """Create a mock Dataset with different configurations based on dataset_type."""
        dataset = MagicMock()
        dataset.name = f"Test {dataset_type.capitalize()} Dataset"
        dataset.twingraph_id = "twingraph-123"
        dataset.tags = None
        dataset.source = MagicMock()
        dataset.source.location = "test_file.csv"

        # Configure connector based on dataset_type
        if dataset_type == "adt":
            dataset.connector = MagicMock()
            dataset.connector.parameters_values = {"AZURE_DIGITAL_TWINS_URL": "https://example.adt.azure.com"}
        elif dataset_type == "legacy_twingraph":
            dataset.connector = MagicMock()
            dataset.connector.parameters_values = {"TWIN_CACHE_NAME": "cache-123"}
            dataset.twingraph_id = None
        elif dataset_type == "storage":
            dataset.connector = MagicMock()
            dataset.connector.parameters_values = {
                "AZURE_STORAGE_CONTAINER_BLOB_PREFIX": "%WORKSPACE_FILE%/test_file.csv"
            }
        elif dataset_type == "workspace_file":
            dataset.connector = MagicMock()
            dataset.connector.parameters_values = {}
            dataset.tags = ["workspaceFile"]
        elif dataset_type == "dataset_part":
            dataset.connector = MagicMock()
            dataset.connector.parameters_values = {}
            dataset.tags = ["dataset_part"]
        else:  # default twingraph
            dataset.connector = MagicMock()
            dataset.connector.parameters_values = {}

        return dataset

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_adt_dataset")
    def test_download_dataset_by_id_adt(
        self, mock_download_adt, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with an ADT dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="adt")

        # Mock download function
        mock_content = {"entities": [{"id": "entity1"}]}
        mock_download_adt.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        mock_get_api_client.assert_called_once()
        mock_dataset_api_class.assert_called_once_with(mock_api_client)
        mock_dataset_api.find_dataset_by_id.assert_called_once_with(
            organization_id=organization_id, dataset_id=dataset_id
        )

        # Verify correct download function was called
        mock_download_adt.assert_called_once_with(
            adt_address="https://example.adt.azure.com", target_folder=target_folder
        )

        # Verify result structure
        assert result_info["type"] == "adt"
        assert result_info["content"] == mock_content
        assert result_info["name"] == "Test Adt Dataset"
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_legacy_twingraph_dataset")
    def test_download_dataset_by_id_legacy_twingraph(
        self, mock_download_legacy, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with a legacy TwinGraph dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="legacy_twingraph")

        # Mock download function
        mock_content = {"nodes": [{"id": "node1"}], "edges": [{"id": "edge1"}]}
        mock_download_legacy.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        # Verify correct download function was called
        mock_download_legacy.assert_called_once_with(
            organization_id=organization_id, cache_name="cache-123", target_folder=target_folder
        )

        # Verify result structure
        assert result_info["type"] == "twincache"
        assert result_info["content"] == mock_content
        assert result_info["name"] == "Test Legacy_twingraph Dataset"
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_file_dataset")
    def test_download_dataset_by_id_storage(
        self, mock_download_file, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with a storage dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="storage")

        # Mock download function
        mock_content = {"test": [{"id": 1, "name": "Test"}]}
        mock_download_file.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        # Verify correct download function was called
        mock_download_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name="test_file.csv",
            target_folder=target_folder,
        )

        # Verify result structure
        assert result_info["type"] == "csv"
        assert result_info["content"] == mock_content
        assert result_info["name"] == "Test Storage Dataset"
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_file_dataset")
    def test_download_dataset_by_id_workspace_file(
        self, mock_download_file, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with a workspace file dataset."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="workspace_file")

        # Mock download function
        mock_content = {"test": [{"id": 1, "name": "Test"}]}
        mock_download_file.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        # Verify correct download function was called
        mock_download_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name="test_file.csv",
            target_folder=target_folder,
        )

        # Verify result structure
        assert result_info["type"] == "csv"
        assert result_info["content"] == mock_content
        assert result_info["name"] == "Test Workspace_file Dataset"
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_file_dataset")
    def test_download_dataset_by_id_dataset_part(
        self, mock_download_file, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with a dataset_part tag."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="dataset_part")

        # Mock download function
        mock_content = {"test": [{"id": 1, "name": "Test"}]}
        mock_download_file.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        # Verify correct download function was called
        mock_download_file.assert_called_once_with(
            organization_id=organization_id,
            workspace_id=workspace_id,
            file_name="test_file.csv",
            target_folder=target_folder,
        )

        # Verify result structure
        assert result_info["type"] == "csv"
        assert result_info["content"] == mock_content
        assert result_info["name"] == "Test Dataset_part Dataset"
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_twingraph_dataset")
    def test_download_dataset_by_id_twingraph(
        self, mock_download_twingraph, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with a TwinGraph dataset (default case)."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="default")

        # Mock download function
        mock_content = {"nodes": [{"id": "node1"}], "edges": [{"id": "edge1"}]}
        mock_download_twingraph.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        # Verify correct download function was called
        mock_download_twingraph.assert_called_once_with(
            organization_id=organization_id, dataset_id=dataset_id, target_folder=target_folder
        )

        # Verify result structure
        assert result_info["type"] == "twincache"
        assert result_info["content"] == mock_content
        assert result_info["name"] == "Test Default Dataset"
        assert result_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("tempfile.mkdtemp")  # Patch the actual tempfile.mkdtemp
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_twingraph_dataset")
    def test_download_dataset_by_id_no_target_folder(
        self,
        mock_download_twingraph,
        mock_mkdtemp,
        mock_dataset_api_class,
        mock_get_api_client,
        mock_api_client,
        mock_dataset_api,
    ):
        """Test download_dataset_by_id without a target folder."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        temp_dir = "/tmp/temp_dir"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset
        mock_dataset_api.find_dataset_by_id.return_value = self.create_mock_dataset(dataset_type="default")

        # Mock temp directory
        mock_mkdtemp.return_value = temp_dir

        # Mock download function
        mock_content = {"nodes": [{"id": "node1"}], "edges": [{"id": "edge1"}]}
        mock_download_twingraph.return_value = (mock_content, Path(temp_dir))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id, workspace_id=workspace_id, dataset_id=dataset_id
        )

        # Assert
        # Verify temp directory was not created (it's passed to the download function)
        mock_mkdtemp.assert_not_called()

        # Verify correct download function was called with None target_folder
        mock_download_twingraph.assert_called_once_with(
            organization_id=organization_id, dataset_id=dataset_id, target_folder=None
        )

        # Verify result structure
        assert result_info["type"] == "twincache"
        assert result_path == Path(temp_dir)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.DatasetApi")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.common.download_twingraph_dataset")
    def test_download_dataset_by_id_no_connector(
        self, mock_download_twingraph, mock_dataset_api_class, mock_get_api_client, mock_api_client, mock_dataset_api
    ):
        """Test download_dataset_by_id with a dataset that has no connector."""
        # Arrange
        organization_id = "org-123"
        workspace_id = "ws-123"
        dataset_id = "dataset-123"
        target_folder = "/tmp/target"

        # Mock API client
        mock_get_api_client.return_value = (mock_api_client, "Azure Entra Connection")

        # Mock dataset API
        mock_dataset_api_class.return_value = mock_dataset_api

        # Mock dataset with no connector
        dataset = self.create_mock_dataset(dataset_type="default")
        dataset.connector = None
        mock_dataset_api.find_dataset_by_id.return_value = dataset

        # Mock download function
        mock_content = {"nodes": [{"id": "node1"}], "edges": [{"id": "edge1"}]}
        mock_download_twingraph.return_value = (mock_content, Path(target_folder))

        # Act
        result_info, result_path = download_dataset_by_id(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            target_folder=target_folder,
        )

        # Assert
        # Verify correct download function was called
        mock_download_twingraph.assert_called_once_with(
            organization_id=organization_id, dataset_id=dataset_id, target_folder=target_folder
        )

        # Verify result structure
        assert result_info["type"] == "twincache"
