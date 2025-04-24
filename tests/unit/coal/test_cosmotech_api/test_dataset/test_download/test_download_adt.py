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
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential

from cosmotech.coal.cosmotech_api.dataset.download.adt import download_adt_dataset


class TestAdtFunctions:
    """Tests for top-level functions in the adt module."""

    @pytest.fixture
    def mock_twins_data(self):
        """Create mock twins data."""
        return [
            {
                "$dtId": "twin1",
                "$metadata": {"$model": "dtmi:com:example:Room;1"},
                "name": "Room 1",
                "temperature": 22.5,
            },
            {
                "$dtId": "twin2",
                "$metadata": {"$model": "dtmi:com:example:Room;1"},
                "name": "Room 2",
                "temperature": 23.1,
            },
            {
                "$dtId": "twin3",
                "$metadata": {"$model": "dtmi:com:example:Device;1"},
                "name": "Device 1",
                "status": "online",
            },
        ]

    @pytest.fixture
    def mock_relations_data(self):
        """Create mock relations data."""
        return [
            {
                "$relationshipId": "rel1",
                "$sourceId": "twin1",
                "$targetId": "twin3",
                "$relationshipName": "contains",
                "since": "2023-01-01",
            },
            {
                "$relationshipId": "rel2",
                "$sourceId": "twin2",
                "$targetId": "twin3",
                "$relationshipName": "contains",
                "since": "2023-01-02",
            },
        ]

    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.DigitalTwinsClient")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.convert_dataset_to_files")
    @patch("tempfile.mkdtemp")
    def test_download_adt_dataset_basic(
        self, mock_mkdtemp, mock_convert, mock_get_api_client, mock_client_class, mock_twins_data, mock_relations_data
    ):
        """Test the basic functionality of download_adt_dataset."""
        # Arrange
        adt_address = "https://example.adt.azure.com"
        target_folder = "/tmp/target"
        temp_dir = "/tmp/temp_dir"
        mock_mkdtemp.return_value = temp_dir

        # Mock API client
        mock_get_api_client.return_value = (MagicMock(), "Azure Entra Connection")

        # Mock ADT client
        mock_client = mock_client_class.return_value
        mock_client.query_twins.side_effect = [mock_twins_data, mock_relations_data]

        # Mock convert_dataset_to_files
        mock_convert.return_value = Path(target_folder)

        # Act
        content, folder_path = download_adt_dataset(adt_address=adt_address, target_folder=target_folder)

        # Assert
        # Verify client was created with correct parameters
        mock_client_class.assert_called_once()
        args, kwargs = mock_client_class.call_args
        assert args[0] == adt_address
        assert isinstance(args[1], DefaultAzureCredential)

        # Verify queries were executed
        assert mock_client.query_twins.call_count == 2
        mock_client.query_twins.assert_has_calls(
            [call("SELECT * FROM digitaltwins"), call("SELECT * FROM relationships")]
        )

        # Verify content structure
        assert "Room" in content
        assert "Device" in content
        assert "contains" in content
        assert len(content["Room"]) == 2
        assert len(content["Device"]) == 1
        assert len(content["contains"]) == 2

        # Verify content transformation
        assert content["Room"][0]["id"] == "twin1"
        assert content["Room"][0]["name"] == "Room 1"
        assert content["Room"][0]["temperature"] == 22.5
        assert "$dtId" not in content["Room"][0]
        assert "$metadata" not in content["Room"][0]

        assert content["contains"][0]["id"] == "rel1"
        assert content["contains"][0]["source"] == "twin1"
        assert content["contains"][0]["target"] == "twin3"
        assert content["contains"][0]["since"] == "2023-01-01"
        assert "$relationshipId" not in content["contains"][0]
        assert "$sourceId" not in content["contains"][0]
        assert "$targetId" not in content["contains"][0]
        assert "$relationshipName" not in content["contains"][0]

        # Verify convert_dataset_to_files was called
        mock_convert.assert_called_once()
        convert_args = mock_convert.call_args[0]
        assert convert_args[0]["type"] == "adt"
        assert convert_args[0]["content"] == content
        assert convert_args[0]["name"] == "ADT Dataset"
        assert convert_args[1] == target_folder

        # Verify results
        assert folder_path == Path(target_folder)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.DigitalTwinsClient")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.get_api_client")
    @patch("tempfile.mkdtemp")
    def test_download_adt_dataset_no_target_folder(
        self, mock_mkdtemp, mock_get_api_client, mock_client_class, mock_twins_data, mock_relations_data
    ):
        """Test download_adt_dataset without a target folder."""
        # Arrange
        adt_address = "https://example.adt.azure.com"
        temp_dir = "/tmp/temp_dir"
        mock_mkdtemp.return_value = temp_dir

        # Mock API client
        mock_get_api_client.return_value = (MagicMock(), "Azure Entra Connection")

        # Mock ADT client
        mock_client = mock_client_class.return_value
        mock_client.query_twins.side_effect = [mock_twins_data, mock_relations_data]

        # Act
        content, folder_path = download_adt_dataset(adt_address=adt_address)

        # Assert
        mock_mkdtemp.assert_called_once()
        assert folder_path == Path(temp_dir)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.DigitalTwinsClient")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.get_api_client")
    def test_download_adt_dataset_with_credentials(
        self, mock_get_api_client, mock_client_class, mock_twins_data, mock_relations_data
    ):
        """Test download_adt_dataset with provided credentials."""
        # Arrange
        adt_address = "https://example.adt.azure.com"
        mock_credentials = MagicMock(spec=DefaultAzureCredential)

        # Mock API client
        mock_get_api_client.return_value = (MagicMock(), "Some other connection type")

        # Mock ADT client
        mock_client = mock_client_class.return_value
        mock_client.query_twins.side_effect = [mock_twins_data, mock_relations_data]

        # Act
        content, folder_path = download_adt_dataset(adt_address=adt_address, credentials=mock_credentials)

        # Assert
        # Verify client was created with provided credentials
        mock_client_class.assert_called_once_with(adt_address, mock_credentials)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.DigitalTwinsClient")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.get_api_client")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.DefaultAzureCredential")
    def test_download_adt_dataset_default_credentials(
        self, mock_default_credential, mock_get_api_client, mock_client_class, mock_twins_data, mock_relations_data
    ):
        """Test download_adt_dataset with default credentials."""
        # Arrange
        adt_address = "https://example.adt.azure.com"
        mock_creds = MagicMock(spec=DefaultAzureCredential)
        mock_default_credential.return_value = mock_creds

        # Mock API client
        mock_get_api_client.return_value = (MagicMock(), "Azure Entra Connection")

        # Mock ADT client
        mock_client = mock_client_class.return_value
        mock_client.query_twins.side_effect = [mock_twins_data, mock_relations_data]

        # Act
        content, folder_path = download_adt_dataset(adt_address=adt_address)

        # Assert
        # Verify DefaultAzureCredential was created
        mock_default_credential.assert_called_once()
        # Verify client was created with default credentials
        mock_client_class.assert_called_once_with(adt_address, mock_creds)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.get_api_client")
    def test_download_adt_dataset_no_credentials(self, mock_get_api_client):
        """Test download_adt_dataset with no credentials available."""
        # Arrange
        adt_address = "https://example.adt.azure.com"

        # Mock API client to return non-Azure connection type
        mock_get_api_client.return_value = (MagicMock(), "Some other connection type")

        # Act & Assert
        with pytest.raises(ValueError, match="No credentials available for ADT connection"):
            download_adt_dataset(adt_address=adt_address)

    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.DigitalTwinsClient")
    @patch("cosmotech.coal.cosmotech_api.dataset.download.adt.get_api_client")
    def test_download_adt_dataset_empty_results(self, mock_get_api_client, mock_client_class):
        """Test download_adt_dataset with empty query results."""
        # Arrange
        adt_address = "https://example.adt.azure.com"

        # Mock API client
        mock_get_api_client.return_value = (MagicMock(), "Azure Entra Connection")

        # Mock ADT client with empty results
        mock_client = mock_client_class.return_value
        mock_client.query_twins.side_effect = [[], []]

        # Act
        content, folder_path = download_adt_dataset(adt_address=adt_address)

        # Assert
        assert content == {}
        assert mock_client.query_twins.call_count == 2
