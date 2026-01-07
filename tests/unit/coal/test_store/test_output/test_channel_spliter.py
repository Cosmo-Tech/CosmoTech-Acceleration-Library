# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.store.output.channel_spliter import ChannelSpliter
from cosmotech.coal.utils.configuration import Dotdict


class TestChannelSpliter:
    """Tests for the ChannelSpliter class."""

    def test_init_with_single_target(self):
        """Test ChannelSpliter initialization with a single valid target."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        # Mock the channel class in the available_interfaces dict
        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = True
        mock_channel_class.return_value = mock_channel_instance

        # Act
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            spliter = ChannelSpliter(mock_config)

        # Assert
        assert len(spliter.targets) == 1
        assert spliter.targets[0] == mock_channel_instance
        mock_channel_class.assert_called_once_with({"test": "config"})

    @patch("cosmotech.coal.store.output.channel_spliter.PostgresChannel")
    @patch("cosmotech.coal.store.output.channel_spliter.AzureStorageChannel")
    @patch("cosmotech.coal.store.output.channel_spliter.AwsChannel")
    def test_init_with_multiple_targets(self, mock_aws, mock_azure, mock_postgres):
        """Test ChannelSpliter initialization with multiple valid targets."""
        # Arrange
        mock_output1 = MagicMock()
        mock_output1.type = "s3"
        mock_output1.conf = {"s3": "config"}

        mock_output2 = MagicMock()
        mock_output2.type = "az_storage"
        mock_output2.conf = {"azure": "config"}

        mock_output3 = MagicMock()
        mock_output3.type = "postgres"
        mock_output3.conf = {"postgres": "config"}

        mock_config = Dotdict({"outputs": [mock_output1, mock_output2, mock_output3]})

        # Create mock instances
        mock_aws_instance = MagicMock()
        mock_aws_instance.is_available.return_value = True
        mock_aws.return_value = mock_aws_instance

        mock_azure_instance = MagicMock()
        mock_azure_instance.is_available.return_value = True
        mock_azure.return_value = mock_azure_instance

        mock_postgres_instance = MagicMock()
        mock_postgres_instance.is_available.return_value = True
        mock_postgres.return_value = mock_postgres_instance

        # Act
        with patch.dict(
            ChannelSpliter.available_interfaces, {"s3": mock_aws, "az_storage": mock_azure, "postgres": mock_postgres}
        ):
            spliter = ChannelSpliter(mock_config)

        # Assert
        assert len(spliter.targets) == 3

    def test_init_with_unavailable_target(self):
        """Test ChannelSpliter initialization when target is not available."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = False
        mock_channel_class.return_value = mock_channel_instance

        # Act & Assert
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            with pytest.raises(AttributeError):
                ChannelSpliter(mock_config)

    @patch("cosmotech.coal.store.output.channel_spliter.AzureStorageChannel")
    @patch("cosmotech.coal.store.output.channel_spliter.AwsChannel")
    def test_init_with_mixed_availability(self, mock_aws, mock_azure):
        """Test ChannelSpliter initialization with mixed target availability."""
        # Arrange
        mock_output1 = MagicMock()
        mock_output1.type = "s3"
        mock_output1.conf = {"s3": "config"}

        mock_output2 = MagicMock()
        mock_output2.type = "az_storage"
        mock_output2.conf = {"azure": "config"}

        mock_config = Dotdict({"outputs": [mock_output1, mock_output2]})

        # AWS is available
        mock_aws_instance = MagicMock()
        mock_aws_instance.is_available.return_value = True
        mock_aws.return_value = mock_aws_instance

        # Azure is NOT available
        mock_azure_instance = MagicMock()
        mock_azure_instance.is_available.return_value = False
        mock_azure.return_value = mock_azure_instance

        # Act
        with patch.dict(
            ChannelSpliter.available_interfaces,
            {
                "s3": mock_aws,
                "az_storage": mock_azure,
            },
        ):
            spliter = ChannelSpliter(mock_config)

        # Assert
        assert len(spliter.targets) == 1
        assert spliter.targets[0] == mock_aws_instance

    def test_send_success(self):
        """Test send method when all targets succeed."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = True
        mock_channel_instance.send.return_value = True
        mock_channel_class.return_value = mock_channel_instance

        # Act
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            spliter = ChannelSpliter(mock_config)
            result = spliter.send()

        # Assert
        assert result is True
        mock_channel_instance.send.assert_called_once_with(filter=None)

    def test_send_with_filter(self):
        """Test send method with filter."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = True
        mock_channel_instance.send.return_value = True
        mock_channel_class.return_value = mock_channel_instance

        # Act
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            spliter = ChannelSpliter(mock_config)
            result = spliter.send(filter=["table1", "table2"])

        # Assert
        assert result is True
        mock_channel_instance.send.assert_called_once_with(filter=["table1", "table2"])

    @patch("cosmotech.coal.store.output.channel_spliter.AzureStorageChannel")
    @patch("cosmotech.coal.store.output.channel_spliter.AwsChannel")
    def test_send_multiple_targets(self, mock_aws, mock_azure):
        """Test send method with multiple targets."""
        # Arrange
        mock_output1 = MagicMock()
        mock_output1.type = "s3"
        mock_output1.conf = {"s3": "config"}

        mock_output2 = MagicMock()
        mock_output2.type = "az_storage"
        mock_output2.conf = {"azure": "config"}

        mock_config = Dotdict({"outputs": [mock_output1, mock_output2]})

        mock_aws_instance = MagicMock()
        mock_aws_instance.is_available.return_value = True
        mock_aws_instance.send.return_value = True
        mock_aws.return_value = mock_aws_instance

        mock_azure_instance = MagicMock()
        mock_azure_instance.is_available.return_value = True
        mock_azure_instance.send.return_value = True
        mock_azure.return_value = mock_azure_instance

        # Act
        with patch.dict(
            ChannelSpliter.available_interfaces,
            {
                "s3": mock_aws,
                "az_storage": mock_azure,
            },
        ):
            spliter = ChannelSpliter(mock_config)

        # Act
        result = spliter.send()

        # Assert
        assert result is True
        mock_aws_instance.send.assert_called_once()
        mock_azure_instance.send.assert_called_once()

    def test_send_with_exception(self):
        """Test send method when target raises exception."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = True
        mock_channel_instance.send.side_effect = Exception("Test error")
        mock_channel_class.return_value = mock_channel_instance

        # Act
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            spliter = ChannelSpliter(mock_config)

            with pytest.raises(Exception):
                result = spliter.send()
                # Assert
                assert result is False

    def test_delete_success(self):
        """Test delete method when all targets succeed."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = True
        mock_channel_instance.delete.return_value = True
        mock_channel_class.return_value = mock_channel_instance

        # Act
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            spliter = ChannelSpliter(mock_config)
            result = spliter.delete()

        # Assert
        assert result is True
        mock_channel_instance.delete.assert_called_once()

    @patch("cosmotech.coal.store.output.channel_spliter.AzureStorageChannel")
    @patch("cosmotech.coal.store.output.channel_spliter.AwsChannel")
    def test_delete_multiple_targets(self, mock_aws, mock_azure):
        """Test delete method with multiple targets."""
        # Arrange
        mock_output1 = MagicMock()
        mock_output1.type = "s3"
        mock_output1.conf = {"s3": "config"}

        mock_output2 = MagicMock()
        mock_output2.type = "az_storage"
        mock_output2.conf = {"azure": "config"}

        mock_config = Dotdict({"outputs": [mock_output1, mock_output2]})

        mock_aws_instance = MagicMock()
        mock_aws_instance.is_available.return_value = True
        mock_aws_instance.delete.return_value = True
        mock_aws.return_value = mock_aws_instance

        mock_azure_instance = MagicMock()
        mock_azure_instance.is_available.return_value = True
        mock_azure_instance.delete.return_value = True
        mock_azure.return_value = mock_azure_instance

        # Act
        with patch.dict(
            ChannelSpliter.available_interfaces,
            {
                "s3": mock_aws,
                "az_storage": mock_azure,
            },
        ):
            spliter = ChannelSpliter(mock_config)

        result = spliter.delete()

        # Assert
        assert result is True
        mock_aws_instance.delete.assert_called_once()
        mock_azure_instance.delete.assert_called_once()

    def test_delete_with_exception(self):
        """Test delete method when target raises exception."""
        # Arrange
        mock_output = MagicMock()
        mock_output.type = "s3"
        mock_output.conf = {"test": "config"}
        mock_config = Dotdict({"outputs": [mock_output]})

        mock_channel_class = MagicMock()
        mock_channel_instance = MagicMock()
        mock_channel_instance.is_available.return_value = True
        mock_channel_instance.delete.side_effect = Exception("Test error")
        mock_channel_class.return_value = mock_channel_instance

        # Act
        with patch.dict(ChannelSpliter.available_interfaces, {"s3": mock_channel_class}):
            spliter = ChannelSpliter(mock_config)
            with pytest.raises(Exception):
                result = spliter.delete()
                # Assert
                assert result is False

    def test_available_interfaces(self):
        """Test that available_interfaces are properly defined."""
        # Assert
        assert "s3" in ChannelSpliter.available_interfaces
        assert "az_storage" in ChannelSpliter.available_interfaces
        assert "postgres" in ChannelSpliter.available_interfaces
