# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from unittest.mock import MagicMock, patch

import pytest

from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.store.output.channel_spliter import ChannelSpliter


class TestChannelSpliter:
    """Tests for ChannelSpliter class."""

    def test_init_no_available_interfaces_raises_error(self):
        """Test that initialization raises error when no interfaces are available."""
        # Arrange & Act & Assert
        with pytest.raises(AttributeError):
            ChannelSpliter()

    @patch("cosmotech.coal.store.output.channel_spliter.ChannelInterface")
    def test_init_with_available_interface(self, mock_interface_class):
        """Test initialization with an available interface."""
        # Arrange
        mock_instance = MagicMock(spec=ChannelInterface)
        mock_instance.is_available.return_value = True
        mock_interface_class.return_value = mock_instance

        # Override the possible_interfaces and reset targets
        with (
            patch.object(ChannelSpliter, "possible_interfaces", [mock_interface_class]),
            patch.object(ChannelSpliter, "targets", []),
        ):
            # Act
            spliter = ChannelSpliter()

            # Assert
            assert len(spliter.targets) == 1
            assert spliter.targets[0] == mock_instance

    @patch("cosmotech.coal.store.output.channel_spliter.ChannelInterface")
    def test_init_skips_unavailable_interface(self, mock_interface_class):
        """Test initialization skips interfaces that are not available."""
        # Arrange
        mock_instance = MagicMock(spec=ChannelInterface)
        mock_instance.is_available.return_value = False
        mock_interface_class.return_value = mock_instance
        mock_interface_class.requirement_string = "test requirements"

        # Override the possible_interfaces and targets
        with (
            patch.object(ChannelSpliter, "possible_interfaces", [mock_interface_class]),
            patch.object(ChannelSpliter, "targets", []),
        ):
            # Act & Assert
            with pytest.raises(AttributeError):
                ChannelSpliter()

    @patch("cosmotech.coal.store.output.channel_spliter.ChannelInterface")
    def test_init_handles_exception_during_interface_creation(self, mock_interface_class):
        """Test initialization handles exceptions when creating interfaces."""
        # Arrange
        mock_interface_class.side_effect = Exception("Interface creation failed")

        # Override the possible_interfaces
        with patch.object(ChannelSpliter, "possible_interfaces", [mock_interface_class]):
            # Act & Assert
            with pytest.raises(AttributeError):
                ChannelSpliter()

    def test_send_calls_targets(self):
        """Test send method calls send on target interfaces."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.send.return_value = True

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1]

        # Act
        result = spliter.send(filter=["table1"])

        # Assert
        assert result is True
        mock_target1.send.assert_called_once_with(filter=["table1"])

    def test_send_returns_true_if_any_success(self):
        """Test send returns True if any target succeeds."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.send.return_value = False
        mock_target2 = MagicMock(spec=ChannelInterface)
        mock_target2.send.return_value = True

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1, mock_target2]

        # Act
        result = spliter.send()

        # Assert
        assert result is True

    def test_send_returns_false_if_all_fail(self):
        """Test send returns False if all targets fail."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.send.return_value = False
        mock_target2 = MagicMock(spec=ChannelInterface)
        mock_target2.send.return_value = False

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1, mock_target2]

        # Act
        result = spliter.send()

        # Assert
        assert result is False

    def test_send_handles_exception_in_target(self):
        """Test send continues when a target raises an exception."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.send.side_effect = Exception("Send failed")
        mock_target2 = MagicMock(spec=ChannelInterface)
        mock_target2.send.return_value = True

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1, mock_target2]

        # Act
        result = spliter.send()

        # Assert
        assert result is True
        mock_target2.send.assert_called_once()

    def test_delete_calls_targets(self):
        """Test delete method calls delete on target interfaces."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.delete.return_value = True

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1]

        # Act
        result = spliter.delete()

        # Assert
        assert result is True
        mock_target1.delete.assert_called_once()

    def test_delete_returns_true_if_any_success(self):
        """Test delete returns True if any target succeeds."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.delete.return_value = False
        mock_target2 = MagicMock(spec=ChannelInterface)
        mock_target2.delete.return_value = True

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1, mock_target2]

        # Act
        result = spliter.delete()

        # Assert
        assert result is True

    def test_delete_handles_exception_in_target(self):
        """Test delete continues when a target raises an exception."""
        # Arrange
        mock_target1 = MagicMock(spec=ChannelInterface)
        mock_target1.delete.side_effect = Exception("Delete failed")
        mock_target2 = MagicMock(spec=ChannelInterface)
        mock_target2.delete.return_value = True

        spliter = object.__new__(ChannelSpliter)
        spliter.targets = [mock_target1, mock_target2]

        # Act
        result = spliter.delete()

        # Assert
        assert result is True
        mock_target2.delete.assert_called_once()
